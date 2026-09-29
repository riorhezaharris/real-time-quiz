// Package hub fans leaderboard updates out to the WebSocket clients connected
// to this instance.
//
// Each server instance holds one Redis pub/sub connection and subscribes only
// to the channels of quizzes that have local clients. A score change anywhere
// in the cluster publishes a tiny "quiz X changed" event; every instance with
// clients in quiz X marks it dirty. A single flush loop then, at most once per
// BroadcastInterval per quiz, reads the top-N from Redis, encodes it ONCE and
// enqueues the same bytes to every local client. This coalescing is what keeps
// a hot quiz (thousands of answers per second) from causing thousands of
// leaderboard reads and broadcasts per second.
package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/keys"
	"github.com/riorhezaharris/real-time-quiz/internal/leaderboard"
	"github.com/riorhezaharris/real-time-quiz/internal/observability"
	"github.com/riorhezaharris/real-time-quiz/internal/protocol"
)

// Client is a connected participant that can receive encoded messages.
type Client interface {
	// Enqueue must not block. It returns false when the client's buffer is
	// full, in which case the hub disconnects it as a slow consumer.
	Enqueue(msg []byte) bool
	// Close disconnects the client. It must be idempotent and non-blocking.
	Close(reason string)
}

// Snapshotter reads the current top of a leaderboard.
type Snapshotter interface {
	Top(ctx context.Context, quizID string, n int) ([]leaderboard.Entry, int64, error)
}

// Options configures a Hub.
type Options struct {
	TopN              int
	BroadcastInterval time.Duration
	ResyncInterval    time.Duration
	// MaxConcurrentFlushes bounds parallel leaderboard reads per tick.
	MaxConcurrentFlushes int
}

type room struct {
	clients  map[Client]struct{}
	version  int64
	inflight bool
}

// Hub tracks rooms (one per quiz with local clients) and broadcasts to them.
type Hub struct {
	pubsub *redis.PubSub
	lb     Snapshotter
	opts   Options
	log    *slog.Logger
	m      *observability.Metrics

	mu    sync.Mutex
	rooms map[string]*room
	dirty map[string]struct{}

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// New creates a Hub. Call Run to start it and Close to stop it.
func New(rdb redis.UniversalClient, lb Snapshotter, opts Options, log *slog.Logger, m *observability.Metrics) *Hub {
	if opts.MaxConcurrentFlushes <= 0 {
		opts.MaxConcurrentFlushes = 32
	}
	return &Hub{
		// Subscribing with no channels creates an idle pub/sub connection
		// that we add channels to as rooms appear. go-redis re-subscribes
		// automatically after a reconnect.
		pubsub: rdb.Subscribe(context.Background()),
		lb:     lb,
		opts:   opts,
		log:    log,
		m:      m,
		rooms:  make(map[string]*room),
		dirty:  make(map[string]struct{}),
		done:   make(chan struct{}),
	}
}

// Join adds c to quizID's room, subscribing to the quiz's events if this is
// the first local client. The quiz is marked dirty so the newcomer receives a
// fresh broadcast on the next tick.
func (h *Hub) Join(ctx context.Context, quizID string, c Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[quizID]
	if !ok {
		r = &room{clients: make(map[Client]struct{})}
		h.rooms[quizID] = r
		h.m.ActiveQuizzes.Inc()
		// Done under the lock so subscribe/unsubscribe for the same quiz
		// can never be reordered.
		if err := h.pubsub.Subscribe(ctx, keys.EventsChannel(quizID)); err != nil {
			// go-redis retries the subscription on reconnect, and the
			// periodic resync covers the gap, so this is not fatal.
			h.m.RedisErrors.WithLabelValues("subscribe").Inc()
			h.log.Warn("subscribe failed", "quizId", quizID, "err", err)
		}
	}
	r.clients[c] = struct{}{}
	h.dirty[quizID] = struct{}{}
}

// Leave removes c from quizID's room. It is safe to call more than once.
func (h *Hub) Leave(quizID string, c Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[quizID]
	if !ok {
		return
	}
	delete(r.clients, c)
	if len(r.clients) > 0 {
		return
	}
	delete(h.rooms, quizID)
	delete(h.dirty, quizID)
	h.m.ActiveQuizzes.Dec()
	if err := h.pubsub.Unsubscribe(context.Background(), keys.EventsChannel(quizID)); err != nil {
		h.m.RedisErrors.WithLabelValues("unsubscribe").Inc()
		h.log.Warn("unsubscribe failed", "quizId", quizID, "err", err)
	}
}

// ClientCount returns the number of local clients in quizID (for tests).
func (h *Hub) ClientCount(quizID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[quizID]; ok {
		return len(r.clients)
	}
	return 0
}

// Run starts the receive and flush loops. It returns immediately.
func (h *Hub) Run() {
	h.wg.Add(2)
	go h.receiveLoop()
	go h.flushLoop()
}

// Close stops the loops and disconnects every local client. It is safe to
// call more than once.
func (h *Hub) Close() {
	h.closeOnce.Do(h.close)
}

func (h *Hub) close() {
	close(h.done)
	h.pubsub.Close()
	h.wg.Wait()

	h.mu.Lock()
	var all []Client
	for _, r := range h.rooms {
		for c := range r.clients {
			all = append(all, c)
		}
	}
	h.mu.Unlock()
	for _, c := range all {
		c.Close("server shutting down")
	}
}

// receiveLoop turns pub/sub events into dirty marks. It does no I/O itself,
// so it keeps up with any event rate.
func (h *Hub) receiveLoop() {
	defer h.wg.Done()
	// A buffered channel; if it ever fills, go-redis drops messages after a
	// timeout. That is acceptable: a dropped event only delays an update
	// until the next event or resync, since snapshots are complete.
	ch := h.pubsub.Channel(redis.WithChannelSize(4096))
	for {
		select {
		case <-h.done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			quizID, ok := keys.QuizFromChannel(msg.Channel)
			if !ok {
				continue
			}
			h.mu.Lock()
			if _, active := h.rooms[quizID]; active {
				h.dirty[quizID] = struct{}{}
			}
			h.mu.Unlock()
		}
	}
}

// AI-ASSISTED (Claude Code): the coalescing flush loop (dirty set + ticker +
// in-flight guard + bounded concurrency) was proposed by Claude when asked
// how to stop a hot quiz from triggering a broadcast per answer. Verified by
// hub_test.go (coalescing, slow-consumer drop, resync) and the two-instance
// integration test; the in-flight guard was added after reviewing that a slow
// Redis read could otherwise let two broadcasts for one quiz race and deliver
// versions out of order.
func (h *Hub) flushLoop() {
	defer h.wg.Done()
	tick := time.NewTicker(h.opts.BroadcastInterval)
	defer tick.Stop()
	lastResync := time.Now()
	sem := make(chan struct{}, h.opts.MaxConcurrentFlushes)
	var flights sync.WaitGroup
	defer flights.Wait()

	for {
		select {
		case <-h.done:
			return
		case <-tick.C:
		}

		h.mu.Lock()
		if h.opts.ResyncInterval > 0 && time.Since(lastResync) >= h.opts.ResyncInterval {
			// Heal anything lost while the pub/sub connection was down.
			for id := range h.rooms {
				h.dirty[id] = struct{}{}
			}
			lastResync = time.Now()
		}
		var batch []string
		for id := range h.dirty {
			r := h.rooms[id]
			if r == nil {
				delete(h.dirty, id)
				continue
			}
			if r.inflight {
				continue // stays dirty; picked up next tick
			}
			r.inflight = true
			delete(h.dirty, id)
			batch = append(batch, id)
		}
		h.mu.Unlock()

		for _, id := range batch {
			sem <- struct{}{}
			flights.Add(1)
			go func() {
				defer func() { <-sem; flights.Done() }()
				h.broadcast(id)
			}()
		}
	}
}

// broadcast reads one snapshot and enqueues it to every local client.
func (h *Hub) broadcast(quizID string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	entries, total, err := h.lb.Top(ctx, quizID, h.opts.TopN)

	h.mu.Lock()
	r := h.rooms[quizID]
	if r == nil {
		h.mu.Unlock()
		return
	}
	r.inflight = false
	if err != nil {
		h.dirty[quizID] = struct{}{} // retry next tick
		h.mu.Unlock()
		h.m.RedisErrors.WithLabelValues("leaderboard_top").Inc()
		h.log.Warn("leaderboard read failed", "quizId", quizID, "err", err)
		return
	}
	r.version++
	version := r.version
	clients := make([]Client, 0, len(r.clients))
	for c := range r.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	msg, err := json.Marshal(protocol.Leaderboard{
		Type:         protocol.TypeLeaderboard,
		QuizID:       quizID,
		Version:      version,
		ServerTimeMs: time.Now().UnixMilli(),
		Participants: total,
		Top:          ToProtocol(entries),
	})
	if err != nil {
		h.log.Error("encode leaderboard", "err", err)
		return
	}
	for _, c := range clients {
		if !c.Enqueue(msg) {
			h.m.DroppedClients.Inc()
			c.Close("slow consumer")
			continue
		}
		h.m.MessagesOut.WithLabelValues(protocol.TypeLeaderboard).Inc()
	}
	h.m.BroadcastDuration.Observe(time.Since(start).Seconds())
}

// ToProtocol converts leaderboard entries to their wire form.
func ToProtocol(entries []leaderboard.Entry) []protocol.LeaderboardEntry {
	out := make([]protocol.LeaderboardEntry, len(entries))
	for i, e := range entries {
		out[i] = protocol.LeaderboardEntry{
			UserID: e.UserID, DisplayName: e.DisplayName, Score: e.Score, Rank: e.Rank,
		}
	}
	return out
}
