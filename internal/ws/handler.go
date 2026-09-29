// Package ws terminates client WebSocket connections and translates protocol
// messages into scoring and hub operations.
package ws

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/riorhezaharris/real-time-quiz/internal/config"
	"github.com/riorhezaharris/real-time-quiz/internal/hub"
	"github.com/riorhezaharris/real-time-quiz/internal/leaderboard"
	"github.com/riorhezaharris/real-time-quiz/internal/observability"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/scoring"
)

// Deps are the collaborators a connection needs.
type Deps struct {
	Scoring     *scoring.Service
	Quizzes     quiz.Repository
	Leaderboard *leaderboard.Reader
	Hub         *hub.Hub
	Config      config.Config
	Log         *slog.Logger
	Metrics     *observability.Metrics
}

// Handler upgrades HTTP requests to WebSocket connections.
type Handler struct {
	d Deps

	mu       sync.Mutex
	shutdown bool
	conns    sync.WaitGroup
	stop     chan struct{}
}

// NewHandler constructs a Handler.
func NewHandler(d Deps) *Handler {
	return &Handler{d: d, stop: make(chan struct{})}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.shutdown {
		h.mu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	h.conns.Add(1)
	h.mu.Unlock()
	defer h.conns.Done()

	// NOTE: origin checks are disabled for the demo. In production, restrict
	// OriginPatterns and authenticate the user here (e.g. a JWT in the
	// Sec-WebSocket-Protocol header) instead of trusting the join message.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		h.d.Log.Debug("websocket accept failed", "err", err)
		return
	}
	c.SetReadLimit(4 << 10) // client messages are tiny; cap memory per read

	h.d.Metrics.Connections.Inc()
	defer h.d.Metrics.Connections.Dec()
	newConn(h, c).run()
}

// Shutdown stops accepting connections, asks every open connection to close
// with StatusGoingAway (clients should reconnect to another instance), and
// waits for them to finish or for ctx to expire.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if !h.shutdown {
		h.shutdown = true
		close(h.stop)
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() { h.conns.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const (
	writeTimeout = 5 * time.Second
	opTimeout    = 3 * time.Second
)
