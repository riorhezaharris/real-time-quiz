package hub_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/riorhezaharris/real-time-quiz/internal/hub"
	"github.com/riorhezaharris/real-time-quiz/internal/keys"
	"github.com/riorhezaharris/real-time-quiz/internal/leaderboard"
	"github.com/riorhezaharris/real-time-quiz/internal/observability"
	"github.com/riorhezaharris/real-time-quiz/internal/protocol"
	"github.com/riorhezaharris/real-time-quiz/internal/testutil"
)

type fakeClient struct {
	mu     sync.Mutex
	msgs   [][]byte
	full   bool
	closed atomic.Bool
}

func (c *fakeClient) Enqueue(b []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.full {
		return false
	}
	c.msgs = append(c.msgs, b)
	return true
}

func (c *fakeClient) Close(string) { c.closed.Store(true) }

func (c *fakeClient) boards(t *testing.T) []protocol.Leaderboard {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]protocol.Leaderboard, len(c.msgs))
	for i, m := range c.msgs {
		if err := json.Unmarshal(m, &out[i]); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

type countingSnapshotter struct{ calls atomic.Int64 }

func (s *countingSnapshotter) Top(context.Context, string, int) ([]leaderboard.Entry, int64, error) {
	s.calls.Add(1)
	return []leaderboard.Entry{{UserID: "a", DisplayName: "A", Score: 10, Rank: 1}}, 1, nil
}

func newHub(t *testing.T, opts hub.Options) (*hub.Hub, *countingSnapshotter, func(quizID string)) {
	t.Helper()
	rdb := testutil.Redis(t)
	snap := &countingSnapshotter{}
	m := observability.NewMetrics(prometheus.NewRegistry())
	h := hub.New(rdb, snap, opts, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	h.Run()
	t.Cleanup(h.Close)
	publish := func(quizID string) {
		rdb.Publish(context.Background(), keys.EventsChannel(quizID), "x")
	}
	return h, snap, publish
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestBroadcastCoalescesBursts(t *testing.T) {
	h, snap, publish := newHub(t, hub.Options{TopN: 10, BroadcastInterval: 100 * time.Millisecond})
	c := &fakeClient{}
	h.Join(context.Background(), "demo", c)
	waitFor(t, func() bool { return len(c.boards(t)) == 1 }) // join triggers one

	time.Sleep(50 * time.Millisecond) // let the subscription settle
	before := snap.calls.Load()
	for range 200 {
		publish("demo")
	}
	time.Sleep(400 * time.Millisecond)

	reads := snap.calls.Load() - before
	if reads < 1 || reads > 5 {
		t.Fatalf("200 events caused %d leaderboard reads; want coalesced to a handful", reads)
	}
	boards := c.boards(t)
	for i := 1; i < len(boards); i++ {
		if boards[i].Version <= boards[i-1].Version {
			t.Fatalf("versions not increasing: %d then %d", boards[i-1].Version, boards[i].Version)
		}
	}
}

func TestEventsForOtherQuizzesAreIgnored(t *testing.T) {
	h, snap, publish := newHub(t, hub.Options{TopN: 10, BroadcastInterval: 50 * time.Millisecond})
	c := &fakeClient{}
	h.Join(context.Background(), "demo", c)
	waitFor(t, func() bool { return len(c.boards(t)) == 1 })
	before := snap.calls.Load()
	publish("other")
	time.Sleep(200 * time.Millisecond)
	if snap.calls.Load() != before {
		t.Fatal("event for an unjoined quiz triggered a broadcast")
	}
}

func TestSlowConsumerIsDropped(t *testing.T) {
	h, _, _ := newHub(t, hub.Options{TopN: 10, BroadcastInterval: 20 * time.Millisecond})
	slow := &fakeClient{full: true}
	ok := &fakeClient{}
	h.Join(context.Background(), "demo", slow)
	h.Join(context.Background(), "demo", ok)
	waitFor(t, func() bool { return slow.closed.Load() })
	if ok.closed.Load() {
		t.Fatal("healthy client was closed")
	}
	if len(ok.boards(t)) == 0 {
		t.Fatal("healthy client got no broadcast")
	}
}

func TestResyncWithoutEvents(t *testing.T) {
	h, _, _ := newHub(t, hub.Options{
		TopN: 10, BroadcastInterval: 20 * time.Millisecond, ResyncInterval: 100 * time.Millisecond,
	})
	c := &fakeClient{}
	h.Join(context.Background(), "demo", c)
	waitFor(t, func() bool { return len(c.boards(t)) >= 3 })
}

func TestLeaveRemovesRoom(t *testing.T) {
	h, _, _ := newHub(t, hub.Options{TopN: 10, BroadcastInterval: 20 * time.Millisecond})
	a, b := &fakeClient{}, &fakeClient{}
	h.Join(context.Background(), "demo", a)
	h.Join(context.Background(), "demo", b)
	if n := h.ClientCount("demo"); n != 2 {
		t.Fatalf("count = %d", n)
	}
	h.Leave("demo", a)
	h.Leave("demo", a) // idempotent
	h.Leave("demo", b)
	if n := h.ClientCount("demo"); n != 0 {
		t.Fatalf("count after leave = %d", n)
	}
}
