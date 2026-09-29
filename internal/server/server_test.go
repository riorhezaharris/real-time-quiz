package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/config"
	"github.com/riorhezaharris/real-time-quiz/internal/protocol"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/server"
	"github.com/riorhezaharris/real-time-quiz/internal/testutil"
)

func testConfig() config.Config {
	return config.Config{
		PointsPerCorrect:  10,
		LeaderboardTopN:   10,
		BroadcastInterval: 20 * time.Millisecond,
		StateTTL:          time.Hour,
		SendBuffer:        256,
		MessagesPerSecond: 1000,
		MessageBurst:      1000,
	}
}

type instance struct {
	srv  *server.Server
	http *httptest.Server
	url  string
}

// startInstance starts a server instance sharing rdb with any others, which
// is exactly how horizontally scaled instances share Redis in production.
func startInstance(t *testing.T, rdb *redis.Client) *instance {
	t.Helper()
	repo, err := quiz.NewFixtureRepository()
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(testConfig(), rdb, repo, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Start()
	hs := httptest.NewServer(s.Handler())
	inst := &instance{srv: s, http: hs, url: "ws" + strings.TrimPrefix(hs.URL, "http") + "/ws"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.Shutdown(ctx)
		hs.Close()
	})
	return inst
}

type client struct {
	t  *testing.T
	ws *websocket.Conn
}

func dial(t *testing.T, url string) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.SetReadLimit(1 << 20)
	t.Cleanup(func() { c.CloseNow() })
	return &client{t: t, ws: c}
}

func (c *client) send(v any) {
	c.t.Helper()
	b, _ := json.Marshal(v)
	if err := c.ws.Write(context.Background(), websocket.MessageText, b); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// next reads messages until one of the wanted type arrives.
func (c *client) next(typ string) []byte {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		_, data, err := c.ws.Read(ctx)
		if err != nil {
			c.t.Fatalf("waiting for %q: %v", typ, err)
		}
		var env struct{ Type string }
		json.Unmarshal(data, &env)
		if env.Type == typ {
			return data
		}
	}
}

func (c *client) join(quizID, userID string) protocol.Joined {
	c.t.Helper()
	c.send(protocol.ClientMessage{Type: protocol.TypeJoin, QuizID: quizID, UserID: userID, DisplayName: strings.ToUpper(userID)})
	var j protocol.Joined
	json.Unmarshal(c.next(protocol.TypeJoined), &j)
	return j
}

func (c *client) answer(reqID, qID, ans string) protocol.AnswerResult {
	c.t.Helper()
	c.send(protocol.ClientMessage{Type: protocol.TypeAnswer, RequestID: reqID, QuestionID: qID, Answer: ans})
	var r protocol.AnswerResult
	json.Unmarshal(c.next(protocol.TypeAnswerResult), &r)
	return r
}

// waitBoard waits for a leaderboard broadcast satisfying cond.
func (c *client) waitBoard(cond func(protocol.Leaderboard) bool) protocol.Leaderboard {
	c.t.Helper()
	for {
		var lb protocol.Leaderboard
		json.Unmarshal(c.next(protocol.TypeLeaderboard), &lb)
		if cond(lb) {
			return lb
		}
	}
}

func scoreOf(lb protocol.Leaderboard, user string) int64 {
	for _, e := range lb.Top {
		if e.UserID == user {
			return e.Score
		}
	}
	return -1
}

func TestLeaderboardPropagatesAcrossInstances(t *testing.T) {
	rdb := testutil.Redis(t)
	a, b := startInstance(t, rdb), startInstance(t, rdb)

	alice := dial(t, a.url)
	bob := dial(t, b.url)

	j := alice.join("demo", "alice")
	if j.Title == "" || len(j.Questions) != 10 || j.Score != 0 {
		t.Fatalf("joined = %+v", j)
	}
	for _, q := range j.Questions {
		if q.ID == "" || len(q.Options) == 0 {
			t.Fatalf("bad question %+v", q)
		}
	}
	bob.join("demo", "bob")

	r := alice.answer("r1", "q1", "joyful")
	if !r.Correct || r.Score != 10 || r.Rank != 1 || r.RequestID != "r1" {
		t.Fatalf("answer result = %+v", r)
	}

	// Bob is connected to a different instance and must still see Alice's
	// new score, delivered via Redis pub/sub.
	lb := bob.waitBoard(func(lb protocol.Leaderboard) bool { return scoreOf(lb, "alice") == 10 })
	if lb.Top[0].UserID != "alice" || lb.Top[0].DisplayName != "ALICE" || lb.Participants != 2 {
		t.Fatalf("leaderboard = %+v", lb)
	}
}

func TestReconnectToOtherInstanceKeepsScore(t *testing.T) {
	rdb := testutil.Redis(t)
	a, b := startInstance(t, rdb), startInstance(t, rdb)

	c1 := dial(t, a.url)
	c1.join("demo", "alice")
	c1.answer("r1", "q1", "joyful")
	c1.ws.Close(websocket.StatusNormalClosure, "")

	c2 := dial(t, b.url)
	j := c2.join("demo", "alice")
	if j.Score != 10 {
		t.Fatalf("score after reconnect = %d, want 10", j.Score)
	}
	// A retry of an answer sent before the disconnect must not double count.
	r := c2.answer("r1-retry", "q1", "joyful")
	if !r.AlreadyAnswered || r.Score != 10 {
		t.Fatalf("retry = %+v", r)
	}
}

func TestProtocolErrors(t *testing.T) {
	inst := startInstance(t, testutil.Redis(t))
	c := dial(t, inst.url)

	expectErr := func(code string) {
		t.Helper()
		var e protocol.Error
		json.Unmarshal(c.next(protocol.TypeError), &e)
		if e.Code != code {
			t.Fatalf("error code = %q (%s), want %q", e.Code, e.Message, code)
		}
	}

	c.send(protocol.ClientMessage{Type: protocol.TypeAnswer, QuestionID: "q1", Answer: "x"})
	expectErr(protocol.CodeNotJoined)

	c.ws.Write(context.Background(), websocket.MessageText, []byte("{not json"))
	expectErr(protocol.CodeBadRequest)

	c.send(map[string]string{"type": "dance"})
	expectErr(protocol.CodeBadRequest)

	c.send(protocol.ClientMessage{Type: protocol.TypeJoin, QuizID: "missing", UserID: "u1"})
	expectErr(protocol.CodeQuizNotFound)

	c.send(protocol.ClientMessage{Type: protocol.TypeJoin, QuizID: "demo", UserID: "bad user!"})
	expectErr(protocol.CodeBadRequest)

	c.join("demo", "u1")
	c.send(protocol.ClientMessage{Type: protocol.TypeJoin, QuizID: "demo", UserID: "u1"})
	expectErr(protocol.CodeAlreadyJoined)

	c.send(protocol.ClientMessage{Type: protocol.TypeAnswer, RequestID: "r9", QuestionID: "nope", Answer: "x"})
	expectErr(protocol.CodeUnknownQuestion)

	// The connection survives all of the above.
	if r := c.answer("ok", "q2", "modern"); !r.Correct {
		t.Fatalf("answer after errors = %+v", r)
	}
}

func TestOversizedMessageClosesConnection(t *testing.T) {
	inst := startInstance(t, testutil.Redis(t))
	c := dial(t, inst.url)
	c.ws.Write(context.Background(), websocket.MessageText, []byte(strings.Repeat("x", 10_000)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := c.ws.Read(ctx)
	if got := websocket.CloseStatus(err); got != websocket.StatusMessageTooBig {
		t.Fatalf("close status = %v (%v), want StatusMessageTooBig", got, err)
	}
}

func TestGracefulShutdownSendsGoingAway(t *testing.T) {
	inst := startInstance(t, testutil.Redis(t))
	c := dial(t, inst.url)
	c.join("demo", "alice")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		inst.srv.Shutdown(ctx)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		_, _, err := c.ws.Read(ctx)
		if err == nil {
			continue // drain any in-flight broadcast
		}
		if got := websocket.CloseStatus(err); got != websocket.StatusGoingAway {
			t.Fatalf("close status = %v (%v), want StatusGoingAway", got, err)
		}
		return
	}
}

// TestManyClientsConsistentTotals runs 40 users split across two instances,
// each answering every question (with a duplicate retry), and checks that the
// final broadcast leaderboard matches the expected totals exactly.
func TestManyClientsConsistentTotals(t *testing.T) {
	rdb := testutil.Redis(t)
	insts := []*instance{startInstance(t, rdb), startInstance(t, rdb)}

	const users = 40
	clients := make([]*client, users)
	for i := range users {
		clients[i] = dial(t, insts[i%2].url)
		clients[i].join("business-101", fmt.Sprintf("u%02d", i))
	}

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Users answer (i % 5) questions of business-101 correctly.
			correct := map[string]string{"q1": "a final time limit", "q2": "bargain", "q3": "income", "q4": "loss", "q5": "a list of meeting topics"}
			n := 0
			for _, q := range []string{"q1", "q2", "q3", "q4", "q5"} {
				ans := "wrong"
				if n < i%5 {
					ans = correct[q]
				}
				n++
				for attempt := range 2 {
					b, _ := json.Marshal(protocol.ClientMessage{Type: protocol.TypeAnswer, RequestID: fmt.Sprintf("%s-%d", q, attempt), QuestionID: q, Answer: ans})
					c.ws.Write(context.Background(), websocket.MessageText, b)
				}
			}
		}()
	}
	wg.Wait()

	// Expected: users with i%5 == 4 have 40 points and share rank 1.
	want := int64(0)
	for i := range users {
		want += int64(i%5) * 10
	}
	clients[0].waitBoard(func(lb protocol.Leaderboard) bool {
		if lb.Participants != users || len(lb.Top) != 10 {
			return false
		}
		for _, e := range lb.Top[:8] {
			if e.Score != 40 || e.Rank != 1 {
				return false
			}
		}
		return lb.Top[8].Score == 30 && lb.Top[8].Rank == 9
	})

	// Cross-check the full totals directly in Redis.
	var total int64
	zs, err := rdb.ZRangeWithScores(context.Background(), "quiz:{business-101}:lb", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, z := range zs {
		total += int64(z.Score)
	}
	if total != want {
		t.Fatalf("sum of scores = %d, want %d", total, want)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	inst := startInstance(t, testutil.Redis(t))
	base := inst.http.URL
	for path, want := range map[string]string{"/healthz": "ok", "/readyz": "ready"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(body) != want {
			t.Fatalf("%s = %d %q", path, resp.StatusCode, body)
		}
	}
	c := dial(t, inst.url)
	c.join("demo", "alice")
	c.answer("r1", "q1", "joyful")

	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		"quiz_ws_connections 1",
		`quiz_answers_total{result="correct"} 1`,
		"quiz_answer_duration_seconds_count 1",
		"quiz_active_quizzes 1",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
