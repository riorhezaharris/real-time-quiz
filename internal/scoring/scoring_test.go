package scoring_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riorhezaharris/real-time-quiz/internal/keys"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/scoring"
	"github.com/riorhezaharris/real-time-quiz/internal/testutil"
)

func newService(t *testing.T) (*scoring.Service, context.Context) {
	t.Helper()
	repo, err := quiz.NewFixtureRepository()
	if err != nil {
		t.Fatal(err)
	}
	return scoring.New(testutil.Redis(t), repo, 10, time.Hour), context.Background()
}

func mustJoin(t *testing.T, s *scoring.Service, ctx context.Context, user string) {
	t.Helper()
	if _, _, err := s.Join(ctx, "demo", user, user); err != nil {
		t.Fatalf("join %s: %v", user, err)
	}
}

func TestJoinIsIdempotentAndKeepsScore(t *testing.T) {
	s, ctx := newService(t)
	score, rank, err := s.Join(ctx, "demo", "alice", "Alice")
	if err != nil || score != 0 || rank != 1 {
		t.Fatalf("first join = (%d,%d,%v), want (0,1,nil)", score, rank, err)
	}
	if _, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "joyful"); err != nil {
		t.Fatal(err)
	}
	score, _, err = s.Join(ctx, "demo", "alice", "Alice")
	if err != nil || score != 10 {
		t.Fatalf("re-join score = %d (%v), want 10 preserved", score, err)
	}
}

func TestJoinValidation(t *testing.T) {
	s, ctx := newService(t)
	cases := []struct {
		quiz, user, name string
		want             error
	}{
		{"demo", "", "x", scoring.ErrInvalidID},
		{"demo", "bad{user}", "x", scoring.ErrInvalidID},
		{"demo", "alice", "", scoring.ErrInvalidName},
		{"demo", "alice", "this display name is way too long!!", scoring.ErrInvalidName},
		{"nope", "alice", "Alice", quiz.ErrNotFound},
	}
	for _, c := range cases {
		if _, _, err := s.Join(ctx, c.quiz, c.user, c.name); !errors.Is(err, c.want) {
			t.Errorf("Join(%q,%q,%q) err = %v, want %v", c.quiz, c.user, c.name, err, c.want)
		}
	}
}

func TestSubmitAnswer(t *testing.T) {
	s, ctx := newService(t)
	mustJoin(t, s, ctx, "alice")

	r, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "  JOYFUL ")
	if err != nil || !r.Correct || r.AlreadyAnswered || r.Score != 10 || r.Rank != 1 {
		t.Fatalf("correct answer = %+v, %v", r, err)
	}
	r, err = s.SubmitAnswer(ctx, "demo", "alice", "q2", "old")
	if err != nil || r.Correct || r.AlreadyAnswered || r.Score != 10 {
		t.Fatalf("incorrect answer = %+v, %v", r, err)
	}
	if _, err := s.SubmitAnswer(ctx, "demo", "alice", "q999", "x"); !errors.Is(err, scoring.ErrUnknownQuestion) {
		t.Fatalf("unknown question err = %v", err)
	}
}

func TestDuplicateSubmissionIsIgnored(t *testing.T) {
	s, ctx := newService(t)
	mustJoin(t, s, ctx, "alice")

	// A wrong first answer locks the question: retrying with the right
	// answer must not score.
	if _, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "angry"); err != nil {
		t.Fatal(err)
	}
	r, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "joyful")
	if err != nil || !r.AlreadyAnswered || r.Correct || r.Score != 0 {
		t.Fatalf("retry = %+v, %v; want ignored with score 0", r, err)
	}
}

// TestConcurrentSubmissionsScoreExactlyOnce is the core consistency check:
// many goroutines (simulating duplicate messages across server instances)
// race to submit the same correct answer; exactly one must be applied.
func TestConcurrentSubmissionsScoreExactlyOnce(t *testing.T) {
	s, ctx := newService(t)
	mustJoin(t, s, ctx, "alice")

	const n = 100
	var applied atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "joyful")
			if err != nil {
				t.Error(err)
				return
			}
			if !r.AlreadyAnswered {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	if applied.Load() != 1 {
		t.Fatalf("applied %d times, want exactly 1", applied.Load())
	}
	score, _, _ := s.Join(ctx, "demo", "alice", "alice")
	if score != 10 {
		t.Fatalf("final score %d, want 10", score)
	}
}

// TestConcurrentUsersTotals checks many users answering all questions in
// parallel end with exactly the expected totals.
func TestConcurrentUsersTotals(t *testing.T) {
	s, ctx := newService(t)
	answers := map[string]string{"q1": "joyful", "q2": "modern", "q3": "unwilling", "q4": "fast", "q5": "easily broken"}

	const users = 20
	var wg sync.WaitGroup
	for u := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("user-%d", u)
			if _, _, err := s.Join(ctx, "demo", id, id); err != nil {
				t.Error(err)
				return
			}
			// user u answers the first u%6 questions correctly.
			i := 0
			for q, a := range answers {
				if i >= u%6 {
					a = "wrong"
				}
				// Submit twice to exercise idempotency under concurrency.
				s.SubmitAnswer(ctx, "demo", id, q, a)
				s.SubmitAnswer(ctx, "demo", id, q, a)
				i++
			}
		}()
	}
	wg.Wait()
	for u := range users {
		id := fmt.Sprintf("user-%d", u)
		score, _, _ := s.Join(ctx, "demo", id, id)
		if want := int64(u%6) * 10; score != want {
			t.Errorf("%s score %d, want %d", id, score, want)
		}
	}
}

func TestRanksUseCompetitionRanking(t *testing.T) {
	s, ctx := newService(t)
	for _, u := range []string{"a", "b", "c", "d"} {
		mustJoin(t, s, ctx, u)
	}
	s.SubmitAnswer(ctx, "demo", "a", "q1", "joyful") // a=10
	s.SubmitAnswer(ctx, "demo", "a", "q2", "modern") // a=20
	s.SubmitAnswer(ctx, "demo", "b", "q1", "joyful") // b=10
	r, _ := s.SubmitAnswer(ctx, "demo", "c", "q1", "joyful")
	if r.Rank != 2 {
		t.Fatalf("c rank %d, want 2 (tied with b)", r.Rank)
	}
	r, _ = s.SubmitAnswer(ctx, "demo", "d", "q1", "wrong")
	if r.Rank != 4 {
		t.Fatalf("d rank %d, want 4", r.Rank)
	}
}

func TestScorePublishesEvent(t *testing.T) {
	repo, _ := quiz.NewFixtureRepository()
	rdb := testutil.Redis(t)
	s := scoring.New(rdb, repo, 10, time.Hour)
	ctx := context.Background()

	sub := rdb.Subscribe(ctx, keys.EventsChannel("demo"))
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil { // subscription confirmation
		t.Fatal(err)
	}
	if _, _, err := s.Join(ctx, "demo", "alice", "Alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitAnswer(ctx, "demo", "alice", "q1", "joyful"); err != nil {
		t.Fatal(err)
	}
	for i := range 2 { // one for join, one for the score change
		select {
		case m := <-sub.Channel():
			if m.Payload != "alice" {
				t.Fatalf("event %d payload %q", i, m.Payload)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no event %d published", i)
		}
	}
}
