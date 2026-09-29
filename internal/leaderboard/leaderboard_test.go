package leaderboard_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/riorhezaharris/real-time-quiz/internal/leaderboard"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/scoring"
	"github.com/riorhezaharris/real-time-quiz/internal/testutil"
)

func TestTopOrderingTiesAndNames(t *testing.T) {
	rdb := testutil.Redis(t)
	repo, _ := quiz.NewFixtureRepository()
	s := scoring.New(rdb, repo, 10, time.Hour)
	ctx := context.Background()

	for _, u := range []string{"dave", "carol", "bob", "alice"} {
		if _, _, err := s.Join(ctx, "demo", u, "Name-"+u); err != nil {
			t.Fatal(err)
		}
	}
	s.SubmitAnswer(ctx, "demo", "carol", "q1", "joyful")
	s.SubmitAnswer(ctx, "demo", "carol", "q2", "modern")
	s.SubmitAnswer(ctx, "demo", "bob", "q1", "joyful")
	s.SubmitAnswer(ctx, "demo", "alice", "q1", "joyful")

	got, total, err := leaderboard.NewReader(rdb).Top(ctx, "demo", 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []leaderboard.Entry{
		{UserID: "carol", DisplayName: "Name-carol", Score: 20, Rank: 1},
		{UserID: "alice", DisplayName: "Name-alice", Score: 10, Rank: 2},
		{UserID: "bob", DisplayName: "Name-bob", Score: 10, Rank: 2},
	}
	if total != 4 {
		t.Errorf("total = %d, want 4", total)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Top =\n%+v\nwant\n%+v", got, want)
	}
}

func TestTopEmptyQuiz(t *testing.T) {
	got, total, err := leaderboard.NewReader(testutil.Redis(t)).Top(context.Background(), "empty", 10)
	if err != nil || len(got) != 0 || total != 0 {
		t.Fatalf("Top(empty) = %v, %d, %v", got, total, err)
	}
}
