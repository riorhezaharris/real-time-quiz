// Package leaderboard reads quiz standings from Redis.
package leaderboard

import (
	"context"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/keys"
)

// Entry is one row of the leaderboard.
type Entry struct {
	UserID      string
	DisplayName string
	Score       int64
	Rank        int64
}

// Reader fetches leaderboard snapshots.
type Reader struct {
	rdb redis.Cmdable
}

// NewReader constructs a Reader.
func NewReader(rdb redis.Cmdable) *Reader { return &Reader{rdb: rdb} }

// Top returns the n highest-scoring participants and the total participant
// count. Ranks use standard competition ranking (1, 2, 2, 4); within a tie,
// entries are ordered by user ID so the output is deterministic.
//
// Cost: O(log N + n) on the sorted set plus an O(n) HMGET, in two round trips.
func (r *Reader) Top(ctx context.Context, quizID string, n int) ([]Entry, int64, error) {
	if n <= 0 {
		return nil, 0, nil
	}
	pipe := r.rdb.Pipeline()
	zs := pipe.ZRevRangeWithScores(ctx, keys.Leaderboard(quizID), 0, int64(n-1))
	card := pipe.ZCard(ctx, keys.Leaderboard(quizID))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, 0, fmt.Errorf("leaderboard range: %w", err)
	}
	members := zs.Val()
	if len(members) == 0 {
		return nil, card.Val(), nil
	}

	ids := make([]string, len(members))
	entries := make([]Entry, len(members))
	for i, m := range members {
		id, _ := m.Member.(string)
		ids[i] = id
		entries[i] = Entry{UserID: id, Score: int64(m.Score)}
	}

	names, err := r.rdb.HMGet(ctx, keys.Names(quizID), ids...).Result()
	if err != nil && err != redis.Nil {
		return nil, 0, fmt.Errorf("leaderboard names: %w", err)
	}
	for i := range entries {
		if i < len(names) {
			if s, ok := names[i].(string); ok {
				entries[i].DisplayName = s
			}
		}
		if entries[i].DisplayName == "" {
			entries[i].DisplayName = entries[i].UserID
		}
	}

	// Redis orders equal scores by reverse lexicographic member; re-sort
	// ties by ascending user ID for a friendlier, stable order.
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		return entries[i].UserID < entries[j].UserID
	})
	// The slice starts at the global top, so competition ranks can be
	// computed locally: a rank only advances when the score drops.
	for i := range entries {
		if i > 0 && entries[i].Score == entries[i-1].Score {
			entries[i].Rank = entries[i-1].Rank
		} else {
			entries[i].Rank = int64(i + 1)
		}
	}
	return entries, card.Val(), nil
}
