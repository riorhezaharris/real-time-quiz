// Package testutil provides shared test helpers.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Redis returns a client for tests. By default it is backed by an in-process
// miniredis. Set REDIS_TEST_ADDR (e.g. localhost:6379) to run the same tests
// against a real Redis server; DB 15 is flushed before and after each test.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	if addr := os.Getenv("REDIS_TEST_ADDR"); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
		ctx := context.Background()
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatalf("flush real redis: %v", err)
		}
		t.Cleanup(func() {
			rdb.FlushDB(ctx)
			rdb.Close()
		})
		return rdb
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}
