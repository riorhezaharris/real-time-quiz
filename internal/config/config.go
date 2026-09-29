// Package config loads server configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all tunables for a server instance. Every field has a sane
// default so the server runs with zero configuration in development.
type Config struct {
	// Addr is the HTTP listen address (WebSocket, health and metrics).
	Addr string
	// RedisAddr is the host:port of the Redis instance holding quiz state.
	RedisAddr string
	// InstanceID identifies this process in logs; defaults to the hostname.
	InstanceID string
	// LogLevel is one of debug, info, warn, error.
	LogLevel string

	// PointsPerCorrect is awarded for the first correct answer to a question.
	PointsPerCorrect int64
	// LeaderboardTopN is how many entries are broadcast to clients.
	LeaderboardTopN int
	// BroadcastInterval is the minimum gap between leaderboard broadcasts
	// for a single quiz. Score changes inside the window are coalesced.
	BroadcastInterval time.Duration
	// ResyncInterval forces a leaderboard refresh for every active quiz even
	// without events, healing any pub/sub messages lost during a reconnect.
	ResyncInterval time.Duration
	// StateTTL bounds how long quiz state lives in Redis after the last write.
	StateTTL time.Duration

	// SendBuffer is the per-connection outbound queue size. A client whose
	// queue fills up is considered too slow and is disconnected.
	SendBuffer int
	// PingInterval is how often the server pings idle clients.
	PingInterval time.Duration
	// MessagesPerSecond and MessageBurst rate-limit inbound client messages.
	MessagesPerSecond float64
	MessageBurst      int
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment, falling back to defaults.
func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		Addr:              env("HTTP_ADDR", ":8080"),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		InstanceID:        env("INSTANCE_ID", host),
		LogLevel:          env("LOG_LEVEL", "info"),
		PointsPerCorrect:  10,
		LeaderboardTopN:   10,
		BroadcastInterval: 150 * time.Millisecond,
		ResyncInterval:    5 * time.Second,
		StateTTL:          24 * time.Hour,
		SendBuffer:        64,
		PingInterval:      20 * time.Second,
		MessagesPerSecond: 20,
		MessageBurst:      40,
		ShutdownTimeout:   10 * time.Second,
	}

	var err error
	if c.PointsPerCorrect, err = envInt("POINTS_PER_CORRECT", c.PointsPerCorrect); err != nil {
		return c, err
	}
	topN, err := envInt("LEADERBOARD_TOP_N", int64(c.LeaderboardTopN))
	if err != nil {
		return c, err
	}
	c.LeaderboardTopN = int(topN)
	if c.BroadcastInterval, err = envDuration("BROADCAST_INTERVAL", c.BroadcastInterval); err != nil {
		return c, err
	}
	if c.ResyncInterval, err = envDuration("RESYNC_INTERVAL", c.ResyncInterval); err != nil {
		return c, err
	}
	if c.StateTTL, err = envDuration("STATE_TTL", c.StateTTL); err != nil {
		return c, err
	}
	buf, err := envInt("SEND_BUFFER", int64(c.SendBuffer))
	if err != nil {
		return c, err
	}
	c.SendBuffer = int(buf)
	if c.PingInterval, err = envDuration("PING_INTERVAL", c.PingInterval); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// Validate rejects configurations that would break invariants at runtime.
func (c Config) Validate() error {
	switch {
	case c.PointsPerCorrect <= 0:
		return fmt.Errorf("POINTS_PER_CORRECT must be > 0")
	case c.LeaderboardTopN <= 0:
		return fmt.Errorf("LEADERBOARD_TOP_N must be > 0")
	case c.BroadcastInterval <= 0:
		return fmt.Errorf("BROADCAST_INTERVAL must be > 0")
	case c.SendBuffer <= 0:
		return fmt.Errorf("SEND_BUFFER must be > 0")
	}
	return nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
