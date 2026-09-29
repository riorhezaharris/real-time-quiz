// Package observability wires structured logging and Prometheus metrics.
package observability

import (
	"io"
	"log/slog"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// NewLogger returns a JSON slog logger at the given level.
func NewLogger(w io.Writer, level, instanceID string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})).
		With("instance", instanceID)
}

// Metrics are the server's Prometheus instruments. They are registered on a
// caller-supplied registry so tests can run several servers in one process.
type Metrics struct {
	Connections       prometheus.Gauge
	ActiveQuizzes     prometheus.Gauge
	MessagesIn        *prometheus.CounterVec
	MessagesOut       *prometheus.CounterVec
	Answers           *prometheus.CounterVec
	AnswerLatency     prometheus.Histogram
	BroadcastDuration prometheus.Histogram
	RedisErrors       *prometheus.CounterVec
	DroppedClients    prometheus.Counter
}

// NewMetrics creates and registers all metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quiz_ws_connections",
			Help: "Open WebSocket connections on this instance.",
		}),
		ActiveQuizzes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quiz_active_quizzes",
			Help: "Quizzes with at least one connected client on this instance.",
		}),
		MessagesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quiz_messages_in_total",
			Help: "Client messages received, by type.",
		}, []string{"type"}),
		MessagesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quiz_messages_out_total",
			Help: "Server messages enqueued to clients, by type.",
		}, []string{"type"}),
		Answers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quiz_answers_total",
			Help: "Answers processed, by result (correct, incorrect, duplicate, error).",
		}, []string{"result"}),
		AnswerLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "quiz_answer_duration_seconds",
			Help:    "Time to score an answer, including the Redis round trip.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12), // 0.5ms .. ~1s
		}),
		BroadcastDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "quiz_broadcast_duration_seconds",
			Help:    "Time to fetch and fan out one leaderboard snapshot.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
		}),
		RedisErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quiz_redis_errors_total",
			Help: "Redis operation failures, by operation.",
		}, []string{"op"}),
		DroppedClients: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quiz_dropped_slow_clients_total",
			Help: "Clients disconnected because their send buffer was full.",
		}),
	}
	reg.MustRegister(m.Connections, m.ActiveQuizzes, m.MessagesIn, m.MessagesOut,
		m.Answers, m.AnswerLatency, m.BroadcastDuration, m.RedisErrors, m.DroppedClients)
	return m
}
