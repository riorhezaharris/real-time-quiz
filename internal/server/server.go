// Package server assembles a complete quiz server instance: WebSocket
// endpoint, hub, health checks and metrics.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/config"
	"github.com/riorhezaharris/real-time-quiz/internal/hub"
	"github.com/riorhezaharris/real-time-quiz/internal/leaderboard"
	"github.com/riorhezaharris/real-time-quiz/internal/observability"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/scoring"
	"github.com/riorhezaharris/real-time-quiz/internal/ws"
)

// Server is one stateless quiz server instance.
type Server struct {
	rdb redis.UniversalClient
	hub *hub.Hub
	ws  *ws.Handler
	mux *http.ServeMux
	log *slog.Logger
}

// New wires a server. Call Start before serving and Shutdown when done.
func New(cfg config.Config, rdb redis.UniversalClient, repo quiz.Repository, log *slog.Logger) *Server {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := observability.NewMetrics(reg)

	lb := leaderboard.NewReader(rdb)
	h := hub.New(rdb, lb, hub.Options{
		TopN:              cfg.LeaderboardTopN,
		BroadcastInterval: cfg.BroadcastInterval,
		ResyncInterval:    cfg.ResyncInterval,
	}, log, m)
	wsh := ws.NewHandler(ws.Deps{
		Scoring:     scoring.New(rdb, repo, cfg.PointsPerCorrect, cfg.StateTTL),
		Quizzes:     repo,
		Leaderboard: lb,
		Hub:         h,
		Config:      cfg,
		Log:         log,
		Metrics:     m,
	})

	s := &Server{rdb: rdb, hub: h, ws: wsh, mux: http.NewServeMux(), log: log}
	s.mux.Handle("GET /ws", wsh)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	s.mux.HandleFunc("GET /readyz", s.ready)
	return s
}

// Handler returns the HTTP handler for all endpoints.
func (s *Server) Handler() http.Handler { return s.mux }

// Start launches background loops.
func (s *Server) Start() { s.hub.Run() }

// Shutdown drains WebSocket connections (clients get StatusGoingAway and are
// expected to reconnect elsewhere), then stops the hub.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.ws.Shutdown(ctx)
	s.hub.Close()
	return err
}

// ready reports whether this instance can serve traffic, i.e. Redis is
// reachable. Load balancers should route only to ready instances.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("readiness check failed", "err", err)
		}
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ready"))
}
