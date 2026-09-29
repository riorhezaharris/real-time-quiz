// Command server runs one real-time quiz server instance.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/config"
	"github.com/riorhezaharris/real-time-quiz/internal/observability"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
	"github.com/riorhezaharris/real-time-quiz/internal/server"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, cfg.LogLevel, cfg.InstanceID)

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     100,
	})
	defer rdb.Close()

	// Fail fast at startup, but keep running if Redis blips later: /readyz
	// reports it and requests get a retryable error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = rdb.Ping(ctx).Err()
	cancel()
	if err != nil {
		return err
	}

	repo, err := quiz.NewFixtureRepository()
	if err != nil {
		return err
	}

	srv := server.New(cfg, rdb, repo, log)
	srv.Start()
	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Addr, "redis", cfg.RedisAddr)
		errCh <- httpSrv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	// Stop accepting new HTTP requests first; hijacked WebSocket connections
	// are not tracked by http.Server, so drain them explicitly afterwards.
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("websocket drain incomplete", "err", err)
	}
	log.Info("stopped")
	return nil
}
