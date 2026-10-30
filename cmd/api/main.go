// Command api runs the AI job gateway.
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

	"github.com/N1912004/go-ai-job-gateway/internal/config"
	"github.com/N1912004/go-ai-job-gateway/internal/httpapi"
	"github.com/N1912004/go-ai-job-gateway/internal/jobs"
	"github.com/N1912004/go-ai-job-gateway/internal/provider"
	"github.com/N1912004/go-ai-job-gateway/internal/ratelimit"
	"github.com/N1912004/go-ai-job-gateway/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// store
	var st store.Store = store.NewMem()
	var ready httpapi.Pinger
	if cfg.DatabaseURL != "" {
		pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			log.Error("postgres", "err", err)
			os.Exit(1)
		}
		defer pg.Close()
		st, ready = pg, pg
	}

	// rate limiter
	var lim ratelimit.Limiter = ratelimit.NewMemory(cfg.RateLimit, cfg.RateWindow)
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Error("redis url", "err", err)
			os.Exit(1)
		}
		lim = ratelimit.NewRedis(redis.NewClient(opt), cfg.RateLimit, cfg.RateWindow)
	}

	// providers: primary may fail, secondary is the fallback
	prov := provider.NewFallback(
		provider.NewFake("primary", cfg.ProviderDelay, cfg.ProviderFail),
		provider.NewFake("secondary", cfg.ProviderDelay*2, 0),
	)
	pool := jobs.NewPool(st, prov, cfg.Workers, cfg.QueueSize, 30*time.Second, log)
	pool.Start(ctx, cfg.Workers)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           httpapi.New(st, pool, lim, ready, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("listening", "addr", cfg.Addr, "workers", cfg.Workers, "store", map[bool]string{true: "postgres", false: "memory"}[cfg.DatabaseURL != ""])
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx) // stop accepting, drain requests
	pool.Wait()                   // finish in-flight jobs
	log.Info("stopped")
}
