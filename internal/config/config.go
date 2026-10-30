// Package config reads runtime settings from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr          string
	DatabaseURL   string // empty => in-memory store
	RedisURL      string // empty => in-memory rate limiter
	Workers       int
	QueueSize     int
	RateLimit     int           // requests per window per device
	RateWindow    time.Duration //
	ProviderDelay time.Duration // simulated AI latency
	ProviderFail  float64       // simulated primary-provider failure rate 0..1
}

func Load() Config {
	return Config{
		Addr:          env("ADDR", ":8080"),
		DatabaseURL:   env("DATABASE_URL", ""),
		RedisURL:      env("REDIS_URL", ""),
		Workers:       envInt("WORKERS", 4),
		QueueSize:     envInt("QUEUE_SIZE", 256),
		RateLimit:     envInt("RATE_LIMIT", 60),
		RateWindow:    envDur("RATE_WINDOW", time.Minute),
		ProviderDelay: envDur("PROVIDER_DELAY", 300*time.Millisecond),
		ProviderFail:  envFloat("PROVIDER_FAIL_RATE", 0.2),
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}
func envDur(k string, d time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return d
}
func envFloat(k string, d float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return d
}
