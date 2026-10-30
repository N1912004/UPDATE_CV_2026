// Package httpapi exposes the job gateway over HTTP (stdlib net/http, Go 1.22+ mux patterns).
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/N1912004/go-ai-job-gateway/internal/jobs"
	"github.com/N1912004/go-ai-job-gateway/internal/ratelimit"
	"github.com/N1912004/go-ai-job-gateway/internal/store"
)

type Pinger interface{ Ping(context.Context) error }

type Server struct {
	store   store.Store
	pool    *jobs.Pool
	limiter ratelimit.Limiter
	ready   Pinger // nil => always ready
	log     *slog.Logger
}

func New(s store.Store, p *jobs.Pool, l ratelimit.Limiter, ready Pinger, log *slog.Logger) http.Handler {
	srv := &Server{store: s, pool: p, limiter: l, ready: ready, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.healthz)
	mux.HandleFunc("GET /readyz", srv.readyz)
	mux.Handle("POST /v1/jobs", srv.device(srv.rateLimited(http.HandlerFunc(srv.createJob))))
	mux.Handle("GET /v1/jobs/{id}", srv.device(http.HandlerFunc(srv.getJob)))
	mux.Handle("GET /v1/jobs/{id}/events", srv.device(http.HandlerFunc(srv.streamJob)))
	return srv.logging(mux)
}

// --- middleware ---

type ctxKey struct{}

func deviceFrom(ctx context.Context) string { v, _ := ctx.Value(ctxKey{}).(string); return v }

// device requires X-Device-ID (mobile install id). Swap for JWT when a real auth service exists.
func (s *Server) device(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Device-ID"))
		if id == "" || len(id) > 128 {
			writeErr(w, http.StatusUnauthorized, "missing or invalid X-Device-ID")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

func (s *Server) rateLimited(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, rem, err := s.limiter.Allow(r.Context(), deviceFrom(r.Context()))
		if err != nil {
			s.log.Error("ratelimit", "err", err)
			writeErr(w, http.StatusServiceUnavailable, "rate limiter unavailable")
			return
		}
		w.Header().Set("X-RateLimit-Remaining", ratelimit.Itoa(rem))
		if !ok {
			writeErr(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(rw, r)
		s.log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rw.code, "ms", time.Since(start).Milliseconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// --- handlers ---

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) }

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.ready.Ping(ctx); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "database not ready")
			return
		}
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

type createReq struct {
	Kind  string `json:"kind"`
	Input string `json:"input"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.Kind = strings.TrimSpace(req.Kind)
	if req.Kind == "" || len(req.Kind) > 32 || strings.TrimSpace(req.Input) == "" {
		writeErr(w, http.StatusUnprocessableEntity, "kind (<=32 chars) and input are required")
		return
	}
	now := time.Now()
	j := &store.Job{ID: newID(), DeviceID: deviceFrom(r.Context()), Kind: req.Kind, Input: req.Input,
		Status: store.StatusQueued, CreatedAt: now, UpdatedAt: now}
	if err := s.store.Create(r.Context(), j); err != nil {
		s.log.Error("create", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not save job")
		return
	}
	if err := s.pool.Enqueue(j.ID); err != nil {
		j.Status, j.Error = store.StatusFailed, "server busy, retry later"
		_ = s.store.Update(r.Context(), j)
		w.Header().Set("Retry-After", "2")
		writeErr(w, http.StatusServiceUnavailable, j.Error)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (s *Server) loadOwned(w http.ResponseWriter, r *http.Request) *store.Job {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) || (err == nil && j.DeviceID != deviceFrom(r.Context())) {
		writeErr(w, http.StatusNotFound, "job not found")
		return nil
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not load job")
		return nil
	}
	return j
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	if j := s.loadOwned(w, r); j != nil {
		writeJSON(w, 200, j)
	}
}

// streamJob pushes status changes over Server-Sent Events until the job is terminal or the client leaves.
func (s *Server) streamJob(w http.ResponseWriter, r *http.Request) {
	j := s.loadOwned(w, r)
	if j == nil {
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	last := store.Status("")
	for {
		if j.Status != last {
			b, _ := json.Marshal(j)
			_, _ = w.Write([]byte("event: status\ndata: " + string(b) + "\n\n"))
			fl.Flush()
			last = j.Status
		}
		if j.Status == store.StatusDone || j.Status == store.StatusFailed {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			if nj, err := s.store.Get(r.Context(), j.ID); err == nil {
				j = nj
			}
		}
	}
}

// --- helpers ---

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
