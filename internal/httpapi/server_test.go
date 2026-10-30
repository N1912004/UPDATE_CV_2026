package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/N1912004/go-ai-job-gateway/internal/jobs"
	"github.com/N1912004/go-ai-job-gateway/internal/provider"
	"github.com/N1912004/go-ai-job-gateway/internal/ratelimit"
	"github.com/N1912004/go-ai-job-gateway/internal/store"
)

func newTestServer(t *testing.T, limit int) *httptest.Server {
	t.Helper()
	s := store.NewMem()
	pool := jobs.NewPool(s, provider.NewFallback(provider.NewFake("fake", 10*time.Millisecond, 0)), 2, 16, time.Second, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx, 2)
	ts := httptest.NewServer(New(s, pool, ratelimit.NewMemory(limit, time.Minute), nil, slog.Default()))
	t.Cleanup(func() { ts.Close(); cancel(); pool.Wait() })
	return ts
}

func do(t *testing.T, ts *httptest.Server, method, path, device, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if device != "" {
		req.Header.Set("X-Device-ID", device)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestCreateThenGetJob(t *testing.T) {
	ts := newTestServer(t, 100)
	res := do(t, ts, "POST", "/v1/jobs", "dev-1", `{"kind":"translate","input":"xin chao"}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", res.StatusCode)
	}
	var j store.Job
	_ = json.NewDecoder(res.Body).Decode(&j)
	res.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		r := do(t, ts, "GET", "/v1/jobs/"+j.ID, "dev-1", "")
		_ = json.NewDecoder(r.Body).Decode(&j)
		r.Body.Close()
		if j.Status == store.StatusDone || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if j.Status != store.StatusDone || j.Output != "XIN CHAO" {
		t.Fatalf("got %+v", j)
	}
}

func TestOwnershipAndAuth(t *testing.T) {
	ts := newTestServer(t, 100)
	res := do(t, ts, "POST", "/v1/jobs", "dev-1", `{"kind":"x","input":"y"}`)
	var j store.Job
	_ = json.NewDecoder(res.Body).Decode(&j)
	res.Body.Close()

	if r := do(t, ts, "GET", "/v1/jobs/"+j.ID, "dev-2", ""); r.StatusCode != http.StatusNotFound {
		t.Fatalf("other device should get 404, got %d", r.StatusCode)
	}
	if r := do(t, ts, "GET", "/v1/jobs/"+j.ID, "", ""); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing device should get 401, got %d", r.StatusCode)
	}
}

func TestValidationAndRateLimit(t *testing.T) {
	ts := newTestServer(t, 2)
	if r := do(t, ts, "POST", "/v1/jobs", "d", `{"kind":"","input":""}`); r.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", r.StatusCode)
	}
	if r := do(t, ts, "POST", "/v1/jobs", "d", `not json`); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", r.StatusCode)
	}
	// limit=2: the two calls above consumed the window
	if r := do(t, ts, "POST", "/v1/jobs", "d", `{"kind":"x","input":"y"}`); r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", r.StatusCode)
	}
}

func TestSSEStreamsUntilDone(t *testing.T) {
	ts := newTestServer(t, 100)
	res := do(t, ts, "POST", "/v1/jobs", "d", `{"kind":"summarize","input":"hello"}`)
	var j store.Job
	_ = json.NewDecoder(res.Body).Decode(&j)
	res.Body.Close()

	r := do(t, ts, "GET", "/v1/jobs/"+j.ID+"/events", "d", "")
	defer r.Body.Close()
	if ct := r.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}
	sc := bufio.NewScanner(r.Body)
	var last string
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			last = sc.Text()
		}
	}
	if !strings.Contains(last, `"status":"done"`) {
		t.Fatalf("stream did not end with done: %s", last)
	}
}
