package jobs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/N1912004/go-ai-job-gateway/internal/provider"
	"github.com/N1912004/go-ai-job-gateway/internal/store"
)

func waitStatus(t *testing.T, s store.Store, id string, want store.Status) *store.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, _ := s.Get(context.Background(), id)
		if j != nil && j.Status == want {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s", id, want)
	return nil
}

func newJob(s store.Store, id, kind, in string) {
	_ = s.Create(context.Background(), &store.Job{ID: id, DeviceID: "d", Kind: kind, Input: in, Status: store.StatusQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()})
}

func TestPoolFallsBackToSecondary(t *testing.T) {
	s := store.NewMem()
	primary := provider.NewFake("primary", 0, 1) // always fails
	secondary := provider.NewFake("secondary", 0, 0)
	p := NewPool(s, provider.NewFallback(primary, secondary), 2, 8, time.Second, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx, 2)

	newJob(s, "j1", "translate", "hello")
	if err := p.Enqueue("j1"); err != nil {
		t.Fatal(err)
	}
	j := waitStatus(t, s, "j1", store.StatusDone)
	if j.Output != "HELLO" || j.Provider != "secondary" {
		t.Fatalf("got %+v", j)
	}
}

func TestPoolMarksFailedWhenAllProvidersFail(t *testing.T) {
	s := store.NewMem()
	p := NewPool(s, provider.NewFallback(provider.NewFake("a", 0, 1), provider.NewFake("b", 0, 1)), 1, 8, time.Second, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx, 1)
	newJob(s, "j2", "x", "y")
	_ = p.Enqueue("j2")
	j := waitStatus(t, s, "j2", store.StatusFailed)
	if j.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestPoolTimesOutSlowProvider(t *testing.T) {
	s := store.NewMem()
	slow := provider.NewFake("slow", time.Second, 0)
	p := NewPool(s, provider.NewFallback(slow), 1, 8, 20*time.Millisecond, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx, 1)
	newJob(s, "j3", "x", "y")
	_ = p.Enqueue("j3")
	j := waitStatus(t, s, "j3", store.StatusFailed)
	if j.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("want deadline exceeded, got %q", j.Error)
	}
}

func TestEnqueueBackPressure(t *testing.T) {
	p := NewPool(store.NewMem(), provider.NewFallback(), 1, 1, time.Second, slog.Default())
	if err := p.Enqueue("a"); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue("b"); err != ErrQueueFull {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}
