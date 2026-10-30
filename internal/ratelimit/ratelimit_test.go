package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemorySlidingWindow(t *testing.T) {
	now := time.Unix(0, 0)
	m := NewMemory(2, time.Minute)
	m.now = func() time.Time { return now }
	ctx := context.Background()

	ok, rem, _ := m.Allow(ctx, "d1")
	if !ok || rem != 1 {
		t.Fatalf("1st: ok=%v rem=%d", ok, rem)
	}
	ok, rem, _ = m.Allow(ctx, "d1")
	if !ok || rem != 0 {
		t.Fatalf("2nd: ok=%v rem=%d", ok, rem)
	}
	if ok, _, _ = m.Allow(ctx, "d1"); ok {
		t.Fatal("3rd should be blocked")
	}
	if ok, _, _ = m.Allow(ctx, "d2"); !ok {
		t.Fatal("other key must be independent")
	}
	now = now.Add(61 * time.Second)
	if ok, _, _ = m.Allow(ctx, "d1"); !ok {
		t.Fatal("window should have slid")
	}
}
