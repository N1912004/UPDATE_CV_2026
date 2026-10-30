// Package jobs runs AI jobs asynchronously on a fixed goroutine pool.
package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/N1912004/go-ai-job-gateway/internal/provider"
	"github.com/N1912004/go-ai-job-gateway/internal/store"
)

var ErrQueueFull = errors.New("queue full")

type Pool struct {
	store   store.Store
	prov    *provider.Fallback
	queue   chan string
	timeout time.Duration
	wg      sync.WaitGroup
	log     *slog.Logger
}

func NewPool(s store.Store, p *provider.Fallback, workers, queueSize int, jobTimeout time.Duration, log *slog.Logger) *Pool {
	if workers < 1 {
		workers = 1
	}
	return &Pool{store: s, prov: p, queue: make(chan string, queueSize), timeout: jobTimeout, log: log, wg: sync.WaitGroup{}}
}

// Start launches workers; they exit when ctx is cancelled and Wait returns once in-flight jobs finish.
func (p *Pool) Start(ctx context.Context, workers int) {
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *Pool) Wait() { p.wg.Wait() }

// Enqueue is non-blocking: back-pressure is reported to the caller as ErrQueueFull (HTTP 429/503).
func (p *Pool) Enqueue(id string) error {
	select {
	case p.queue <- id:
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *Pool) worker(ctx context.Context, n int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-p.queue:
			p.process(ctx, id)
		}
	}
}

func (p *Pool) process(ctx context.Context, id string) {
	j, err := p.store.Get(ctx, id)
	if err != nil {
		p.log.Error("job load", "id", id, "err", err)
		return
	}
	j.Status = store.StatusRunning
	_ = p.store.Update(ctx, j)

	jctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	out, name, err := p.prov.RunNamed(jctx, j.Kind, j.Input)
	if err != nil {
		j.Status, j.Error = store.StatusFailed, err.Error()
	} else {
		j.Status, j.Output, j.Provider = store.StatusDone, out, name
	}
	if err := p.store.Update(ctx, j); err != nil {
		p.log.Error("job save", "id", id, "err", err)
	}
}
