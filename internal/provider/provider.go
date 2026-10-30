// Package provider abstracts an AI backend (LLM / vision API) and adds fallback routing.
// Only a simulated provider ships here; wire a real client by implementing Provider.
package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

type Provider interface {
	Name() string
	Run(ctx context.Context, kind, input string) (string, error)
}

var ErrUnavailable = errors.New("provider unavailable")

// Fake simulates latency and a failure rate so fallback and timeouts are exercised.
type Fake struct {
	name     string
	delay    time.Duration
	failRate float64
	rnd      func() float64
}

func NewFake(name string, delay time.Duration, failRate float64) *Fake {
	return &Fake{name: name, delay: delay, failRate: failRate, rnd: rand.Float64}
}

func (f *Fake) Name() string { return f.name }

func (f *Fake) Run(ctx context.Context, kind, input string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(f.delay):
	}
	if f.failRate > 0 && f.rnd() < f.failRate {
		return "", fmt.Errorf("%s: %w", f.name, ErrUnavailable)
	}
	switch kind {
	case "translate":
		return strings.ToUpper(input), nil
	case "summarize":
		if len(input) > 32 {
			input = input[:32] + "…"
		}
		return input, nil
	default:
		return fmt.Sprintf("[%s] %s", kind, input), nil
	}
}

// Fallback tries providers in order; the first success wins.
// Context errors stop the chain immediately (no point retrying a cancelled request).
type Fallback struct{ chain []Provider }

func NewFallback(p ...Provider) *Fallback { return &Fallback{chain: p} }

func (f *Fallback) Name() string { return "fallback" }

// RunNamed is like Run but also reports which provider answered.
func (f *Fallback) RunNamed(ctx context.Context, kind, input string) (out, name string, err error) {
	var errs []error
	for _, p := range f.chain {
		out, err = p.Run(ctx, kind, input)
		if err == nil {
			return out, p.Name(), nil
		}
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		errs = append(errs, err)
	}
	return "", "", errors.Join(errs...)
}

func (f *Fallback) Run(ctx context.Context, kind, input string) (string, error) {
	out, _, err := f.RunNamed(ctx, kind, input)
	return out, err
}
