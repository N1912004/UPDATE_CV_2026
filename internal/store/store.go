// Package store persists jobs. MemStore is used in tests and when DATABASE_URL is unset.
package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Job struct {
	ID        string    `json:"id"`
	DeviceID  string    `json:"device_id"`
	Kind      string    `json:"kind"`
	Input     string    `json:"input"`
	Output    string    `json:"output,omitempty"`
	Provider  string    `json:"provider,omitempty"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

var ErrNotFound = errors.New("job not found")

type Store interface {
	Create(ctx context.Context, j *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	Update(ctx context.Context, j *Job) error
}

type MemStore struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewMem() *MemStore { return &MemStore{jobs: map[string]Job{}} }

func (m *MemStore) Create(_ context.Context, j *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = *j
	return nil
}

func (m *MemStore) Get(_ context.Context, id string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &j, nil
}

func (m *MemStore) Update(_ context.Context, j *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[j.ID]; !ok {
		return ErrNotFound
	}
	j.UpdatedAt = time.Now()
	m.jobs[j.ID] = *j
	return nil
}
