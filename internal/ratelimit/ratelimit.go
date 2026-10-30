// Package ratelimit implements per-key sliding-window limits (memory for tests/dev, Redis for prod).
package ratelimit

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter interface {
	// Allow reports whether key may proceed and how many requests remain in the window.
	Allow(ctx context.Context, key string) (ok bool, remaining int, err error)
}

// Memory keeps timestamps per key; fine for a single instance.
type Memory struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	hits   map[string][]time.Time
	now    func() time.Time
}

func NewMemory(limit int, window time.Duration) *Memory {
	return &Memory{limit: limit, window: window, hits: map[string][]time.Time{}, now: time.Now}
}

func (m *Memory) Allow(_ context.Context, key string) (bool, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	cut := now.Add(-m.window)
	ts := m.hits[key][:0]
	for _, t := range m.hits[key] {
		if t.After(cut) {
			ts = append(ts, t)
		}
	}
	if len(ts) >= m.limit {
		m.hits[key] = ts
		return false, 0, nil
	}
	ts = append(ts, now)
	m.hits[key] = ts
	return true, m.limit - len(ts), nil
}

// Redis sliding window on a ZSET, atomic via Lua so many API replicas share one limit.
type Redis struct {
	rdb    *redis.Client
	limit  int
	window time.Duration
}

var slidingWindow = redis.NewScript(`
local key, now, window, limit = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local n = redis.call('ZCARD', key)
if n >= limit then return {0, 0} end
redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
redis.call('PEXPIRE', key, window)
return {1, limit - n - 1}`)

func NewRedis(rdb *redis.Client, limit int, window time.Duration) *Redis {
	return &Redis{rdb: rdb, limit: limit, window: window}
}

func (r *Redis) Allow(ctx context.Context, key string) (bool, int, error) {
	res, err := slidingWindow.Run(ctx, r.rdb, []string{"rl:" + key},
		time.Now().UnixMilli(), r.window.Milliseconds(), r.limit).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, int(res[1]), nil
}

// Header value helper.
func Itoa(n int) string { return strconv.Itoa(n) }
