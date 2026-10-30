package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id          TEXT PRIMARY KEY,
  device_id   TEXT NOT NULL,
  kind        TEXT NOT NULL,
  input       TEXT NOT NULL,
  output      TEXT NOT NULL DEFAULT '',
  provider    TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL,
  error       TEXT NOT NULL DEFAULT '',
  created_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_device_created_idx ON jobs (device_id, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status) WHERE status IN ('queued','running');`

type PGStore struct{ pool *pgxpool.Pool }

// NewPostgres connects and applies the idempotent schema.
func NewPostgres(ctx context.Context, url string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &PGStore{pool: pool}, nil
}

func (p *PGStore) Close() { p.pool.Close() }

func (p *PGStore) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *PGStore) Create(ctx context.Context, j *Job) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO jobs (id,device_id,kind,input,output,provider,status,error,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		j.ID, j.DeviceID, j.Kind, j.Input, j.Output, j.Provider, j.Status, j.Error, j.CreatedAt, j.UpdatedAt)
	return err
}

func (p *PGStore) Get(ctx context.Context, id string) (*Job, error) {
	var j Job
	err := p.pool.QueryRow(ctx, `SELECT id,device_id,kind,input,output,provider,status,error,created_at,updated_at
		FROM jobs WHERE id=$1`, id).
		Scan(&j.ID, &j.DeviceID, &j.Kind, &j.Input, &j.Output, &j.Provider, &j.Status, &j.Error, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

func (p *PGStore) Update(ctx context.Context, j *Job) error {
	j.UpdatedAt = time.Now()
	tag, err := p.pool.Exec(ctx, `UPDATE jobs SET output=$2, provider=$3, status=$4, error=$5, updated_at=$6 WHERE id=$1`,
		j.ID, j.Output, j.Provider, j.Status, j.Error, j.UpdatedAt)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
