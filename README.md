# go-ai-job-gateway

[![ci](https://github.com/N1912004/UPDATE_CV_2026/actions/workflows/ci.yml/badge.svg)](https://github.com/N1912004/UPDATE_CV_2026/actions/workflows/ci.yml)

A small, production-shaped **Go backend for consumer AI mobile apps**: clients submit long-running AI jobs
(translate, summarise, image processing…), the API answers immediately with a job id, a goroutine worker pool
executes the job against a provider chain with **fallback routing**, and the client either polls or subscribes
to **Server-Sent Events** for status updates. Per-device **sliding-window rate limiting** is shared across
replicas through Redis.

Built to practise the backend patterns behind apps like camera translators and AI photo editors:
async inference, back-pressure, timeouts, idempotent status, horizontal scaling.

## Stack

Go 1.26 (stdlib `net/http` mux, `context`, goroutines/channels, `log/slog`) · PostgreSQL 16 (`pgx`) · Redis 7 ·
Docker multi-stage → distroless (~10 MB image) · GitHub Actions (vet, `-race` tests, image push to GHCR) · Kubernetes manifests (probes, HPA).

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/jobs` | Submit `{ "kind": "translate", "input": "..." }` → `202 Accepted` + job. Rate-limited per `X-Device-ID`. |
| `GET` | `/v1/jobs/{id}` | Job status/result. Only the owning device can read it. |
| `GET` | `/v1/jobs/{id}/events` | SSE stream of status changes until `done`/`failed`. |
| `GET` | `/healthz` · `/readyz` | Liveness / readiness (readiness pings Postgres). |

All protected routes require `X-Device-ID` (mobile install id). Swap `httpapi.device` for JWT when an auth service exists.

```bash
curl -s -XPOST localhost:8080/v1/jobs -H 'X-Device-ID: demo' -d '{"kind":"translate","input":"xin chào"}'
curl -N  localhost:8080/v1/jobs/<id>/events -H 'X-Device-ID: demo'
```

## Design notes

- **Worker pool**: `N` goroutines read job ids from a bounded channel. `Enqueue` never blocks; a full queue
  returns `503 + Retry-After` so overload is visible to the client instead of piling up in memory.
- **Provider fallback**: `provider.Fallback` tries providers in order; context cancellation/timeouts stop the chain.
  The shipped `Fake` provider simulates latency and a failure rate so the path is exercised end to end.
  A real OpenAI/Gemini client is one `Provider` implementation away.
- **Per-job timeout** via `context.WithTimeout`; the job is marked `failed` with the reason.
- **Rate limit**: ZSET sliding window executed as a Lua script (atomic, multi-replica safe). In-memory variant for tests/dev.
- **Storage**: `store.Store` interface; `MemStore` for tests, `PGStore` with an idempotent schema and indexes on
  `(device_id, created_at)` and a partial index on active statuses.
- **Graceful shutdown**: SIGTERM → stop accepting → drain HTTP → wait for in-flight jobs.
- **Security basics**: body size cap, strict validation, per-device ownership checks, non-root distroless image.

## Run

```bash
make test                  # go test -race ./...
make run                   # in-memory store + limiter, :8080
docker compose up --build  # Postgres + Redis + API
kubectl apply -f deploy/k8s.yaml
```

## Roadmap

- Real provider clients (OpenAI / Gemini) + token usage accounting per device
- Semantic cache for repeated prompts
- JWT auth + refresh, OpenTelemetry tracing
