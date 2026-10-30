.PHONY: run test lint build docker up down
run:    ; go run ./cmd/api
test:   ; go test -race -count=1 ./...
lint:   ; go vet ./... && staticcheck ./...
build:  ; CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/api ./cmd/api
docker: ; docker build -t go-ai-job-gateway .
up:     ; docker compose up --build -d
down:   ; docker compose down -v
