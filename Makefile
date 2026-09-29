.PHONY: test test-redis lint build up down loadgen

## test: unit + integration tests with the race detector (in-process miniredis)
test:
	go test -race -count=1 ./...

## test-redis: same tests against a real Redis (DB 15 is flushed), e.g.
##   make test-redis REDIS_TEST_ADDR=localhost:6390
REDIS_TEST_ADDR ?= localhost:6379
test-redis:
	REDIS_TEST_ADDR=$(REDIS_TEST_ADDR) go test -race -count=1 -p 1 ./...

lint:
	gofmt -l . | (! grep .) && go vet ./...

build:
	go build -o bin/server ./cmd/server
	go build -o bin/loadgen ./cmd/loadgen

## up: Redis + two server instances on :8081 and :8082
up:
	@docker info >/dev/null 2>&1 || { echo "Docker daemon is not running - start Docker Desktop first."; exit 1; }
	docker compose up --build -d --wait

down:
	docker compose down

## loadgen: 500 simulated users split across both instances
loadgen:
	go run ./cmd/loadgen -users 500
