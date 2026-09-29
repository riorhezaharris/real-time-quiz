# Real-Time Vocabulary Quiz: Server

A horizontally scalable Go server for a real-time quiz feature. Users join by quiz ID, answer questions, get their score back immediately, and watch a live leaderboard that is shared across every server instance.

- **System design:** [`docs/system-design.md`](docs/system-design.md) (architecture, data flow, technology choices, scalability, reliability, observability)
- **AI collaboration and verification:** [`docs/ai-collaboration.md`](docs/ai-collaboration.md)
- **Original brief:** [`requirements.md`](requirements.md)

## How it works (in one paragraph)

Clients hold a WebSocket to any stateless server instance. Answers are checked on the server and applied through one atomic Redis Lua script. The script scores each `(user, question)` exactly once, updates a sorted-set leaderboard and publishes a "quiz changed" event. Every instance with clients in that quiz coalesces those events and broadcasts a top-N snapshot at most every 150 ms.

## Quick start

**Requirements:** Go 1.24+ (the toolchain auto-upgrades to 1.26), plus Redis or Docker.

### Option A: Docker (Redis + 2 server instances)
```bash
make up                     # redis (internal), server-a :8081, server-b :8082
make loadgen                # 500 bots split across both instances
make down
```

### Option B: local processes
```bash
redis-server &                                             # or: docker run -p 6379:6379 redis:7-alpine
HTTP_ADDR=:8081 INSTANCE_ID=a go run ./cmd/server &
HTTP_ADDR=:8082 INSTANCE_ID=b go run ./cmd/server &
go run ./cmd/loadgen -users 1000 -think 200ms
```

Sample loadgen output:
```
=== summary (8.05s) ===
users joined:        1000
answers acked:       10000 (1242/s), correct 2473, duplicates 0
errors / reconnects: 0 / 0
answer ack latency:           p50=270µs p95=1.48ms p99=7.67ms max=15.26ms (n=10000)
answer→leaderboard latency:   p50=79.48ms p95=146.86ms p99=154.44ms (includes broadcast throttle)
final leaderboard (v39, 1000 participants):
  #1   bot8659-0040       70
  #2   bot8659-0468       60
  ...
consistency check: OK (10 leaderboard entries match acknowledged scores)
```

**Failover demo:** start a loadgen run with `-think 500ms`, then stop one server (`Ctrl-C` or `docker compose stop server-a`). Its clients get close code `1001 GoingAway`, reconnect to the other instance and keep their scores.

## Tests

```bash
make test         # go test -race ./...  (hermetic, uses in-process miniredis)
make test-redis   # same suite against a real Redis on localhost:6379 (flushes DB 15)
```

| Suite | Covers |
|---|---|
| `internal/scoring` | Correct and incorrect answers, validation, **100 concurrent duplicate submits → exactly one applied**, parallel users' totals, competition ranking, events published |
| `internal/leaderboard` | Ordering, tie ranks, deterministic tie order, names, empty quiz |
| `internal/hub` | Burst coalescing, ignoring other quizzes' events, dropping slow consumers, periodic resync, leave |
| `internal/server` | **Two instances sharing Redis** (cross-instance propagation), reconnecting to the other instance, every protocol error, oversized frames, graceful shutdown (1001), 40-user consistency, health and metrics |

## WebSocket protocol

Endpoint: `GET /ws`. Messages are JSON objects with a `type` field.

```jsonc
// → client to server
{"type":"join","quizId":"demo","userId":"alice","displayName":"Alice"}
{"type":"answer","requestId":"r1","questionId":"q1","answer":"joyful"}

// ← server to client
{"type":"joined","quizId":"demo","title":"…","userId":"alice","questions":[{"id":"q1","prompt":"…","options":["…"]}],"score":0,"rank":1,"leaderboard":[…]}
{"type":"answer_result","requestId":"r1","questionId":"q1","correct":true,"alreadyAnswered":false,"score":10,"rank":1}
{"type":"leaderboard","quizId":"demo","version":7,"serverTimeMs":1759150000000,"participants":2,"top":[{"userId":"alice","displayName":"Alice","score":10,"rank":1}]}
{"type":"error","requestId":"r1","code":"not_joined","message":"join a quiz first"}
```

Error codes: `bad_request`, `not_joined`, `already_joined`, `quiz_not_found`, `unknown_question`, `rate_limited`, `internal` (retryable).

Try it by hand with [websocat](https://github.com/vi/websocat): `websocat ws://localhost:8081/ws`, then paste the join and answer lines above. The mock quizzes are `demo` and `business-101`, defined in `internal/quiz/fixtures/quizzes.json`.

## Other endpoints

- `GET /healthz`: liveness
- `GET /readyz`: readiness (Redis reachable)
- `GET /metrics`: Prometheus metrics

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `INSTANCE_ID` | hostname | Label in logs |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `POINTS_PER_CORRECT` | `10` | Points per correct answer |
| `LEADERBOARD_TOP_N` | `10` | Entries per broadcast |
| `BROADCAST_INTERVAL` | `150ms` | Minimum gap between broadcasts per quiz |
| `RESYNC_INTERVAL` | `5s` | Forced refresh that heals lost pub/sub messages |
| `STATE_TTL` | `24h` | Expiry of quiz state after the last write |
| `SEND_BUFFER` | `64` | Per-connection outbound queue; a full queue means the client is dropped as a slow consumer |
| `PING_INTERVAL` | `20s` | Heartbeat interval for detecting dead peers |

## Project layout

```
cmd/server/            entrypoint: config, Redis, graceful shutdown
cmd/loadgen/           simulated users, latency stats, consistency check (demo client)
internal/config/       env configuration
internal/quiz/         quiz model + mocked question bank (JSON fixtures)
internal/keys/         Redis key schema (cluster hash tags)
internal/scoring/      atomic join/answer Lua scripts
internal/leaderboard/  top-N reads with competition ranking
internal/hub/          per-quiz rooms, pub/sub, coalesced broadcasts
internal/ws/           WebSocket connection lifecycle + protocol handling
internal/protocol/     wire message types (shared with loadgen)
internal/observability/ slog + Prometheus metrics
internal/server/       wiring + HTTP routes; multi-instance integration tests
docs/                  system design + AI collaboration log
```

## What is mocked

- **Question bank:** in-memory fixtures behind `quiz.Repository`.
- **Authentication:** `userId` is trusted from the `join` message. In production it would come from a JWT verified at the upgrade.
- **Client apps:** represented by `cmd/loadgen`.
