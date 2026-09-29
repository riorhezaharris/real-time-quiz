# AI Collaboration Log

**Tool:** Claude Code (Claude Opus 5.5), an agentic coding assistant running in the terminal. It could read and write files and run commands (`go test`, `redis-server`, the load generator), with each action visible for my review.

**Working style:**
- The decisions were mine; Claude asked about each fork before building anything.
- Code was developed one package at a time, each with tests written alongside it.
- Nothing was accepted until it had passed `go test -race`, had also been run against a **real** Redis, and had been exercised end to end with the load generator.

Code sections with significant AI help carry an `// AI-ASSISTED (Claude Code): …` comment. Each one says what was generated, how it was verified and what was changed afterwards:
- `internal/scoring/scoring.go`: Lua scoring scripts
- `internal/hub/hub.go`: coalescing flush loop
- `internal/ws/conn.go`: connection lifecycle and backpressure
- `cmd/loadgen/main.go`: load generator

> In practice, almost all of the code in this repository was drafted by Claude. The value I added was the decisions, the review, and the verification described below.

---

## 1. Design phase

| Task | Interaction | What I kept or changed |
|---|---|---|
| Read the brief and plan | "Review requirements.md, let's discuss a plan". Claude summarised the implicit grading points and asked me to choose the component, language, infrastructure, quiz flow, scoring rule and demo client. | Chose a Go server, real Redis, self-paced flow, fixed points and a CLI loadgen. |
| Architecture | Claude proposed Redis ZSET + Lua idempotency + pub/sub with throttled per-instance broadcast. | Accepted, after weighing ≤150 ms staleness against bounded fan-out cost. |
| Design doc | Claude drafted `system-design.md` from the plan and the measured results. | Removed a claim of "50k+ connections per instance" because it was never measured. |

## 2. Implementation, with verification

### 2.1 Atomic scoring (Lua) — `internal/scoring`
- **Prompt:** "atomic, idempotent per-(user, question) scoring in Redis that works on Redis Cluster".
- **What it produced:** two scripts. `join` does `ZADD NX`, `HSET` and `PUBLISH` for new users. `answer` does `SADD` as the guard, then `ZINCRBY`, `PUBLISH` and a rank calculation.
- **Verification** (`scoring_test.go`):
  - Correct, incorrect and unknown-question cases, plus input validation (including `{`/`}` in IDs, which would break the cluster hash tag).
  - **100 goroutines submitting the same answer concurrently: exactly one applied.** Run with `-race`.
  - 20 users answering in parallel, each submitting every answer twice: exact expected totals.
  - Competition ranking with ties (1, 2, 2, 4).
  - A publish is emitted on join and on a score change.
  - **Run against both miniredis and a real `redis-server` 8.2** (`REDIS_TEST_ADDR=…`). miniredis uses gopher-lua, which is not the Lua engine Redis ships, so passing on the emulator alone was not considered enough.
- **Refinements:**
  - The rank is computed inside the script, which saves a round trip per answer.
  - TTLs are refreshed on every write.
  - A wrong first answer locks the question. This was an explicit rule decision and has its own test.

### 2.2 Leaderboard reads — `internal/leaderboard`
- **Verification:** ordering, deterministic tie order (by user ID, overriding Redis's reverse-lexicographic order), shared ranks for ties, display-name lookup, and an empty quiz.

### 2.3 Hub / fan-out — `internal/hub`
- **Prompt:** "how do we stop a hot quiz from triggering a leaderboard read and broadcast per answer?"
- **What it produced:** a dirty set, a single ticker, bounded concurrent flushes, a snapshot encoded once per tick, and dropping slow consumers.
- **Verification** (`hub_test.go`):
  - 200 events in a burst cause only a handful of reads (coalescing), with strictly increasing versions.
  - Events for quizzes that have no local clients are ignored.
  - A slow client is dropped while healthy clients keep receiving.
  - The periodic resync works without any events.
  - Leave is idempotent.
- **Refinement:** in review I asked what happens if a Redis read is slow. Two flushes for the same quiz could overlap and deliver versions out of order, so a per-room `inflight` guard was added.

### 2.4 WebSocket connection — `internal/ws`
- **What it produced:** a reader, a single writer, a pinger, a bounded queue, close signalling through `sync.Once`, a rate limiter and error mapping.
- **Verification** (`server/server_test.go`, all run against real HTTP servers):
  - **Two server instances sharing one Redis**: Bob, connected to instance B, receives Alice's score change made on instance A.
  - Reconnecting to the *other* instance keeps the score, and a retried answer is reported as `alreadyAnswered`.
  - Every protocol error code, with the connection surviving all of them.
  - An oversized frame gets close code 1009 (message too big).
  - Graceful shutdown sends 1001 GoingAway.
  - 40 users across two instances sending duplicate answers: the broadcast ranks and the sum of scores in Redis match the expected values exactly.
  - `/healthz`, `/readyz` and `/metrics` contents.

### 2.5 Load generator — `cmd/loadgen`
- **What it produced:** simulated users spread round-robin across instances, reconnecting with backoff to the next instance, measuring ack and propagation latency, and running a final consistency check.
- **Verification:**
  - 1000 users × 10 answers on two instances: 0 errors and a passing consistency check. Instance A's `/metrics` showed `quiz_answer_duration_seconds_count 5000`, exactly its half of the round-robin split, which cross-checks the loadgen's own counts.
  - **Failover:** 300 users, then SIGTERM to instance A mid-run. 150 users reconnected to B with 0 errors and the consistency check still passed.

## 3. Bugs and mistakes caught by the verification process

These are the honest ones. Each was caught by a test or by reading output critically, not by trusting the generated code.

1. **Double-close panic in `Hub.Close`.** The integration test's cleanup called `Shutdown` after the test had already called it. `close(h.done)` panicked. **Fix:** wrapped it in `sync.Once`, since shutdown paths must be idempotent. The test now covers it.
2. **`t.Fatalf` called from a non-test goroutine** in a concurrency test. This is undefined behaviour in Go's testing package. It was caught in review before it could hide a failure, and changed to `t.Error` plus `return`.
3. **The verification command itself was wrong.** In the first failover attempt, `lsof -ti :8081 | xargs kill` matched the *load generator's own client sockets* as well as the server, so the loadgen was killed too. The clue was a report that stopped after one progress line with no summary. It was re-run with `lsof -sTCP:LISTEN` to target only the listening server. Lesson: check that the AI's test harness is testing what it claims to test.
4. **A "duplicate" appeared in the failover run.** Investigation showed this was correct behaviour, not a bug. Instance A scored an answer, then shut down before delivering the ack. The bot retried on B, and the idempotency guard refused to double count. This is the exact scenario the guard exists for.
5. **An unmeasured performance claim in the draft design doc** ("50k+ connections/instance") was removed.

## 4. Limitations of working with AI

- The emulator is not the real thing. AI-written Redis Lua passes on miniredis, but miniredis uses a different Lua VM, so the same tests also had to be run against real Redis.
- The AI will produce a plausible verification harness with confidence (see mistake 3). Its output has to be read as critically as its code.
- It tends to write performance claims into docs. Every number in `system-design.md` now comes from a run in this repo.
- The environment set limits on verification. Docker was not running during development, so the `docker-compose.yml` stack was **not** exercised here. The same binaries were verified running as local processes against a real Redis instead.

## 5. Candidate's own review notes

_TODO (me): add notes from my own line-by-line review, anything I changed by hand, and the screenshots or recordings used in the video._
