# Design Decisions & Trade-offs

## 1. Postgres-as-queue vs. a dedicated message broker

**Decision:** Use PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED` as the
job queue, rather than Redis/RabbitMQ/SQS alongside Postgres for state.

**Why:** The assignment weights database design and reliability/concurrency
heavily (35/100 combined) and explicitly asks for a relational schema
covering the full job lifecycle. Keeping the queue *in* that same schema
means job state and queue state can never drift out of sync — there's one
source of truth, one transaction boundary, one place to look during an
incident.

**Trade-off accepted:** A dedicated broker would out-perform Postgres at
very high throughput (tens of thousands of jobs/sec) and offers built-in
pub/sub for push-based delivery. At the scale this assignment targets, the
consistency and simplicity win is worth more than that ceiling. If this
went to production at high scale, the natural evolution is Postgres for
system-of-record + Redis Streams or SQS for the hot claim path, with the
`jobs` table remaining the audit trail.

## 2. Atomic claiming: `SKIP LOCKED` vs. optimistic locking (version column)

**Decision:** `FOR UPDATE SKIP LOCKED`.

**Why:** Optimistic locking (a `version` column + `WHERE version = :v`
compare-and-swap) also prevents double-claims, but under contention it
wastes work: every losing worker still runs a full query and gets a
rejected update, then has to retry. `SKIP LOCKED` lets losing transactions
simply skip past rows another transaction already holds, so N concurrent
workers polling the same queue degrade gracefully instead of thrashing.
This is directly tested in `tests/test_scheduler.py::TestAtomicClaiming`.

## 3. Retry strategy: worker-computed backoff vs. DB-computed backoff

**Decision:** Backoff delay is computed in the worker (`compute_backoff_seconds`
in `worker.py`), reading strategy/base/max from the queue's `retry_policy`.

**Why:** Keeps the retry math out of the hot-path SQL and easy to unit
test in isolation (see `TestRetryBackoff`). The trade-off is that the
`/jobs/{id}/complete` endpoint currently re-queues failed jobs immediately
rather than applying the delay itself — noted in `main.py` as a
placeholder. The correct production fix is to have the worker pass the
computed `run_at` back to `/complete`, which the schema already supports
(`jobs.run_at` is exactly the field the claim query filters on).

## 4. UUID primary keys vs. auto-increment integers

**Decision:** UUIDs everywhere.

**Why:** Multiple workers and (eventually) multiple API instances insert
concurrently; UUIDs avoid coordination on ID allocation and don't leak
row-count information the way sequential IDs do. Trade-off: slightly
larger index size and worse insert locality than integers — acceptable
given this system isn't insert-bound in the same way a high-frequency
trading log would be.

## 5. Dashboard: polling vs. WebSockets

**Decision:** Polling (3s interval) for both the worker's queue check and
the dashboard's live view, listed as a bonus rather than core feature.

**Why:** WebSockets add real value at scale but also add a second
connection lifecycle to manage (reconnect handling, backpressure) that
wasn't worth the time against the core reliability requirements under a
one-day build. Polling is simple, debuggable, and the swap-in point is
isolated (see `architecture.md`).

## 6. Normalization: separate `job_executions` vs. columns on `jobs`

**Decision:** Retry/attempt history lives in a separate `job_executions`
table (1:N from `jobs`), not as JSON or repeated columns on the `jobs` row.

**Why:** Keeps the `jobs` row — which is under write contention during
claiming — small and hot, while execution history (which can be large per
job after several retries) lives in a table that's only appended to, never
part of the claim query's working set. This is the schema decision most
directly tied to the "performance considerations" the assignment asks
about under Database Design.

## What was deliberately cut, and why

Given the one-day scope, cron scheduling execution, workflow dependency
enforcement, and RBAC enforcement were left at the schema-and-endpoint-shape
level rather than fully wired — the tables and fields exist
(`scheduled_jobs`, `job_dependencies`, `users.role`), but the logic that
walks a cron expression to compute `next_run_at`, or blocks a job from
being claimed while its dependencies are incomplete, is not implemented.
This was a conscious call to protect time for the core lifecycle, atomic
claiming, and retry/DLQ behavior, which the rubric weights far more
heavily than bonus features.
