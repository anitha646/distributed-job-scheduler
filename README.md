# Distributed Job Scheduler

A production-inspired distributed job scheduling platform: REST API for
managing projects/queues/jobs, a concurrent worker service that claims jobs
atomically, and a live dashboard.

## Stack
- **Backend:** FastAPI + SQLAlchemy + PostgreSQL
- **Worker:** Python, thread pool, `SELECT ... FOR UPDATE SKIP LOCKED` for atomic claiming
- **Frontend:** Single-file React (CDN, no build step) polling the API every 3s
- **Auth:** JWT (bcrypt-hashed passwords)

## 1. Database setup

```bash
createdb job_scheduler
psql job_scheduler < db/schema.sql
```

(Requires PostgreSQL 12+ for `SKIP LOCKED` support.)

## 2. Backend

```bash
cd backend
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/job_scheduler"
export JWT_SECRET="pick-a-real-secret"
uvicorn main:app --reload --port 8000
```

API docs auto-generated at `http://localhost:8000/docs` (Swagger UI).

## 3. Worker

Run one or more worker processes (each is independent, so this is how you
get horizontal scaling):

```bash
cd worker
pip install requests
export API_BASE="http://localhost:8000"
export MAX_CONCURRENT_JOBS=4
python worker.py
```

Run a second worker in another terminal to see concurrent claiming in action —
watch the logs: no job is ever claimed by two workers.

## 4. Frontend

No build step. Just open `frontend/index.html` in a browser, or serve it:

```bash
cd frontend
python -m http.server 5500
```

Then visit `http://localhost:5500`. If your API isn't on `localhost:8000`,
set `window.SCHEDULER_API_BASE` before the app script runs.

## 5. Quick end-to-end smoke test

```bash
# Register + login
curl -X POST localhost:8000/auth/register -H "Content-Type: application/json" \
  -d '{"email":"you@test.com","password":"pass123","full_name":"You"}'
TOKEN=$(curl -s -X POST localhost:8000/auth/login -H "Content-Type: application/json" \
  -d '{"email":"you@test.com","password":"pass123"}' | python -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

# Create org/project/queue directly in DB for now, or extend main.py with an /organizations POST
# (organizations endpoint omitted from the walkthrough for brevity — model + table are ready)

# Submit a job
curl -X POST localhost:8000/jobs -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"queue_id":"<queue-uuid>","job_type":"send_email","payload":{"to":"a@b.com"}}'
```

## 6. Tests

```bash
createdb job_scheduler_test
psql job_scheduler_test < db/schema.sql
cd tests
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/job_scheduler_test"
pytest -v
```

## Project structure

```
db/schema.sql           - full relational schema, indexes, constraints
backend/                - FastAPI REST API
worker/worker.py        - concurrent polling worker with backoff + graceful shutdown
frontend/index.html     - live dashboard
tests/                  - pytest suite (atomic claiming, lifecycle, backoff)
docs/                   - architecture, ER diagram, design decisions, API reference
```

## What's implemented vs. bonus

**Implemented:** auth, projects/queues/jobs CRUD, immediate/delayed/batch jobs,
atomic claiming, full lifecycle with retries + dead letter queue, configurable
retry strategies (fixed/linear/exponential with jitter), execution logs and
metrics, live dashboard, pagination/filtering, automated tests.

**Partially implemented / documented as next steps:** cron recurring jobs
(schema + endpoint field ready, cron parser not wired), workflow dependencies
(schema ready via `job_dependencies`, enforcement logic not wired), RBAC
(role column exists, not enforced), WebSocket live updates (currently
polling — swap-in point noted in `docs/design_decisions.md`).
