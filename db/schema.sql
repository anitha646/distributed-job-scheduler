-- ============================================================
-- Distributed Job Scheduler - Relational Schema (PostgreSQL)
-- ============================================================
-- Design principles:
--  - UUID primary keys (safe for distributed inserts, no leakage of row counts)
--  - Every FK indexed for join performance
--  - Partial indexes on hot query paths (status = 'queued', etc.)
--  - Timestamps in UTC, created_at/updated_at on every mutable table
--  - ON DELETE CASCADE only where child rows have no meaning without parent
--    (e.g. job_executions without a job); ON DELETE RESTRICT where accidental
--    deletion would silently orphan business-critical history (users, orgs)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------- Users & Organizations ----------

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    password_hash   VARCHAR(255) NOT NULL,
    full_name       VARCHAR(255) NOT NULL,
    role            VARCHAR(50) NOT NULL DEFAULT 'member', -- admin | member (RBAC bonus extends this)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE organizations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL,
    owner_id        UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_organizations_owner ON organizations(owner_id);

CREATE TABLE organization_members (
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            VARCHAR(50) NOT NULL DEFAULT 'member',
    PRIMARY KEY (organization_id, user_id)
);

-- ---------- Projects & Queues ----------

CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, name)
);
CREATE INDEX idx_projects_org ON projects(organization_id);

CREATE TABLE retry_policies (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(100) NOT NULL,
    strategy        VARCHAR(20) NOT NULL CHECK (strategy IN ('fixed','linear','exponential')),
    base_delay_ms   INTEGER NOT NULL DEFAULT 1000,
    max_delay_ms    INTEGER NOT NULL DEFAULT 60000,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE queues (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id          UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name                VARCHAR(255) NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0, -- higher = served first
    concurrency_limit   INTEGER NOT NULL DEFAULT 5, -- max jobs running at once for this queue
    retry_policy_id     UUID REFERENCES retry_policies(id) ON DELETE SET NULL,
    is_paused           BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);
CREATE INDEX idx_queues_project ON queues(project_id);
CREATE INDEX idx_queues_active ON queues(project_id) WHERE is_paused = false;

-- ---------- Jobs & lifecycle ----------
-- Lifecycle: queued -> scheduled -> claimed -> running -> completed
--                                              \-> failed -> (retry -> queued) | dead_letter

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    job_type        VARCHAR(100) NOT NULL,      -- e.g. 'send_email', 'generate_report'
    payload         JSONB NOT NULL DEFAULT '{}',
    status          VARCHAR(20) NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued','scheduled','claimed','running','completed','failed','dead_letter','cancelled')),
    priority        INTEGER NOT NULL DEFAULT 0,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(), -- when eligible to run (delayed/scheduled jobs)
    idempotency_key VARCHAR(255),                -- optional, enables safe re-submission
    batch_id        UUID,                        -- groups jobs submitted as a batch
    cron_expression VARCHAR(100),                -- set only for recurring parent jobs
    parent_job_id   UUID REFERENCES jobs(id) ON DELETE SET NULL, -- recurring: link instance -> template
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    claimed_by      UUID,                        -- worker id (FK added after workers table)
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (queue_id, idempotency_key)
);

-- Hot path index: worker polling for claimable jobs. Partial index keeps it tiny.
CREATE INDEX idx_jobs_claimable ON jobs(queue_id, priority DESC, run_at)
    WHERE status IN ('queued','scheduled');
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_batch ON jobs(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX idx_jobs_parent ON jobs(parent_job_id) WHERE parent_job_id IS NOT NULL;

-- Workflow dependency support (bonus feature, cheap to include now)
CREATE TABLE job_dependencies (
    job_id            UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, depends_on_job_id)
);

-- ---------- Workers ----------

CREATE TABLE workers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hostname        VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'online' CHECK (status IN ('online','offline','draining')),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_workers_status ON workers(status);

ALTER TABLE jobs ADD CONSTRAINT fk_jobs_claimed_by
    FOREIGN KEY (claimed_by) REFERENCES workers(id) ON DELETE SET NULL;
CREATE INDEX idx_jobs_claimed_by ON jobs(claimed_by) WHERE claimed_by IS NOT NULL;

CREATE TABLE worker_heartbeats (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    worker_id       UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    active_jobs     INTEGER NOT NULL DEFAULT 0,
    cpu_percent     NUMERIC(5,2),
    memory_mb       INTEGER,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_heartbeats_worker_time ON worker_heartbeats(worker_id, recorded_at DESC);

-- ---------- Execution history, logs, retries ----------

CREATE TABLE job_executions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       UUID REFERENCES workers(id) ON DELETE SET NULL,
    attempt_number  INTEGER NOT NULL,
    status          VARCHAR(20) NOT NULL CHECK (status IN ('running','completed','failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_ms     INTEGER,
    error_message   TEXT,
    result          JSONB
);
CREATE INDEX idx_executions_job ON job_executions(job_id);
CREATE INDEX idx_executions_worker ON job_executions(worker_id);

CREATE TABLE job_logs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    execution_id    UUID REFERENCES job_executions(id) ON DELETE CASCADE,
    level           VARCHAR(10) NOT NULL DEFAULT 'info' CHECK (level IN ('debug','info','warn','error')),
    message         TEXT NOT NULL,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_logs_job ON job_logs(job_id, logged_at);

-- Scheduled / recurring job definitions (cron templates live separately from job instances)
CREATE TABLE scheduled_jobs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    job_type        VARCHAR(100) NOT NULL,
    payload_template JSONB NOT NULL DEFAULT '{}',
    cron_expression VARCHAR(100) NOT NULL,
    next_run_at     TIMESTAMPTZ NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_scheduled_due ON scheduled_jobs(next_run_at) WHERE is_active = true;

CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    reason          TEXT NOT NULL,
    last_error      TEXT,
    attempt_count   INTEGER NOT NULL,
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved        BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX idx_dlq_job ON dead_letter_queue(job_id);
CREATE INDEX idx_dlq_unresolved ON dead_letter_queue(resolved) WHERE resolved = false;

-- ============================================================
-- Notes on normalization & performance (for design_decisions.md):
-- - Schema is in 3NF: no repeating groups, all non-key attributes depend
--   only on the primary key (e.g. retry policy is a separate reusable
--   entity, not duplicated columns on every queue).
-- - jobs.claimed_by + partial index idx_jobs_claimable is what makes
--   atomic claiming fast even with millions of historical (completed)
--   rows, since the partial index only covers queued/scheduled jobs.
-- - job_executions is kept separate from jobs (1:N) so retry history
--   doesn't bloat/lock the hot jobs row that workers are polling.
-- ============================================================
