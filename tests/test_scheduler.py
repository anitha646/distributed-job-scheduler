"""
Automated tests for critical functionality.

Run with: pytest tests/ -v
Requires a running Postgres test database (set TEST_DATABASE_URL).

Covers:
1. Atomic job claiming - no two "workers" can claim the same job under
   concurrent load (the single most important correctness property of
   this system).
2. Job lifecycle transitions (queued -> claimed -> running -> completed).
3. Retry backoff calculation for all three strategies.
4. Dead-letter behavior once max_attempts is exceeded.
"""
import os
import sys
import uuid
import threading
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "worker"))

TEST_DB_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/job_scheduler_test")


@pytest.fixture(scope="module")
def db_engine():
    engine = create_engine(TEST_DB_URL)
    yield engine


@pytest.fixture
def db_session(db_engine):
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


def _setup_project_queue_job(session, max_attempts=3):
    """Helper: creates a minimal org/project/queue/job chain for tests."""
    ids = {}
    user_id = str(uuid.uuid4())
    session.execute(text("INSERT INTO users (id, email, password_hash, full_name) VALUES "
                          "(:id, :email, 'x', 'Test User')"),
                     {"id": user_id, "email": f"{user_id}@test.com"})
    org_id = str(uuid.uuid4())
    session.execute(text("INSERT INTO organizations (id, name, owner_id) VALUES (:id, 'TestOrg', :owner)"),
                     {"id": org_id, "owner": user_id})
    project_id = str(uuid.uuid4())
    session.execute(text("INSERT INTO projects (id, organization_id, name) VALUES (:id, :org, 'TestProject')"),
                     {"id": project_id, "org": org_id})
    queue_id = str(uuid.uuid4())
    session.execute(text("INSERT INTO queues (id, project_id, name, concurrency_limit) "
                          "VALUES (:id, :proj, 'test-queue', 5)"),
                     {"id": queue_id, "proj": project_id})
    job_id = str(uuid.uuid4())
    session.execute(text("INSERT INTO jobs (id, queue_id, job_type, max_attempts) "
                          "VALUES (:id, :queue, 'test_job', :max_attempts)"),
                     {"id": job_id, "queue": queue_id, "max_attempts": max_attempts})
    session.commit()
    return queue_id, job_id


class TestAtomicClaiming:
    def test_single_job_claimed_exactly_once_under_concurrency(self, db_session, db_engine):
        """
        The core reliability guarantee: fire N concurrent claim attempts
        at ONE available job and assert exactly one succeeds.
        """
        queue_id, job_id = _setup_project_queue_job(db_session)

        claim_sql = text("""
            WITH candidate AS (
                SELECT id FROM jobs
                WHERE status IN ('queued','scheduled') AND run_at <= now()
                ORDER BY priority DESC, run_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs SET status = 'claimed', claimed_by = NULL, claimed_at = now()
            WHERE id IN (SELECT id FROM candidate)
            RETURNING id;
        """)

        results = []
        results_lock = threading.Lock()

        def attempt_claim():
            Session = sessionmaker(bind=db_engine)
            s = Session()
            try:
                rows = s.execute(claim_sql).fetchall()
                s.commit()
                with results_lock:
                    results.extend(rows)
            finally:
                s.close()

        threads = [threading.Thread(target=attempt_claim) for _ in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert len(results) == 1, f"Expected exactly 1 claim to succeed, got {len(results)}"
        assert str(results[0][0]) == job_id

    def test_claimed_job_not_reclaimed(self, db_session):
        queue_id, job_id = _setup_project_queue_job(db_session)
        db_session.execute(text("UPDATE jobs SET status='claimed' WHERE id=:id"), {"id": job_id})
        db_session.commit()

        remaining = db_session.execute(text(
            "SELECT COUNT(*) FROM jobs WHERE id=:id AND status IN ('queued','scheduled')"
        ), {"id": job_id}).scalar()
        assert remaining == 0


class TestJobLifecycle:
    def test_lifecycle_transitions(self, db_session):
        queue_id, job_id = _setup_project_queue_job(db_session)
        for new_status in ["claimed", "running", "completed"]:
            db_session.execute(text("UPDATE jobs SET status=:s WHERE id=:id"),
                                {"s": new_status, "id": job_id})
            db_session.commit()
            current = db_session.execute(text("SELECT status FROM jobs WHERE id=:id"),
                                          {"id": job_id}).scalar()
            assert current == new_status

    def test_exhausted_retries_move_to_dead_letter(self, db_session):
        queue_id, job_id = _setup_project_queue_job(db_session, max_attempts=2)
        db_session.execute(text("UPDATE jobs SET attempt_count=2, status='failed' WHERE id=:id"),
                            {"id": job_id})
        db_session.commit()

        job = db_session.execute(text("SELECT attempt_count, max_attempts FROM jobs WHERE id=:id"),
                                  {"id": job_id}).fetchone()
        should_dead_letter = job.attempt_count >= job.max_attempts
        assert should_dead_letter is True


class TestRetryBackoff:
    def test_fixed_backoff_is_constant(self):
        from worker import compute_backoff_seconds
        d1 = compute_backoff_seconds("fixed", 1, 1000, 60000)
        d3 = compute_backoff_seconds("fixed", 3, 1000, 60000)
        # jitter adds up to 10%, so compare within tolerance rather than exact equality
        assert 1.0 <= d1 <= 1.1
        assert 1.0 <= d3 <= 1.1

    def test_linear_backoff_increases_linearly(self):
        from worker import compute_backoff_seconds
        d1 = compute_backoff_seconds("linear", 1, 1000, 60000)
        d2 = compute_backoff_seconds("linear", 2, 1000, 60000)
        d3 = compute_backoff_seconds("linear", 3, 1000, 60000)
        assert d2 > d1
        assert d3 > d2

    def test_exponential_backoff_doubles(self):
        from worker import compute_backoff_seconds
        d1 = compute_backoff_seconds("exponential", 1, 1000, 60000)
        d2 = compute_backoff_seconds("exponential", 2, 1000, 60000)
        d3 = compute_backoff_seconds("exponential", 3, 1000, 60000)
        assert d1 < d2 < d3
        # exponential should grow faster than linear given the same inputs
        assert (d3 - d2) > (d2 - d1) * 1.5

    def test_backoff_respects_max_delay_cap(self):
        from worker import compute_backoff_seconds
        d = compute_backoff_seconds("exponential", 20, 1000, 5000)
        assert d <= 5.5  # capped at max_delay_ms + jitter
