import logging
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import (Base, engine, get_db, User, Organization, Project,
                       Queue, Job, Worker, WorkerHeartbeat, JobExecution,
                       JobLog, RetryPolicy, DeadLetterEntry)
from auth import hash_password, verify_password, create_access_token, get_current_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scheduler")

Base.metadata.create_all(bind=engine)  # for local dev; use Alembic migrations in production

app = FastAPI(title="Distributed Job Scheduler API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Schemas ----------

class RegisterRequest(BaseModel):
    email: str
    password: str
    full_name: str


class LoginRequest(BaseModel):
    email: str
    password: str


class OrganizationCreate(BaseModel):
    name: str


class ProjectCreate(BaseModel):
    organization_id: str
    name: str
    description: Optional[str] = None


class QueueCreate(BaseModel):
    project_id: str
    name: str
    priority: int = 0
    concurrency_limit: int = 5
    retry_strategy: str = Field(default="exponential", pattern="^(fixed|linear|exponential)$")
    max_attempts: int = 3


class JobCreate(BaseModel):
    queue_id: str
    job_type: str
    payload: dict = {}
    priority: int = 0
    run_at: Optional[datetime] = None       # for delayed/scheduled jobs
    idempotency_key: Optional[str] = None
    max_attempts: Optional[int] = None
    cron_expression: Optional[str] = None    # if set, creates a recurring template


class BatchJobCreate(BaseModel):
    queue_id: str
    jobs: List[JobCreate]


class ClaimRequest(BaseModel):
    worker_id: str
    queue_id: Optional[str] = None
    limit: int = 1


class JobResultUpdate(BaseModel):
    status: str  # completed | failed
    result: Optional[dict] = None
    error_message: Optional[str] = None


# ---------- Auth ----------

@app.post("/auth/register", status_code=201)
def register(body: RegisterRequest, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(400, "Email already registered")
    user = User(email=body.email, password_hash=hash_password(body.password), full_name=body.full_name)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"id": user.id, "email": user.email}


@app.post("/auth/login")
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(401, "Invalid credentials")
    token = create_access_token({"sub": user.id})
    return {"access_token": token, "token_type": "bearer"}


# ---------- Organizations ----------

@app.post("/organizations", status_code=201)
def create_organization(body: OrganizationCreate, db: Session = Depends(get_db),
                         user: User = Depends(get_current_user)):
    org = Organization(name=body.name, owner_id=user.id)
    db.add(org)
    db.commit()
    db.refresh(org)
    return org


@app.get("/organizations")
def list_organizations(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    return db.query(Organization).filter(Organization.owner_id == user.id).all()


# ---------- Projects & Queues ----------

@app.post("/projects", status_code=201)
def create_project(body: ProjectCreate, db: Session = Depends(get_db),
                    user: User = Depends(get_current_user)):
    project = Project(**body.dict())
    db.add(project)
    db.commit()
    db.refresh(project)
    return project


@app.get("/projects")
def list_projects(db: Session = Depends(get_db)):
    return db.query(Project).all()


@app.post("/queues", status_code=201)
def create_queue(body: QueueCreate, db: Session = Depends(get_db),
                  user: User = Depends(get_current_user)):
    policy = RetryPolicy(name=f"{body.name}-policy", strategy=body.retry_strategy,
                          max_attempts=body.max_attempts)
    db.add(policy)
    db.flush()
    queue = Queue(project_id=body.project_id, name=body.name, priority=body.priority,
                  concurrency_limit=body.concurrency_limit, retry_policy_id=policy.id)
    db.add(queue)
    db.commit()
    db.refresh(queue)
    return queue


@app.get("/queues")
def list_queues(project_id: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(Queue)
    if project_id:
        q = q.filter(Queue.project_id == project_id)
    return q.all()


@app.post("/queues/{queue_id}/pause")
def pause_queue(queue_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    queue = db.query(Queue).filter(Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(404, "Queue not found")
    queue.is_paused = True
    db.commit()
    return {"id": queue.id, "is_paused": True}


@app.post("/queues/{queue_id}/resume")
def resume_queue(queue_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    queue = db.query(Queue).filter(Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(404, "Queue not found")
    queue.is_paused = False
    db.commit()
    return {"id": queue.id, "is_paused": False}


@app.get("/queues/{queue_id}/stats")
def queue_stats(queue_id: str, db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT status, COUNT(*) AS count
        FROM jobs WHERE queue_id = :qid GROUP BY status
    """), {"qid": queue_id}).fetchall()
    return {r.status: r.count for r in rows}


# ---------- Jobs ----------

@app.post("/jobs", status_code=201)
def create_job(body: JobCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    status_ = "scheduled" if body.run_at and body.run_at > datetime.utcnow() else "queued"
    job = Job(
        queue_id=body.queue_id, job_type=body.job_type, payload=body.payload,
        priority=body.priority, run_at=body.run_at or datetime.utcnow(),
        idempotency_key=body.idempotency_key,
        max_attempts=body.max_attempts or 3,
        cron_expression=body.cron_expression,
        status=status_,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@app.post("/jobs/batch", status_code=201)
def create_batch(body: BatchJobCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    import uuid
    batch_id = str(uuid.uuid4())
    created = []
    for jc in body.jobs:
        job = Job(queue_id=body.queue_id, job_type=jc.job_type, payload=jc.payload,
                  priority=jc.priority, run_at=jc.run_at or datetime.utcnow(),
                  batch_id=batch_id, max_attempts=jc.max_attempts or 3)
        db.add(job)
        created.append(job)
    db.commit()
    return {"batch_id": batch_id, "count": len(created)}


@app.get("/jobs")
def list_jobs(queue_id: Optional[str] = None, status_: Optional[str] = Query(None, alias="status"),
              page: int = 1, page_size: int = 25,
              db: Session = Depends(get_db)):
    q = db.query(Job)
    if queue_id:
        q = q.filter(Job.queue_id == queue_id)
    if status_:
        q = q.filter(Job.status == status_)
    total = q.count()
    items = q.order_by(Job.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {"total": total, "page": page, "page_size": page_size, "items": items}


@app.get("/jobs/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    executions = db.query(JobExecution).filter(JobExecution.job_id == job_id).all()
    logs = db.query(JobLog).filter(JobLog.job_id == job_id).order_by(JobLog.logged_at).all()
    return {"job": job, "executions": executions, "logs": logs}


@app.post("/jobs/{job_id}/retry")
def retry_job(job_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    if job.status not in ("failed", "dead_letter"):
        raise HTTPException(400, "Only failed or dead-lettered jobs can be retried")
    job.status = "queued"
    job.claimed_by = None
    job.run_at = datetime.utcnow()
    db.commit()
    return {"id": job.id, "status": job.status}


# ---------- Worker-facing endpoints ----------

@app.post("/workers/register", status_code=201)
def register_worker(hostname: str, db: Session = Depends(get_db)):
    worker = Worker(hostname=hostname)
    db.add(worker)
    db.commit()
    db.refresh(worker)
    return worker


@app.post("/workers/{worker_id}/heartbeat")
def heartbeat(worker_id: str, active_jobs: int = 0, db: Session = Depends(get_db)):
    worker = db.query(Worker).filter(Worker.id == worker_id).first()
    if not worker:
        raise HTTPException(404, "Worker not found")
    worker.last_heartbeat = datetime.utcnow()
    db.add(WorkerHeartbeat(worker_id=worker_id, active_jobs=active_jobs))
    db.commit()
    return {"ok": True}


@app.get("/workers")
def list_workers(db: Session = Depends(get_db)):
    return db.query(Worker).all()


@app.post("/jobs/claim")
def claim_jobs(body: ClaimRequest, db: Session = Depends(get_db)):
    """
    Atomically claims up to `limit` eligible jobs for a worker using
    SELECT ... FOR UPDATE SKIP LOCKED, so concurrent workers polling the
    same queue never claim the same job twice and never block on each
    other's row locks.
    """
    filters = "status IN ('queued','scheduled') AND run_at <= now()"
    params = {"limit": body.limit, "worker_id": body.worker_id}
    if body.queue_id:
        filters += " AND queue_id = :queue_id"
        params["queue_id"] = body.queue_id

    sql = text(f"""
        WITH candidate AS (
            SELECT id FROM jobs
            WHERE {filters}
            ORDER BY priority DESC, run_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
        )
        UPDATE jobs
        SET status = 'claimed', claimed_by = :worker_id, claimed_at = now(),
            attempt_count = attempt_count + 1
        WHERE id IN (SELECT id FROM candidate)
        RETURNING id, queue_id, job_type, payload, attempt_count, max_attempts;
    """)
    rows = db.execute(sql, params).fetchall()
    db.commit()
    logger.info(f"Worker {body.worker_id} claimed {len(rows)} job(s)")
    return [dict(r._mapping) for r in rows]


@app.post("/jobs/{job_id}/start")
def start_job(job_id: str, worker_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    job.status = "running"
    job.started_at = datetime.utcnow()
    execution = JobExecution(job_id=job_id, worker_id=worker_id,
                              attempt_number=job.attempt_count, status="running")
    db.add(execution)
    db.commit()
    db.refresh(execution)
    return {"execution_id": execution.id}


@app.post("/jobs/{job_id}/complete")
def complete_job(job_id: str, body: JobResultUpdate, execution_id: str,
                  db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    execution = db.query(JobExecution).filter(JobExecution.id == execution_id).first()
    if not job or not execution:
        raise HTTPException(404, "Job or execution not found")

    now = datetime.utcnow()
    execution.finished_at = now
    execution.status = body.status
    execution.result = body.result
    execution.error_message = body.error_message
    if execution.started_at:
        execution.duration_ms = int((now - execution.started_at).total_seconds() * 1000)

    if body.status == "completed":
        job.status = "completed"
        job.completed_at = now
    else:
        # failed: retry, or move to dead letter queue if attempts exhausted
        if job.attempt_count >= job.max_attempts:
            job.status = "dead_letter"
            db.add(DeadLetterEntry(job_id=job.id, reason="max_attempts_exceeded",
                                    last_error=body.error_message, attempt_count=job.attempt_count))
        else:
            job.status = "queued"
            # backoff is computed by the worker before re-submitting run_at;
            # kept simple here as immediate re-queue placeholder
    db.commit()
    return {"job_id": job.id, "status": job.status}


@app.post("/admin/reclaim-stale-jobs")
def reclaim_stale_jobs(stale_after_minutes: int = 2, db: Session = Depends(get_db)):
    """
    Reliability safeguard: if a worker crashes or loses connectivity while
    holding a claimed job, that job would otherwise sit stuck forever. This
    sweep finds jobs claimed by workers whose heartbeat has gone silent for
    longer than `stale_after_minutes` and returns them to the queue so a
    healthy worker can pick them up. Intended to run on a schedule (cron /
    external scheduler) or be called periodically by an ops process.
    """
    sql = text("""
        UPDATE jobs
        SET status = 'queued', claimed_by = NULL, claimed_at = NULL
        WHERE status = 'claimed'
          AND claimed_by IN (
              SELECT id FROM workers
              WHERE last_heartbeat < now() - (:minutes || ' minutes')::interval
          )
        RETURNING id, queue_id, job_type;
    """)
    rows = db.execute(sql, {"minutes": stale_after_minutes}).fetchall()
    db.commit()
    if rows:
        logger.warning(f"Reclaimed {len(rows)} stale job(s) from unresponsive workers")
    return {"reclaimed_count": len(rows), "job_ids": [str(r.id) for r in rows]}


@app.get("/dashboard/summary")
def dashboard_summary(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")).fetchall()
    worker_count = db.query(Worker).filter(Worker.status == "online").count()
    return {
        "job_status_breakdown": {r.status: r.count for r in rows},
        "online_workers": worker_count,
    }


@app.get("/health")
def health():
    return {"status": "ok"}