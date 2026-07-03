import os
import uuid
from datetime import datetime

from sqlalchemy import (Column, String, Integer, Boolean, DateTime, ForeignKey,
                         Text, Numeric, CheckConstraint, UniqueConstraint)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import create_engine

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/job_scheduler"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def gen_uuid():
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=False)
    role = Column(String(50), default="member")
    created_at = Column(DateTime, default=datetime.utcnow)


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String(255), nullable=False)
    owner_id = Column(UUID(as_uuid=False), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Project(Base):
    __tablename__ = "projects"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    organization_id = Column(UUID(as_uuid=False), ForeignKey("organizations.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    queues = relationship("Queue", back_populates="project")


class RetryPolicy(Base):
    __tablename__ = "retry_policies"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String(100), nullable=False)
    strategy = Column(String(20), nullable=False)  # fixed | linear | exponential
    base_delay_ms = Column(Integer, default=1000)
    max_delay_ms = Column(Integer, default=60000)
    max_attempts = Column(Integer, default=3)


class Queue(Base):
    __tablename__ = "queues"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    project_id = Column(UUID(as_uuid=False), ForeignKey("projects.id"), nullable=False)
    name = Column(String(255), nullable=False)
    priority = Column(Integer, default=0)
    concurrency_limit = Column(Integer, default=5)
    retry_policy_id = Column(UUID(as_uuid=False), ForeignKey("retry_policies.id"), nullable=True)
    is_paused = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    project = relationship("Project", back_populates="queues")
    jobs = relationship("Job", back_populates="queue")
    retry_policy = relationship("RetryPolicy")


class Job(Base):
    __tablename__ = "jobs"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    queue_id = Column(UUID(as_uuid=False), ForeignKey("queues.id"), nullable=False)
    job_type = Column(String(100), nullable=False)
    payload = Column(JSONB, default=dict)
    status = Column(String(20), default="queued")
    priority = Column(Integer, default=0)
    run_at = Column(DateTime, default=datetime.utcnow)
    idempotency_key = Column(String(255), nullable=True)
    batch_id = Column(UUID(as_uuid=False), nullable=True)
    cron_expression = Column(String(100), nullable=True)
    parent_job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id"), nullable=True)
    max_attempts = Column(Integer, default=3)
    attempt_count = Column(Integer, default=0)
    claimed_by = Column(UUID(as_uuid=False), ForeignKey("workers.id"), nullable=True)
    claimed_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    queue = relationship("Queue", back_populates="jobs")


class Worker(Base):
    __tablename__ = "workers"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    hostname = Column(String(255), nullable=False)
    status = Column(String(20), default="online")
    last_heartbeat = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, default=datetime.utcnow)


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    worker_id = Column(UUID(as_uuid=False), ForeignKey("workers.id"), nullable=False)
    active_jobs = Column(Integer, default=0)
    cpu_percent = Column(Numeric(5, 2), nullable=True)
    memory_mb = Column(Integer, nullable=True)
    recorded_at = Column(DateTime, default=datetime.utcnow)


class JobExecution(Base):
    __tablename__ = "job_executions"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id"), nullable=False)
    worker_id = Column(UUID(as_uuid=False), ForeignKey("workers.id"), nullable=True)
    attempt_number = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    result = Column(JSONB, nullable=True)


class JobLog(Base):
    __tablename__ = "job_logs"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id"), nullable=False)
    execution_id = Column(UUID(as_uuid=False), ForeignKey("job_executions.id"), nullable=True)
    level = Column(String(10), default="info")
    message = Column(Text, nullable=False)
    logged_at = Column(DateTime, default=datetime.utcnow)


class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    queue_id = Column(UUID(as_uuid=False), ForeignKey("queues.id"), nullable=False)
    job_type = Column(String(100), nullable=False)
    payload_template = Column(JSONB, default=dict)
    cron_expression = Column(String(100), nullable=False)
    next_run_at = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)


class DeadLetterEntry(Base):
    __tablename__ = "dead_letter_queue"
    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id"), nullable=False)
    reason = Column(Text, nullable=False)
    last_error = Column(Text, nullable=True)
    attempt_count = Column(Integer, nullable=False)
    moved_at = Column(DateTime, default=datetime.utcnow)
    resolved = Column(Boolean, default=False)
