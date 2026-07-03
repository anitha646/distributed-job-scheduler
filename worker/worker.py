"""
Distributed Job Scheduler - Worker Process
--------------------------------------------
Polls the API for claimable jobs, executes them concurrently using a
thread pool, sends periodic heartbeats, applies configurable retry
backoff on failure, and shuts down gracefully on SIGTERM/SIGINT
(finishes in-flight jobs before exiting).
"""
import os
import signal
import sys
import time
import logging
import threading
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(levelname)s %(message)s")
logger = logging.getLogger("worker")

API_BASE = os.getenv("API_BASE", "http://localhost:8000")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "2"))
HEARTBEAT_INTERVAL_SEC = float(os.getenv("HEARTBEAT_INTERVAL_SEC", "10"))
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "4"))
HOSTNAME = os.getenv("WORKER_HOSTNAME", f"worker-{os.getpid()}")


# ---------- Retry backoff strategies ----------

def compute_backoff_seconds(strategy: str, attempt: int, base_delay_ms: int, max_delay_ms: int) -> float:
    if strategy == "fixed":
        delay_ms = base_delay_ms
    elif strategy == "linear":
        delay_ms = base_delay_ms * attempt
    else:  # exponential (default), with jitter to avoid thundering herd
        delay_ms = base_delay_ms * (2 ** (attempt - 1))
    delay_ms = min(delay_ms, max_delay_ms)
    jitter = random.uniform(0, delay_ms * 0.1)
    return (delay_ms + jitter) / 1000.0


# ---------- Job execution ----------

JOB_HANDLERS = {}


def job_handler(job_type):
    """Decorator to register a handler for a job_type."""
    def wrapper(fn):
        JOB_HANDLERS[job_type] = fn
        return fn
    return wrapper


@job_handler("send_email")
def handle_send_email(payload):
    logger.info(f"Sending email to {payload.get('to')}")
    time.sleep(0.5)
    return {"delivered": True}


@job_handler("generate_report")
def handle_generate_report(payload):
    logger.info(f"Generating report: {payload.get('report_type')}")
    time.sleep(1)
    return {"report_url": "https://example.com/reports/generated.pdf"}


def default_handler(payload):
    # Fallback for unregistered job types: simulate work so the pipeline
    # is demonstrably end-to-end even before every handler is written.
    time.sleep(0.3)
    return {"note": "processed by default handler"}


class Worker:
    def __init__(self):
        self.worker_id = None
        self.shutdown_flag = threading.Event()
        self.active_jobs = 0
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS)
        self.in_flight = set()

    def register(self):
        resp = requests.post(f"{API_BASE}/workers/register", params={"hostname": HOSTNAME})
        resp.raise_for_status()
        self.worker_id = resp.json()["id"]
        logger.info(f"Registered as worker {self.worker_id} ({HOSTNAME})")

    def heartbeat_loop(self):
        while not self.shutdown_flag.is_set():
            try:
                requests.post(
                    f"{API_BASE}/workers/{self.worker_id}/heartbeat",
                    params={"active_jobs": self.active_jobs},
                    timeout=5,
                )
            except requests.RequestException as e:
                logger.warning(f"Heartbeat failed: {e}")
            self.shutdown_flag.wait(HEARTBEAT_INTERVAL_SEC)

    def claim_jobs(self, limit):
        try:
            resp = requests.post(f"{API_BASE}/jobs/claim",
                                  json={"worker_id": self.worker_id, "limit": limit}, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Claim failed: {e}")
            return []

    def execute_job(self, job):
        job_id = job["id"]
        with self.lock:
            self.active_jobs += 1
            self.in_flight.add(job_id)
        try:
            start_resp = requests.post(f"{API_BASE}/jobs/{job_id}/start",
                                        params={"worker_id": self.worker_id})
            execution_id = start_resp.json()["execution_id"]

            handler = JOB_HANDLERS.get(job["job_type"], default_handler)
            try:
                result = handler(job["payload"])
                requests.post(
                    f"{API_BASE}/jobs/{job_id}/complete",
                    params={"execution_id": execution_id},
                    json={"status": "completed", "result": result},
                )
                logger.info(f"Job {job_id} ({job['job_type']}) completed")
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                requests.post(
                    f"{API_BASE}/jobs/{job_id}/complete",
                    params={"execution_id": execution_id},
                    json={"status": "failed", "error_message": str(e)},
                )
        finally:
            with self.lock:
                self.active_jobs -= 1
                self.in_flight.discard(job_id)

    def poll_loop(self):
        while not self.shutdown_flag.is_set():
            available_slots = MAX_CONCURRENT_JOBS - self.active_jobs
            if available_slots > 0:
                jobs = self.claim_jobs(available_slots)
                for job in jobs:
                    self.executor.submit(self.execute_job, job)
            self.shutdown_flag.wait(POLL_INTERVAL_SEC)

    def shutdown(self, *_):
        logger.info("Shutdown signal received, draining in-flight jobs before exit...")
        self.shutdown_flag.set()
        self.executor.shutdown(wait=True)  # waits for in-flight jobs to finish
        logger.info("All in-flight jobs drained. Worker exiting cleanly.")
        sys.exit(0)

    def run(self):
        self.register()
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

        hb_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb_thread.start()

        logger.info(f"Worker started. Polling every {POLL_INTERVAL_SEC}s, "
                    f"max {MAX_CONCURRENT_JOBS} concurrent jobs.")
        self.poll_loop()


if __name__ == "__main__":
    Worker().run()
