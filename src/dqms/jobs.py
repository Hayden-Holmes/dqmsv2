"""Async analysis jobs, so a pipeline can submit work and poll instead of holding a connection.

Jobs expand container sources (a dataset, a bucket folder) into datasets, analyze
each, and record a per-dataset result so one bad table does not lose the rest.
The store is pluggable: in-memory for tests, SQLite for a single instance. Multiple
replicas need a shared store (Postgres/Firestore/etc.) implementing `JobStore`.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Protocol

from pydantic import BaseModel, Field

from dqms.connectors.base import ConnectorError, ProfileOptions
from dqms.rules.engine import EngineOptions
from dqms.scope import ScopeRequest, plan_scope, server_cap
from dqms.service import Analysis, RuleService

log = logging.getLogger("dqms.jobs")


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"  # some datasets failed
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in {JobStatus.SUCCEEDED, JobStatus.PARTIAL, JobStatus.FAILED}


class SourceResult(BaseModel):
    source: str
    status: Literal["ok", "error"]
    error: str | None = None
    analysis: Analysis | None = None


class Job(BaseModel):
    id: str
    status: JobStatus = JobStatus.QUEUED
    owner: str
    created_at: str
    updated_at: str
    sources: list[str]
    total: int = 0  # datasets after expansion; 0 until expansion finishes
    excluded: int = 0  # datasets dropped by include/exclude patterns
    limit: int = 0  # the dataset cap this job ran under
    done: int = 0
    results: list[SourceResult] = Field(default_factory=list)
    error: str | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class JobStore(Protocol):
    def create(self, job: Job, idempotency_key: str | None) -> tuple[Job, bool]:
        """Insert, or return the existing job for (owner, idempotency_key). Second value: created."""
        ...

    def save(self, job: Job) -> None: ...

    def get(self, job_id: str) -> Job | None: ...

    def fail_incomplete(self) -> int:
        """On startup: jobs a previous process left queued/running can never finish."""
        ...


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._keys: dict[tuple[str, str], str] = {}
        self._lock = threading.Lock()

    def create(self, job: Job, idempotency_key: str | None) -> tuple[Job, bool]:
        with self._lock:
            if idempotency_key and (job.owner, idempotency_key) in self._keys:
                return self._jobs[self._keys[(job.owner, idempotency_key)]].model_copy(deep=True), False
            self._jobs[job.id] = job.model_copy(deep=True)
            if idempotency_key:
                self._keys[(job.owner, idempotency_key)] = job.id
            return job, True

    def save(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.id] = job.model_copy(deep=True)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    def fail_incomplete(self) -> int:
        return 0  # nothing survives a restart


class SqliteJobStore:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, owner TEXT NOT NULL, idem TEXT, "
                "payload TEXT NOT NULL, UNIQUE (owner, idem))"
            )
            self._db.commit()

    def create(self, job: Job, idempotency_key: str | None) -> tuple[Job, bool]:
        with self._lock:
            if idempotency_key:
                row = self._db.execute(
                    "SELECT payload FROM jobs WHERE owner = ? AND idem = ?", (job.owner, idempotency_key)
                ).fetchone()
                if row:
                    return Job.model_validate_json(row[0]), False
            self._db.execute(
                "INSERT INTO jobs (id, owner, idem, payload) VALUES (?, ?, ?, ?)",
                (job.id, job.owner, idempotency_key, job.model_dump_json()),
            )
            self._db.commit()
            return job, True

    def save(self, job: Job) -> None:
        with self._lock:
            self._db.execute("UPDATE jobs SET payload = ? WHERE id = ?", (job.model_dump_json(), job.id))
            self._db.commit()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._db.execute("SELECT payload FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Job.model_validate_json(row[0]) if row else None

    def fail_incomplete(self) -> int:
        with self._lock:
            rows = self._db.execute("SELECT id, payload FROM jobs").fetchall()
            count = 0
            for job_id, payload in rows:
                job = Job.model_validate_json(payload)
                if not job.status.terminal:
                    job.status, job.error, job.updated_at = JobStatus.FAILED, "service restarted before the job finished", _now()
                    self._db.execute("UPDATE jobs SET payload = ? WHERE id = ?", (job.model_dump_json(), job_id))
                    count += 1
            self._db.commit()
        return count


class JobRunner:
    def __init__(self, service: RuleService, store: JobStore, max_workers: int = 4, max_datasets: int | None = None):
        self._service, self._store = service, store
        self._cap = max_datasets or server_cap()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dqms-job")
        store.fail_incomplete()

    def submit(
        self,
        owner: str,
        sources: list[str],
        profile: ProfileOptions,
        options: EngineOptions,
        allow: Callable[[str], bool],
        idempotency_key: str | None = None,
        scope: ScopeRequest | None = None,
    ) -> tuple[Job, bool]:
        job = Job(id=uuid.uuid4().hex, owner=owner, created_at=_now(), updated_at=_now(), sources=sources)
        job, created = self._store.create(job, idempotency_key)
        if created:
            self._pool.submit(self._run, job.id, profile, options, allow, scope or ScopeRequest())
        return job, created

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ #

    def _run(
        self, job_id: str, profile: ProfileOptions, options: EngineOptions, allow: Callable[[str], bool],
        scope: ScopeRequest,
    ) -> None:
        job = self._store.get(job_id)
        if job is None:
            return
        try:
            job.status = JobStatus.RUNNING
            self._touch(job)
            # Decide the full set of datasets up front. Nothing is read or profiled until the scope
            # is known to be within the cap, and the cap is an error, never a silent truncation.
            plan = plan_scope(self._service, job.sources, scope, allow, self._cap)
            job.limit, job.excluded = plan.limit, plan.excluded
            job.results.extend(SourceResult(source=p.source, status="error", error=p.error) for p in plan.problems)
            if not plan.within_limit:
                job.status, job.error = JobStatus.FAILED, plan.message
                self._touch(job)
                return
            datasets = [d.uri for d in plan.datasets]
            if not datasets and not plan.problems:
                hint = f" ({plan.excluded} excluded by include/exclude patterns)" if plan.excluded else ""
                job.status, job.error = JobStatus.FAILED, f"the scope matched no datasets{hint}; nothing was read"
                self._touch(job)
                return
            job.total = len(datasets) + len(job.results)
            job.done = len(job.results)
            self._touch(job)

            for uri in datasets:
                job.results.append(self._analyze_one(uri, profile, options))
                job.done += 1
                self._touch(job)

            ok = sum(1 for r in job.results if r.status == "ok")
            job.status = (
                JobStatus.SUCCEEDED if ok == len(job.results) and job.results
                else JobStatus.PARTIAL if ok else JobStatus.FAILED
            )
        except Exception:  # the job must always reach a terminal state
            log.exception("job %s crashed", job_id)
            job.status, job.error = JobStatus.FAILED, "internal error (see service logs)"
        self._touch(job)

    def _analyze_one(self, uri: str, profile: ProfileOptions, options: EngineOptions) -> SourceResult:
        try:
            return SourceResult(source=uri, status="ok", analysis=self._service.analyze_source(uri, profile, options))
        except ConnectorError as exc:
            return SourceResult(source=uri, status="error", error=str(exc))
        except Exception:
            log.exception("analysis of %s failed", uri)
            return SourceResult(source=uri, status="error", error="internal error (see service logs)")

    def _touch(self, job: Job) -> None:
        job.updated_at = _now()
        self._store.save(job)
