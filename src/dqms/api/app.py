"""HTTP surface. Programmatic use is the product; the UI is a test harness.

    POST /v1/plan                  dry run: which datasets would a job touch? (reads no data)
    POST /v1/jobs                  submit sources (datasets or whole containers); returns 202 + job
    GET  /v1/jobs/{id}?wait=30     poll (optionally long-poll) until a terminal status
    POST /v1/sources/analyze       one dataset, synchronous
    POST /v1/sources/discover      list the datasets inside a container
    POST /v1/analyze[/json]        analyze uploaded metadata documents
    POST /v1/diff                  compare two rule sets (drift)
    POST /v1/export/sql            rules -> SQL violation counts
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse

from dqms import __version__
from dqms.api.schemas import (
    MAX_FILES,
    MAX_UPLOAD_BYTES,
    AnalyzeOptions,
    BrowseRequest,
    DiffRequest,
    DiffResponse,
    DiscoverRequest,
    JobRequest,
    JsonAnalyzeRequest,
    PlanRequest,
    SourceAnalyzeRequest,
    SqlExportRequest,
)
from dqms.auth import AuthError, Authenticator, Principal
from dqms.connectors.base import BrowseResult, ConnectorError, ConnectorRegistry, DatasetRef
from dqms.connectors.bigquery import BigQueryConnector
from dqms.connectors.databricks import DatabricksConnector
from dqms.connectors.objectstore import ObjectStoreConnector
from dqms.connectors.snowflake import SnowflakeConnector
from dqms.connectors.uri import parse_source
from dqms.emitters.sql import SqlCheck, emit_sql_checks
from dqms.jobs import InMemoryJobStore, Job, JobRunner, JobStore, SqliteJobStore
from dqms.llm.client import LLMClient
from dqms.llm.config import llm_from_env
from dqms.llm.gateway import LLMMode
from dqms.logging_config import configure_logging
from dqms.normalize.documents import SourceDoc
from dqms.normalize.files import MAX_FILE_BYTES, UnsupportedFile, read_upload
from dqms.rules.diff import diff_rulesets
from dqms.scope import ScopePlan, plan_scope, server_cap
from dqms.service import Analysis, DocumentReport, NoMetadataError, RuleService

log = logging.getLogger("dqms.api")
STATIC = Path(__file__).parent / "static"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def default_connectors() -> ConnectorRegistry:
    registry = ConnectorRegistry()
    for connector in (BigQueryConnector(), SnowflakeConnector(), DatabricksConnector(), ObjectStoreConnector()):
        registry.register(connector)
    return registry


def create_app(
    llm: LLMClient | None = None,
    llm_mode: LLMMode | None = None,
    *,
    auth: Authenticator | None = None,
    connectors: ConnectorRegistry | None = None,
    job_store: JobStore | None = None,
) -> FastAPI:
    if llm is None and llm_mode is None:
        # Misconfiguration raises: the operator must know what leaves the service, so a bad
        # DQMS_LLM_MODE stops startup rather than being guessed at.
        llm, llm_mode = llm_from_env()
    llm_mode = llm_mode or LLMMode.OFF
    auth = auth or Authenticator.from_env()
    connectors = connectors or default_connectors()
    if job_store is None:
        db = os.getenv("DQMS_JOB_DB")
        job_store = SqliteJobStore(db) if db else InMemoryJobStore()

    service = RuleService(llm, connectors, llm_mode)
    log.info("llm_policy", extra={"mode": service.llm_mode.value})
    runner = JobRunner(service, job_store, max_workers=int(os.getenv("DQMS_JOB_WORKERS", "4")), max_datasets=server_cap())

    app = FastAPI(title="dqms", version=__version__,
                  description="Generate validation rules from metadata and cloud datasets.")
    app.router.on_shutdown.append(runner.shutdown)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        incoming = request.headers.get("x-request-id", "")
        request_id = incoming if _REQUEST_ID.match(incoming) else uuid.uuid4().hex
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        if request.url.path not in {"/healthz", "/readyz"}:
            log.info("request", extra={"request_id": request_id, "method": request.method, "path": request.url.path,
                                       "status": response.status_code,
                                       "ms": round((time.perf_counter() - started) * 1000)})
        return response

    def principal(authorization: str | None = Header(None)) -> Principal:
        try:
            return auth.authenticate(authorization)
        except AuthError as exc:
            raise HTTPException(401, str(exc), headers={"WWW-Authenticate": "Bearer"}) from exc

    def require_sources(who: Principal, sources: list[str]) -> None:
        denied = []
        for source in sources:
            try:
                parse_source(source)
            except ConnectorError as exc:
                raise HTTPException(400, str(exc)) from exc
            if not who.can_read(source):
                denied.append(source)
        if denied:
            log.warning("source_denied", extra={"principal": who.name, "sources": denied})
            raise HTTPException(403, f"this API key may not read: {', '.join(denied)}")

    # ------------------------------ infra ------------------------------ #

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        """Liveness + capabilities. No auth, no dependencies touched."""
        return {"status": "ok", "version": __version__, "llm_mode": service.llm_mode.value, "schemes": connectors.schemes,
                "max_datasets": server_cap()}

    @app.get("/readyz")
    def ready() -> dict[str, str]:
        """Readiness: the job store must be usable before we accept traffic."""
        try:
            job_store.get("readiness-probe")
        except Exception as exc:
            raise HTTPException(503, "job store unavailable") from exc
        return {"status": "ready"}

    # ------------------------------ files ------------------------------ #

    @app.post("/v1/analyze", response_model=Analysis)
    def analyze_upload(
        files: list[UploadFile] = File(...),
        dataset_name: str | None = Form(None),
        min_confidence: float = Form(0.0),
        use_llm: bool = Form(True),
        who: Principal = Depends(principal),
    ) -> Analysis:
        if len(files) > MAX_FILES:
            raise HTTPException(413, f"at most {MAX_FILES} files per request")
        docs: list[SourceDoc] = []
        rejected: list[DocumentReport] = []
        budget = MAX_UPLOAD_BYTES
        for upload in files:
            name = upload.filename or "upload"
            data = upload.file.read(min(MAX_FILE_BYTES, budget) + 1)
            budget -= len(data)
            if budget < 0:
                raise HTTPException(413, f"upload exceeds {MAX_UPLOAD_BYTES // (1024 * 1024)} MB in total")
            try:
                docs += read_upload(name, data)
            except UnsupportedFile as exc:
                rejected.append(DocumentReport(name=name, route="rejected", detail=str(exc)))
        options = AnalyzeOptions(min_confidence=min_confidence, use_llm=use_llm).to_engine()
        return _analyze(service, docs, dataset_name, options, rejected)

    @app.post("/v1/analyze/json", response_model=Analysis)
    def analyze_json(req: JsonAnalyzeRequest, who: Principal = Depends(principal)) -> Analysis:
        docs = [SourceDoc(f"document-{i}", d) for i, d in enumerate(req.documents, start=1)]
        return _analyze(service, docs, req.dataset_name, req.options.to_engine(), [])

    # ----------------------------- sources ----------------------------- #

    @app.post("/v1/sources/analyze", response_model=Analysis)
    def analyze_source(req: SourceAnalyzeRequest, who: Principal = Depends(principal)) -> Analysis:
        require_sources(who, [req.source])
        log.info("source_access", extra={"principal": who.name, "source": req.source, "profile": req.profile.enabled})
        try:
            return service.analyze_source(req.source, req.profile, req.options.to_engine())
        except ConnectorError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/v1/sources/discover", response_model=list[DatasetRef])
    def discover(req: DiscoverRequest, who: Principal = Depends(principal)) -> list[DatasetRef]:
        require_sources(who, [req.source])
        try:
            return [r for r in service.discover(req.source, req.limit) if who.can_read(r.uri)]
        except ConnectorError as exc:
            raise HTTPException(400, str(exc)) from exc

    # ----------------------------- explorer ---------------------------- #

    @app.get("/v1/explorer/roots")
    def explorer_roots(who: Principal = Depends(principal)) -> dict[str, Any]:
        """Where to start browsing, derived from what this key may read. No cloud call is made."""
        return {"roots": who.roots(), "max_datasets": server_cap()}

    @app.post("/v1/explorer/browse", response_model=BrowseResult)
    def browse(req: BrowseRequest, who: Principal = Depends(principal)) -> BrowseResult:
        """One level of the hierarchy (lazy). Lists names and sizes only; no table or file is read.
        Entries this key may not read are omitted, so browsing never reveals what exists out of scope."""
        try:
            parse_source(req.uri)
        except ConnectorError as exc:
            raise HTTPException(400, str(exc)) from exc
        if not who.can_browse(req.uri):
            log.warning("browse_denied", extra={"principal": who.name, "uri": req.uri})
            raise HTTPException(403, f"this API key may not browse {req.uri}")
        log.info("browse", extra={"principal": who.name, "uri": req.uri})
        try:
            result = service.browse(req.uri, req.limit)
        except ConnectorError as exc:
            raise HTTPException(400, str(exc)) from exc
        visible = [n for n in result.nodes if (who.can_browse(n.uri) if n.kind == "container" else who.can_read(n.uri))]
        return BrowseResult(nodes=visible, truncated=result.truncated)

    # ------------------------------- jobs ------------------------------ #

    @app.post("/v1/plan", response_model=ScopePlan)
    def plan(req: PlanRequest, who: Principal = Depends(principal)) -> ScopePlan:
        """Dry run: exactly which datasets a job would touch (with sizes where listings give them),
        without reading or profiling any data. `within_limit: false` means a job would be refused."""
        require_sources(who, req.sources)
        return plan_scope(service, req.sources, req.scope, who.can_read, server_cap())

    @app.post("/v1/jobs", response_model=Job, status_code=202)
    def submit_job(
        req: JobRequest,
        response: Response,
        who: Principal = Depends(principal),
        idempotency_key: str | None = Header(None),
    ) -> Job:
        require_sources(who, req.sources)
        log.info("job_submitted", extra={"principal": who.name, "sources": req.sources, "profile": req.profile.enabled})
        job, created = runner.submit(who.name, req.sources, req.profile, req.options.to_engine(), who.can_read,
                                     idempotency_key, req.scope)
        response.headers["Location"] = f"/v1/jobs/{job.id}"
        if not created:
            response.status_code = 200  # idempotent replay of an earlier submission
        return job

    @app.get("/v1/jobs/{job_id}", response_model=Job)
    def get_job(job_id: str, wait: int = Query(0, ge=0, le=60), who: Principal = Depends(principal)) -> Job:
        """`wait` long-polls up to that many seconds for a terminal status."""
        deadline = time.monotonic() + wait
        while True:
            job = job_store.get(job_id)
            if job is None or job.owner != who.name:
                raise HTTPException(404, "job not found")  # 404, not 403: don't reveal other callers' jobs
            if job.status.terminal or time.monotonic() >= deadline:
                return job
            time.sleep(0.25)

    # ------------------------------ derived ---------------------------- #

    @app.post("/v1/diff", response_model=DiffResponse)
    def diff(req: DiffRequest, who: Principal = Depends(principal)) -> DiffResponse:
        d = diff_rulesets(req.old, req.new)
        return DiffResponse(
            changed=not d.is_empty, breaking=d.breaking,
            added=[c.model_dump() for c in d.added], removed=[c.model_dump() for c in d.removed],
            modified=[c.model_dump() for c in d.changed],
            old_fingerprint=req.old.fingerprint, new_fingerprint=req.new.fingerprint,
        )

    @app.post("/v1/export/sql", response_model=list[SqlCheck])
    def export_sql(req: SqlExportRequest, who: Principal = Depends(principal)) -> list[SqlCheck]:
        return emit_sql_checks(req.ruleset, req.metadata, req.dialect, req.table)

    return app


def _analyze(service: RuleService, docs, dataset_name, options, rejected) -> Analysis:
    if not docs:
        raise HTTPException(422, "no readable files: " + "; ".join(r.detail for r in rejected))
    try:
        analysis = service.analyze_documents(docs, dataset_name, options)
    except NoMetadataError as exc:
        raise HTTPException(422, str(exc)) from exc
    analysis.documents += rejected
    return analysis


def app_factory() -> FastAPI:
    """uvicorn --factory dqms.api.app:app_factory"""
    configure_logging(os.getenv("DQMS_LOG_LEVEL", "INFO"))
    return create_app()
