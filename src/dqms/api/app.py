"""HTTP surface: drag-and-drop UI, file upload, JSON analysis, live-source analysis, SQL export."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from dqms import __version__
from dqms.connectors.base import ConnectorError, ConnectorRegistry
from dqms.connectors.bigquery import BigQueryConnector
from dqms.emitters.sql import Dialect, SqlCheck, emit_sql_checks
from dqms.llm.client import LLMClient, client_from_env
from dqms.models import Metadata, RuleSet
from dqms.normalize.documents import SourceDoc
from dqms.normalize.files import MAX_FILE_BYTES, UnsupportedFile, read_upload
from dqms.rules.engine import EngineOptions
from dqms.service import Analysis, DocumentReport, NoMetadataError, RuleService

log = logging.getLogger("dqms")
STATIC = Path(__file__).parent / "static"
MAX_FILES = 25


class AnalyzeOptions(BaseModel):
    min_confidence: float = Field(0.0, ge=0.0, le=1.0)
    use_llm: bool = True
    min_profile_rows: int = Field(100, ge=0)

    def to_engine(self) -> EngineOptions:
        return EngineOptions(
            min_confidence=self.min_confidence, use_llm=self.use_llm, min_profile_rows=self.min_profile_rows
        )


class JsonAnalyzeRequest(BaseModel):
    documents: list[str | dict[str, Any]] = Field(min_length=1, max_length=MAX_FILES)
    dataset_name: str | None = None
    options: AnalyzeOptions = AnalyzeOptions()


class SourceAnalyzeRequest(BaseModel):
    ref: dict[str, Any]
    include_profile: bool = False
    options: AnalyzeOptions = AnalyzeOptions()


class SqlExportRequest(BaseModel):
    metadata: Metadata
    ruleset: RuleSet
    dialect: Dialect = "bigquery"
    table: str | None = None


def create_app(llm: LLMClient | None = None, *, llm_from_env: bool = True) -> FastAPI:
    if llm is None and llm_from_env:
        try:
            llm = client_from_env()
        except Exception:  # missing optional deps / credentials must not stop the service
            log.exception("LLM configuration failed; continuing without an LLM")
    connectors = ConnectorRegistry()
    connectors.register(BigQueryConnector())
    service = RuleService(llm, connectors)

    app = FastAPI(title="dqms", version=__version__, description="Drop in metadata in any format; get validation rules.")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "llm": service.llm_available, "connectors": connectors.kinds}

    @app.post("/v1/analyze", response_model=Analysis)
    def analyze_upload(
        files: list[UploadFile] = File(...),
        dataset_name: str | None = Form(None),
        min_confidence: float = Form(0.0),
        use_llm: bool = Form(True),
    ) -> Analysis:
        """Drag-and-drop entry point: any number of files, any format."""
        if len(files) > MAX_FILES:
            raise HTTPException(413, f"at most {MAX_FILES} files per request")
        docs: list[SourceDoc] = []
        rejected: list[DocumentReport] = []
        for upload in files:
            name = upload.filename or "upload"
            data = upload.file.read(MAX_FILE_BYTES + 1)
            try:
                docs += read_upload(name, data)
            except UnsupportedFile as exc:
                rejected.append(DocumentReport(name=name, route="rejected", detail=str(exc)))
        options = AnalyzeOptions(min_confidence=min_confidence, use_llm=use_llm).to_engine()
        analysis = _analyze(service, docs, dataset_name, options, rejected)
        return analysis

    @app.post("/v1/analyze/json", response_model=Analysis)
    def analyze_json(req: JsonAnalyzeRequest) -> Analysis:
        docs = [SourceDoc(f"document-{i}", d) for i, d in enumerate(req.documents, start=1)]
        return _analyze(service, docs, req.dataset_name, req.options.to_engine(), [])

    @app.post("/v1/sources/{kind}/analyze", response_model=Analysis)
    def analyze_source(kind: str, req: SourceAnalyzeRequest) -> Analysis:
        try:
            return service.analyze_source(kind, req.ref, req.include_profile, req.options.to_engine())
        except ConnectorError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/v1/export/sql", response_model=list[SqlCheck])
    def export_sql(req: SqlExportRequest) -> list[SqlCheck]:
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
    return create_app()
