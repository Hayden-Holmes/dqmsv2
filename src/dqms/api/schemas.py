from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from dqms.connectors.base import ProfileOptions
from dqms.emitters.sql import Dialect
from dqms.models import Metadata, RuleSet
from dqms.rules.engine import EngineOptions
from dqms.scope import ScopeRequest

MAX_FILES = 25
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # across all files in one request
MAX_SOURCES_PER_JOB = 50


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
    source: str = Field(examples=["bigquery://my-project/my_dataset/orders"])
    profile: ProfileOptions = ProfileOptions()
    options: AnalyzeOptions = AnalyzeOptions()


class DiscoverRequest(BaseModel):
    source: str = Field(examples=["gs://my-bucket/exports/"])
    limit: int = Field(500, ge=1, le=500)


class BrowseRequest(BaseModel):
    uri: str = Field(examples=["bigquery://my-project", "gs://my-bucket/exports/"])
    limit: int = Field(500, ge=1, le=1000)


class JobRequest(BaseModel):
    sources: list[str] = Field(min_length=1, max_length=MAX_SOURCES_PER_JOB)
    scope: ScopeRequest = ScopeRequest()
    profile: ProfileOptions = ProfileOptions()
    options: AnalyzeOptions = AnalyzeOptions()


class PlanRequest(BaseModel):
    sources: list[str] = Field(min_length=1, max_length=MAX_SOURCES_PER_JOB)
    scope: ScopeRequest = ScopeRequest()


class SqlExportRequest(BaseModel):
    metadata: Metadata
    ruleset: RuleSet
    dialect: Dialect = "bigquery"
    table: str | None = None


class DiffRequest(BaseModel):
    old: RuleSet
    new: RuleSet


class DiffResponse(BaseModel):
    changed: bool
    breaking: bool
    added: list[dict[str, Any]]
    removed: list[dict[str, Any]]
    modified: list[dict[str, Any]]
    old_fingerprint: str
    new_fingerprint: str
