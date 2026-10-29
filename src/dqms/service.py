"""Application service: the single entry point used by the API (and any other front end)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from dqms.connectors.base import ConnectorRegistry
from dqms.llm.client import LLMClient
from dqms.models import Metadata, Profile, RuleSet
from dqms.normalize.documents import Document, SourceDoc, ingest
from dqms.normalize.universal import LLMNormalizer
from dqms.rules.engine import EngineOptions, RuleEngine


class DocumentReport(BaseModel):
    name: str
    route: str
    detail: str = ""


class Analysis(BaseModel):
    """Everything the UI/API needs: what we understood, and the rules derived from it."""

    metadata: Metadata
    profile: Profile | None = None
    documents: list[DocumentReport] = Field(default_factory=list)
    ruleset: RuleSet


class NoMetadataError(ValueError):
    pass


class RuleService:
    def __init__(self, llm: LLMClient | None = None, connectors: ConnectorRegistry | None = None):
        self._llm = llm
        self._engine = RuleEngine(llm)
        self._normalizer = LLMNormalizer(llm) if llm is not None else None
        self.connectors = connectors or ConnectorRegistry()

    @property
    def llm_available(self) -> bool:
        return self._llm is not None

    def analyze_documents(
        self,
        documents: list[Document | SourceDoc],
        dataset_name: str | None = None,
        options: EngineOptions | None = None,
    ) -> Analysis:
        result = ingest(documents, fallback=self._normalizer)
        metadata = result.metadata
        if dataset_name:
            metadata.name = dataset_name
        if not metadata.columns:
            raise NoMetadataError(
                "No columns could be extracted from the input. "
                + ("" if self._llm else "Unrecognized formats need an LLM configured (DQMS_LLM_BASE_URL). ")
                + "Details: "
                + "; ".join(f"{r.name}: {r.detail}" for r in result.reports)
            )
        metadata.name = metadata.name or "unnamed_dataset"
        return self._finish(metadata, result.profile, options, result.findings, result.reports)

    def analyze_source(
        self, kind: str, ref: dict[str, Any], include_profile: bool = False, options: EngineOptions | None = None
    ) -> Analysis:
        snapshot = self.connectors.get(kind).fetch(ref, include_profile)
        return self._finish(snapshot.metadata, snapshot.profile, options, [], [])

    def analyze_models(
        self, metadata: Metadata, profile: Profile | None = None, options: EngineOptions | None = None
    ) -> Analysis:
        return self._finish(metadata, profile, options, [], [])

    def _finish(self, metadata, profile, options, findings, reports) -> Analysis:
        ruleset = self._engine.generate(metadata, profile, options, findings)
        ruleset.unrecognized_documents = sum(1 for r in reports if r.route == "unrecognized")
        return Analysis(
            metadata=metadata,
            profile=profile,
            documents=[DocumentReport(name=r.name, route=r.route, detail=r.detail) for r in reports],
            ruleset=ruleset,
        )
