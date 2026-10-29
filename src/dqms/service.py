"""Application service: the single entry point used by the API (and any other front end)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from dqms.connectors.base import BrowseResult, ConnectorError, ConnectorRegistry, DatasetRef, ProfileOptions
from dqms.llm.client import LLMClient
from dqms.llm.gateway import LLMMode, Purpose, build_gateway
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
    source: str | None = None  # URI the metadata was read from, when it came from a connector
    documents: list[DocumentReport] = Field(default_factory=list)
    ruleset: RuleSet


class NoMetadataError(ValueError):
    pass


class RuleService:
    def __init__(
        self,
        llm: LLMClient | None = None,
        connectors: ConnectorRegistry | None = None,
        llm_mode: LLMMode = LLMMode.OFF,
    ):
        # The only place an LLM client is wrapped. In `off` (the default) no gateway exists, so
        # nothing downstream holds anything it could send data to.
        gateway = build_gateway(llm, llm_mode)
        self._gateway = gateway
        self._engine = RuleEngine(gateway)
        self._normalizer = LLMNormalizer(gateway) if gateway and gateway.allows(Purpose.DOCUMENT) else None
        self.connectors = connectors or ConnectorRegistry()

    @property
    def llm_mode(self) -> LLMMode:
        return self._gateway.mode if self._gateway else LLMMode.OFF

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
                + ("" if self._normalizer else "Unrecognized formats can only be parsed when the server runs with DQMS_LLM_MODE=full. ")
                + "Details: "
                + "; ".join(f"{r.name}: {r.detail}" for r in result.reports)
            )
        metadata.name = metadata.name or "unnamed_dataset"
        return self._finish(metadata, result.profile, options, result.findings, result.reports)

    def analyze_source(
        self, source: str, profile: ProfileOptions | None = None, options: EngineOptions | None = None
    ) -> Analysis:
        """Read one dataset from a cloud source and derive rules. Containers must be expanded first."""
        connector, uri = self.connectors.resolve(source)
        if connector.is_container(uri):
            raise ConnectorError(f"'{source}' names a group of datasets; use discovery or a job to expand it")
        snapshot = connector.fetch(uri, profile or ProfileOptions())
        analysis = self._finish(snapshot.metadata, snapshot.profile, options, [], [])
        analysis.source = source
        return analysis

    def discover(self, source: str, limit: int = 500) -> list[DatasetRef]:
        connector, uri = self.connectors.resolve(source)
        if not connector.is_container(uri):
            return [DatasetRef(uri=source, name=uri.parts[-1] if uri.parts else uri.authority)]
        return connector.discover(uri, limit)

    def browse(self, source: str, limit: int = 500) -> BrowseResult:
        connector, uri = self.connectors.resolve(source)
        return connector.browse(uri, limit)

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
