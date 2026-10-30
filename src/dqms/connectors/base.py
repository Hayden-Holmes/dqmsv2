"""Connector interface: pull canonical metadata (and an aggregate profile) from a cloud source."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

from pydantic import BaseModel

from dqms.connectors.errors import ConnectorError
from dqms.connectors.uri import SourceURI, parse_source
from dqms.models import Metadata, Profile

__all__ = ["BrowseResult", "ConnectorError", "Connector", "ConnectorRegistry", "DatasetRef", "ExplorerNode", "ProfileOptions", "Snapshot"]


class ProfileOptions(BaseModel):
    """What to compute and how much it may cost. Profiling is always aggregate-only."""

    enabled: bool = False
    include_values: bool = False  # collect complete value lists for low-cardinality string columns
    max_rows: int = 5_000_000  # above this, profile a sample (and mark the profile `sampled`)
    max_bytes: int = 32 * 1024 * 1024  # file sources: bytes read per file
    timeout_s: int = 300


class DatasetRef(BaseModel):
    uri: str
    name: str
    bytes: int | None = None  # size hints from listings, when the source gives them for free
    rows: int | None = None


class ExplorerNode(BaseModel):
    """One entry in the explorer: a container to drill into, or a dataset (table/file) to pick."""

    uri: str
    name: str
    kind: Literal["container", "dataset"]
    bytes: int | None = None
    rows: int | None = None


class BrowseResult(BaseModel):
    nodes: list[ExplorerNode]
    truncated: bool = False  # the listing hit the limit; narrow down to see the rest


@dataclass
class Snapshot:
    metadata: Metadata
    profile: Profile | None = None
    warnings: list[str] = field(default_factory=list)  # reported as findings; never fatal


class Connector(Protocol):
    schemes: tuple[str, ...]

    def is_container(self, uri: SourceURI) -> bool:
        """True if the URI names a group of datasets (dataset/schema/folder) rather than one."""
        ...

    def discover(self, uri: SourceURI, limit: int = 500) -> list[DatasetRef]:
        """Expand a container into the datasets inside it."""
        ...

    def browse(self, uri: SourceURI, limit: int = 500) -> BrowseResult:
        """List the *immediate* children of a node (lazy, one level). Reads no table or file contents."""
        ...

    def fetch(self, uri: SourceURI, profile: ProfileOptions) -> Snapshot:
        """Read one dataset. Raw rows never leave the source; profiles are aggregates."""
        ...


class ConnectorRegistry:
    def __init__(self) -> None:
        self._by_scheme: dict[str, Connector] = {}

    def register(self, connector: Connector) -> None:
        for scheme in connector.schemes:
            self._by_scheme[scheme] = connector

    def resolve(self, source: str) -> tuple[Connector, SourceURI]:
        uri = parse_source(source)
        try:
            return self._by_scheme[uri.scheme], uri
        except KeyError:
            raise ConnectorError(
                f"no connector for '{uri.scheme}://'; available: {', '.join(self.schemes) or 'none'}"
            ) from None

    @property
    def schemes(self) -> list[str]:
        return sorted(self._by_scheme)
