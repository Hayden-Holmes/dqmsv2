"""Connector interface: pull canonical metadata (and aggregate profile) from a live source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dqms.models import Metadata, Profile


@dataclass
class Snapshot:
    metadata: Metadata
    profile: Profile | None = None


class Connector(Protocol):
    kind: str

    def fetch(self, ref: dict[str, Any], include_profile: bool = False) -> Snapshot:
        """Read one dataset. `ref` is connector-specific (e.g. project/dataset/table).

        Profiles must be aggregate statistics only; raw rows never leave the source.
        """
        ...


class ConnectorError(RuntimeError):
    pass


class ConnectorRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, Connector] = {}

    def register(self, connector: Connector) -> None:
        self._connectors[connector.kind] = connector

    def get(self, kind: str) -> Connector:
        try:
            return self._connectors[kind]
        except KeyError:
            raise ConnectorError(f"unknown connector '{kind}'; available: {sorted(self._connectors)}") from None

    @property
    def kinds(self) -> list[str]:
        return sorted(self._connectors)
