"""Scope control: decide *which* datasets a request touches, before anything is read or profiled.

A container source (`bigquery://project`, `gs://bucket/`) can expand to thousands of tables.
Nothing in the service is allowed to silently grow or silently shrink that set:

  * explicit dataset URIs are always included; include/exclude patterns narrow containers only
  * the result is capped (`DQMS_MAX_DATASETS`, default 50; a request may lower it, never raise it)
  * going over the cap is an *error with guidance*, never a quiet truncation
  * `plan_scope` returns the resolved list (with sizes where listings provide them) without
    reading any data, so a pipeline or a person can look before committing
  * the caller's source allowlist is applied to every expanded dataset
"""

from __future__ import annotations

import os
from collections.abc import Callable
from fnmatch import fnmatchcase

from pydantic import BaseModel, Field

from dqms.connectors.base import ConnectorError, DatasetRef
from dqms.service import RuleService

DISCOVERY_LIMIT = 5000  # most we will enumerate to *find* a selection; beyond this, narrow the source


def server_cap() -> int:
    return max(1, int(os.getenv("DQMS_MAX_DATASETS", "50")))


class ScopeRequest(BaseModel):
    include: list[str] = Field(default_factory=list, description="glob on dataset name or URI; empty = everything found")
    exclude: list[str] = Field(default_factory=list, description="glob on dataset name or URI")
    max_datasets: int | None = Field(None, ge=1, description="lower the cap for this request; cannot exceed the server cap")


class SourceProblem(BaseModel):
    source: str
    error: str


class ScopePlan(BaseModel):
    datasets: list[DatasetRef]
    excluded: int = 0  # dropped by include/exclude (never counts datasets hidden by the allowlist)
    problems: list[SourceProblem] = Field(default_factory=list)  # sources that could not be enumerated
    limit: int
    within_limit: bool
    message: str = ""
    total_bytes: int | None = None  # sum of the sizes that are known
    total_rows: int | None = None


def plan_scope(
    service: RuleService,
    sources: list[str],
    scope: ScopeRequest | None = None,
    allow: Callable[[str], bool] = lambda uri: True,
    cap: int | None = None,
) -> ScopePlan:
    scope = scope or ScopeRequest()
    cap = cap or server_cap()
    limit = min(scope.max_datasets or cap, cap)
    chosen: dict[str, DatasetRef] = {}
    problems: list[SourceProblem] = []
    excluded = 0

    for source in sources:
        try:
            connector, uri = service.connectors.resolve(source)
            container = connector.is_container(uri)
            refs = service.discover(source, DISCOVERY_LIMIT + 1)
        except ConnectorError as exc:
            problems.append(SourceProblem(source=source, error=str(exc)))
            continue
        if len(refs) > DISCOVERY_LIMIT:
            return ScopePlan(
                datasets=[], problems=problems, limit=limit, within_limit=False,
                message=f"'{source}' contains more than {DISCOVERY_LIMIT} datasets; point at a narrower dataset, schema or folder.",
            )
        for ref in refs:
            if not allow(ref.uri):
                continue  # invisible to this caller: not listed, not counted
            if container and not _selected(ref, scope):
                excluded += 1
                continue
            chosen.setdefault(ref.uri, ref)

    datasets = list(chosen.values())
    within = len(datasets) <= limit
    message = "" if within else (
        f"this scope matches {len(datasets)} datasets, over the limit of {limit}. Narrow the source, add "
        f"include/exclude patterns, or raise max_datasets (the server cap is {cap}). Nothing was read."
    )
    sizes = [d.bytes for d in datasets if d.bytes is not None]
    rows = [d.rows for d in datasets if d.rows is not None]
    return ScopePlan(
        datasets=datasets, excluded=excluded, problems=problems, limit=limit, within_limit=within,
        message=message, total_bytes=sum(sizes) if sizes else None, total_rows=sum(rows) if rows else None,
    )


def _selected(ref: DatasetRef, scope: ScopeRequest) -> bool:
    def matches(patterns: list[str]) -> bool:
        return any(fnmatchcase(ref.name, p) or fnmatchcase(ref.uri, p) for p in patterns)

    if scope.include and not matches(scope.include):
        return False
    return not (scope.exclude and matches(scope.exclude))
