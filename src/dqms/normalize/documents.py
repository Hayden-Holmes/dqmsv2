"""Route heterogeneous metadata documents into one canonical `Metadata`.

Each document (DDL text, information_schema dump, catalog entry, lineage
record, ...) is sniffed by shape and converted to a *partial* `Metadata`.
Partials are merged; when two sources disagree on a fact the first one wins and
the disagreement is reported as a `Finding` instead of being silently dropped.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from dqms.models import (
    Column,
    ExistingValidation,
    Finding,
    ForeignKey,
    GlossaryTerm,
    Metadata,
    Profile,
    TableRef,
)
from dqms.normalize.ddl import looks_like_ddl, parse_ddl
from dqms.normalize.profile import looks_like_profile, normalize_profile
from dqms.normalize.rowdata import ROW_DATA_MESSAGE, looks_like_row_data

Document = str | dict[str, Any]
# Handles any shape the deterministic handlers do not recognize (normally an LLM).
# Receives (document name, document text); returns whatever it could extract.
Fallback = Callable[[str, str], tuple[Metadata | None, Profile | None]]

_STRING_TYPES = {"CHAR", "VARCHAR", "NVARCHAR", "NCHAR", "STRING", "TEXT"}
_COLUMN_FIELDS = ("datatype", "nullable", "unique", "max_length", "description", "semantic_type", "allowed_values")


@dataclass
class SourceDoc:
    """One unit of input: a named document, as text or an already-parsed object."""

    name: str
    content: Document


@dataclass
class DocReport:
    """How one input was handled, so the user can see what the tool understood."""

    name: str
    route: str  # "deterministic" | "llm" | "unrecognized"
    detail: str = ""


@dataclass
class IngestResult:
    metadata: Metadata
    profile: Profile | None = None
    findings: list[Finding] = field(default_factory=list)
    reports: list[DocReport] = field(default_factory=list)

    @property
    def unrecognized(self) -> int:
        return sum(1 for r in self.reports if r.route == "unrecognized")


def ingest(documents: list[Document | SourceDoc], fallback: Fallback | None = None) -> IngestResult:
    result = IngestResult(metadata=Metadata(name=""))
    profiles: list[Profile] = []

    for i, raw in enumerate(documents, start=1):
        src = raw if isinstance(raw, SourceDoc) else SourceDoc(f"document-{i}", raw)
        partial, profile, route, detail = _ingest_one(src, fallback)
        result.reports.append(DocReport(src.name, route, detail))
        if partial is not None:
            merge_metadata(result.metadata, partial, result.findings)
        if profile is not None:
            profiles.append(profile)

    if profiles:
        result.profile = _merge_profiles(profiles)
    return result


def _ingest_one(src: SourceDoc, fallback: Fallback | None):
    parsed = _parse(src.content)
    if isinstance(parsed, Metadata):
        return parsed, None, "deterministic", "DDL"
    if isinstance(parsed, dict):
        if looks_like_profile(parsed):
            return None, normalize_profile(parsed), "deterministic", "profile"
        partial = _to_partial(parsed)
        if partial is not None:
            return partial, None, "deterministic", "recognized schema"
    text = src.content if isinstance(src.content, str) else json.dumps(src.content)
    if looks_like_row_data(text):
        return None, None, "unrecognized", ROW_DATA_MESSAGE
    if fallback is not None:
        if text.strip():
            try:
                meta, prof = fallback(src.name, text)
            except Exception as exc:  # the LLM path must never take down the whole batch
                return None, None, "unrecognized", f"LLM normalization failed: {exc}"
            if meta is not None or prof is not None:
                return meta, prof, "llm", "normalized by LLM"
    return None, None, "unrecognized", "format not recognized and no LLM configured" if fallback is None else "nothing extractable"


# --------------------------------------------------------------------------- #
# Parsing / sniffing
# --------------------------------------------------------------------------- #


def _parse(raw: Document) -> dict[str, Any] | Metadata | None:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        loaded = json.loads(raw)
        return loaded if isinstance(loaded, dict) else None
    except json.JSONDecodeError:
        pass
    if looks_like_ddl(raw):
        return parse_ddl(raw)
    return None


def _to_partial(doc: dict[str, Any]) -> Metadata | None:
    columns = doc.get("columns")
    first = columns[0] if isinstance(columns, list) and columns and isinstance(columns[0], dict) else {}

    if "classifications" in doc:
        return _classification(doc)
    if "column_semantics" in doc:
        return _semantics(doc)
    if "upstream" in doc or "downstream" in doc:
        return _lineage(doc)
    if "existing_validations" in doc:
        return _validations(doc)
    if "terms" in doc:
        return _glossary(doc)
    if "column_name" in first or "data_type" in first:
        return _information_schema(doc)
    if "asset_name" in doc:
        return _catalog(doc)
    if "name" in doc and "name" in first:
        try:
            return Metadata.model_validate(doc)
        except Exception:
            return None
    return None


# --------------------------------------------------------------------------- #
# Handlers: source shape -> partial Metadata
# --------------------------------------------------------------------------- #


def _classification(doc: dict[str, Any]) -> Metadata:
    meta = Metadata(name=_name(doc, "table", "dataset", "entity"))
    for item in doc.get("classifications", []):
        if item.get("column") and item.get("classification"):
            meta.ensure_column(item["column"]).classifications.append(str(item["classification"]))
    return meta


def _semantics(doc: dict[str, Any]) -> Metadata:
    meta = Metadata(name=_name(doc, "table", "dataset", "entity"))
    for item in doc.get("column_semantics", []):
        if item.get("column") and item.get("semantic_type"):
            meta.ensure_column(item["column"]).semantic_type = slug(item["semantic_type"])
    return meta


def _lineage(doc: dict[str, Any]) -> Metadata:
    def refs(items: Any) -> list[TableRef]:
        return [TableRef(system=i.get("system"), table=i["table"]) for i in items or [] if i.get("table")]

    return Metadata(
        name=_name(doc, "entity", "table", "dataset"),
        upstream=refs(doc.get("upstream")),
        downstream=refs(doc.get("downstream")),
    )


def _validations(doc: dict[str, Any]) -> Metadata:
    return Metadata(
        name=_name(doc, "dataset", "table", "entity"),
        existing_validations=[
            ExistingValidation(column=v.get("column"), rule=str(v["rule"]))
            for v in doc.get("existing_validations", [])
            if v.get("rule")
        ],
    )


def _glossary(doc: dict[str, Any]) -> Metadata:
    return Metadata(
        name="",
        glossary=[
            GlossaryTerm(term=t["term"], definition=t.get("definition", ""))
            for t in doc.get("terms", [])
            if t.get("term")
        ],
    )


def _information_schema(doc: dict[str, Any]) -> Metadata:
    meta = Metadata(name=_name(doc, "table_name", "table", "name"))
    for entry in doc.get("columns", []):
        name = entry.get("column_name") or entry.get("name")
        if not name:
            continue
        col = meta.ensure_column(name)
        data_type = entry.get("data_type")
        max_length = entry.get("max_length") or entry.get("character_maximum_length")
        if data_type:
            col.datatype = (
                f"{str(data_type).upper()}({max_length})"
                if max_length and str(data_type).upper() in _STRING_TYPES
                else str(data_type).upper()
            )
        if max_length:
            col.max_length = int(max_length)
        col.nullable = _as_bool(entry.get("is_nullable"))
        if entry.get("is_unique") is True:
            col.unique = True
        if entry.get("is_primary_key") is True:
            meta.primary_keys.append(name)
            col.nullable = False
        col.description = entry.get("description") or entry.get("comment")
    for fk in doc.get("foreign_keys", []) or []:
        meta.foreign_keys.append(ForeignKey(**fk))
    return meta


def _catalog(doc: dict[str, Any]) -> Metadata:
    meta = Metadata(
        name=_name(doc, "asset_name"),
        description=doc.get("description"),
        domain=doc.get("domain"),
        owner=doc.get("owner"),
        steward=doc.get("steward"),
        tags=[str(t) for t in doc.get("tags", [])],
    )
    for entry in doc.get("columns", []):
        if entry.get("name"):
            meta.ensure_column(entry["name"]).description = entry.get("description")
    return meta


# --------------------------------------------------------------------------- #
# Merging
# --------------------------------------------------------------------------- #


def merge_metadata(base: Metadata, other: Metadata, findings: list[Finding]) -> None:
    if not base.name and other.name:
        base.name = other.name
    elif other.name and other.name.lower() != base.name.lower():
        findings.append(
            Finding(kind="dataset_name_conflict", message=f"Sources disagree on dataset name: '{base.name}' vs '{other.name}'; kept '{base.name}'.")
        )

    for attr in ("description", "owner", "steward", "domain"):
        _fill(base, other, attr, None, findings)

    for other_col in other.columns:
        col = base.ensure_column(other_col.name)
        for attr in _COLUMN_FIELDS:
            _fill(col, other_col, attr, col.name, findings)
        for tag in other_col.classifications:
            if tag not in col.classifications:
                col.classifications.append(tag)

    base.primary_keys += [k for k in other.primary_keys if k not in base.primary_keys]
    base.tags += [t for t in other.tags if t not in base.tags]
    known_fks = {(f.column.lower(), f.references_table.lower(), f.references_column.lower()) for f in base.foreign_keys}
    for fk in other.foreign_keys:
        if (fk.column.lower(), fk.references_table.lower(), fk.references_column.lower()) not in known_fks:
            base.foreign_keys.append(fk)
    base.upstream += [r for r in other.upstream if r not in base.upstream]
    base.downstream += [r for r in other.downstream if r not in base.downstream]
    base.glossary += [g for g in other.glossary if g not in base.glossary]
    base.existing_validations += [v for v in other.existing_validations if v not in base.existing_validations]


def _fill(base: Any, other: Any, attr: str, column: str | None, findings: list[Finding]) -> None:
    new, current = getattr(other, attr), getattr(base, attr)
    if new in (None, "", []):
        return
    if current in (None, "", []):
        setattr(base, attr, new)
    elif attr == "description":
        # Different sources describe different aspects (a catalog blurb vs. a DDL comment
        # that lists allowed values); keep both so rule extraction sees everything.
        if new.strip() not in current:
            setattr(base, attr, f"{current.rstrip()} {new.strip()}")
    elif current != new:
        findings.append(
            Finding(
                column=column,
                kind="source_conflict",
                message=f"Sources disagree on {attr}: {current!r} vs {new!r}; kept {current!r}.",
            )
        )


def _merge_profiles(profiles: list[Profile]) -> Profile:
    merged = Profile(row_count=next((p.row_count for p in profiles if p.row_count), None))
    seen: set[str] = set()
    for profile in profiles:
        for col in profile.columns:
            if col.column.lower() not in seen:
                seen.add(col.column.lower())
                merged.columns.append(col)
    return merged


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def slug(value: str) -> str:
    return "_".join(str(value).strip().lower().replace("-", " ").split())


def _name(doc: dict[str, Any], *keys: str) -> str:
    return next((str(doc[k]) for k in keys if isinstance(doc.get(k), str) and doc[k]), "")


def _as_bool(value: Any) -> bool | None:
    """Interpret an `is_nullable` value; returns nullable (True/False) or None."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return {"yes": True, "true": True, "no": False, "false": False}.get(value.strip().lower())
    return None


__all__ = ["IngestResult", "SourceDoc", "DocReport", "Document", "Fallback", "ingest", "merge_metadata", "slug"]
