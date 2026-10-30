"""Upload data files together with the metadata that describes them (any platform, no cloud access).

    table + metadata      orders.csv  + orders.sql              -> rules for `orders`
    dataset + metadata    orders.csv, customers.parquet + schema.sql / catalog.csv (describes all tables)
    metadata only         schema.sql with several tables         -> rules for each table

Files are classified by content: a CSV whose header *describes columns* (column_name, data_type, ...) is
metadata; one whose header *is* the columns is data. Data files are profiled in the service's memory into
aggregates (nulls, distinct, min/max, lengths); the rows themselves are never stored, logged, or sent to an
LLM. The metadata is authoritative; the data fills gaps and is checked against it, so a column the metadata
declares numeric but the data shows holding text becomes a finding.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import BaseModel, Field

from dqms.connectors.base import ConnectorError, ProfileOptions
from dqms.connectors.objectstore import ObjectInfo, ObjectStoreConnector
from dqms.connectors.uri import parse_source
from dqms.models import Finding, Metadata, Profile
from dqms.normalize.documents import SourceDoc, TableSet, ingest_tables, table_key
from dqms.normalize.files import UnsupportedFile, _decode, read_upload
from dqms.normalize.rowdata import metadata_shaped, records_look_like_metadata
from dqms.rules.base import type_family
from dqms.rules.engine import EngineOptions
from dqms.service import Analysis, DocumentReport, NoMetadataError, RuleService

# Cloud Run rejects request bodies over 32 MiB, so the whole upload has to fit under that.
MAX_BATCH_BYTES = 30 * 1024 * 1024
DATA_EXTENSIONS = {"csv", "tsv", "jsonl", "ndjson", "parquet", "json"}


@dataclass
class Upload:
    name: str
    kind: str  # "data" | "metadata"
    docs: list[SourceDoc] = field(default_factory=list)
    data: bytes | None = None
    ext: str = ""


class PairingRow(BaseModel):
    table: str
    data_file: str | None = None
    metadata_files: list[str] = Field(default_factory=list)
    note: str = ""


class BatchAnalysis(BaseModel):
    analyses: list[Analysis]
    pairing: list[PairingRow]
    notes: list[str] = Field(default_factory=list)
    rejected: list[DocumentReport] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Classifying uploads
# --------------------------------------------------------------------------- #


def classify(name: str, data: bytes) -> Upload:
    """Data or metadata? Raises UnsupportedFile for unreadable input."""
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext == "parquet":
        return Upload(name, "data", data=data, ext="parquet")
    if ext in {"csv", "tsv", "jsonl", "ndjson", "json"} and b"\x00" not in data[:4096] and data.strip():
        text = _decode(data)
        if ext in {"csv", "tsv"}:
            header = _csv_header(text, "\t" if ext == "tsv" else None)
            if header and not metadata_shaped(header):
                return Upload(name, "data", data=data, ext=ext)
        elif ext in {"jsonl", "ndjson"}:
            keys = _first_record_keys(text)
            if keys is not None and not records_look_like_metadata(keys):
                return Upload(name, "data", data=data, ext="jsonl")
        else:  # .json: an array of records is data, anything else is a metadata document
            as_lines = _array_of_records_as_jsonl(text)
            if as_lines is not None:
                return Upload(name, "data", data=as_lines, ext="jsonl")
    return Upload(name, "metadata", docs=read_upload(name, data))


def _csv_header(text: str, delimiter: str | None) -> list[str] | None:
    try:
        if delimiter is None:
            try:
                delimiter = csv.Sniffer().sniff(text[:8192], delimiters=",;|\t").delimiter
            except csv.Error:
                delimiter = ","
        return next(csv.reader(io.StringIO(text), delimiter=delimiter), None)
    except csv.Error:
        return None


def _first_record_keys(text: str) -> list[str] | None:
    for line in text.splitlines():
        if line.strip():
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                return None
            return list(obj) if isinstance(obj, dict) else None
    return None


def _array_of_records_as_jsonl(text: str) -> bytes | None:
    if not text.lstrip().startswith("["):
        return None
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not (isinstance(loaded, list) and loaded and all(isinstance(r, dict) for r in loaded)):
        return None
    if records_look_like_metadata(list(loaded[0])):
        return None
    return "\n".join(json.dumps(r, default=str) for r in loaded).encode()


def read_uploads(files: list[tuple[str, bytes]]) -> tuple[list[Upload], list[DocumentReport]]:
    uploads: list[Upload] = []
    rejected: list[DocumentReport] = []
    for name, data in files:
        try:
            uploads.append(classify(name, data))
        except UnsupportedFile as exc:
            rejected.append(DocumentReport(name=name, route="rejected", detail=str(exc)))
    return uploads, rejected


# --------------------------------------------------------------------------- #
# Profiling uploaded data (in memory, aggregates only)
# --------------------------------------------------------------------------- #


class _UploadStore:
    def __init__(self, files: dict[str, bytes]):
        self._files = files

    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]:
        return [ObjectInfo(k, len(v)) for k, v in self._files.items() if k.startswith(prefix)][:limit]

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes:
        return self._files[key][start : start + length]


def _profile_upload(upload: Upload, include_values: bool):
    """Reuses the object-store profiler on in-memory bytes: type inference, nulls, distinct, min/max, lengths."""
    key = f"data.{upload.ext}"
    connector = ObjectStoreConnector(store_factory=lambda scheme, authority: _UploadStore({key: upload.data}))
    options = ProfileOptions(enabled=True, include_values=include_values, max_bytes=MAX_BATCH_BYTES)
    return connector.fetch(parse_source(f"gs://upload/{key}"), options)


# --------------------------------------------------------------------------- #
# Reconciling metadata with what the data shows
# --------------------------------------------------------------------------- #


def merge_data(meta: Metadata, inferred: Metadata, profile: Profile) -> list[Finding]:
    """Metadata stays authoritative. Data fills in missing types, adds columns the metadata forgot, and is
    checked against what the metadata declares."""
    findings: list[Finding] = []
    inferred_by = {c.name.lower(): c for c in inferred.columns}
    declared = {c.name.lower() for c in meta.columns}

    for col in list(meta.columns):
        seen = inferred_by.get(col.name.lower())
        if seen is None:
            findings.append(Finding(column=col.name, kind="column_missing_from_data",
                                    message=f"{col.name} is described in the metadata but is not in the data file."))
            continue
        stats = profile.column(seen.name)
        if stats is not None:
            stats.column = col.name  # use the metadata's spelling so rules and statistics line up
        if col.datatype is None:
            col.datatype = seen.datatype
        elif type_family(col) in {"numeric", "date", "timestamp", "boolean"} and type_family(seen) == "string":
            findings.append(Finding(column=col.name, kind="type_mismatch",
                                    message=f"{col.name} is declared {col.datatype} but the data contains values that do not parse as {type_family(col)}."))

    for seen in inferred.columns:
        if seen.name.lower() not in declared:
            meta.columns.append(seen.model_copy())
            findings.append(Finding(column=seen.name, kind="column_not_in_metadata",
                                    message=f"{seen.name} is in the data file but not described in the metadata (type inferred from the data: {seen.datatype})."))
    return findings


# --------------------------------------------------------------------------- #
# Pairing + analysis
# --------------------------------------------------------------------------- #


def analyze_uploads(
    service: RuleService,
    uploads: list[Upload],
    options: EngineOptions | None = None,
    pairs: dict[str, str] | None = None,
    truncated: set[str] | None = None,
    include_values: bool = False,
) -> BatchAnalysis:
    """`pairs` maps a data file name to a table name; anything not listed is paired by file name."""
    pairs, truncated = pairs or {}, truncated or set()
    metadata_uploads = [u for u in uploads if u.kind == "metadata"]
    data_uploads = [u for u in uploads if u.kind == "data"]
    tableset = ingest_tables([d for u in metadata_uploads for d in u.docs], fallback=service.normalizer)
    notes = list(tableset.notes)

    assigned, leftovers = _pair(tableset, data_uploads, pairs)
    named = {k: m for k, m in tableset.tables.items() if k}

    # Exactly one data file and exactly one table in the whole upload can only belong together, even if the
    # names differ. With several files a name-less match would be a guess, so those are reported, not paired.
    if len(data_uploads) == 1 and len(named) == 1 and not assigned:
        assigned[next(iter(named))] = leftovers.pop()
    if "" in tableset.tables and len(data_uploads) == 1 and not assigned and len(tableset.tables) == 1:
        assigned[""] = leftovers.pop() if leftovers else data_uploads[0]

    meta_reports = [DocumentReport(name=r.name, route=r.route, detail=r.detail) for r in tableset.reports]
    analyses: list[Analysis] = []
    pairing: list[PairingRow] = []
    metadata_names = [u.name for u in metadata_uploads]

    for key, meta in tableset.tables.items():
        upload = assigned.get(key)
        name = meta.name or (_stem(upload.name) if upload else "unnamed_dataset")
        meta.name = name
        profile = tableset.profiles.get(key) or (tableset.loose_profile if len(tableset.tables) == 1 else None)
        documents = list(meta_reports)
        findings = [f for f in tableset.findings if f.column is None or meta.column(f.column) is not None]
        row = PairingRow(table=name, data_file=upload.name if upload else None, metadata_files=metadata_names)

        if upload is not None:
            try:
                snapshot = _profile_upload(upload, include_values)
            except ConnectorError as exc:
                row.note = f"data file could not be profiled: {exc}"
                notes.append(f"{upload.name}: {exc}")
            else:
                data_profile = snapshot.profile
                findings += merge_data(meta, snapshot.metadata, data_profile)
                if profile is not None:
                    notes.append(f"{name}: statistics found in the metadata files were replaced by the profile of {upload.name}.")
                profile = data_profile
                profile.table = name
                if upload.name in truncated:
                    profile.sampled, profile.total_rows = True, None
                    profile.notes.append("profile covers only the first part of the file (the browser sent a truncated copy)")
                documents.append(DocumentReport(
                    name=upload.name, route="data",
                    detail=f"profiled {profile.row_count:,} rows into aggregates{' (sampled)' if profile.sampled else ''}; rows were not kept",
                ))
        else:
            row.note = "no data file for this table; rules come from the metadata only"
        pairing.append(row)
        analysis = service.analyze_models(meta, profile, options, findings)
        analysis.documents = documents
        analyses.append(analysis)

    for upload in leftovers:  # data with no metadata: still useful (types and statistics are inferred)
        try:
            snapshot = _profile_upload(upload, include_values)
        except ConnectorError as exc:
            notes.append(f"{upload.name}: {exc}")
            continue
        meta, profile = snapshot.metadata, snapshot.profile
        meta.name = pairs.get(upload.name) or _stem(upload.name)
        profile.table = meta.name
        if upload.name in truncated:
            profile.sampled, profile.total_rows = True, None
        analysis = service.analyze_models(meta, profile, options, [])
        analysis.documents = [DocumentReport(name=upload.name, route="data",
                                             detail=f"profiled {profile.row_count:,} rows into aggregates; rows were not kept")]
        analyses.append(analysis)
        known = ", ".join(sorted(m.name for m in named.values()))
        note = (f"no matching table in the metadata (tables found: {known}); rename the file to match a table name, or pass an explicit pairing"
                if known else "no metadata was provided, so types and rules come from the data alone")
        row = PairingRow(table=meta.name, data_file=upload.name, note=note)
        pairing.append(row)
        notes.append(f"{upload.name}: {row.note}")

    if not analyses:
        raise NoMetadataError("Nothing could be analyzed. " + "; ".join(notes or [r.detail for r in meta_reports]))
    return BatchAnalysis(analyses=analyses, pairing=pairing, notes=notes)


def _pair(tableset: TableSet, data_uploads: list[Upload], pairs: dict[str, str]) -> tuple[dict[str, Upload], list[Upload]]:
    assigned: dict[str, Upload] = {}
    leftovers: list[Upload] = []
    for upload in data_uploads:
        key = table_key(pairs[upload.name]) if upload.name in pairs else table_key(_stem(upload.name))
        if key in tableset.tables and key not in assigned:
            assigned[key] = upload
        else:
            leftovers.append(upload)
    return assigned, leftovers


def _stem(filename: str) -> str:
    return Path(filename).name.rsplit(".", 1)[0]
