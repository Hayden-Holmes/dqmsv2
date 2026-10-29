"""Turn uploaded files (any format) into `SourceDoc`s for ingestion.

Formats with a cheap, lossless structured reading (JSON, YAML, CSV/TSV) become
objects so the deterministic handlers can try them first. Everything else (DDL,
SQL, markdown, XML, plain text, unknown extensions) is passed through as text
and falls to the LLM normalizer when no deterministic parser matches.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any

import yaml

from dqms.normalize.documents import SourceDoc

MAX_FILE_BYTES = 10 * 1024 * 1024


class UnsupportedFile(ValueError):
    pass


def read_upload(filename: str, data: bytes) -> list[SourceDoc]:
    if len(data) > MAX_FILE_BYTES:
        raise UnsupportedFile(f"{filename}: larger than {MAX_FILE_BYTES // (1024 * 1024)} MB")
    if b"\x00" in data[:4096]:
        raise UnsupportedFile(f"{filename}: binary files are not supported (export to CSV/JSON/text first)")
    text = _decode(data)
    if not text.strip():
        raise UnsupportedFile(f"{filename}: file is empty")

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "json":
        return _from_json(filename, text)
    if ext in {"yaml", "yml"}:
        return _from_yaml(filename, text)
    if ext in {"csv", "tsv"}:
        return [SourceDoc(filename, _from_csv(text, "\t" if ext == "tsv" else ","))]
    # Unknown/other extension: content may still be JSON.
    stripped = text.lstrip()
    if stripped.startswith(("{", "[")):
        try:
            return _wrap(filename, json.loads(text))
        except json.JSONDecodeError:
            pass
    return [SourceDoc(filename, text)]


def _decode(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _from_json(filename: str, text: str) -> list[SourceDoc]:
    try:
        return _wrap(filename, json.loads(text))
    except json.JSONDecodeError:
        return [SourceDoc(filename, text)]  # maybe JSON Lines or malformed: let the LLM cope


def _from_yaml(filename: str, text: str) -> list[SourceDoc]:
    try:
        docs = [d for d in yaml.safe_load_all(text) if d is not None]
    except yaml.YAMLError:
        return [SourceDoc(filename, text)]
    out: list[SourceDoc] = []
    for i, doc in enumerate(docs, start=1):
        out += _wrap(filename if len(docs) == 1 else f"{filename}#{i}", doc)
    return out or [SourceDoc(filename, text)]


def _wrap(name: str, loaded: Any) -> list[SourceDoc]:
    if isinstance(loaded, dict):
        return [SourceDoc(name, loaded)]
    if isinstance(loaded, list) and loaded and all(isinstance(x, dict) for x in loaded):
        return [SourceDoc(f"{name}[{i}]", item) for i, item in enumerate(loaded, start=1)]
    return [SourceDoc(name, json.dumps(loaded, default=str))]


def _from_csv(text: str, delimiter: str) -> dict[str, Any] | str:
    try:
        rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
    except csv.Error:
        return text
    if not rows or not rows[0]:
        return text
    entries = [{(k or "").strip().lower(): _coerce(v) for k, v in row.items() if k} for row in rows]
    entries = [{k: v for k, v in e.items() if v is not None} for e in entries]
    doc: dict[str, Any] = {"columns": entries}
    if "name" in entries[0]:
        doc["name"] = ""  # lets generic "name,description,..." tables take the canonical path
    return doc


def _coerce(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    low = text.lower()
    if low in {"true", "false"}:
        return low == "true"
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text
