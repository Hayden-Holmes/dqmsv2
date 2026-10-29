"""Deterministic parser for CREATE TABLE statements and COMMENT ON statements.

Handles the common ANSI/Postgres/BigQuery shapes: inline and table-level
PRIMARY KEY / UNIQUE / FOREIGN KEY, NOT NULL, REFERENCES, sized types.
Anything it does not understand is left as unknown rather than guessed.
"""

from __future__ import annotations

import re

from dqms.models import Column, ForeignKey, Metadata

_CREATE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[\w.\"`]+)\s*\(",
    re.IGNORECASE,
)
_COMMENT = re.compile(
    r"COMMENT\s+ON\s+(?P<kind>TABLE|COLUMN)\s+(?P<target>[\w.\"`]+)\s+IS\s+'(?P<text>(?:[^']|'')*)'",
    re.IGNORECASE | re.DOTALL,
)
_COLUMN_DEF = re.compile(
    r"^(?P<name>\"[^\"]+\"|`[^`]+`|\w+)\s+(?P<type>[A-Za-z_][\w]*(?:\s*\(\s*[\d,\s]+\))?)(?P<rest>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_REFERENCES = re.compile(r"REFERENCES\s+(?P<table>[\w.\"`]+)\s*\(\s*(?P<col>[\w\"`]+)\s*\)", re.IGNORECASE)
_TABLE_FK = re.compile(
    r"FOREIGN\s+KEY\s*\(\s*(?P<col>[\w\"`]+)\s*\)\s*REFERENCES\s+(?P<table>[\w.\"`]+)\s*\(\s*(?P<ref>[\w\"`]+)\s*\)",
    re.IGNORECASE,
)
_TABLE_KEYS = re.compile(r"(?:PRIMARY\s+KEY|UNIQUE)\s*\(([^)]*)\)", re.IGNORECASE)
_LENGTH = re.compile(r"\(\s*(\d+)\s*\)")
_STRING_TYPES = {"CHAR", "VARCHAR", "NVARCHAR", "NCHAR", "CHARACTER", "STRING"}


def looks_like_ddl(text: str) -> bool:
    return bool(_CREATE.search(text))


def parse_ddl(text: str) -> Metadata | None:
    match = _CREATE.search(text)
    if not match:
        return None
    body, _ = _balanced_body(text, match.end() - 1)
    table = _bare(match.group("name"))
    meta = Metadata(name=table)

    for item in _split_top_level(body):
        _apply_item(meta, item.strip())

    for comment in _COMMENT.finditer(text):
        content = comment.group("text").replace("''", "'")
        target = _bare(comment.group("target"))
        if comment.group("kind").upper() == "TABLE":
            meta.description = content
        else:
            col_name = target.split(".")[-1]
            meta.ensure_column(col_name).description = content
    return meta


def _apply_item(meta: Metadata, item: str) -> None:
    if not item:
        return
    upper = item.upper()

    if upper.startswith(("PRIMARY KEY", "UNIQUE", "CONSTRAINT", "FOREIGN KEY")):
        fk = _TABLE_FK.search(item)
        if fk:
            meta.foreign_keys.append(
                ForeignKey(
                    column=_bare(fk.group("col")),
                    references_table=_bare(fk.group("table")),
                    references_column=_bare(fk.group("ref")),
                )
            )
        keys = _TABLE_KEYS.search(item)
        if keys:
            cols = [_bare(c) for c in keys.group(1).split(",") if c.strip()]
            if "PRIMARY KEY" in upper:
                meta.primary_keys = cols
            elif len(cols) == 1:
                meta.ensure_column(cols[0]).unique = True
        return

    col_match = _COLUMN_DEF.match(item)
    if not col_match:
        return
    column = meta.ensure_column(_bare(col_match.group("name")))
    column.datatype = re.sub(r"\s+", "", col_match.group("type")).upper()
    rest = col_match.group("rest").upper()

    if column.base_type in _STRING_TYPES:
        length = _LENGTH.search(column.datatype)
        if length:
            column.max_length = int(length.group(1))

    if "NOT NULL" in rest:
        column.nullable = False
    elif "PRIMARY KEY" not in rest:
        column.nullable = True  # DDL columns are nullable unless declared otherwise
    if "PRIMARY KEY" in rest:
        if column.name not in meta.primary_keys:
            meta.primary_keys.append(column.name)
        column.nullable = False
    if re.search(r"\bUNIQUE\b", rest):
        column.unique = True
    ref = _REFERENCES.search(col_match.group("rest"))
    if ref:
        meta.foreign_keys.append(
            ForeignKey(
                column=column.name,
                references_table=_bare(ref.group("table")),
                references_column=_bare(ref.group("col")),
            )
        )


def _balanced_body(text: str, open_idx: int) -> tuple[str, int]:
    depth = 0
    for i in range(open_idx, len(text)):
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[open_idx + 1 : i], i
    return text[open_idx + 1 :], len(text)


def _split_top_level(body: str) -> list[str]:
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _bare(identifier: str) -> str:
    return identifier.strip().strip('"`')
