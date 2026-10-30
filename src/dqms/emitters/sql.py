"""Translate platform-neutral rules into SQL that counts violating rows.

Every query returns one number; zero means the rule holds. Identifiers and
literals originate from uploaded metadata, so identifiers are validated
(unsafe names yield no SQL rather than escaped SQL) and literals are escaped.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel

from dqms.models import Metadata, Rule, RuleSet, RuleType
from dqms.rules.base import type_family

Dialect = Literal["bigquery", "postgres"]
_SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_SAFE_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$\-]*(\.[A-Za-z_][A-Za-z0-9_$\-]*){0,2}$")
_OPERATORS = {"<", "<=", ">", ">=", "=", "!="}


class SqlCheck(BaseModel):
    rule_id: str
    sql: str | None
    note: str = ""


class _Unsafe(ValueError):
    pass


def emit_sql_checks(
    ruleset: RuleSet, metadata: Metadata, dialect: Dialect = "bigquery", table: str | None = None
) -> list[SqlCheck]:
    target = table or ruleset.dataset
    return [_emit(r, metadata, dialect, target) for r in ruleset.rules]


def _emit(rule: Rule, metadata: Metadata, dialect: Dialect, table: str) -> SqlCheck:
    try:
        sql = _build(rule, metadata, dialect, table)
    except _Unsafe as exc:
        return SqlCheck(rule_id=rule.id, sql=None, note=f"not emitted: {exc}")
    if sql is None:
        return SqlCheck(rule_id=rule.id, sql=None, note=_UNSUPPORTED.get(rule.type, "no SQL form for this rule"))
    return SqlCheck(rule_id=rule.id, sql=sql)


_UNSUPPORTED = {RuleType.DATATYPE: "enforced by the table schema, not a row check"}


def _build(rule: Rule, metadata: Metadata, d: Dialect, table: str) -> str | None:
    t = _table(table, d)
    cols = [_ident(c, d) for c in rule.columns]
    c = cols[0]
    p = rule.params

    def count(where: str) -> str:
        return f"SELECT COUNT(*) AS violations FROM {t} WHERE {where}"

    match rule.type:
        case RuleType.NOT_NULL:
            return count(f"{c} IS NULL")
        case RuleType.UNIQUE:
            return f"SELECT COUNT(*) AS violations FROM (SELECT {c} FROM {t} WHERE {c} IS NOT NULL GROUP BY {c} HAVING COUNT(*) > 1)"
        case RuleType.COMPOSITE_UNIQUE:
            joined = ", ".join(cols)
            return f"SELECT COUNT(*) AS violations FROM (SELECT {joined} FROM {t} GROUP BY {joined} HAVING COUNT(*) > 1)"
        case RuleType.LENGTH:
            clauses = []
            if "exact" in p:
                clauses.append(f"LENGTH({c}) != {_num(p['exact'])}")
            if "min" in p:
                clauses.append(f"LENGTH({c}) < {_num(p['min'])}")
            if "max" in p:
                clauses.append(f"LENGTH({c}) > {_num(p['max'])}")
            return count(f"{c} IS NOT NULL AND ({' OR '.join(clauses)})")
        case RuleType.ACCEPTED_VALUES:
            if not p.get("values"):
                return None  # reference sets need a lookup table the emitter does not know
            values = ", ".join(_lit(v, d) for v in p["values"])
            return count(f"{c} IS NOT NULL AND {c} NOT IN ({values})")
        case RuleType.REGEX:
            pattern = _lit(p["pattern"], d)
            predicate = f"NOT REGEXP_CONTAINS({c}, {pattern})" if d == "bigquery" else f"{c} !~ {pattern}"
            return count(f"{c} IS NOT NULL AND {predicate}")
        case RuleType.RANGE:
            clauses = []
            if "min" in p:
                clauses.append(f"{c} {'<=' if p.get('exclusive_min') else '<'} {_num(p['min'])}")
            if "max" in p:
                clauses.append(f"{c} {'>=' if p.get('exclusive_max') else '>'} {_num(p['max'])}")
            return count(f"{c} IS NOT NULL AND ({' OR '.join(clauses)})")
        case RuleType.NOT_FUTURE:
            col = metadata.column(rule.columns[0])
            family = type_family(col) if col else "unknown"
            now = "CURRENT_DATE()" if d == "bigquery" and family == "date" else "CURRENT_TIMESTAMP()" if d == "bigquery" else (
                "CURRENT_DATE" if family == "date" else "CURRENT_TIMESTAMP")
            return count(f"{c} > {now}")
        case RuleType.AGE_RANGE:
            clauses = []
            if "max_years" in p:
                clauses.append(f"CAST({c} AS DATE) < {_years_ago(p['max_years'], d)}")
            if "min_years" in p:
                clauses.append(f"CAST({c} AS DATE) > {_years_ago(p['min_years'], d)}")
            return count(f"{c} IS NOT NULL AND ({' OR '.join(clauses)})")
        case RuleType.COMPARE:
            op = p.get("operator")
            if op not in _OPERATORS:
                raise _Unsafe(f"unsupported operator {op!r}")
            left, right = cols
            sql_op = "!=" if op == "!=" else op
            return count(f"{left} IS NOT NULL AND {right} IS NOT NULL AND NOT ({left} {sql_op} {right})")
        case RuleType.FOREIGN_KEY:
            ref_t = _table(p["references_table"], d)
            ref_c = _ident(p["references_column"], d)
            return (
                f"SELECT COUNT(*) AS violations FROM {t} AS child WHERE child.{c} IS NOT NULL AND NOT EXISTS "
                f"(SELECT 1 FROM {ref_t} AS parent WHERE parent.{ref_c} = child.{c})"
            )
        case _:
            return None


def _years_ago(years: object, d: Dialect) -> str:
    n = _num(years)
    return f"DATE_SUB(CURRENT_DATE(), INTERVAL {n} YEAR)" if d == "bigquery" else f"(CURRENT_DATE - INTERVAL '{n} years')"


def _quote(name: str, d: Dialect) -> str:
    return f"`{name}`" if d == "bigquery" else f'"{name}"'


def _ident(name: str, d: Dialect) -> str:
    if not _SAFE_NAME.match(name):
        raise _Unsafe(f"column name {name!r} has characters that cannot be safely quoted")
    return _quote(name, d)


def _table(name: str, d: Dialect) -> str:
    if not _SAFE_TABLE.match(name):
        raise _Unsafe(f"table name {name!r} has characters that cannot be safely quoted")
    return ".".join(_quote(part, d) for part in name.split("."))


def _lit(value: object, d: Dialect) -> str:
    text = str(value)
    if "\n" in text or "\r" in text or "\x00" in text:
        raise _Unsafe("literal contains control characters")
    escaped = text.replace("\\", "\\\\").replace("'", "\\'") if d == "bigquery" else text.replace("'", "''")
    return f"'{escaped}'"


def _num(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _Unsafe(f"non-numeric bound {value!r}")
    return repr(value)
