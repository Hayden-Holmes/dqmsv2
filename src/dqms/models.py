"""Canonical models shared by every layer.

Connectors and normalizers produce `Metadata` and `Profile`; the rule engine
consumes them and produces `Rule`s. Nothing downstream of normalization ever
sees source-specific shapes.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


# --------------------------------------------------------------------------- #
# Metadata
# --------------------------------------------------------------------------- #


_SIZED_STRING_TYPES = {"CHAR", "VARCHAR", "NVARCHAR", "NCHAR", "CHARACTER", "STRING"}


class Column(BaseModel):
    name: str
    datatype: str | None = None  # raw source type, e.g. "VARCHAR(100)"
    nullable: bool | None = None  # None = unknown (never guessed)
    unique: bool | None = None
    max_length: int | None = None
    description: str | None = None
    semantic_type: str | None = None  # slug, e.g. "email_address"
    classifications: list[str] = Field(default_factory=list)
    allowed_values: list[str] | None = None
    rules: list[str] = Field(default_factory=list)  # stated business rules in prose, e.g. from catalog aspects
    term: str | None = None  # business glossary term the column implements

    @model_validator(mode="after")
    def _length_from_type(self) -> "Column":
        """A length written only inside the type ('VARCHAR(100)') is still a declared length,
        whichever source supplied it."""
        if self.max_length is None and self.datatype and self.base_type in _SIZED_STRING_TYPES:
            sized = re.search(r"\(\s*(\d+)\s*\)", self.datatype)
            if sized:
                self.max_length = int(sized.group(1))
        return self

    @property
    def base_type(self) -> str | None:
        """Upper-cased type without length/precision: 'VARCHAR(100)' -> 'VARCHAR'."""
        if not self.datatype:
            return None
        return self.datatype.split("(")[0].strip().upper()


class ForeignKey(BaseModel):
    column: str
    references_table: str
    references_column: str


class TableRef(BaseModel):
    system: str | None = None
    table: str


class ExistingValidation(BaseModel):
    column: str | None = None
    rule: str  # source vocabulary, e.g. "not_null"


class GlossaryTerm(BaseModel):
    term: str
    definition: str


class Metadata(BaseModel):
    name: str
    description: str | None = None
    columns: list[Column] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    upstream: list[TableRef] = Field(default_factory=list)
    downstream: list[TableRef] = Field(default_factory=list)
    owner: str | None = None
    steward: str | None = None
    domain: str | None = None
    tags: list[str] = Field(default_factory=list)
    glossary: list[GlossaryTerm] = Field(default_factory=list)
    existing_validations: list[ExistingValidation] = Field(default_factory=list)

    def column(self, name: str) -> Column | None:
        lowered = name.lower()
        return next((c for c in self.columns if c.name.lower() == lowered), None)

    def ensure_column(self, name: str) -> Column:
        col = self.column(name)
        if col is None:
            col = Column(name=name)
            self.columns.append(col)
        return col


# --------------------------------------------------------------------------- #
# Profile (aggregate statistics only; raw rows never enter the system)
# --------------------------------------------------------------------------- #


class ColumnProfile(BaseModel):
    column: str
    null_rate: float | None = None
    distinct_count: int | None = None
    distinct_rate: float | None = None
    min_value: Any = None
    max_value: Any = None
    min_length: int | None = None
    max_length: int | None = None
    top_values: list[str] | None = None  # complete value set when low-cardinality
    pattern: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)  # unmapped metrics, preserved


class Profile(BaseModel):
    table: str | None = None  # which table the statistics describe, when the source says
    row_count: int | None = None  # rows actually profiled (the sample size when sampled)
    total_rows: int | None = None  # rows in the source, when known and different
    sampled: bool = False  # a sample cannot prove uniqueness or true min/max
    notes: list[str] = Field(default_factory=list)  # e.g. columns that could not be profiled, and why
    columns: list[ColumnProfile] = Field(default_factory=list)

    def column(self, name: str) -> ColumnProfile | None:
        lowered = name.lower()
        return next((c for c in self.columns if c.column.lower() == lowered), None)


# --------------------------------------------------------------------------- #
# Rules
# --------------------------------------------------------------------------- #


class RuleType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    COMPOSITE_UNIQUE = "composite_unique"
    DATATYPE = "datatype"
    LENGTH = "length"  # params: min / max / exact
    ACCEPTED_VALUES = "accepted_values"  # params: values | reference_set
    REGEX = "regex"  # params: pattern | format
    RANGE = "range"  # params: min / max (inclusive)
    NOT_FUTURE = "not_future"
    AGE_RANGE = "age_range"  # date column; params: min_years / max_years
    COMPARE = "compare"  # columns=[left, right]; params: operator
    FOREIGN_KEY = "foreign_key"  # params: references_table / references_column


class Tier(str, Enum):
    """Where the evidence for a rule came from, strongest first."""

    DECLARED = "declared"  # DDL / schema / contract states it outright
    DOCUMENTED = "documented"  # stated in descriptions or comments
    SEMANTIC = "semantic"  # implied by an explicit semantic type
    OBSERVED = "observed"  # seen in profiling; a candidate, not a requirement
    INFERRED = "inferred"  # naming conventions or LLM reasoning


TIER_ORDER = [Tier.DECLARED, Tier.DOCUMENTED, Tier.SEMANTIC, Tier.OBSERVED, Tier.INFERRED]


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Rule(BaseModel):
    type: RuleType
    columns: list[str]
    params: dict[str, Any] = Field(default_factory=dict)
    tier: Tier
    confidence: float = Field(ge=0.0, le=1.0)
    severity: Severity = Severity.WARNING
    rationale: str
    evidence: list[str] = Field(default_factory=list)
    already_enforced: bool = False  # an equivalent validation already exists at the source
    id: str = ""  # stable within a dataset; assigned by the engine

    @property
    def key(self) -> tuple:
        """Identity used to dedupe rules from different generators.

        Two rules with the same key are the same check; the stronger tier wins.
        """
        wanted = _IDENTITY_PARAMS.get(self.type, ())
        values = tuple(sorted((k, _freeze(v)) for k, v in self.params.items() if k in wanted))
        # For bounds, *which* bounds are present is the identity (exact vs max, min vs min+max).
        shape = tuple(sorted(self.params)) if self.type in _SHAPE_IDENTITY else ()
        # Reference-set vs explicit-values are different checks.
        if self.type is RuleType.ACCEPTED_VALUES:
            shape = ("reference_set",) if "reference_set" in self.params else ("values",)
        return (self.type, tuple(c.lower() for c in self.columns), values, shape)


# Param *values* that distinguish two rules of the same type on the same columns.
_IDENTITY_PARAMS: dict[RuleType, tuple[str, ...]] = {
    RuleType.COMPARE: ("operator",),
    RuleType.FOREIGN_KEY: ("references_table", "references_column"),
}
_SHAPE_IDENTITY = {RuleType.LENGTH, RuleType.RANGE}


def _freeze(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(sorted(str(v) for v in value))
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    return value


class Finding(BaseModel):
    """A disagreement between sources, e.g. declared NOT NULL but profile shows nulls."""

    column: str | None = None
    kind: str
    message: str


SCHEMA_VERSION = "1"


class RuleSet(BaseModel):
    """Stable, versioned output. Pipelines can pin `schema_version` and compare `fingerprint`
    between runs to detect that the rules (and therefore the source) changed."""

    schema_version: str = SCHEMA_VERSION
    dataset: str
    fingerprint: str = ""
    rules: list[Rule]
    findings: list[Finding] = Field(default_factory=list)
    unrecognized_documents: int = 0
    llm_used: bool = False
