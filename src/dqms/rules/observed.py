"""Candidate rules suggested by profiling statistics.

Profiling shows what the data *does*, not what it *should* do, so everything
here is low-confidence and low-severity, and only emitted when the profile is
large enough to mean something. Declared/documented rules always outrank these
after merging.
"""

from __future__ import annotations

import re

from dqms.models import ColumnProfile, Rule, RuleType, Tier
from dqms.rules.base import Context, rule, type_family

MAX_ENUM_CARDINALITY = 20
CONFIDENCE = 0.5


class ObservedGenerator:
    name = "observed"

    def generate(self, ctx: Context) -> list[Rule]:
        profile = ctx.profile
        if profile is None:
            return []
        enough_rows = profile.row_count is not None and profile.row_count >= ctx.min_profile_rows
        rules: list[Rule] = []

        for stats in profile.columns:
            col = ctx.metadata.column(stats.column)
            family = type_family(col) if col else "unknown"
            name = col.name if col else stats.column
            n = f"n={profile.row_count}" if profile.row_count is not None else "n=unknown"

            if enough_rows and stats.null_rate == 0:
                rules.append(
                    rule(RuleType.NOT_NULL, name, Tier.OBSERVED, CONFIDENCE,
                         f"No nulls observed in {name}.", [f"profile: null_rate=0 ({n})"])
                )
            if enough_rows and _all_distinct(stats, profile.row_count):
                rules.append(
                    rule(RuleType.UNIQUE, name, Tier.OBSERVED, CONFIDENCE,
                         f"All observed values of {name} are distinct.", [f"profile: distinct_rate=1 ({n})"])
                )

            values = _closed_value_set(stats)
            if values and family in {"string", "unknown"}:
                rules.append(
                    rule(RuleType.ACCEPTED_VALUES, name, Tier.OBSERVED, CONFIDENCE,
                         f"{name} only takes {len(values)} distinct values in the profile.",
                         [f"profile: values={values}"], values=values)
                )

            if enough_rows and family == "numeric" and _is_number(stats.min_value) and _is_number(stats.max_value):
                rules.append(
                    rule(RuleType.RANGE, name, Tier.OBSERVED, 0.4,
                         f"Observed range of {name} is {stats.min_value}..{stats.max_value}.",
                         [f"profile: min={stats.min_value}, max={stats.max_value} ({n})"],
                         min=stats.min_value, max=stats.max_value)
                )

            if stats.pattern and family in {"string", "unknown"}:
                regex = pattern_to_regex(stats.pattern)
                if regex:
                    rules.append(
                        rule(RuleType.REGEX, name, Tier.OBSERVED, 0.4,
                             f"Values of {name} follow the observed pattern {stats.pattern!r}.",
                             [f"profile: pattern={stats.pattern!r}"], pattern=regex)
                    )
        return rules


def pattern_to_regex(pattern: str) -> str | None:
    """Accept a regex as-is, or convert a simple glob like '*@*.*'. None if unusable."""
    if re.search(r"[\\^$\[\](){}|+]", pattern):
        try:
            re.compile(pattern)
            return pattern
        except re.error:
            return None
    if "*" in pattern or "?" in pattern:
        escaped = re.escape(pattern).replace(r"\*", ".+").replace(r"\?", ".")
        return f"^{escaped}$"
    return None


def _all_distinct(stats: ColumnProfile, row_count: int | None) -> bool:
    if stats.distinct_rate is not None:
        return stats.distinct_rate >= 1.0 and (stats.null_rate or 0) == 0
    return bool(row_count) and stats.distinct_count == row_count and (stats.null_rate or 0) == 0


def _closed_value_set(stats: ColumnProfile) -> list[str] | None:
    """The value list, only if it is demonstrably the complete set (not a top-N sample)."""
    values = stats.top_values
    if not values or len(values) > MAX_ENUM_CARDINALITY:
        return None
    if stats.distinct_count is not None and stats.distinct_count != len(values):
        return None
    return values


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
