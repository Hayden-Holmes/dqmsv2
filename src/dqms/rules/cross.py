"""Cross-column rules inferred from naming pairs (start/end, created/updated, min/max...).

Only emitted when both columns exist and are plausibly the same kind of value.
Confidence is modest: names suggest an ordering, they do not prove it.
"""

from __future__ import annotations

import re

from dqms.models import Column, Rule, RuleType, Tier
from dqms.rules.base import Context, is_temporal, rule, type_family
from dqms.rules.semantic import is_birth_date

# (earlier segment, later segment): earlier <= later
_PAIRS = [
    ("start", "end"),
    ("begin", "end"),
    ("open", "close"),
    ("opened", "closed"),
    ("created", "updated"),
    ("created", "modified"),
    ("inserted", "updated"),
    ("from", "to"),
    ("min", "max"),
]
_TEMPORAL_NAME = re.compile(r"(_at|_on|_date|_time|_ts|_timestamp)$|^(created|updated|modified)$", re.IGNORECASE)
_REGISTRATION = re.compile(r"(registration|signup|sign_up|enroll\w*|account_open\w*)_?(date|at|on|ts)?$", re.IGNORECASE)
CONFIDENCE = 0.65


class CrossColumnGenerator:
    name = "cross_column"

    def generate(self, ctx: Context) -> list[Rule]:
        cols = ctx.metadata.columns
        by_lower = {c.name.lower(): c for c in cols}
        rules: list[Rule] = []
        seen: set[tuple[str, str]] = set()

        for col in cols:
            segments = col.name.lower().split("_")
            for early, late in _PAIRS:
                if early not in segments:
                    continue
                other = by_lower.get("_".join(late if s == early else s for s in segments))
                if other is None or other.name == col.name or (col.name, other.name) in seen:
                    continue
                if not _comparable(col, other):
                    continue
                seen.add((col.name, other.name))
                rules.append(
                    rule(RuleType.COMPARE, [col.name, other.name], Tier.INFERRED, CONFIDENCE,
                         f"By naming, {col.name} should not be after {other.name}.",
                         [f"column names: {col.name} / {other.name}"], operator="<=")
                )

        for birth in (c for c in cols if is_birth_date(c) and is_temporal(c)):
            for other in cols:
                if other.name != birth.name and is_temporal(other) and _REGISTRATION.search(other.name):
                    rules.append(
                        rule(RuleType.COMPARE, [birth.name, other.name], Tier.INFERRED, 0.6,
                             f"A person must be born before their {other.name}.",
                             [f"column names: {birth.name} / {other.name}"], operator="<=")
                    )
        return rules


def _comparable(a: Column, b: Column) -> bool:
    fa, fb = type_family(a), type_family(b)
    if fa == "unknown" and fb == "unknown":
        return bool(_TEMPORAL_NAME.search(a.name) and _TEMPORAL_NAME.search(b.name))
    if "unknown" in (fa, fb):
        return True
    if is_temporal(a) and is_temporal(b):
        return True
    return fa == fb == "numeric"
