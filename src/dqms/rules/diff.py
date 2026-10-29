"""Compare two rule sets, so a pipeline can detect that a source drifted between runs."""

from __future__ import annotations

from pydantic import BaseModel, Field

from dqms.models import Rule, RuleSet, Severity, Tier


class RuleChange(BaseModel):
    id: str
    before: dict | None = None
    after: dict | None = None


class RuleDiff(BaseModel):
    added: list[RuleChange] = Field(default_factory=list)
    removed: list[RuleChange] = Field(default_factory=list)
    changed: list[RuleChange] = Field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.changed)

    @property
    def breaking(self) -> bool:
        """Drift that likely matters to consumers: an authoritative rule was removed or altered,
        or a new authoritative (error-severity) rule appeared."""

        def authoritative(change: RuleChange) -> bool:
            side = change.before or change.after or {}
            return side.get("tier") in {Tier.DECLARED.value, Tier.DOCUMENTED.value}

        return any(authoritative(c) for c in self.removed + self.changed) or any(
            (c.after or {}).get("severity") == Severity.ERROR.value for c in self.added
        )


def diff_rulesets(old: RuleSet, new: RuleSet) -> RuleDiff:
    before = {r.id: r for r in old.rules}
    after = {r.id: r for r in new.rules}
    diff = RuleDiff()
    for rule_id in sorted(after.keys() - before.keys()):
        diff.added.append(RuleChange(id=rule_id, after=_view(after[rule_id])))
    for rule_id in sorted(before.keys() - after.keys()):
        diff.removed.append(RuleChange(id=rule_id, before=_view(before[rule_id])))
    for rule_id in sorted(before.keys() & after.keys()):
        if _view(before[rule_id]) != _view(after[rule_id]):
            diff.changed.append(RuleChange(id=rule_id, before=_view(before[rule_id]), after=_view(after[rule_id])))
    return diff


def _view(rule: Rule) -> dict:
    """The parts of a rule whose change means different behaviour."""
    return {"params": rule.params, "tier": rule.tier.value, "severity": rule.severity.value}
