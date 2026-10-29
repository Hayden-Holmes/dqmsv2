"""Rules the source schema states outright. No inference, no LLM."""

from __future__ import annotations

from dqms.models import Metadata, Rule, RuleType, Tier
from dqms.rules.base import Context, rule, type_family

_FIXED_WIDTH = {"CHAR", "NCHAR", "CHARACTER"}


class DeclaredGenerator:
    name = "declared"

    def generate(self, ctx: Context) -> list[Rule]:
        meta: Metadata = ctx.metadata
        rules: list[Rule] = []
        pk = {c.lower() for c in meta.primary_keys}

        for col in meta.columns:
            is_pk = col.name.lower() in pk
            if col.nullable is False or is_pk:
                reason = "is part of the primary key" if is_pk else "is declared NOT NULL"
                rules.append(
                    rule(RuleType.NOT_NULL, col.name, Tier.DECLARED, 1.0,
                         f"{col.name} {reason}.", ["schema"])
                )
            if col.unique or (is_pk and len(pk) == 1):
                reason = "is the primary key" if is_pk else "is declared UNIQUE"
                rules.append(
                    rule(RuleType.UNIQUE, col.name, Tier.DECLARED, 1.0,
                         f"{col.name} {reason}.", ["schema"])
                )
            if col.base_type:
                family = type_family(col)
                rules.append(
                    rule(RuleType.DATATYPE, col.name, Tier.DECLARED, 1.0,
                         f"{col.name} is declared as {col.datatype}.", ["schema"],
                         type=col.base_type, family=family)
                )
            if col.max_length and family_is_string(col):
                if col.base_type in _FIXED_WIDTH:
                    rules.append(
                        rule(RuleType.LENGTH, col.name, Tier.DECLARED, 1.0,
                             f"{col.name} is fixed-width {col.datatype}.", ["schema"], exact=col.max_length)
                    )
                else:
                    rules.append(
                        rule(RuleType.LENGTH, col.name, Tier.DECLARED, 1.0,
                             f"{col.name} is limited to {col.max_length} characters by its type.",
                             ["schema"], max=col.max_length)
                    )
            if col.allowed_values:
                rules.append(
                    rule(RuleType.ACCEPTED_VALUES, col.name, Tier.DECLARED, 1.0,
                         f"{col.name} is constrained to a declared value set.", ["schema"],
                         values=list(col.allowed_values))
                )

        if len(pk) > 1:
            rules.append(
                rule(RuleType.COMPOSITE_UNIQUE, list(meta.primary_keys), Tier.DECLARED, 1.0,
                     "The combination of primary key columns must be unique.", ["schema"])
            )
        for fk in meta.foreign_keys:
            rules.append(
                rule(RuleType.FOREIGN_KEY, fk.column, Tier.DECLARED, 1.0,
                     f"{fk.column} references {fk.references_table}.{fk.references_column}.", ["schema"],
                     references_table=fk.references_table, references_column=fk.references_column)
            )
        return rules


def family_is_string(col) -> bool:
    return type_family(col) == "string"
