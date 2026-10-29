from dqms.models import Column, Metadata, Profile, ColumnProfile, RuleType, Severity, Tier
from dqms.rules.engine import EngineOptions, RuleEngine
from dqms.service import RuleService


def find(rules, type_, *columns, **params):
    return [
        r for r in rules
        if r.type is type_ and r.columns == list(columns) and all(r.params.get(k) == v for k, v in params.items())
    ]


def test_customer_end_to_end(all_docs):
    analysis = RuleService().analyze_documents(all_docs)
    rules = analysis.ruleset.rules

    # declared: authoritative, error severity, confidence 1
    [nn] = find(rules, RuleType.NOT_NULL, "first_name")
    assert nn.tier is Tier.DECLARED and nn.severity is Severity.ERROR and nn.confidence == 1.0
    assert find(rules, RuleType.LENGTH, "country_code", exact=2)
    assert find(rules, RuleType.LENGTH, "email", max=255)
    assert not find(rules, RuleType.NOT_NULL, "email")  # nullable column must not get a not_null rule

    # documented: enum survives even though another source described the column first
    [status] = find(rules, RuleType.ACCEPTED_VALUES, "customer_status")
    assert status.tier is Tier.DOCUMENTED
    assert status.params["values"] == ["ACTIVE", "INACTIVE", "SUSPENDED"]
    assert status.confidence > 0.9  # corroborated by the profile

    # semantic + inferred
    assert find(rules, RuleType.AGE_RANGE, "date_of_birth", max_years=120)
    assert find(rules, RuleType.COMPARE, "created_at", "updated_at", operator="<=")

    # existing validations are recognised so the user sees only the gaps
    assert find(rules, RuleType.UNIQUE, "customer_id")[0].already_enforced
    assert not find(rules, RuleType.NOT_NULL, "first_name")[0].already_enforced

    # one rule per identity
    assert len({r.id for r in rules}) == len(rules)


def test_identical_rules_from_several_tiers_collapse_to_the_strongest():
    meta = Metadata(
        name="t",
        columns=[Column(name="country_code", datatype="CHAR(2)", max_length=2, semantic_type="iso_country_code",
                        description="ISO 3166-1 alpha-2 country code")],
    )
    rules = RuleEngine().generate(meta).rules
    lengths = find(rules, RuleType.LENGTH, "country_code", exact=2)
    assert len(lengths) == 1 and lengths[0].tier is Tier.DECLARED


def test_observed_rules_need_a_meaningful_profile():
    meta = Metadata(name="t", columns=[Column(name="x", datatype="VARCHAR(10)")])
    tiny = Profile(row_count=5, columns=[ColumnProfile(column="x", null_rate=0, distinct_rate=1.0)])
    big = Profile(row_count=10_000, columns=[ColumnProfile(column="x", null_rate=0, distinct_rate=1.0)])
    assert not find(RuleEngine().generate(meta, tiny).rules, RuleType.UNIQUE, "x")
    [unique] = find(RuleEngine().generate(meta, big).rules, RuleType.UNIQUE, "x")
    assert unique.tier is Tier.OBSERVED and unique.severity is Severity.INFO


def test_top_n_sample_is_not_mistaken_for_a_closed_value_set():
    meta = Metadata(name="t", columns=[Column(name="c", datatype="STRING")])
    sample = Profile(row_count=1000, columns=[ColumnProfile(column="c", distinct_count=500, top_values=["a", "b"])])
    closed = Profile(row_count=1000, columns=[ColumnProfile(column="c", distinct_count=2, top_values=["a", "b"])])
    assert not find(RuleEngine().generate(meta, sample).rules, RuleType.ACCEPTED_VALUES, "c")
    assert find(RuleEngine().generate(meta, closed).rules, RuleType.ACCEPTED_VALUES, "c")


def test_profile_that_contradicts_a_declared_rule_becomes_a_finding():
    meta = Metadata(
        name="t",
        columns=[Column(name="id", datatype="INT", nullable=False), Column(name="status", datatype="STRING",
                 description="Allowed values: A, B")],
    )
    profile = Profile(row_count=1000, columns=[
        ColumnProfile(column="id", null_rate=0.03),
        ColumnProfile(column="status", top_values=["A", "B", "Z"]),
    ])
    messages = " ".join(f.message for f in RuleEngine().generate(meta, profile).findings)
    assert "id is required but the profile shows a null rate of 3.00%" in messages
    assert "['Z']" in messages


def test_unknown_semantic_type_does_not_block_name_detection():
    meta = Metadata(name="t", columns=[Column(name="date_of_birth", datatype="DATE", semantic_type="date")])
    assert find(RuleEngine().generate(meta).rules, RuleType.NOT_FUTURE, "date_of_birth")


def test_name_heuristics_respect_types():
    meta = Metadata(name="t", columns=[Column(name="email_count", datatype="INT"), Column(name="email", datatype="INT")])
    rules = RuleEngine().generate(meta).rules
    assert not find(rules, RuleType.REGEX, "email")  # an INT column is not an email
    assert find(rules, RuleType.RANGE, "email_count", min=0)


def test_min_confidence_filters_low_evidence_rules():
    meta = Metadata(name="t", columns=[Column(name="created_at", datatype="TIMESTAMP"),
                                       Column(name="updated_at", datatype="TIMESTAMP")])
    everything = RuleEngine().generate(meta).rules
    strict = RuleEngine().generate(meta, options=EngineOptions(min_confidence=0.9)).rules
    assert len(strict) < len(everything)
    assert all(r.confidence >= 0.9 for r in strict)


def test_composite_primary_key_and_foreign_key():
    meta = Metadata(name="li", columns=[Column(name="a", datatype="INT"), Column(name="b", datatype="INT")],
                    primary_keys=["a", "b"])
    rules = RuleEngine().generate(meta).rules
    assert find(rules, RuleType.COMPOSITE_UNIQUE, "a", "b")
    assert not find(rules, RuleType.UNIQUE, "a")  # a alone need not be unique
