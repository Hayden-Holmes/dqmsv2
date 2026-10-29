"""Universal-format normalization and LLM rule enrichment, with a scripted fake model."""

from conftest import FakeLLM

from dqms.models import RuleType, Tier
from dqms.normalize.documents import SourceDoc
from dqms.service import RuleService

CONTRACT = """
# Orders data contract
The `orders` dataset. Each order has an order_id (required, unique) and a status.
status must be one of: NEW, SHIPPED, CANCELLED.
Fields: order_id (integer), status (string), placed_at (timestamp), shipped_at (timestamp).
"""


def contract_reply(**overrides):
    reply = {
        "metadata": {
            "name": "orders",
            "columns": [
                {"name": "order_id", "datatype": "INTEGER", "nullable": False, "unique": True},
                {"name": "status", "datatype": "STRING", "allowed_values": ["NEW", "SHIPPED", "CANCELLED"]},
                {"name": "placed_at", "datatype": "TIMESTAMP"},
                {"name": "shipped_at", "datatype": "TIMESTAMP"},
            ],
        },
        "profile": None,
    }
    reply.update(overrides)
    return reply


def test_unknown_format_is_normalized_by_the_llm():
    llm = FakeLLM(contract_reply(), {"rules": []})
    analysis = RuleService(llm).analyze_documents([SourceDoc("contract.md", CONTRACT)])
    assert analysis.documents[0].route == "llm"
    rules = analysis.ruleset.rules
    assert any(r.type is RuleType.ACCEPTED_VALUES and r.columns == ["status"] for r in rules)
    assert any(r.type is RuleType.NOT_NULL and r.columns == ["order_id"] and r.tier is Tier.DECLARED for r in rules)
    # no naming convention pairs placed/shipped, so no ordering rule is invented without the LLM
    assert not any(r.type is RuleType.COMPARE for r in rules)


def test_hallucinated_facts_are_dropped_by_grounding():
    reply = contract_reply()
    reply["metadata"]["columns"] += [
        {"name": "credit_card_number", "datatype": "STRING", "nullable": False},  # never mentioned
    ]
    reply["metadata"]["columns"][1]["allowed_values"].append("REFUNDED")  # never mentioned
    reply["metadata"]["owner"] = "Finance Team"  # never mentioned
    analysis = RuleService(FakeLLM(reply, {"rules": []})).analyze_documents([SourceDoc("c.md", CONTRACT)])
    meta = analysis.metadata
    assert meta.column("credit_card_number") is None
    assert meta.column("status").allowed_values == ["NEW", "SHIPPED", "CANCELLED"]
    assert meta.owner is None


def test_llm_failure_on_one_document_does_not_lose_the_others(all_docs):
    llm = FakeLLM(RuntimeError("model down"), {"rules": []})
    docs = all_docs + [SourceDoc("weird.txt", "just prose about customers")]
    analysis = RuleService(llm).analyze_documents(docs)
    weird = next(d for d in analysis.documents if d.name == "weird.txt")
    assert weird.route == "unrecognized" and "model down" in weird.detail
    assert analysis.metadata.name == "customer" and analysis.ruleset.rules


def test_llm_rules_are_validated_capped_and_limited_to_known_columns():
    proposed = {"rules": [
        {"type": "compare", "columns": ["placed_at", "shipped_at"], "params": {"operator": "<="}, "rationale": "ordering"},
        {"type": "range", "columns": ["order_id"], "params": {"min": 1}, "rationale": "ids are positive"},
        {"type": "not_null", "columns": ["ghost"], "params": {}, "rationale": "unknown column"},
        {"type": "regex", "columns": ["status"], "params": {"pattern": "([unclosed"}, "rationale": "bad regex"},
        {"type": "bogus", "columns": ["status"], "params": {}, "rationale": "unknown type"},
        {"type": "range", "columns": ["order_id"], "params": {}, "rationale": "missing bounds"},
        "not even a dict",
    ]}
    llm = FakeLLM(contract_reply(), proposed)
    rules = RuleService(llm).analyze_documents([SourceDoc("c.md", CONTRACT)]).ruleset.rules
    llm_rules = [r for r in rules if "llm" in r.evidence]
    kept = sorted((r.type.value, tuple(r.columns)) for r in llm_rules)
    assert kept == [("compare", ("placed_at", "shipped_at")), ("range", ("order_id",))]
    assert all(r.confidence <= 0.6 and r.tier is Tier.INFERRED for r in llm_rules)


def test_enrichment_failure_keeps_deterministic_rules_and_reports_it(all_docs):
    llm = FakeLLM(RuntimeError("rate limited"))
    analysis = RuleService(llm).analyze_documents(all_docs)  # all deterministic; only enrichment calls the LLM
    assert analysis.ruleset.rules and not analysis.ruleset.llm_used
    assert any(f.kind == "llm_enrichment_failed" for f in analysis.ruleset.findings)


def test_sensitive_column_values_are_not_sent_to_the_llm(all_docs):
    llm = FakeLLM({"rules": []})
    RuleService(llm).analyze_documents(all_docs)
    prompt = llm.calls[0][1]
    assert "1920-01-01" not in prompt  # date_of_birth is classified sensitive: min/max withheld
    assert "ACTIVE" in prompt  # non-sensitive status values are fine


def test_no_llm_and_unknown_format_gives_actionable_error():
    import pytest
    from dqms.service import NoMetadataError

    with pytest.raises(NoMetadataError, match="LLM"):
        RuleService().analyze_documents([SourceDoc("contract.md", CONTRACT)])
