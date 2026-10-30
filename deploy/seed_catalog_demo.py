#!/usr/bin/env python3
"""Build a BigQuery demo dataset together with a Knowledge Catalog (Dataplex) around it.

Creates, in your project:
  * a BigQuery dataset with four related tables (customers, products, orders, order_items):
    descriptions, labels, primary/foreign keys, an ARRAY column, and a few deliberate data-quality
    problems for dqms to find (listed at the end of the run and in the manifest);
  * two aspect types (table governance, column semantics) and aspects attached to every table
    and column;
  * a business glossary with categories and terms (the term for each column is also named in the
    column's semantics aspect and description, so it is readable even where term links are not);
  * optionally a policy-tag taxonomy applied to sensitive columns.

    python deploy/seed_catalog_demo.py --project my-project            # build everything
    python deploy/seed_catalog_demo.py --project my-project --dry-run  # print the plan, touch nothing
    python deploy/seed_catalog_demo.py --project my-project --teardown # remove what this created

Needs `pip install google-cloud-bigquery` and credentials (`gcloud auth application-default login`
or Cloud Shell), plus the Dataplex API: `gcloud services enable dataplex.googleapis.com`.
The data is synthetic; nothing is read from your own tables. Re-running rebuilds the tables.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

PREFIX = "dqms-demo"
GOVERNANCE = f"{PREFIX}-governance"
SEMANTICS = f"{PREFIX}-column-semantics"
GLOSSARY = f"{PREFIX}-glossary"
TAXONOMY = f"{PREFIX}-classification"
DATAPLEX = "https://dataplex.googleapis.com/v1"
DATACATALOG = "https://datacatalog.googleapis.com/v1"

# ---------------------------------------------------------------- the catalog definition


@dataclass(frozen=True)
class Col:
    name: str
    type: str
    description: str
    mode: str = "NULLABLE"
    semantic: str | None = None  # semantic_type aspect field
    classification: str = "internal"  # public | internal | confidential | restricted
    term: str | None = None  # glossary term (display name)
    rule: str | None = None  # business rule, free text
    valid_values: str | None = None  # comma separated


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    domain: str
    owner_team: str
    steward: str
    criticality: str
    refresh: str
    retention_days: int
    contains_pii: bool
    sla: str
    primary_key: tuple[str, ...]
    columns: tuple[Col, ...]
    foreign_keys: tuple[tuple[str, str, str], ...] = ()  # (column, ref table, ref column)


TABLES: tuple[Table, ...] = (
    Table(
        "customers", "One row per registered customer.", "customer", "crm-platform", "dana.reyes", "high", "daily", 2555,
        True, "Loaded by 06:00 UTC; customer_id is unique and never reused.", ("customer_id",),
        (
            Col("customer_id", "STRING", "Unique customer identifier, format C000123.", "REQUIRED", "identifier", "internal",
                "Customer ID", "Unique, never reused"),
            Col("email", "STRING", "Primary contact email address.", semantic="email", classification="confidential",
                term="Customer Email", rule="Must be a valid email address"),
            Col("full_name", "STRING", "Customer's full legal name.", classification="confidential"),
            Col("country_code", "STRING", "ISO 3166-1 alpha-2 country code.", semantic="country_code", classification="public",
                term="Country Code", valid_values="US,CA,GB,DE,FR,IN,AU"),
            Col("signup_date", "DATE", "Date the customer registered.", semantic="date", term="Signup Date",
                rule="Cannot be in the future"),
            Col("loyalty_tier", "STRING", "Loyalty programme tier.", semantic="category", term="Loyalty Tier",
                valid_values="bronze,silver,gold,platinum"),
            Col("marketing_opt_in", "BOOL", "Whether the customer consented to marketing.", semantic="flag"),
            Col("tags", "ARRAY<STRING>", "Free-form segmentation tags (repeated field)."),
        ),
    ),
    Table(
        "products", "Product catalogue; one row per SKU.", "product", "merch-platform", "li.wei", "medium", "daily", 1825,
        False, "Prices are current as of the last nightly load.", ("sku",),
        (
            Col("sku", "STRING", "Stock keeping unit, format SKU-0001.", "REQUIRED", "identifier", "internal", "SKU",
                "Unique across the catalogue"),
            Col("product_name", "STRING", "Display name of the product.", "REQUIRED"),
            Col("category", "STRING", "Merchandising category.", semantic="category", classification="public",
                valid_values="apparel,electronics,home,outdoor,toys"),
            Col("unit_price", "NUMERIC", "List price in the product currency.", semantic="currency_amount", term="Unit Price",
                rule="Must be greater than 0"),
            Col("unit_cost", "NUMERIC", "Landed cost per unit.", semantic="currency_amount", classification="confidential",
                rule="Must not exceed unit_price"),
            Col("active", "BOOL", "False once the product is discontinued.", semantic="flag"),
        ),
    ),
    Table(
        "orders", "One row per customer order.", "sales", "commerce-data", "sam.okafor", "high", "hourly", 2555,
        False, "Orders appear within 15 minutes of checkout.", ("order_id",),
        (
            Col("order_id", "STRING", "Unique order identifier, format O0000123.", "REQUIRED", "identifier", "internal",
                "Order ID", "Unique"),
            Col("customer_id", "STRING", "Customer who placed the order.", "REQUIRED", "identifier", "internal", "Customer ID",
                "Must exist in customers"),
            Col("order_ts", "TIMESTAMP", "When the order was placed (UTC).", "REQUIRED", "timestamp", term="Order Timestamp",
                rule="Cannot be in the future"),
            Col("status", "STRING", "Fulfilment status.", "REQUIRED", "status", "internal", "Order Status",
                valid_values="placed,paid,shipped,delivered,cancelled,refunded"),
            Col("currency", "STRING", "ISO 4217 currency code.", semantic="currency_code", classification="public",
                valid_values="USD,EUR,GBP"),
            Col("order_total", "NUMERIC", "Order value after discounts, in `currency`.", semantic="currency_amount",
                term="Net Revenue", rule="Must be zero or greater"),
            Col("shipped_ts", "TIMESTAMP", "When the order left the warehouse (UTC).", semantic="timestamp",
                rule="Must not be before order_ts"),
        ),
        (("customer_id", "customers", "customer_id"),),
    ),
    Table(
        "order_items", "One row per product line on an order.", "sales", "commerce-data", "sam.okafor", "medium", "hourly",
        2555, False, "Loaded together with orders.", ("order_id", "line_no"),
        (
            Col("order_id", "STRING", "Order this line belongs to.", "REQUIRED", "identifier", "internal", "Order ID",
                "Must exist in orders"),
            Col("line_no", "INT64", "Line number within the order, starting at 1.", "REQUIRED", "identifier"),
            Col("sku", "STRING", "Product sold on this line.", "REQUIRED", "identifier", "internal", "SKU",
                "Must exist in products"),
            Col("quantity", "INT64", "Units sold.", "REQUIRED", "quantity", term="Quantity", rule="Must be at least 1"),
            Col("unit_price", "NUMERIC", "Price per unit at time of sale.", semantic="currency_amount",
                term="Unit Price", rule="Must be greater than 0"),
            Col("discount_pct", "FLOAT64", "Discount applied, as a fraction between 0 and 1.", semantic="percentage",
                term="Discount", rule="Between 0 and 1"),
        ),
        (("order_id", "orders", "order_id"), ("sku", "products", "sku")),
    ),
)

SEMANTIC_TYPES = ("identifier", "email", "country_code", "currency_code", "currency_amount", "quantity", "percentage",
                  "status", "category", "flag", "date", "timestamp")
CLASSIFICATIONS = ("public", "internal", "confidential", "restricted")

GOVERNANCE_FIELDS = [
    ("data_domain", "enum", "Business domain that owns the table", True, ("customer", "product", "sales", "finance")),
    ("owner_team", "string", "Team accountable for the table", True, ()),
    ("data_steward", "string", "Person who answers questions about the data", False, ()),
    ("criticality", "enum", "How damaging bad data in this table is", True, ("low", "medium", "high")),
    ("refresh_cadence", "enum", "How often the table is refreshed", False, ("hourly", "daily", "weekly")),
    ("retention_days", "int", "How long rows are kept", False, ()),
    ("contains_pii", "bool", "Whether the table holds personal data", True, ()),
    ("sla", "string", "Freshness / quality commitment", False, ()),
]
SEMANTICS_FIELDS = [
    ("semantic_type", "enum", "What the column means, beyond its datatype", False, SEMANTIC_TYPES),
    ("classification", "enum", "Data sensitivity", True, CLASSIFICATIONS),
    ("glossary_term", "string", "Business glossary term this column implements", False, ()),
    ("business_rule", "string", "Rule the values are expected to follow", False, ()),
    ("valid_values", "string", "Comma separated list of allowed values", False, ()),
]

GLOSSARY_CATEGORIES = {
    "Customer": "Terms about the people who buy from us.",
    "Sales": "Terms about orders and revenue.",
    "Product": "Terms about what we sell.",
}
GLOSSARY_TERMS = {  # term -> (category, definition)
    "Customer ID": ("Customer", "The permanent identifier of a customer. Assigned once at signup and never reused."),
    "Customer Email": ("Customer", "The address we use to contact a customer. Personal data."),
    "Country Code": ("Customer", "Two-letter ISO 3166-1 code of the customer's country of residence."),
    "Signup Date": ("Customer", "The calendar day a customer first registered."),
    "Loyalty Tier": ("Customer", "A customer's standing in the loyalty programme: bronze, silver, gold or platinum."),
    "Order ID": ("Sales", "The permanent identifier of an order."),
    "Order Timestamp": ("Sales", "The moment checkout completed, in UTC."),
    "Order Status": ("Sales", "Where an order is in fulfilment: placed, paid, shipped, delivered, cancelled or refunded."),
    "Net Revenue": ("Sales", "Order value after discounts, before tax and shipping."),
    "Quantity": ("Sales", "Number of units of a product on an order line. At least one."),
    "Discount": ("Sales", "Share of the list price removed from a line, expressed as a fraction from 0 to 1."),
    "SKU": ("Product", "Stock keeping unit: the identifier of a sellable product."),
    "Unit Price": ("Product", "Price of one unit of a product. Always greater than zero."),
}

# ---------------------------------------------------------------- data


@dataclass
class Generated:
    rows: dict[str, list[dict[str, Any]]]
    issues: list[dict[str, Any]] = field(default_factory=list)


def generate(now: datetime | None = None, seed: int = 7) -> Generated:
    """Synthetic rows with deliberate problems. Deterministic for a given `now` and seed."""
    now = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    rng = random.Random(seed)
    out = Generated(rows={})

    def issue(table: str, column: str, problem: str, count: int, rule: str) -> None:
        out.issues.append({"table": table, "column": column, "problem": problem, "rows": count, "expected_rule": rule})

    money = lambda x: str(Decimal(str(round(x, 2))).quantize(Decimal("0.01")))  # noqa: E731
    ts = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731

    countries = ["US", "CA", "GB", "DE", "FR", "IN", "AU"]
    tiers = ["bronze", "silver", "gold", "platinum"]
    customers = []
    for i in range(1, 201):
        customers.append({
            "customer_id": f"C{i:06d}", "email": f"user{i}@example.com", "full_name": f"Customer {i}",
            "country_code": rng.choice(countries), "signup_date": (now.date() - timedelta(days=rng.randint(30, 1500))).isoformat(),
            "loyalty_tier": rng.choices(tiers, [50, 30, 15, 5])[0], "marketing_opt_in": rng.random() < 0.6,
            "tags": rng.sample(["vip", "newsletter", "wholesale", "returns-risk", "beta"], rng.randint(0, 3)),
        })
    for c in customers[:3]:
        c["email"] = None
    for c, bad in zip(customers[3:5], ("not-an-email", "missing-at.example.com")):
        c["email"] = bad
    customers[5]["signup_date"] = (now.date() + timedelta(days=45)).isoformat()
    customers[6]["country_code"] = "ZZ"
    customers.append({**customers[7]})  # duplicate customer_id
    issue("customers", "email", "null email", 3, "not_null / completeness")
    issue("customers", "email", "malformed email", 2, "regex (email)")
    issue("customers", "signup_date", "signup in the future", 1, "not_future")
    issue("customers", "country_code", "country code outside the allowed set", 1, "accepted_values")
    issue("customers", "customer_id", "duplicated key", 1, "unique")

    categories = ["apparel", "electronics", "home", "outdoor", "toys"]
    products = []
    for i in range(1, 41):
        price = rng.uniform(5, 300)
        products.append({"sku": f"SKU-{i:04d}", "product_name": f"Product {i}", "category": rng.choice(categories),
                         "unit_price": money(price), "unit_cost": money(price * rng.uniform(0.3, 0.7)), "active": rng.random() < 0.9})
    products[0]["unit_price"] = money(0)
    products[1]["unit_cost"] = money(float(products[1]["unit_price"]) + 20)
    issue("products", "unit_price", "price of zero", 1, "range (> 0)")
    issue("products", "unit_cost", "cost above price", 1, "compare (unit_cost <= unit_price)")

    orders, items = [], []
    for i in range(1, 601):
        placed = now - timedelta(days=rng.randint(1, 365), seconds=rng.randint(0, 86399))
        status = rng.choices(["placed", "paid", "shipped", "delivered", "cancelled", "refunded"], [5, 10, 15, 55, 10, 5])[0]
        shipped = min(placed + timedelta(hours=rng.randint(4, 120)), now - timedelta(minutes=1)) if status in ("shipped", "delivered") else None
        total = Decimal("0")
        for line in range(1, rng.randint(1, 4) + 1):
            p, qty, disc = rng.choice(products), rng.randint(1, 5), rng.choice([0, 0, 0, 0.05, 0.1, 0.2])
            price = Decimal(p["unit_price"])
            items.append({"order_id": f"O{i:07d}", "line_no": line, "sku": p["sku"], "quantity": qty,
                          "unit_price": str(price), "discount_pct": disc})
            total += price * qty * Decimal(str(1 - disc))
        orders.append({"order_id": f"O{i:07d}", "customer_id": rng.choice(customers[:200])["customer_id"],
                       "order_ts": ts(placed), "status": status, "currency": rng.choices(["USD", "EUR", "GBP"], [70, 20, 10])[0],
                       "order_total": str(total.quantize(Decimal("0.01"))), "shipped_ts": ts(shipped) if shipped else None})
    for o, days in ((orders[10], 30), (orders[11], 2)):  # unshipped, so each planted problem stays a single problem
        o["order_ts"], o["status"], o["shipped_ts"] = ts(now + timedelta(days=days)), "placed", None
    for o in orders[20:22]:
        o["status"], o["shipped_ts"] = "delivered", ts(datetime.strptime(o["order_ts"], "%Y-%m-%dT%H:%M:%SZ") - timedelta(days=1))
    orders[30]["customer_id"] = "C999999"
    orders[31]["status"] = "SHIPPD"
    orders[32]["order_total"] = "-15.00"
    issue("orders", "order_ts", "order placed in the future", 2, "not_future")
    issue("orders", "shipped_ts", "shipped before it was ordered", 2, "compare (shipped_ts >= order_ts)")
    issue("orders", "customer_id", "customer that does not exist", 1, "foreign_key -> customers")
    issue("orders", "status", "misspelt status", 1, "accepted_values")
    issue("orders", "order_total", "negative total", 1, "range (>= 0)")

    items[5]["quantity"] = 0
    items[6]["quantity"] = -2
    items[7]["discount_pct"] = 1.5
    items[8]["sku"] = "SKU-9999"
    issue("order_items", "quantity", "quantity of zero or less", 2, "range (>= 1)")
    issue("order_items", "discount_pct", "discount above 100%", 1, "range (0..1)")
    issue("order_items", "sku", "product that does not exist", 1, "foreign_key -> products")

    out.rows = {"customers": customers, "products": products, "orders": orders, "order_items": items}
    return out


# ---------------------------------------------------------------- payload builders (pure)


def dataplex_location(bq_location: str) -> str:
    return bq_location.lower()  # BigQuery 'US' / 'EU' / 'asia-northeast1' -> Dataplex 'us' / 'eu' / same


def aspect_type_body(aspect_id: str, display: str, description: str, fields: list[tuple]) -> dict[str, Any]:
    record = []
    for index, (name, ftype, desc, required, enum) in enumerate(fields, start=1):
        entry: dict[str, Any] = {
            "name": name, "type": ftype, "index": index,
            "annotations": {"displayName": name.replace("_", " ").capitalize(), "description": desc},
            "constraints": {"required": required},
        }
        if ftype == "enum":
            entry["enumValues"] = [{"name": v, "index": i} for i, v in enumerate(enum, start=1)]
        record.append(entry)
    return {"displayName": display, "description": description,
            "metadataTemplate": {"name": aspect_id, "type": "record", "recordFields": record}}


def entry_name(project: str, bq_location: str, dataset: str, table: str) -> str:
    return (f"projects/{project}/locations/{dataplex_location(bq_location)}/entryGroups/@bigquery/entries/"
            f"bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}")


def aspect_key(project: str, bq_location: str, aspect_id: str, path: str = "") -> str:
    base = f"{project}.{dataplex_location(bq_location)}.{aspect_id}"
    return f"{base}@{path}" if path else base


def table_aspects(project: str, bq_location: str, table: Table) -> dict[str, dict[str, Any]]:
    """Every aspect for one table entry, keyed the way the Dataplex API expects."""
    type_ref = lambda a: f"projects/{project}/locations/{dataplex_location(bq_location)}/aspectTypes/{a}"  # noqa: E731
    aspects = {aspect_key(project, bq_location, GOVERNANCE): {
        "aspectType": type_ref(GOVERNANCE),
        "data": {"data_domain": table.domain, "owner_team": table.owner_team, "data_steward": table.steward,
                 "criticality": table.criticality, "refresh_cadence": table.refresh, "retention_days": table.retention_days,
                 "contains_pii": table.contains_pii, "sla": table.sla}}}
    for col in table.columns:
        data = {"classification": col.classification}
        for key, value in (("semantic_type", col.semantic), ("glossary_term", col.term),
                           ("business_rule", col.rule), ("valid_values", col.valid_values)):
            if value:
                data[key] = value
        aspects[aspect_key(project, bq_location, SEMANTICS, f"Schema.{col.name}")] = {
            "aspectType": type_ref(SEMANTICS), "data": data}
    return aspects


def column_description(col: Col) -> str:
    """BigQuery description: also carries the glossary term and rule so tools that ignore aspects still see them."""
    parts = [col.description]
    if col.term:
        parts.append(f"Glossary: {col.term}.")
    if col.valid_values:
        parts.append(f"Allowed values: {col.valid_values}.")
    return " ".join(parts)


def constraint_ddl(project: str, dataset: str, table: Table) -> list[str]:
    fq = lambda t: f"`{project}.{dataset}.{t}`"  # noqa: E731
    statements = [f"ALTER TABLE {fq(table.name)} ADD PRIMARY KEY ({', '.join(table.primary_key)}) NOT ENFORCED"]
    for column, ref_table, ref_column in table.foreign_keys:
        statements.append(
            f"ALTER TABLE {fq(table.name)} ADD CONSTRAINT fk_{table.name}_{column} FOREIGN KEY ({column}) "
            f"REFERENCES {fq(ref_table)}({ref_column}) NOT ENFORCED")
    return statements


def plan(project: str, dataset: str, location: str, generated: Generated, glossary: bool, policy_tags: bool) -> list[str]:
    lines = [f"BigQuery dataset {project}.{dataset} in {location}"]
    for t in TABLES:
        n_aspects = len(table_aspects(project, location, t))
        lines.append(f"  table {t.name}: {len(generated.rows[t.name])} rows, {len(t.columns)} columns, "
                     f"{len(t.foreign_keys)} foreign key(s), {n_aspects} aspects")
    lines.append(f"Aspect types: {GOVERNANCE}, {SEMANTICS} ({dataplex_location(location)})")
    if glossary:
        lines.append(f"Glossary {GLOSSARY}: {len(GLOSSARY_CATEGORIES)} categories, {len(GLOSSARY_TERMS)} terms")
    if policy_tags:
        lines.append(f"Policy-tag taxonomy {TAXONOMY}: applied to confidential/restricted columns")
    return lines


def bq_type(col: Col) -> tuple[str, str]:
    """('INT64', 'NULLABLE') or, for ARRAY<T>, ('STRING', 'REPEATED')."""
    if col.type.startswith("ARRAY<"):
        return col.type[6:-1], "REPEATED"
    return col.type, col.mode


# ---------------------------------------------------------------- Google Cloud calls


class Failures:
    def __init__(self) -> None:
        self.items: list[str] = []

    def add(self, step: str, detail: str) -> None:
        self.items.append(f"{step}: {detail}")
        print(f"  ! {step} failed: {detail}", file=sys.stderr)


class Dataplex:
    """Thin REST client; the catalog APIs are called directly so no extra SDK is needed."""

    def __init__(self, project: str):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.session = AuthorizedSession(credentials)
        self.project = project

    def call(self, method: str, url: str, **kw: Any) -> tuple[int, dict[str, Any]]:
        response = self.session.request(method, url, headers={"x-goog-user-project": self.project}, timeout=60, **kw)
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {"error": {"message": response.text[:300]}}
        return response.status_code, body

    def wait(self, body: dict[str, Any], base: str = DATAPLEX, timeout: int = 180) -> dict[str, Any]:
        """Follow a long-running operation, if the response is one."""
        name = body.get("name", "")
        if "/operations/" not in name:
            return body
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, op = self.call("GET", f"{base}/{name}")
            if status == 200 and op.get("done"):
                if "error" in op:
                    raise RuntimeError(op["error"].get("message", "operation failed"))
                return op.get("response", op)
            time.sleep(3)
        raise RuntimeError(f"timed out waiting for {name}")

    def create(self, url: str, body: dict[str, Any], label: str, failures: Failures) -> dict[str, Any] | None:
        status, result = self.call("POST", url, json=body)
        if status == 409:
            print(f"  = {label} already exists")
            return {}
        if status >= 400:
            failures.add(label, _api_error(status, result))
            return None
        try:
            created = self.wait(result)
        except RuntimeError as error:
            failures.add(label, str(error))
            return None
        print(f"  + {label}")
        return created


def _api_error(status: int, body: dict[str, Any]) -> str:
    message = (body.get("error") or {}).get("message", str(body))[:300]
    hint = " (enable it: gcloud services enable dataplex.googleapis.com)" if "has not been used" in message or "disabled" in message else ""
    return f"HTTP {status}: {message}{hint}"


def build_bigquery(project: str, dataset: str, location: str, generated: Generated,
                   policy_tag_names: dict[str, str]) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = location
    ds.description = "Synthetic demo data for dqms, with a Knowledge Catalog (glossary, aspects) around it."
    ds.labels = {"purpose": "dqms-demo"}
    client.create_dataset(ds, exists_ok=True)
    print(f"  + dataset {project}.{dataset}")

    for table in TABLES:
        fields = []
        for col in table.columns:
            kind, mode = bq_type(col)
            tags = policy_tag_names.get(col.classification) if col.classification in ("confidential", "restricted") else None
            fields.append(bigquery.SchemaField(
                col.name, kind, mode=mode, description=column_description(col),
                policy_tags=bigquery.PolicyTagList([tags]) if tags else None))
        ref = f"{project}.{dataset}.{table.name}"
        client.delete_table(ref, not_found_ok=True)
        obj = bigquery.Table(ref, schema=fields)
        obj.description = table.description
        obj.labels = {"domain": table.domain, "owner": table.owner_team.replace(".", "-"), "purpose": "dqms-demo"}
        client.create_table(obj)
        job = client.load_table_from_json(generated.rows[table.name], ref, job_config=bigquery.LoadJobConfig(
            schema=fields, write_disposition="WRITE_TRUNCATE"))
        job.result()
        print(f"  + table {table.name} ({len(generated.rows[table.name])} rows)")

    for table in TABLES:  # keys last: foreign keys need their referenced tables to exist
        for statement in constraint_ddl(project, dataset, table):
            client.query(statement).result()
    print("  + primary and foreign keys")


def build_aspect_types(api: Dataplex, project: str, location: str, failures: Failures) -> bool:
    base = f"{DATAPLEX}/projects/{project}/locations/{dataplex_location(location)}/aspectTypes"
    ok = True
    for aspect_id, display, description, fields in (
        (GOVERNANCE, "Table governance", "Ownership, criticality and freshness of a table.", GOVERNANCE_FIELDS),
        (SEMANTICS, "Column semantics", "What a column means, how sensitive it is, and the rule it should follow.", SEMANTICS_FIELDS),
    ):
        result = api.create(f"{base}?aspectTypeId={aspect_id}", aspect_type_body(aspect_id, display, description, fields),
                            f"aspect type {aspect_id}", failures)
        ok = ok and result is not None
    return ok


def attach_aspects(api: Dataplex, project: str, dataset: str, location: str, failures: Failures) -> None:
    for table in TABLES:
        aspects = table_aspects(project, location, table)
        name = entry_name(project, location, dataset, table.name)
        url = f"{DATAPLEX}/{name}?updateMask=aspects&aspectKeys=" + ",".join(aspects)
        # BigQuery metadata reaches the catalog a little after the table is created.
        for attempt in range(12):
            status, body = api.call("PATCH", url, json={"aspects": aspects})
            if status != 404:
                break
            time.sleep(10)
        if status >= 400:
            failures.add(f"aspects on {table.name}", _api_error(status, body))
        else:
            print(f"  + {len(aspects)} aspects on {table.name}")


def build_glossary(api: Dataplex, project: str, location: str, failures: Failures) -> None:
    root = f"projects/{project}/locations/{dataplex_location(location)}"
    glossary = f"{root}/glossaries/{GLOSSARY}"
    if api.create(f"{DATAPLEX}/{root}/glossaries?glossaryId={GLOSSARY}",
                  {"displayName": "DQMS demo glossary", "description": "Business terms used by the demo dataset."},
                  f"glossary {GLOSSARY}", failures) is None:
        return
    slugs = {}
    for name, description in GLOSSARY_CATEGORIES.items():
        slug = name.lower()
        slugs[name] = f"{glossary}/categories/{slug}"
        api.create(f"{DATAPLEX}/{glossary}/categories?categoryId={slug}",
                   {"displayName": name, "description": description, "parent": glossary}, f"category {name}", failures)
    for term, (category, definition) in GLOSSARY_TERMS.items():
        term_id = term.lower().replace(" ", "-")
        api.create(f"{DATAPLEX}/{glossary}/terms?termId={term_id}",
                   {"displayName": term, "description": definition, "parent": slugs[category]}, f"term {term}", failures)


def build_policy_tags(project: str, location: str, failures: Failures) -> dict[str, str]:
    """Taxonomy with one policy tag per sensitive classification; returns {classification: policy tag resource}."""
    api = Dataplex(project)
    parent = f"{DATACATALOG}/projects/{project}/locations/{location.lower()}"
    status, existing = api.call("GET", f"{parent}/taxonomies")
    taxonomy = next((t for t in existing.get("taxonomies", []) if t.get("displayName") == TAXONOMY), None) if status == 200 else None
    if taxonomy is None:
        status, taxonomy = api.call("POST", f"{parent}/taxonomies", json={
            "displayName": TAXONOMY, "description": "Sensitivity classes for the dqms demo.",
            "activatedPolicyTypes": ["FINE_GRAINED_ACCESS_CONTROL"]})
        if status >= 400:
            failures.add("policy taxonomy", _api_error(status, taxonomy))
            return {}
        print(f"  + taxonomy {TAXONOMY}")
    tags: dict[str, str] = {}
    status, listed = api.call("GET", f"{DATACATALOG}/{taxonomy['name']}/policyTags")
    have = {p["displayName"]: p["name"] for p in listed.get("policyTags", [])} if status == 200 else {}
    for cls in ("confidential", "restricted"):
        if cls in have:
            tags[cls] = have[cls]
            continue
        status, created = api.call("POST", f"{DATACATALOG}/{taxonomy['name']}/policyTags", json={"displayName": cls})
        if status >= 400:
            failures.add(f"policy tag {cls}", _api_error(status, created))
        else:
            tags[cls] = created["name"]
            print(f"  + policy tag {cls}")
    return tags


def teardown(project: str, dataset: str, location: str) -> None:
    from google.cloud import bigquery

    failures = Failures()
    bigquery.Client(project=project).delete_dataset(f"{project}.{dataset}", delete_contents=True, not_found_ok=True)
    print(f"  - dataset {project}.{dataset}")
    api = Dataplex(project)
    root = f"{DATAPLEX}/projects/{project}/locations/{dataplex_location(location)}"
    for label, url in ((f"aspect type {GOVERNANCE}", f"{root}/aspectTypes/{GOVERNANCE}"),
                       (f"aspect type {SEMANTICS}", f"{root}/aspectTypes/{SEMANTICS}"),
                       (f"glossary {GLOSSARY}", f"{root}/glossaries/{GLOSSARY}?force=true")):
        status, body = api.call("DELETE", url)
        if status in (200, 404):
            print(f"  - {label}" if status == 200 else f"  = {label} not found")
        else:
            failures.add(label, _api_error(status, body))
    print("Policy-tag taxonomies are left in place; delete them in the console if you created one.")
    if failures.items:
        sys.exit(1)


# ---------------------------------------------------------------- entry point


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build a BigQuery demo dataset with a Knowledge Catalog around it.")
    p.add_argument("--project", required=True, help="GCP project to create everything in")
    p.add_argument("--dataset", default="dqms_demo", help="BigQuery dataset name (default: dqms_demo)")
    p.add_argument("--location", default="US", help="BigQuery location (default: US)")
    p.add_argument("--skip-glossary", action="store_true", help="do not create the business glossary")
    p.add_argument("--policy-tags", action="store_true",
                   help="also create policy tags on confidential columns (profiling those columns then needs Fine-Grained Reader)")
    p.add_argument("--manifest", default="dqms_demo_manifest.json", help="where to write what was built")
    p.add_argument("--dry-run", action="store_true", help="print the plan; create nothing")
    p.add_argument("--teardown", action="store_true", help="delete the dataset, aspect types and glossary")
    args = p.parse_args(argv)

    if args.teardown:
        teardown(args.project, args.dataset, args.location)
        return 0

    generated = generate()
    steps = plan(args.project, args.dataset, args.location, generated, not args.skip_glossary, args.policy_tags)
    print("\n".join(steps))
    if args.dry_run:
        print("\nKnown data problems the rules should catch:")
        _print_issues(generated)
        return 0

    failures = Failures()
    print("\nBuilding:")
    api = Dataplex(args.project)
    tags = build_policy_tags(args.project, args.location, failures) if args.policy_tags else {}
    build_bigquery(args.project, args.dataset, args.location, generated, tags)
    if build_aspect_types(api, args.project, args.location, failures):
        attach_aspects(api, args.project, args.dataset, args.location, failures)
    else:
        failures.add("aspects", "skipped because the aspect types could not be created")
    if not args.skip_glossary:
        build_glossary(api, args.project, args.location, failures)

    manifest = {"project": args.project, "dataset": args.dataset, "location": args.location,
                "source_uri": f"bigquery://{args.project}/{args.dataset}", "aspect_types": [GOVERNANCE, SEMANTICS],
                "glossary": None if args.skip_glossary else GLOSSARY, "known_issues": generated.issues,
                "failures": failures.items}
    with open(args.manifest, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print("\nKnown data problems the rules should catch:")
    _print_issues(generated)
    print(f"\nAnalyze it:  dqms analyze bigquery://{args.project}/{args.dataset}   (manifest: {args.manifest})")
    if failures.items:
        print(f"\n{len(failures.items)} catalog step(s) failed; the data was still built. See messages above.", file=sys.stderr)
        return 1
    return 0


def _print_issues(generated: Generated) -> None:
    for i in generated.issues:
        print(f"  {i['table']}.{i['column']}: {i['problem']} ({i['rows']} row(s)) -> {i['expected_rule']}")


if __name__ == "__main__":
    sys.exit(main())
