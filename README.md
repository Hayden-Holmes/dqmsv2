# dqms: metadata → validation rules

A service your pipelines call to turn *any* metadata (files in any format, or live cloud datasets) into platform-neutral validation rules, each with the evidence behind it. A small web page is included for trying things out; the product is the API, the Python client and the CLI.

## Using it from a pipeline

**Python** (an Airflow task, a Dagster op, a notebook):

```python
from dqms.client import DqmsClient

client = DqmsClient("https://dqms.internal.example.com", api_key=os.environ["DQMS_API_KEY"])
results = client.analyze_sources(
    ["bigquery://my-proj/sales/", "gs://my-bucket/exports/"],     # folders/datasets expand to every table/file
    profile={"enabled": True},                                      # aggregate statistics, never raw rows
    idempotency_key=f"nightly-{run_date}",                          # a retried task does not start a second job
)
for r in results:
    print(r.source, r.ruleset.fingerprint, len(r.ruleset.rules), "findings:", len(r.ruleset.findings))
```

**CLI** (a CI step; the exit code is the contract):

```bash
dqms analyze bigquery://my-proj/sales/orders --url $DQMS_URL --profile --out rules/orders.json --fail-on findings
dqms diff rules/orders.json rules/orders.new.json --fail-on-breaking
```

| Exit code | Meaning |
|---|---|
| 0 | ok |
| 1 | gate tripped: `--fail-on findings` (profile contradicts a rule, sources disagree), `--fail-on-change` / `--fail-on-breaking` (drift) |
| 2 | operational failure: bad input, auth, a source could not be read |

Without `--url` the CLI runs in-process using *your* cloud credentials, which is convenient for a laptop or a one-off job.

**Source URIs** name any dataset as a plain string:

| URI | Container form (expands to its tables/files) |
|---|---|
| `bigquery://project/dataset/table` | `bigquery://project/dataset`, `bigquery://project` |
| `snowflake://account/database/schema/table` | `…/database`, `…/database/schema` |
| `databricks://workspace-host/catalog/schema/table` | `…/catalog`, `…/catalog/schema` |
| `gs://bucket/path/file.parquet` · `s3://bucket/key.csv` · `az://account/container/path/file.jsonl` | any prefix ending in `/` |

Files: CSV/TSV, JSONL and Parquet. Parquet statistics come from the footer alone (exact, no scan).

## What you get back

A `RuleSet`: `schema_version`, `dataset`, `fingerprint`, `rules[]`, `findings[]`. Each rule has a stable `id`, a structured `type` + `params`, the `tier` it came from, `confidence`, `severity` and its `evidence`.

- **Tiers:** *declared* (schema states it) and *documented* (descriptions state it) are errors; *semantic* and *inferred* are warnings; *observed* (profile suggests it) is info. Profiling shows what data *does*, not what it *should* do, so it never outranks a declared rule.
- **Sampled profiles are honest.** Above `max_rows` (or `max_bytes` for files) the profile is a sample and is marked `sampled`; the engine then refuses to claim uniqueness, true min/max or a closed value set from it.
- **`fingerprint`** hashes what the rules *check*, not their wording. Compare it between runs to notice that a source changed; `dqms diff` / `POST /v1/diff` says what changed and whether it is *breaking* (an authoritative rule was removed or altered, or a new required rule appeared).
- **Findings** are disagreements: two sources conflict, or a profile contradicts a declared/documented rule (nulls in a NOT NULL column, values outside an enum).

## API

All `/v1` routes need your key as `X-API-Key: <key>` (`Authorization: Bearer <key>` also works, but gateways and proxies such as Cloud Shell Web Preview or Cloud Run IAM may claim that header, so prefer `X-API-Key`). OpenAPI is served at `/docs`.

| Endpoint | Purpose |
|---|---|
| `GET /v1/explorer/roots` · `POST /v1/explorer/browse` | the UI's table explorer: where to start (derived from your key's allowlist) and one level of the hierarchy. Lists names and sizes only; **runs no queries and reads no files**; entries your key cannot read are omitted |
| `POST /v1/plan` | **dry run**: exactly which datasets a job would touch, with sizes where known; reads no data |
| `POST /v1/jobs` | submit sources (+ optional `scope`) → `202` + job. Header `Idempotency-Key` makes retries safe |
| `GET /v1/jobs/{id}?wait=30` | poll, or long-poll up to 60 s. Status: `queued · running · succeeded · partial · failed`; one result per dataset |
| `POST /v1/sources/analyze` | one dataset, synchronous |
| `POST /v1/sources/discover` | list the datasets inside a container (filtered to what your key may read) |
| `POST /v1/analyze/batch` | **data files + their metadata, from any platform**: tables, whole datasets, or metadata describing several tables (see below) |
| `POST /v1/analyze` · `/v1/analyze/json` | analyze one table's uploaded metadata documents (any format) |
| `POST /v1/diff` · `POST /v1/export/sql` | drift between rule sets · SQL violation counts (BigQuery, Postgres) |
| `GET /healthz` · `GET /readyz` | liveness (no auth) · readiness (job store usable) |

Metadata documents in any format: deterministic parsers handle DDL, JSON, YAML and CSV shapes with no LLM. Unknown formats can be parsed by an LLM **only if the operator enables `DQMS_LLM_MODE=full`** (see below); otherwise they are reported as unrecognized.

## Security model

- **Callers authenticate with API keys**, stored only as SHA-256 hashes (`dqms keygen --name airflow --sources 'bigquery://my-proj/*'`).
- **Each key has a source allowlist.** The service reads cloud data with *its own* identity, so without this any key holder could profile anything that identity can see. Allowlists are enforced at submission and again on container expansion. Anonymous access (`DQMS_ALLOW_ANONYMOUS=1`, local dev only) never grants source access.
- The service **refuses to start** with no auth configured. An invalid key never falls back to anonymous. Jobs are private to the key that created them (404 otherwise).
- Profiling is **aggregate-only**: no raw rows leave the source; string min/max is never collected; value lists are opt-in. See *Data and LLMs* for what a model can ever see.
- Identifiers are validated before being placed in SQL; generated SQL refuses unsafe names and escapes literals. Cost caps: BigQuery `maximum_bytes_billed`, sampling above `max_rows`, a per-file byte limit, and a per-job dataset cap.
- Webhook callbacks are deliberately **not** offered yet (they need SSRF protection); pipelines poll or long-poll.

## Data and LLMs

The constraint: **actual data never goes to an LLM.** Metadata and profile *statistics* (null rates, min/max, lengths, counts) are fine to send; row values and enumerated value lists are not. It is enforced in code, not left to prompts:

| `DQMS_LLM_MODE` | What can leave the service |
|---|---|
| `off` (**default**) | Nothing. No client is constructed and no LLM SDK is imported. |
| `rules` | Metadata and profile **statistics**: dataset/column names, types, nullability, descriptions, semantic types, classification labels, glossary, tags, and per-column aggregates (null/distinct rates and counts, min/max, lengths). Never data: no enumerated value lists, no patterns, no row content; value lists are stripped from the params of existing rules too. Columns classified sensitive (or covered by a policy tag) keep even their min/max back. |
| `full` | Everything in `rules`, plus unknown-format **metadata documents** (size-capped; refused if they look like row data) so they can be parsed. Choose this only if those documents may leave your boundary. |

How it is enforced:

- **One gate.** Only `LLMGateway` holds a client. It enforces the mode and the *purpose* of each call, refuses oversize payloads (no truncation), redacts emails, SSNs, card numbers, long account numbers, cloud keys and `password=…` strings, and logs every call (purpose, size, hash, redaction counts) **never its content**.
- **Allowlisted payload.** The prompt is built field by field (`rules/llm.py: structural_view`), so a field added to a model later cannot leak by default.
- **Data is not metadata.** On the metadata-only path, a CSV/JSONL/JSON array/markdown table whose header is the columns (`id, name, email`) is recognized as records and refused, rather than having its values turned into "columns" or sent as documents. Data takes a different, explicit route (*Data + metadata uploads*): it is profiled in the service's memory into aggregates and never reaches a model.
- **A request can only narrow** the server's mode (`use_llm=false`), never widen it. `/healthz` reports the effective `llm_mode`. Misconfiguring `DQMS_LLM_MODE` stops startup instead of being guessed at; a generic `BASE_URL` variable no longer enables anything.
- **Tests plant canary values** in value lists, patterns, descriptions and uploaded files and assert none ever appears in a prompt (and that statistics *do*); each control is mutation-checked (removing it makes a test fail).

What code cannot prove: the row-data guard is a heuristic backstop, and redaction is pattern-based. For a hard guarantee, run `off` and also deny the service network egress to any model endpoint (VPC/firewall rule); then it cannot reach one even if misconfigured.

## Data + metadata uploads (any platform, no cloud access)

For data that is not in a cloud warehouse: export a table from anywhere (Oracle, SQL Server, a spreadsheet, a data lake) and upload it with its description.

| You upload | You get |
|---|---|
| `orders.csv` + `orders.sql` | rules for `orders`, from the metadata, checked against the data |
| `orders.csv`, `customers.parquet` + `schema.sql` (describes both tables) | one analysis per table, each paired with its data by file name |
| `schema.sql` / a catalog or information-schema CSV describing several tables | an analysis per table, from metadata alone |
| a data file with no metadata | types and rules inferred from the data alone |

In the page, drop everything into the same box. From code or the CLI:

```bash
dqms analyze orders.csv customers.csv schema.sql --out rules/
curl -X POST "$URL/v1/analyze/batch" -H "X-API-Key: $KEY" -F files=@orders.csv -F files=@schema.sql
```

- **Classified by content, paired by name.** A CSV whose header *describes columns* (`column_name, data_type, …`) is metadata; one whose header *is* the columns is data. `orders.csv` pairs with the table `orders` (case- and schema-insensitive). A single data file with a single table pairs even if the names differ. With several files a name-less match would be a guess, so unmatched files are reported with the table names found; override with `pairs` (`{"file.csv": "table"}`).
- **Metadata is authoritative; data is checked against it.** Types the metadata lacks are inferred from the data; columns only in the data are added and flagged (`column_not_in_metadata`); described columns missing from the data are flagged (`column_missing_from_data`); a column declared numeric/date whose data holds text is flagged (`type_mismatch`).
- **Privacy.** Data is read into memory, summarized (nulls, distinct, min/max, lengths), and discarded; rows are not stored, logged, returned, or sent to an LLM. Value lists are returned only if you ask (`include_values`), and are never sent to a model. Tests plant canary values in uploaded data and assert none appear in a prompt, a response or a log line.
- **Size.** The hosted request limit is about 32 MB, so the batch endpoint accepts 30 MB in total. The page sends large CSV/TSV/JSONL files as a sample of their first rows and the profile is marked `sampled` (so it claims no uniqueness or true extremes). Parquet cannot be cut, so a Parquet file too large for the limit must be pointed at in cloud storage or sent as a sample.

## Knowledge Catalog aspects (BigQuery)

For BigQuery tables the service also reads the table's Knowledge Catalog (Dataplex) aspects and uses them as
metadata. Aspect types are your own, so fields are matched by meaning rather than one fixed schema:

| Aspect field (any of these names) | Used as |
|---|---|
| `data_steward` / `steward`, `owner_team` / `owner`, `data_domain` / `domain` | table steward, owner, domain |
| other table-level fields (`criticality`, `contains_pii`, ...) | tags |
| `semantic_type` | the column's semantic type (drives email, not-future, identifier, ... rules) |
| `classification` / `sensitivity`, `contains_pii` | classifications (sensitive columns never have values sent to an LLM) |
| `business_rule` / `rule` / `validation_rule` | stated rules, parsed into rules (documented tier) |
| `valid_values` / `allowed_values` | allowed-values rule |
| `glossary_term` | the glossary term the column implements |

Stated rules are plain sentences; these phrasings are understood: *"Must be greater than 0"*, *"at least 1"*,
*"at most 99"*, *"zero or greater"*, *"Cannot be in the future"*, *"Must not be before `order_ts`"*,
*"Must not exceed `unit_price`"*, *"Must be a valid email address"*, *"Unique"*, *"Required"*. A sentence that is
not recognised produces no rule, and a column name in it must be a real column of the same table. Aspect fields
that match nothing are listed as a finding, never guessed at.

The service account needs `roles/dataplex.catalogViewer` and the Dataplex API enabled (`deploy/cloudrun.sh` does
both). If the catalog cannot be read, the table is still analyzed and a finding says why. `DQMS_CATALOG=off`
disables catalog reads. Only aspects are read: no table data, and glossary term *definitions* are not fetched.

Related behaviour: observed ranges are dropped where the data breaks a stated or inferred range (they would only
copy the bad values), a looser range is dropped when a tighter stated one exists, and inferred ranges and
not-future rules are checked against the profile too.

## Demo data with a Knowledge Catalog (BigQuery)

`deploy/seed_catalog_demo.py` builds a small BigQuery dataset plus the catalog around it, so you can see
how metadata-driven rules look when the metadata is rich. Synthetic data only.

```bash
pip install google-cloud-bigquery
gcloud services enable dataplex.googleapis.com
python deploy/seed_catalog_demo.py --project MY_PROJECT --dry-run   # show the plan
python deploy/seed_catalog_demo.py --project MY_PROJECT             # build it
python deploy/seed_catalog_demo.py --project MY_PROJECT --teardown  # remove it
```

It creates four related tables (`customers`, `products`, `orders`, `order_items`) with descriptions, labels,
primary/foreign keys and an array column; two aspect types (table governance, column semantics) attached to every
table and column; and a business glossary. `--policy-tags` adds policy tags on confidential columns. It also plants
known data problems (nulls, malformed emails, orphan keys, future dates, ...) and lists them at the end and in
`dqms_demo_manifest.json`, so you can check that the generated rules catch them.
The catalog calls use the Dataplex REST API directly; each catalog step reports its own failure and the data is
still built.

## Controlling scope

**Tables are specified on the call.** Nothing is read or profiled unless it (or a container you name) is in `sources`; there is no ambient or default scope, and the UI's explorer submits exactly the tables you picked. Profiling is **off by default**, and a container (`bigquery://project`, `gs://bucket/`) is resolved to a concrete dataset list *first*:

- **Cap.** At most `DQMS_MAX_DATASETS` datasets per job (default 50); a request may lower it, never raise it. Over the cap is an **error with guidance, not a silent truncation**, and nothing has been read when it fires.
- **Filter.** `scope: {include: ["orders_*"], exclude: ["*_tmp"]}` (globs on dataset name or URI) narrows containers. Explicitly named datasets are always included. A scope that matches nothing says so.
- **Preview.** `POST /v1/plan` / `dqms analyze … --plan` / `client.plan(...)` list exactly what would be touched, with sizes where listings provide them (files, Snowflake), reading no data.
- **Allowlist.** A key's source allowlist is applied to every expanded dataset; datasets it cannot see are neither listed nor counted.
- **Per-dataset cost limits.** `profile.max_rows` samples big tables (marked `sampled`), `max_bytes` bounds file reads, and BigQuery profiling queries have a bytes-billed cap.

```bash
dqms analyze bigquery://my-proj/sales --plan --exclude '*_tmp' '*_backup'          # look first
dqms analyze bigquery://my-proj/sales --include 'orders*' 'customers' --profile    # then run just those
```

Not offered yet: a per-job *total* row/byte budget (limits are per dataset plus the dataset count).

## Hosting it (Google Cloud Run)

Host it once; then any laptop or pipeline calls it with a URL and a key, or you open the page and paste the key. In Google Cloud Shell, from the repo folder:

```bash
bash deploy/cloudrun.sh
```

It enables the needed services, creates a service account that can only read BigQuery, generates an API key (only its fingerprint is stored, in Secret Manager), builds and deploys the container, checks it, and prints the **URL** and the **key** (shown once). Settings go in front of the command: `PROJECT`, `DATA_PROJECTS="a b"` (BigQuery projects it may read), `REGION`, `MAX_BYTES` (bytes-billed cap per profiling query; default 10 GB), `MAX_DATASETS` (most tables per run; default 100). Re-running makes a new key and redeploys.

Choices the script makes, and why:
- **One always-on instance** (`--min-instances 1 --max-instances 1 --no-cpu-throttling`): jobs run in the background after the "accepted" reply and keep their state in memory, so a scaled-to-zero or second instance would lose them. It costs a continuously running small instance; delete it with `gcloud run services delete` when you are done testing. For several replicas, implement `JobStore` against a shared database.
- **Public URL, protected by the key** (`--allow-unauthenticated`). If your organisation blocks public Cloud Run, use `gcloud run services proxy dqms --port 8080` from Cloud Shell and Web Preview instead.
- **LLM off** (`DQMS_LLM_MODE=off`) and the BigQuery cost cap set explicitly.
- BigQuery access is granted at project level (`dataViewer` + `jobUser`); the key's `sources` allowlist (`bigquery://PROJECT/*`) is what narrows what callers can see. Grant dataset-level roles instead if you want the service account itself limited.

Call it from anywhere, no install:

```bash
curl -s -X POST "$URL/v1/sources/analyze" -H "X-API-Key: $DQMS_KEY" -H "content-type: application/json" \
  -d '{"source": "bigquery://MY_PROJECT/MY_DATASET/MY_TABLE"}'
```

Or `pip install dqms` and use `DqmsClient(url, api_key)` / `dqms analyze ... --url $URL` (key in `DQMS_API_KEY`). The script has been syntax-checked and its key generation is tested, but it has **not been run against a real project yet**; expect to fix small things on the first run.

## Running it

```bash
pip install ".[cloud]"                      # or only the extras you need: bigquery snowflake databricks files gcs s3 azure azure-llm
dqms keygen --name airflow --sources 'bigquery://my-proj/*' 'gs://my-bucket/*'   # put the printed entry in a key file
DQMS_AUTH_FILE=keys.yaml DQMS_JOB_DB=/data/jobs.db dqms serve --host 0.0.0.0 --port 8080
docker build -t dqms . && docker run -p 8080:8080 -e DQMS_AUTH_FILE=/etc/dqms/keys.yaml ... dqms
```

| Variable | Purpose |
|---|---|
| `DQMS_AUTH_FILE` | key file (required unless `DQMS_ALLOW_ANONYMOUS=1`) |
| `DQMS_JOB_DB` | SQLite path for durable jobs; unset = in-memory. **Multiple replicas need a shared store**: implement `JobStore` against Postgres/Firestore |
| `DQMS_JOB_WORKERS` | concurrent jobs (default 4) |
| `DQMS_LOG_LEVEL` | JSON logs to stdout, with a request id on every line |
| `DQMS_LLM_MODE` | `off` (default) · `rules` · `full`; see *Data and LLMs* |
| `DQMS_LLM_BASE_URL`, `DQMS_LLM_MODEL` | Azure OpenAI endpoint/deployment (Entra ID auth); required when the mode is not `off` |
| `DQMS_MAX_DATASETS` | most datasets one job may touch (default 50) |
| `DQMS_BQ_MAX_BYTES_BILLED` | BigQuery cost cap per profiling query (default 100 GiB; `0` disables) |
| `DQMS_SNOWFLAKE_USER/ROLE/WAREHOUSE` + `…_PRIVATE_KEY_PATH` or `…_PASSWORD` | Snowflake service credentials |
| `DQMS_DATABRICKS_HTTP_PATH`, `DQMS_DATABRICKS_TOKEN` | Databricks service credentials |
| GCP / AWS / Azure | the platform's default credential chain (ADC, IAM role, managed identity) |

Give the service identity **read-only** access (BigQuery `dataViewer` + `jobUser`, Snowflake a read-only role, etc.); the allowlist narrows it further per caller.

## Layout

```
normalize/   files → documents → canonical Metadata/Profile   (deterministic first, LLM fallback, grounding)
connectors/  bigquery · snowflake · databricks · objectstore (+ shared sqlprofile / rowprofile)
rules/       declared · documented · semantic · observed · cross · llm · post (merge, severity, reconcile) · diff
emitters/    sql.py        jobs.py   auth.py   client.py   cli.py   api/
```

Extending: a new source is one `Connector` (`is_container`, `discover`, `fetch`); a new rule source is a `RuleGenerator`; a new output platform is an emitter beside `emitters/sql.py`.

## Status and known limits

- **Tested** (199 tests): ingestion, every rule tier, sampling rules, merge/reconcile/drift, auth and allowlists, jobs and idempotency, SQLite store, SQL safety, CSV/JSONL/Parquet profiling (Parquet via real pyarrow files), client and CLI exit codes, and a real server process.
- **Not verified against live services:** BigQuery, Snowflake, Databricks, GCS/S3/Azure and the Azure OpenAI client are exercised through fakes that check our mapping, profiling math and safety controls, **not vendor SQL dialect details**. Expect small fixes on first contact with each (the Databricks constraint/tag queries and BigQuery's `table_constraints` property are the likeliest). The Docker image has not been built.
- Not yet built: Data Catalog/Dataplex tags and lineage APIs, multi-file (partitioned) datasets as one table, per-user cloud login, webhooks, a shared job store, job retention/purging.
- `.xlsx`, PDF and other binary uploads are rejected with a message; `legacy/` keeps the original prototype for reference.
