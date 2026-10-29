"""Command line: run the service, mint API keys, and gate pipelines.

Exit codes (so a pipeline step can branch on them):
    0  ok
    1  gate tripped (--fail-on findings / --fail-on-change / --fail-on-breaking)
    2  operational failure (bad input, auth, a source could not be read)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dqms.auth import generate_key
from dqms.models import RuleSet
from dqms.rules.diff import diff_rulesets
from dqms.service import Analysis

EXIT_OK, EXIT_GATE, EXIT_ERROR = 0, 1, 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dqms", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the HTTP service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    keygen = sub.add_parser("keygen", help="create an API key and the auth-file entry for it")
    keygen.add_argument("--name", required=True)
    keygen.add_argument("--sources", nargs="*", default=[], help="allowed source patterns, e.g. 'bigquery://proj/*'")

    analyze = sub.add_parser("analyze", help="generate rules for sources (URIs) or metadata files")
    analyze.add_argument("targets", nargs="+", help="source URIs (bigquery://, gs://, s3://, az://, snowflake://, databricks://) or files")
    analyze.add_argument("--url", default=os.getenv("DQMS_URL"), help="call a deployed service instead of running locally (env DQMS_URL; key in DQMS_API_KEY)")
    analyze.add_argument("--profile", action="store_true", help="profile the data (aggregates only)")
    analyze.add_argument("--include-values", action="store_true", help="collect complete value lists for low-cardinality columns")
    analyze.add_argument("--max-rows", type=int, default=5_000_000, help="profile a sample above this many rows")
    analyze.add_argument("--min-confidence", type=float, default=0.0)
    analyze.add_argument("--no-llm", action="store_true")
    analyze.add_argument("--include", nargs="*", default=[], metavar="GLOB", help="only datasets whose name/URI matches (applies to containers)")
    analyze.add_argument("--exclude", nargs="*", default=[], metavar="GLOB", help="skip datasets whose name/URI matches (applies to containers)")
    analyze.add_argument("--max-datasets", type=int, default=None, help="cap datasets for this run (cannot exceed the server cap, DQMS_MAX_DATASETS, default 50)")
    analyze.add_argument("--plan", action="store_true", help="dry run: list the datasets that would be touched and exit; reads no data")
    analyze.add_argument("--out", help="rule set file (one result) or directory (several results)")
    analyze.add_argument("--fail-on", choices=["none", "findings"], default="none",
                         help="exit 1 if any result has findings (profile contradicts a rule, sources disagree)")

    diff = sub.add_parser("diff", help="compare two rule sets (drift detection)")
    diff.add_argument("old")
    diff.add_argument("new")
    gate = diff.add_mutually_exclusive_group()
    gate.add_argument("--fail-on-change", action="store_true")
    gate.add_argument("--fail-on-breaking", action="store_true")

    args = parser.parse_args(argv)
    try:
        return {"serve": _serve, "keygen": _keygen, "analyze": _analyze, "diff": _diff}[args.command](args)
    except Exception as exc:  # one line for the pipeline log, not a traceback
        print(f"dqms: error: {exc}", file=sys.stderr)
        return EXIT_ERROR


# --------------------------------------------------------------------------- #


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run("dqms.api.app:app_factory", factory=True, host=args.host, port=args.port, log_config=None)
    return EXIT_OK


def _keygen(args: argparse.Namespace) -> int:
    key, digest = generate_key()
    entry = {"name": args.name, "sha256": digest, "sources": args.sources}
    print(f"API key (shown once; give it to the caller via a secret manager):\n  {key}\n", file=sys.stderr)
    print("Key-file entry below (stdout). Save it, or append the entries under one `keys:`, as the file named by DQMS_AUTH_FILE.", file=sys.stderr)
    print("keys:\n  - " + "\n    ".join(f"{k}: {json.dumps(v)}" for k, v in entry.items()))
    return EXIT_OK


def _analyze(args: argparse.Namespace) -> int:
    sources = [t for t in args.targets if "://" in t]
    files = [t for t in args.targets if "://" not in t]
    missing = [f for f in files if not Path(f).is_file()]
    if missing:
        raise FileNotFoundError(f"no such file: {', '.join(missing)}")

    profile = {"enabled": args.profile, "include_values": args.include_values, "max_rows": args.max_rows}
    options = {"min_confidence": args.min_confidence, "use_llm": not args.no_llm}
    scope = {"include": args.include, "exclude": args.exclude, "max_datasets": args.max_datasets}
    runner = _remote(args, profile, options, scope) if args.url else _local(args, profile, options, scope)
    if args.plan:
        if files:
            raise ValueError("--plan applies to cloud sources, not files")
        return _show_plan(runner.plan(sources))
    results: list[Analysis] = []
    failures: list[str] = []
    if files:
        analyses, notes = runner.files(files)
        results += analyses
        for note in notes:
            print(f"NOTE    {note}", file=sys.stderr)
    if sources:
        got, failed = runner.sources(sources)
        results += got
        failures += failed

    for r in results:
        _summarize(r)
    for message in failures:
        print(f"FAILED  {message}", file=sys.stderr)
    if args.out:
        _write(args.out, results)
    if failures or not results:
        return EXIT_ERROR
    if args.fail_on == "findings" and any(r.ruleset.findings for r in results):
        return EXIT_GATE
    return EXIT_OK


def _diff(args: argparse.Namespace) -> int:
    old, new = _load_ruleset(args.old), _load_ruleset(args.new)
    d = diff_rulesets(old, new)
    print(f"fingerprint {old.fingerprint or '-'} -> {new.fingerprint or '-'}", file=sys.stderr)
    for label, changes in (("added", d.added), ("removed", d.removed), ("changed", d.changed)):
        for c in changes:
            print(f"{label.upper():8}{c.id}")
    if d.is_empty:
        print("no rule changes", file=sys.stderr)
    if (args.fail_on_change and not d.is_empty) or (args.fail_on_breaking and d.breaking):
        return EXIT_GATE
    return EXIT_OK


# --------------------------------------------------------------------------- #


class _local:
    """Run in-process using this machine's cloud credentials (no allowlist: it is your own access)."""

    def __init__(self, args: argparse.Namespace, profile: dict, options: dict, scope: dict):
        from dqms.api.app import default_connectors
        from dqms.connectors.base import ProfileOptions
        from dqms.llm.config import llm_from_env
        from dqms.rules.engine import EngineOptions
        from dqms.service import RuleService

        client, mode = (None, None) if args.no_llm else llm_from_env()  # same DQMS_LLM_MODE policy as the server
        self._service = RuleService(client, default_connectors(), mode) if mode else RuleService(None, default_connectors())
        self._profile = ProfileOptions(**profile)
        self._options = EngineOptions(min_confidence=options["min_confidence"], use_llm=options["use_llm"])
        from dqms.scope import ScopeRequest

        self._scope = ScopeRequest(**{k: v for k, v in scope.items() if v})

    def files(self, paths: list[str]):
        from dqms.batch import analyze_uploads, read_uploads

        uploads, rejected = read_uploads([(Path(p).name, Path(p).read_bytes()) for p in paths])
        if not uploads:
            raise ValueError("; ".join(r.detail for r in rejected) or "no readable files")
        batch = analyze_uploads(self._service, uploads, self._options)
        return batch.analyses, batch.notes + [f"{r.name}: {r.detail}" for r in rejected]

    def plan(self, sources: list[str]):
        from dqms.scope import plan_scope

        return plan_scope(self._service, sources, self._scope)

    def sources(self, sources: list[str]) -> tuple[list[Analysis], list[str]]:
        from dqms.connectors.base import ConnectorError

        plan = self.plan(sources)
        failed = [f"{p.source}: {p.error}" for p in plan.problems]
        if not plan.within_limit:
            return [], failed + [plan.message]  # refuse before reading anything
        if not plan.datasets and not failed:
            return [], [f"the scope matched no datasets ({plan.excluded} excluded by include/exclude)"]
        done = []
        for ref in plan.datasets:
            try:
                done.append(self._service.analyze_source(ref.uri, self._profile, self._options))
            except ConnectorError as exc:
                failed.append(f"{ref.uri}: {exc}")
        return done, failed


class _remote:
    def __init__(self, args: argparse.Namespace, profile: dict, options: dict, scope: dict):
        from dqms.client import DqmsClient

        self._client = DqmsClient(args.url, os.getenv("DQMS_API_KEY"))
        self._profile, self._options, self._use_llm = profile, options, not args.no_llm
        self._scope = scope

    def files(self, paths: list[str]):
        batch = self._client.analyze_batch(paths, use_llm=self._use_llm)
        return batch.analyses, batch.notes + [f"{r.name}: {r.detail}" for r in batch.rejected]

    def plan(self, sources: list[str]):
        return self._client.plan(sources, self._scope["include"], self._scope["exclude"], self._scope["max_datasets"])

    def sources(self, sources: list[str]) -> tuple[list[Analysis], list[str]]:
        job = self._client.wait_job(self._client.submit_job(
            sources, self._profile, self._options, include=self._scope["include"], exclude=self._scope["exclude"],
            max_datasets=self._scope["max_datasets"]).id)
        done = [r.analysis for r in job.results if r.analysis is not None]
        failed = [f"{r.source}: {r.error}" for r in job.results if r.status == "error"]
        if job.error:
            failed.append(job.error)
        return done, failed


def _show_plan(plan) -> int:
    for d in plan.datasets:
        size = f"  {d.bytes:,} bytes" if d.bytes is not None else ""
        rows = f"  {d.rows:,} rows" if d.rows is not None else ""
        print(f"{d.uri}{size}{rows}")
    totals = [f"{len(plan.datasets)} datasets", f"limit {plan.limit}"]
    if plan.total_bytes is not None:
        totals.append(f"{plan.total_bytes:,} bytes (known sizes)")
    if plan.excluded:
        totals.append(f"{plan.excluded} excluded by patterns")
    print("PLAN    " + ", ".join(totals) + " (nothing was read)", file=sys.stderr)
    for p in plan.problems:
        print(f"PROBLEM {p.source}: {p.error}", file=sys.stderr)
    if not plan.within_limit:
        print(f"OVER    {plan.message}", file=sys.stderr)
    return EXIT_OK if plan.within_limit and not plan.problems else EXIT_ERROR


def _summarize(a: Analysis) -> None:
    rules = a.ruleset.rules
    by_sev = {s: sum(1 for r in rules if r.severity.value == s) for s in ("error", "warning", "info")}
    label = a.source or a.ruleset.dataset
    print(f"OK      {label}: {len(rules)} rules ({by_sev['error']} error, {by_sev['warning']} warning, "
          f"{by_sev['info']} info), fingerprint {a.ruleset.fingerprint}", file=sys.stderr)
    for f in a.ruleset.findings:
        print(f"  finding [{f.kind}] {f.message}", file=sys.stderr)


def _write(out: str, results: list[Analysis]) -> None:
    path = Path(out)
    if len(results) == 1 and path.suffix:
        path.write_text(results[0].ruleset.model_dump_json(indent=2))
        return
    path.mkdir(parents=True, exist_ok=True)
    used: set[str] = set()
    for r in results:
        stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", r.source or r.ruleset.dataset).strip("_") or "rules"
        while stem in used:
            stem += "_"
        used.add(stem)
        (path / f"{stem}.rules.json").write_text(r.ruleset.model_dump_json(indent=2))


def _load_ruleset(path: str) -> RuleSet:
    data: dict[str, Any] = json.loads(Path(path).read_text())
    return RuleSet.model_validate(data.get("ruleset", data))  # accept a saved Analysis or a bare RuleSet


if __name__ == "__main__":
    sys.exit(main())
