"""Python client for calling a deployed dqms service from a pipeline.

    from dqms.client import DqmsClient

    client = DqmsClient("https://dqms.internal.example.com", api_key=os.environ["DQMS_API_KEY"])
    results = client.analyze_sources(["bigquery://proj/sales/"], profile={"enabled": True},
                                     idempotency_key=f"nightly-{run_date}")
    for r in results:
        print(r.ruleset.dataset, r.ruleset.fingerprint, len(r.ruleset.findings))
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import httpx

from dqms.batch import BatchAnalysis
from dqms.jobs import Job, JobStatus
from dqms.models import RuleSet
from dqms.rules.diff import RuleDiff
from dqms.scope import ScopePlan
from dqms.service import Analysis


class DqmsError(RuntimeError):
    def __init__(self, status: int, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status, self.detail = status, detail


class JobFailed(DqmsError):
    def __init__(self, job: Job):
        super().__init__(500, job.error or "job failed")
        self.job = job


class DqmsClient:
    def __init__(self, base_url: str, api_key: str | None = None, timeout: float = 60.0, http: httpx.Client | None = None):
        headers = {"X-API-Key": api_key} if api_key else {}  # not Authorization: proxies often reserve that
        # `http` lets tests (or callers with custom TLS/proxy needs) supply their own transport.
        self._http = http or httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=timeout)
        if http is not None and headers:
            self._http.headers.update(headers)

    # ------------------------------ sources ------------------------------ #

    def analyze_source(self, source: str, profile: dict[str, Any] | None = None, options: dict[str, Any] | None = None) -> Analysis:
        """One dataset, synchronously. Fine for small tables; use `analyze_sources` for big ones."""
        body = {"source": source, "profile": profile or {}, "options": options or {}}
        return Analysis.model_validate(self._call("POST", "/v1/sources/analyze", json=body))

    def discover(self, source: str, limit: int = 500) -> list[str]:
        return [r["uri"] for r in self._call("POST", "/v1/sources/discover", json={"source": source, "limit": limit})]

    def plan(self, sources: list[str], include: list[str] | None = None, exclude: list[str] | None = None,
             max_datasets: int | None = None) -> ScopePlan:
        """Dry run: which datasets would be touched, and is that within the cap? Reads no data."""
        body = {"sources": sources, "scope": _scope(include, exclude, max_datasets)}
        return ScopePlan.model_validate(self._call("POST", "/v1/plan", json=body))

    def submit_job(
        self, sources: list[str], profile: dict[str, Any] | None = None, options: dict[str, Any] | None = None,
        idempotency_key: str | None = None, include: list[str] | None = None, exclude: list[str] | None = None,
        max_datasets: int | None = None,
    ) -> Job:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
        body = {"sources": sources, "profile": profile or {}, "options": options or {},
                "scope": _scope(include, exclude, max_datasets)}
        return Job.model_validate(self._call("POST", "/v1/jobs", json=body, headers=headers))

    def wait_job(self, job_id: str, timeout: float = 900.0) -> Job:
        """Long-poll until the job finishes. Raises TimeoutError if it does not within `timeout`."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"job {job_id} did not finish within {timeout:.0f}s")
            job = Job.model_validate(
                self._call("GET", f"/v1/jobs/{job_id}", params={"wait": int(min(30, max(1, remaining)))})
            )
            if job.status.terminal:
                return job

    def analyze_sources(
        self, sources: list[str], profile: dict[str, Any] | None = None, options: dict[str, Any] | None = None,
        idempotency_key: str | None = None, timeout: float = 900.0, include: list[str] | None = None,
        exclude: list[str] | None = None, max_datasets: int | None = None,
    ) -> list[Analysis]:
        """Submit, wait, and return the successful analyses. Raises JobFailed if nothing succeeded;
        partial failures are available on the job (`submit_job` + `wait_job`) when you need them."""
        job = self.wait_job(self.submit_job(sources, profile, options, idempotency_key, include, exclude, max_datasets).id, timeout)
        if job.status is JobStatus.FAILED:
            raise JobFailed(job)
        return [r.analysis for r in job.results if r.analysis is not None]

    # ------------------------------ documents ---------------------------- #

    def analyze_files(self, paths: list[str | Path], dataset_name: str | None = None, use_llm: bool = True) -> Analysis:
        files = [("files", (Path(p).name, Path(p).read_bytes())) for p in paths]
        data = {"use_llm": str(use_llm).lower(), **({"dataset_name": dataset_name} if dataset_name else {})}
        return Analysis.model_validate(self._call("POST", "/v1/analyze", files=files, data=data))

    def analyze_batch(self, paths: list[str | Path], pairs: dict[str, str] | None = None, use_llm: bool = True,
                      include_values: bool = False) -> BatchAnalysis:
        """Data files with their metadata (any platform): files are classified by content and paired by name."""
        import json

        files = [("files", (Path(p).name, Path(p).read_bytes())) for p in paths]
        data = {"use_llm": str(use_llm).lower(), "include_values": str(include_values).lower(),
                **({"pairs": json.dumps(pairs)} if pairs else {})}
        return BatchAnalysis.model_validate(self._call("POST", "/v1/analyze/batch", files=files, data=data))

    # ------------------------------- derived ----------------------------- #

    def diff(self, old: RuleSet, new: RuleSet) -> dict[str, Any]:
        body = {"old": old.model_dump(mode="json"), "new": new.model_dump(mode="json")}
        return self._call("POST", "/v1/diff", json=body)

    def close(self) -> None:
        self._http.close()

    def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._http.request(method, path, **kwargs)
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise DqmsError(response.status_code, detail if isinstance(detail, str) else str(detail))
        return response.json()


def _scope(include: list[str] | None, exclude: list[str] | None, max_datasets: int | None) -> dict[str, Any]:
    scope: dict[str, Any] = {"include": include or [], "exclude": exclude or []}
    if max_datasets:
        scope["max_datasets"] = max_datasets
    return scope


__all__ = ["DqmsClient", "DqmsError", "JobFailed", "RuleDiff"]
