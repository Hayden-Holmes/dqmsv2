"""Test doubles for cloud systems. They verify our logic (mapping, profiling math, cost and
safety controls); they cannot verify vendor SQL dialect details against a live service."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from dqms.connectors.objectstore import ObjectInfo


class FakeWarehouse:
    """Answers queries by substring match, recording every statement."""

    def __init__(self, *handlers: tuple[str, Any]):
        self.handlers = list(handlers)
        self.sql: list[str] = []

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.sql.append(sql)
        for needle, rows in self.handlers:
            if needle in sql:
                if isinstance(rows, Exception):
                    raise rows
                return rows(sql) if callable(rows) else rows
        raise AssertionError(f"unexpected query: {sql}")


class FakeCursor:
    def __init__(self, conn: "FakeConnection"):
        self._conn, self.description, self._rows = conn, None, []

    def execute(self, sql: str) -> None:
        self._conn.sql.append(sql)
        for needle, rows in self._conn.handlers:
            if needle in sql:
                if isinstance(rows, Exception):
                    raise rows
                if rows:
                    self.description = [(k,) for k in rows[0]]
                    self._rows = [tuple(r.values()) for r in rows]
                return
        self.description, self._rows = None, []

    def fetchall(self):
        return self._rows

    def close(self) -> None:
        pass


class FakeConnection(FakeWarehouse):
    """DB-API style connection (Snowflake / Databricks)."""

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        pass


class MemStore:
    """In-memory object store. Keys are (container, key)."""

    def __init__(self, objects: dict[tuple[str, str], bytes]):
        self.objects = objects
        self.reads: list[tuple[str, int, int]] = []

    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]:
        keys = sorted(k for (c, k) in self.objects if c == container and k.startswith(prefix))
        return [ObjectInfo(k, len(self.objects[(container, k)])) for k in keys[:limit]]

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes:
        self.reads.append((key, start, length))
        return self.objects[(container, key)][start : start + length]


def bq_field(name, field_type, mode="NULLABLE", description=None, policy_tags=None, max_length=None):
    return SimpleNamespace(
        name=name, field_type=field_type, mode=mode, description=description, max_length=max_length,
        policy_tags=SimpleNamespace(names=tuple(policy_tags)) if policy_tags else None,
    )


class FakeBigQueryClient:
    def __init__(self, table, answers=None, datasets=None):
        self._table, self.answers, self.datasets, self.sql = table, answers or [], datasets or {}, []

    def get_table(self, ref):
        return self._table

    def list_datasets(self):
        return [SimpleNamespace(dataset_id=d) for d in self.datasets]

    def list_tables(self, dataset):
        return [SimpleNamespace(table_id=t, table_type=kind) for t, kind in self.datasets[dataset]]

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        for needle, rows in self.answers:
            if needle in sql:
                return SimpleNamespace(result=lambda timeout=None, rows=rows: rows)
        raise AssertionError(f"unexpected query: {sql}")
