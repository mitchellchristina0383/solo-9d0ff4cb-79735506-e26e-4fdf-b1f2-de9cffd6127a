"""SQLite 访问层：单连接 + 写锁，WAL 模式。"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable

from . import config

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    base_url     TEXT NOT NULL,
    capacity_mb  INTEGER NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deployments (
    id                    TEXT PRIMARY KEY,
    model_name            TEXT NOT NULL,
    mem_per_replica_mb    INTEGER NOT NULL,
    replica_count         INTEGER NOT NULL,
    status                TEXT NOT NULL,
    reason                TEXT,
    epoch                 INTEGER NOT NULL DEFAULT 1,
    attempts              INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replicas (
    id             TEXT PRIMARY KEY,
    deployment_id  TEXT NOT NULL REFERENCES deployments(id),
    node_id        TEXT NOT NULL REFERENCES nodes(node_id),
    seq            INTEGER NOT NULL,
    epoch          INTEGER NOT NULL,
    status         TEXT NOT NULL,
    mem_mb         INTEGER NOT NULL,
    last_error     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    confirmed_at   TEXT,
    released_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_replicas_dep ON replicas(deployment_id);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id  TEXT,
    replica_id     TEXT,
    node_id        TEXT,
    event_type     TEXT NOT NULL,
    detail         TEXT,
    created_at     TEXT NOT NULL
);
-- 加载可靠性样本：每次加载下发一条。状态一经判定为按时/失败/逾期即冻结，
-- 后续对账、迟到回执、轮询都只更新同一行，不会新增样本或改写结论。
CREATE TABLE IF NOT EXISTS load_samples (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    replica_id     TEXT NOT NULL,
    deployment_id  TEXT NOT NULL,
    epoch          INTEGER NOT NULL,
    node_id        TEXT NOT NULL,
    dispatched_at  TEXT NOT NULL,
    deadline_at    TEXT NOT NULL,
    status         TEXT NOT NULL,
    decided_at     TEXT,
    UNIQUE(replica_id, epoch)
);
CREATE INDEX IF NOT EXISTS idx_samples_node_time ON load_samples(node_id, dispatched_at);
CREATE TABLE IF NOT EXISTS node_alert_settings (
    node_id           TEXT PRIMARY KEY,
    window_seconds    REAL NOT NULL,
    min_samples       INTEGER NOT NULL,
    abnormal_ratio    REAL NOT NULL,
    updated_at        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS node_alerts (
    node_id          TEXT PRIMARY KEY,
    status           TEXT NOT NULL,
    firing_since     TEXT,
    recovered_at     TEXT,
    last_evaluated_at TEXT NOT NULL,
    window_seconds   REAL NOT NULL,
    min_samples      INTEGER NOT NULL,
    abnormal_ratio   REAL NOT NULL,
    sample_count     INTEGER NOT NULL,
    on_time_count    INTEGER NOT NULL,
    failed_count     INTEGER NOT NULL,
    overdue_count    INTEGER NOT NULL,
    pending_count    INTEGER NOT NULL DEFAULT 0,
    abnormal_ratio_actual REAL,
    detail           TEXT
);
"""


def connect() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(config.DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=30000")
        _conn.executescript(SCHEMA)
    return _conn


@contextmanager
def tx():
    """串行化所有写事务，保证并发部署下的预留检查与插入原子。"""
    with _lock:
        conn = connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def query(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with _lock:
        return connect().execute(sql, tuple(params)).fetchall()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    with _lock:
        return connect().execute(sql, tuple(params)).fetchone()
