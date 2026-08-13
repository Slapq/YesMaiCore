"""SQLite persistence and fencing for the YesMai Core Cron service."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA_VERSION = 3
_ACTIVE_STATUSES = ("DISPATCHING", "RUNNING")


class CronStoreError(RuntimeError):
    """Cron persistence is unavailable or inconsistent."""


@dataclass(frozen=True, slots=True)
class LeaderLease:
    leader_id: str
    process_nonce: str
    epoch: int
    lease_token: str
    lease_expires_at_ms: int


@dataclass(frozen=True, slots=True)
class DispatchAttempt:
    run_id: str
    occurrence_id: str
    job_id: str
    owner_plugin_id: str
    handler_full_name: str
    api_version: str
    scheduled_for_ms: int
    timeout_seconds: int
    token: str
    token_expires_at_ms: int
    leader_epoch: int
    leader_lease_token: str


class CronStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._thread_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._thread_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._thread_lock:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS schema_meta (
                        key TEXT PRIMARY KEY,
                        value_text TEXT NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS scheduler_leader (
                        singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                        leader_id TEXT,
                        process_nonce TEXT,
                        epoch INTEGER NOT NULL DEFAULT 0,
                        lease_token TEXT,
                        lease_expires_at_ms INTEGER,
                        heartbeat_at_ms INTEGER,
                        updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS cron_jobs (
                        job_id TEXT PRIMARY KEY,
                        owner_plugin_id TEXT NOT NULL,
                        handler_full_name TEXT NOT NULL,
                        api_version TEXT NOT NULL,
                        stable_name TEXT NOT NULL,
                        job_revision INTEGER NOT NULL,
                        catalog_fingerprint TEXT NOT NULL,
                        schedule_json TEXT NOT NULL,
                        timezone TEXT NOT NULL,
                        misfire_grace_seconds INTEGER NOT NULL,
                        coalesce INTEGER NOT NULL CHECK (coalesce IN (0, 1)),
                        max_instances INTEGER NOT NULL,
                        timeout_seconds INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        next_fire_at_ms INTEGER,
                        last_fire_at_ms INTEGER,
                        last_seen_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        UNIQUE(owner_plugin_id, handler_full_name, stable_name)
                    );
                    CREATE TABLE IF NOT EXISTS cron_occurrences (
                        occurrence_id TEXT PRIMARY KEY,
                        job_id TEXT NOT NULL REFERENCES cron_jobs(job_id),
                        job_revision INTEGER NOT NULL,
                        scheduled_for_ms INTEGER NOT NULL,
                        local_scheduled_text TEXT NOT NULL,
                        local_fold INTEGER NOT NULL,
                        utc_offset_seconds INTEGER NOT NULL,
                        tzdata_version TEXT NOT NULL,
                        status TEXT NOT NULL,
                        unknown_hold_until_ms INTEGER,
                        final_code TEXT,
                        final_message TEXT,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        UNIQUE(job_id, job_revision, scheduled_for_ms)
                    );
                    CREATE TABLE IF NOT EXISTS cron_run_attempts (
                        run_id TEXT PRIMARY KEY,
                        occurrence_id TEXT NOT NULL REFERENCES cron_occurrences(occurrence_id),
                        attempt_no INTEGER NOT NULL,
                        leader_epoch INTEGER NOT NULL,
                        process_nonce TEXT NOT NULL,
                        leader_lease_token TEXT,
                        token_hash BLOB,
                        token_expires_at_ms INTEGER,
                        token_consumed_at_ms INTEGER,
                        status TEXT NOT NULL,
                        result_code TEXT,
                        result_message TEXT,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        UNIQUE(occurrence_id, attempt_no)
                    );
                    CREATE TABLE IF NOT EXISTS cron_misfire_backlog_audit (
                        audit_id TEXT PRIMARY KEY,
                        job_id TEXT NOT NULL REFERENCES cron_jobs(job_id),
                        job_revision INTEGER NOT NULL,
                        first_skipped_scheduled_for_ms INTEGER NOT NULL,
                        resume_at_ms INTEGER NOT NULL,
                        cutoff_ms INTEGER NOT NULL,
                        final_code TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        UNIQUE(job_id, job_revision, first_skipped_scheduled_for_ms, resume_at_ms)
                    );
                    CREATE INDEX IF NOT EXISTS cron_jobs_due_idx
                        ON cron_jobs(status, next_fire_at_ms);
                    CREATE INDEX IF NOT EXISTS cron_occurrences_ready_idx
                        ON cron_occurrences(status, scheduled_for_ms);
                    CREATE INDEX IF NOT EXISTS cron_occurrences_job_status_idx
                        ON cron_occurrences(job_id, status);
                    CREATE INDEX IF NOT EXISTS cron_attempts_status_idx
                        ON cron_run_attempts(status, token_expires_at_ms);
                    CREATE INDEX IF NOT EXISTS cron_backlog_audit_created_idx
                        ON cron_misfire_backlog_audit(created_at_ms);
                    """
                )
                now_ms = 0
                existing = connection.execute(
                    "SELECT value_text FROM schema_meta WHERE key = 'schema_version'"
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO schema_meta(key, value_text, updated_at_ms) VALUES('schema_version', ?, ?)",
                        (str(_SCHEMA_VERSION), now_ms),
                    )
                else:
                    try:
                        schema_version = int(existing["value_text"])
                    except (TypeError, ValueError) as exc:
                        raise CronStoreError(f"无效的 Cron 数据库版本：{existing['value_text']}") from exc
                    if schema_version in {1, 2}:
                        if schema_version == 1:
                            columns = {
                                str(row["name"])
                                for row in connection.execute("PRAGMA table_info(cron_run_attempts)").fetchall()
                            }
                            if "leader_lease_token" not in columns:
                                connection.execute("ALTER TABLE cron_run_attempts ADD COLUMN leader_lease_token TEXT")
                        connection.execute(
                            "UPDATE schema_meta SET value_text=?, updated_at_ms=? WHERE key='schema_version'",
                            (str(_SCHEMA_VERSION), now_ms),
                        )
                    elif schema_version != _SCHEMA_VERSION:
                        raise CronStoreError(f"不支持的 Cron 数据库版本：{existing['value_text']}")
                connection.execute(
                    "INSERT OR IGNORE INTO schema_meta(key, value_text, updated_at_ms) VALUES('database_id', ?, ?)",
                    (str(uuid.uuid4()), now_ms),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO scheduler_leader(singleton_id, epoch, updated_at_ms) VALUES(1, 0, ?)",
                    (now_ms,),
                )
            except sqlite3.DatabaseError as exc:
                raise CronStoreError(f"Cron SQLite 初始化失败：{exc}") from exc
            finally:
                connection.close()

    def acquire_leader(
        self,
        leader_id: str,
        process_nonce: str,
        now_ms: int,
        lease_ms: int,
    ) -> LeaderLease | None:
        lease_token = secrets.token_urlsafe(32)
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id = 1").fetchone()
            if row is None:
                raise CronStoreError("Cron leader row 缺失")
            same_owner = row["leader_id"] == leader_id and row["process_nonce"] == process_nonce
            expired = row["lease_expires_at_ms"] is None or int(row["lease_expires_at_ms"]) <= now_ms
            if not expired:
                if not same_owner:
                    return None
                return LeaderLease(
                    leader_id,
                    process_nonce,
                    int(row["epoch"]),
                    str(row["lease_token"]),
                    int(row["lease_expires_at_ms"]),
                )
            epoch = int(row["epoch"]) + 1
            expires = now_ms + lease_ms
            connection.execute(
                """
                UPDATE scheduler_leader
                SET leader_id=?, process_nonce=?, epoch=?, lease_token=?,
                    lease_expires_at_ms=?, heartbeat_at_ms=?, updated_at_ms=?
                WHERE singleton_id=1
                """,
                (leader_id, process_nonce, epoch, lease_token, expires, now_ms, now_ms),
            )
            return LeaderLease(leader_id, process_nonce, epoch, lease_token, expires)

    def renew_leader(self, lease: LeaderLease, now_ms: int, lease_ms: int) -> LeaderLease | None:
        expires = now_ms + lease_ms
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduler_leader
                SET lease_expires_at_ms=?, heartbeat_at_ms=?, updated_at_ms=?
                WHERE singleton_id=1 AND leader_id=? AND process_nonce=?
                  AND epoch=? AND lease_token=? AND lease_expires_at_ms>?
                """,
                (
                    expires,
                    now_ms,
                    now_ms,
                    lease.leader_id,
                    lease.process_nonce,
                    lease.epoch,
                    lease.lease_token,
                    now_ms,
                ),
            )
            if cursor.rowcount != 1:
                return None
        return LeaderLease(lease.leader_id, lease.process_nonce, lease.epoch, lease.lease_token, expires)

    def release_leader(self, lease: LeaderLease, now_ms: int) -> bool:
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduler_leader
                SET leader_id=NULL, process_nonce=NULL, lease_token=NULL,
                    lease_expires_at_ms=NULL, heartbeat_at_ms=?, updated_at_ms=?
                WHERE singleton_id=1 AND leader_id=? AND process_nonce=?
                  AND epoch=? AND lease_token=?
                """,
                (now_ms, now_ms, lease.leader_id, lease.process_nonce, lease.epoch, lease.lease_token),
            )
            return cursor.rowcount == 1

    @staticmethod
    def job_id(owner_plugin_id: str, handler_full_name: str, stable_name: str) -> str:
        identity = "\0".join((owner_plugin_id, handler_full_name, stable_name))
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def reconcile_jobs(self, definitions: list[dict[str, Any]], now_ms: int) -> dict[str, int]:
        seen: set[str] = set()
        created = updated = unchanged = 0
        with self._transaction() as connection:
            for definition in definitions:
                job_id = str(definition["job_id"])
                seen.add(job_id)
                existing = connection.execute("SELECT * FROM cron_jobs WHERE job_id=?", (job_id,)).fetchone()
                fingerprint = str(definition["catalog_fingerprint"])
                next_fire = int(definition["next_fire_at_ms"])
                values = (
                    definition["owner_plugin_id"],
                    definition["handler_full_name"],
                    definition["api_version"],
                    definition["stable_name"],
                    fingerprint,
                    json.dumps(definition["schedule"], ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    definition["timezone"],
                    int(definition["misfire_grace_seconds"]),
                    int(bool(definition["coalesce"])),
                    int(definition["max_instances"]),
                    int(definition["timeout_seconds"]),
                )
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO cron_jobs(
                            job_id, owner_plugin_id, handler_full_name, api_version, stable_name,
                            job_revision, catalog_fingerprint, schedule_json, timezone,
                            misfire_grace_seconds, coalesce, max_instances, timeout_seconds,
                            status, next_fire_at_ms, last_seen_at_ms, updated_at_ms
                        ) VALUES(?,?,?,?,?,1,?,?,?,?,?,?,?,'ACTIVE',?,?,?)
                        """,
                        (job_id, *values, next_fire, now_ms, now_ms),
                    )
                    created += 1
                elif existing["catalog_fingerprint"] != fingerprint:
                    new_revision = int(existing["job_revision"]) + 1
                    connection.execute(
                        """
                        UPDATE cron_occurrences SET status='CANCELLED_REVISION_CHANGED', updated_at_ms=?
                        WHERE job_id=? AND status IN ('PLANNED','READY','DISPATCHING')
                        """,
                        (now_ms, job_id),
                    )
                    connection.execute(
                        """
                        UPDATE cron_jobs SET owner_plugin_id=?, handler_full_name=?, api_version=?,
                            stable_name=?, catalog_fingerprint=?, schedule_json=?, timezone=?,
                            misfire_grace_seconds=?, coalesce=?, max_instances=?, timeout_seconds=?,
                            job_revision=?, status='ACTIVE', next_fire_at_ms=?, last_seen_at_ms=?, updated_at_ms=?
                        WHERE job_id=?
                        """,
                        (*values, new_revision, next_fire, now_ms, now_ms, job_id),
                    )
                    updated += 1
                else:
                    connection.execute(
                        "UPDATE cron_jobs SET status='ACTIVE', last_seen_at_ms=?, updated_at_ms=? WHERE job_id=?",
                        (now_ms, now_ms, job_id),
                    )
                    unchanged += 1
            if seen:
                placeholders = ",".join("?" for _ in seen)
                connection.execute(
                    (
                        "UPDATE cron_jobs SET status='OFFLINE', updated_at_ms=? "
                        f"WHERE status='ACTIVE' AND job_id NOT IN ({placeholders})"
                    ),
                    (now_ms, *sorted(seen)),
                )
            else:
                connection.execute(
                    "UPDATE cron_jobs SET status='OFFLINE', updated_at_ms=? WHERE status='ACTIVE'", (now_ms,)
                )
        return {"created": created, "updated": updated, "unchanged": unchanged}

    def list_active_jobs(self) -> list[dict[str, Any]]:
        with self._thread_lock:
            connection = self._connect()
            try:
                rows = connection.execute(
                    "SELECT * FROM cron_jobs WHERE status='ACTIVE' ORDER BY owner_plugin_id, job_id"
                ).fetchall()
                return [self._job_dict(row) for row in rows]
            finally:
                connection.close()

    @staticmethod
    def _job_dict(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["schedule"] = json.loads(value.pop("schedule_json"))
        value["coalesce"] = bool(value["coalesce"])
        return value

    def compact_expired_backlog(
        self,
        job_id: str,
        revision: int,
        expected_next_fire_at_ms: int,
        resume_at_ms: int,
        cutoff_ms: int,
        lease: LeaderLease,
        now_ms: int,
    ) -> bool:
        if expected_next_fire_at_ms >= cutoff_ms or resume_at_ms <= expected_next_fire_at_ms:
            return False
        audit_id = hashlib.sha256(
            f"{job_id}\0{revision}\0{expected_next_fire_at_ms}\0{resume_at_ms}".encode()
        ).hexdigest()
        with self._transaction() as connection:
            leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
            if not self._leader_matches(leader, lease, now_ms):
                return False
            job = connection.execute(
                "SELECT status, job_revision, next_fire_at_ms FROM cron_jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if (
                job is None
                or job["status"] != "ACTIVE"
                or int(job["job_revision"]) != revision
                or int(job["next_fire_at_ms"] or -1) != expected_next_fire_at_ms
            ):
                return False
            connection.execute(
                """
                INSERT OR IGNORE INTO cron_misfire_backlog_audit(
                    audit_id, job_id, job_revision, first_skipped_scheduled_for_ms,
                    resume_at_ms, cutoff_ms, final_code, created_at_ms
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    audit_id,
                    job_id,
                    revision,
                    expected_next_fire_at_ms,
                    resume_at_ms,
                    cutoff_ms,
                    "CRON_MISFIRED_BACKLOG",
                    now_ms,
                ),
            )
            cursor = connection.execute(
                """
                UPDATE cron_jobs SET next_fire_at_ms=?, updated_at_ms=?
                WHERE job_id=? AND status='ACTIVE' AND job_revision=? AND next_fire_at_ms=?
                """,
                (resume_at_ms, now_ms, job_id, revision, expected_next_fire_at_ms),
            )
            return cursor.rowcount == 1

    def materialize(
        self,
        job_id: str,
        revision: int,
        scheduled_for_ms: int,
        occurrence: dict[str, Any],
        status: str,
        next_fire_at_ms: int | None,
        now_ms: int,
    ) -> bool:
        occurrence_id = hashlib.sha256(f"{job_id}\0{revision}\0{scheduled_for_ms}".encode()).hexdigest()
        with self._transaction() as connection:
            job = connection.execute(
                "SELECT job_revision, next_fire_at_ms, status FROM cron_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if (
                job is None
                or job["status"] != "ACTIVE"
                or int(job["job_revision"]) != revision
                or int(job["next_fire_at_ms"] or -1) != scheduled_for_ms
            ):
                return False
            connection.execute(
                """
                INSERT OR IGNORE INTO cron_occurrences(
                    occurrence_id, job_id, job_revision, scheduled_for_ms,
                    local_scheduled_text, local_fold, utc_offset_seconds, tzdata_version,
                    status, final_code, created_at_ms, updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    occurrence_id,
                    job_id,
                    revision,
                    scheduled_for_ms,
                    occurrence["local_scheduled_text"],
                    int(occurrence["fold"]),
                    int(occurrence["utc_offset_seconds"]),
                    occurrence["tzdata_version"],
                    status,
                    "CRON_MISFIRED" if status == "MISFIRED" else None,
                    now_ms,
                    now_ms,
                ),
            )
            connection.execute(
                "UPDATE cron_jobs SET last_fire_at_ms=?, next_fire_at_ms=?, updated_at_ms=? WHERE job_id=?",
                (scheduled_for_ms, next_fire_at_ms, now_ms, job_id),
            )
            return True

    def list_ready_occurrences(self, now_ms: int, limit: int = 32) -> list[dict[str, Any]]:
        with self._thread_lock:
            connection = self._connect()
            try:
                rows = connection.execute(
                    """
                    SELECT o.*, j.owner_plugin_id, j.handler_full_name, j.api_version,
                           j.max_instances, j.timeout_seconds, j.misfire_grace_seconds
                    FROM cron_occurrences o JOIN cron_jobs j ON j.job_id=o.job_id
                    WHERE o.status='READY' AND j.status='ACTIVE'
                    ORDER BY o.scheduled_for_ms LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
                return [dict(row) for row in rows]
            finally:
                connection.close()

    def claim_occurrence(
        self,
        occurrence_id: str,
        lease: LeaderLease,
        now_ms: int,
        token_ttl_ms: int = 30000,
    ) -> DispatchAttempt | None:
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode("utf-8")).digest()
        run_id = str(uuid.uuid4())
        with self._transaction() as connection:
            leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
            if not self._leader_matches(leader, lease, now_ms):
                return None
            row = connection.execute(
                """
                SELECT o.*, j.owner_plugin_id, j.handler_full_name, j.api_version,
                       j.max_instances, j.timeout_seconds
                FROM cron_occurrences o JOIN cron_jobs j ON j.job_id=o.job_id
                WHERE o.occurrence_id=? AND o.status='READY' AND j.status='ACTIVE'
                """,
                (occurrence_id,),
            ).fetchone()
            if row is None:
                return None
            active = connection.execute(
                """
                SELECT COUNT(*) AS count FROM cron_occurrences
                WHERE job_id=? AND (
                    status IN ('DISPATCHING','RUNNING')
                    OR (status='UNKNOWN' AND unknown_hold_until_ms>?)
                )
                """,
                (row["job_id"], now_ms),
            ).fetchone()
            if int(active["count"]) >= int(row["max_instances"]):
                return None
            attempt_no_row = connection.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS attempt_no FROM cron_run_attempts WHERE occurrence_id=?",
                (occurrence_id,),
            ).fetchone()
            attempt_no = int(attempt_no_row["attempt_no"])
            expires = now_ms + token_ttl_ms
            connection.execute(
                (
                    "UPDATE cron_occurrences SET status='DISPATCHING', updated_at_ms=? "
                    "WHERE occurrence_id=? AND status='READY'"
                ),
                (now_ms, occurrence_id),
            )
            connection.execute(
                """
                INSERT INTO cron_run_attempts(
                    run_id, occurrence_id, attempt_no, leader_epoch, process_nonce,
                    leader_lease_token, token_hash, token_expires_at_ms, status,
                    created_at_ms, updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,'TOKEN_ISSUED',?,?)
                """,
                (
                    run_id,
                    occurrence_id,
                    attempt_no,
                    lease.epoch,
                    lease.process_nonce,
                    lease.lease_token,
                    token_hash,
                    expires,
                    now_ms,
                    now_ms,
                ),
            )
            return DispatchAttempt(
                run_id=run_id,
                occurrence_id=occurrence_id,
                job_id=str(row["job_id"]),
                owner_plugin_id=str(row["owner_plugin_id"]),
                handler_full_name=str(row["handler_full_name"]),
                api_version=str(row["api_version"]),
                scheduled_for_ms=int(row["scheduled_for_ms"]),
                timeout_seconds=int(row["timeout_seconds"]),
                token=token,
                token_expires_at_ms=expires,
                leader_epoch=lease.epoch,
                leader_lease_token=lease.lease_token,
            )

    @staticmethod
    def _leader_matches(row: sqlite3.Row | None, lease: LeaderLease, now_ms: int) -> bool:
        return bool(
            row is not None
            and row["leader_id"] == lease.leader_id
            and row["process_nonce"] == lease.process_nonce
            and int(row["epoch"]) == lease.epoch
            and row["lease_token"] == lease.lease_token
            and int(row["lease_expires_at_ms"] or 0) > now_ms
        )

    @staticmethod
    def _leader_matches_attempt(row: sqlite3.Row | None, attempt: sqlite3.Row, now_ms: int) -> bool:
        return bool(
            row is not None
            and int(row["epoch"]) == int(attempt["leader_epoch"])
            and row["process_nonce"] == attempt["process_nonce"]
            and row["lease_token"] == attempt["leader_lease_token"]
            and int(row["lease_expires_at_ms"] or 0) > now_ms
        )

    def authorize(self, run_id: str, token: str, now_ms: int) -> bool:
        digest = hashlib.sha256(str(token).encode("utf-8")).digest()
        with self._transaction() as connection:
            attempt = connection.execute("SELECT * FROM cron_run_attempts WHERE run_id=?", (str(run_id),)).fetchone()
            if attempt is None or attempt["status"] != "TOKEN_ISSUED":
                return False
            leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
            if (
                not self._leader_matches_attempt(leader, attempt, now_ms)
                or int(attempt["token_expires_at_ms"] or 0) <= now_ms
                or not secrets.compare_digest(bytes(attempt["token_hash"]), digest)
            ):
                return False
            occurrence = connection.execute(
                """
                SELECT o.status, o.job_revision AS occurrence_revision,
                       j.status AS job_status, j.job_revision AS current_revision
                FROM cron_occurrences o JOIN cron_jobs j ON j.job_id=o.job_id
                WHERE o.occurrence_id=?
                """,
                (attempt["occurrence_id"],),
            ).fetchone()
            if (
                occurrence is None
                or occurrence["status"] != "DISPATCHING"
                or occurrence["job_status"] != "ACTIVE"
                or int(occurrence["occurrence_revision"]) != int(occurrence["current_revision"])
            ):
                return False
            connection.execute(
                """
                UPDATE cron_run_attempts SET status='AUTHORIZED', token_consumed_at_ms=?, updated_at_ms=?
                WHERE run_id=? AND status='TOKEN_ISSUED'
                """,
                (now_ms, now_ms, run_id),
            )
            connection.execute(
                (
                    "UPDATE cron_occurrences SET status='RUNNING', updated_at_ms=? "
                    "WHERE occurrence_id=? AND status='DISPATCHING'"
                ),
                (now_ms, attempt["occurrence_id"]),
            )
            return True

    def attempt_status(self, run_id: str) -> str | None:
        with self._thread_lock:
            connection = self._connect()
            try:
                row = connection.execute("SELECT status FROM cron_run_attempts WHERE run_id=?", (run_id,)).fetchone()
                return str(row["status"]) if row is not None else None
            finally:
                connection.close()

    def finish(self, run_id: str, *, succeeded: bool, code: str, message: str, now_ms: int) -> bool:
        attempt_status = "SUCCEEDED" if succeeded else "FAILED"
        occurrence_status = attempt_status
        with self._transaction() as connection:
            attempt = connection.execute("SELECT * FROM cron_run_attempts WHERE run_id=?", (run_id,)).fetchone()
            if attempt is None or attempt["status"] != "AUTHORIZED":
                return False
            leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
            if not self._leader_matches_attempt(leader, attempt, now_ms):
                return False
            connection.execute(
                (
                    "UPDATE cron_run_attempts SET status=?, result_code=?, result_message=?, "
                    "token_hash=NULL, updated_at_ms=? WHERE run_id=?"
                ),
                (attempt_status, str(code)[:128], str(message)[:512], now_ms, run_id),
            )
            connection.execute(
                (
                    "UPDATE cron_occurrences SET status=?, final_code=?, final_message=?, updated_at_ms=? "
                    "WHERE occurrence_id=? AND status='RUNNING'"
                ),
                (occurrence_status, str(code)[:128], str(message)[:512], now_ms, attempt["occurrence_id"]),
            )
            return True

    def mark_unknown(self, run_id: str, code: str, now_ms: int, hold_until_ms: int) -> bool:
        with self._transaction() as connection:
            attempt = connection.execute("SELECT * FROM cron_run_attempts WHERE run_id=?", (run_id,)).fetchone()
            if attempt is None or attempt["status"] != "AUTHORIZED":
                return False
            leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
            if not self._leader_matches_attempt(leader, attempt, now_ms):
                return False
            connection.execute(
                (
                    "UPDATE cron_run_attempts SET status='UNKNOWN', result_code=?, token_hash=NULL, updated_at_ms=? "
                    "WHERE run_id=?"
                ),
                (str(code)[:128], now_ms, run_id),
            )
            connection.execute(
                """
                UPDATE cron_occurrences SET status='UNKNOWN', unknown_hold_until_ms=?, final_code=?, updated_at_ms=?
                WHERE occurrence_id=? AND status='RUNNING'
                """,
                (hold_until_ms, str(code)[:128], now_ms, attempt["occurrence_id"]),
            )
            return True

    def recover(self, current_epoch: int, now_ms: int) -> dict[str, int]:
        with self._transaction() as connection:
            stale = connection.execute(
                """
                SELECT run_id, occurrence_id FROM cron_run_attempts
                WHERE status='AUTHORIZED' AND leader_epoch<>?
                """,
                (current_epoch,),
            ).fetchall()
            for row in stale:
                connection.execute(
                    (
                        "UPDATE cron_run_attempts SET status='UNKNOWN', result_code='CRON_STALE_LEADER', "
                        "token_hash=NULL, updated_at_ms=? WHERE run_id=?"
                    ),
                    (now_ms, row["run_id"]),
                )
                connection.execute(
                    (
                        "UPDATE cron_occurrences SET status='UNKNOWN', unknown_hold_until_ms=?, "
                        "final_code='CRON_STALE_LEADER', updated_at_ms=? "
                        "WHERE occurrence_id=? AND status='RUNNING'"
                    ),
                    (now_ms + 60000, now_ms, row["occurrence_id"]),
                )
            expired = connection.execute(
                """
                SELECT run_id, occurrence_id FROM cron_run_attempts
                WHERE status='TOKEN_ISSUED' AND token_expires_at_ms<=?
                """,
                (now_ms,),
            ).fetchall()
            for row in expired:
                connection.execute(
                    (
                        "UPDATE cron_run_attempts SET status='REJECTED', result_code='CRON_TOKEN_EXPIRED', "
                        "token_hash=NULL, updated_at_ms=? WHERE run_id=?"
                    ),
                    (now_ms, row["run_id"]),
                )
                connection.execute(
                    (
                        "UPDATE cron_occurrences SET status='READY', updated_at_ms=? "
                        "WHERE occurrence_id=? AND status='DISPATCHING'"
                    ),
                    (now_ms, row["occurrence_id"]),
                )
            return {"unknown": len(stale), "requeued": len(expired)}

    def mark_overlap_expired(self, now_ms: int) -> int:
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT o.occurrence_id, o.job_id, j.max_instances
                FROM cron_occurrences o JOIN cron_jobs j ON j.job_id=o.job_id
                WHERE o.status='READY' AND o.scheduled_for_ms + j.misfire_grace_seconds * 1000 < ?
                """,
                (now_ms,),
            ).fetchall()
            for row in rows:
                active = connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM cron_occurrences
                    WHERE job_id=? AND (
                        status IN ('DISPATCHING','RUNNING')
                        OR (status='UNKNOWN' AND unknown_hold_until_ms>?)
                    )
                    """,
                    (row["job_id"], now_ms),
                ).fetchone()
                overlapped = int(active["count"]) >= int(row["max_instances"])
                status = "SKIPPED_OVERLAP" if overlapped else "MISFIRED"
                code = "CRON_MAX_INSTANCES" if overlapped else "CRON_MISFIRED"
                connection.execute(
                    "UPDATE cron_occurrences SET status=?, final_code=?, updated_at_ms=? WHERE occurrence_id=?",
                    (status, code, now_ms, row["occurrence_id"]),
                )
            return len(rows)

    def status_summary(self, now_ms: int) -> dict[str, Any]:
        with self._thread_lock:
            connection = self._connect()
            try:
                leader = connection.execute("SELECT * FROM scheduler_leader WHERE singleton_id=1").fetchone()
                job_rows = connection.execute(
                    "SELECT status, COUNT(*) AS count FROM cron_jobs GROUP BY status"
                ).fetchall()
                occurrence_rows = connection.execute(
                    "SELECT status, COUNT(*) AS count FROM cron_occurrences GROUP BY status"
                ).fetchall()
                backlog_row = connection.execute(
                    "SELECT COUNT(*) AS count FROM cron_misfire_backlog_audit"
                ).fetchone()
                return {
                    "leader": {
                        "active": bool(leader and int(leader["lease_expires_at_ms"] or 0) > now_ms),
                        "epoch": int(leader["epoch"]) if leader else 0,
                        "lease_expires_at_ms": int(leader["lease_expires_at_ms"] or 0) if leader else 0,
                    },
                    "jobs": {str(row["status"]): int(row["count"]) for row in job_rows},
                    "occurrences": {str(row["status"]): int(row["count"]) for row in occurrence_rows},
                    "backlog_compactions": int(backlog_row["count"]) if backlog_row is not None else 0,
                }
            finally:
                connection.close()

    def prune(self, terminal_before_ms: int, unknown_before_ms: int, limit: int = 500) -> int:
        with self._transaction() as connection:
            connection.execute(
                "DELETE FROM cron_misfire_backlog_audit WHERE created_at_ms<?",
                (terminal_before_ms,),
            )
            rows = connection.execute(
                """
                SELECT occurrence_id FROM cron_occurrences
                WHERE (
                    status IN (
                        'SUCCEEDED','FAILED','MISFIRED','SKIPPED_OVERLAP',
                        'REJECTED','CANCELLED_REVISION_CHANGED'
                    )
                    AND updated_at_ms<?
                ) OR (status='UNKNOWN' AND updated_at_ms<?)
                ORDER BY updated_at_ms LIMIT ?
                """,
                (terminal_before_ms, unknown_before_ms, int(limit)),
            ).fetchall()
            ids = [str(row["occurrence_id"]) for row in rows]
            for occurrence_id in ids:
                connection.execute("DELETE FROM cron_run_attempts WHERE occurrence_id=?", (occurrence_id,))
                connection.execute("DELETE FROM cron_occurrences WHERE occurrence_id=?", (occurrence_id,))
            return len(ids)


__all__ = ["CronStore", "CronStoreError", "DispatchAttempt", "LeaderLease"]
