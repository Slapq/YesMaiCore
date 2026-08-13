"""Owner-bound Cron catalog and dispatch service for YesMaiCore."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from .cron_calendar import (
        CronCalendarError,
        minimum_interval_seconds,
        next_occurrence,
        normalize_schedule,
    )
    from .cron_store import CronStore, DispatchAttempt, LeaderLease
except ImportError:
    from cron_calendar import (
        CronCalendarError,
        minimum_interval_seconds,
        next_occurrence,
        normalize_schedule,
    )
    from cron_store import CronStore, DispatchAttempt, LeaderLease

UTC = timezone.utc
_CRON_PROTOCOL = "cron.handler@1"


@dataclass(frozen=True, slots=True)
class CronSettings:
    enabled: bool = True
    default_timezone: str = "Asia/Shanghai"
    catalog_refresh_seconds: float = 5.0
    owner_job_limit: int = 32
    global_job_limit: int = 256
    owner_dispatch_limit: int = 4
    minimum_interval_seconds: int = 60
    maximum_timeout_seconds: int = 7200
    leader_lease_seconds: int = 15
    leader_heartbeat_seconds: int = 5
    token_ttl_seconds: int = 30


class CronService:
    def __init__(self, ctx: Any, data_dir: Path, settings: CronSettings) -> None:
        self.ctx = ctx
        self.settings = settings
        self.store = CronStore(Path(data_dir) / "cron-v1.sqlite3")
        self.process_nonce = secrets.token_urlsafe(24)
        self.leader_id = "com.yesmai.core"
        self._lease: LeaderLease | None = None
        self._runner: asyncio.Task[None] | None = None
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._stopping = False
        self._last_catalog_refresh = 0.0
        self._last_heartbeat = 0.0
        self._last_prune = 0.0
        self._last_error_code = ""
        self._definition_cache: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        if not self.settings.enabled or self._runner is not None:
            return
        await asyncio.to_thread(self.store.initialize)
        self._stopping = False
        self._runner = asyncio.create_task(self._run(), name="yesmai-core-cron")

    async def stop(self) -> None:
        self._stopping = True
        runner, self._runner = self._runner, None
        if runner is not None:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        tasks, self._dispatch_tasks = list(self._dispatch_tasks), set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        lease, self._lease = self._lease, None
        if lease is not None:
            await asyncio.to_thread(self.store.release_leader, lease, self._now_ms())

    async def authorize(self, run_id: str, token: str) -> bool:
        if self._stopping or not str(run_id).strip() or not str(token):
            return False
        return await asyncio.to_thread(self.store.authorize, str(run_id), str(token), self._now_ms())

    async def status(self) -> dict[str, Any]:
        summary = await asyncio.to_thread(self.store.status_summary, self._now_ms())
        summary.update(
            {
                "enabled": self.settings.enabled,
                "running": self._runner is not None and not self._runner.done(),
                "is_leader": self._lease is not None,
                "dispatching": len(self._dispatch_tasks),
                "last_error_code": self._last_error_code,
                "protocol": _CRON_PROTOCOL,
            }
        )
        return summary

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    async def _run(self) -> None:
        try:
            while not self._stopping:
                now_ms = self._now_ms()
                await self._maintain_leader(now_ms)
                if self._lease is not None:
                    now_monotonic = time.monotonic()
                    if now_monotonic - self._last_catalog_refresh >= self.settings.catalog_refresh_seconds:
                        await self._refresh_catalog(now_ms)
                        self._last_catalog_refresh = now_monotonic
                    await self._tick(now_ms)
                    if now_monotonic - self._last_prune >= 3600:
                        await asyncio.to_thread(
                            self.store.prune,
                            now_ms - 30 * 86400000,
                            now_ms - 180 * 86400000,
                        )
                        self._last_prune = now_monotonic
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error_code = "CRON_SERVICE_FAILED"
            self.ctx.logger.error("YesMai Cron service 异常停止：%s", exc, exc_info=True)

    async def _maintain_leader(self, now_ms: int) -> None:
        now_monotonic = time.monotonic()
        if self._lease is None:
            self._lease = await asyncio.to_thread(
                self.store.acquire_leader,
                self.leader_id,
                self.process_nonce,
                now_ms,
                self.settings.leader_lease_seconds * 1000,
            )
            if self._lease is not None:
                await asyncio.to_thread(self.store.recover, self._lease.epoch, now_ms)
                self._last_heartbeat = now_monotonic
            return
        if now_monotonic - self._last_heartbeat < self.settings.leader_heartbeat_seconds:
            return
        renewed = await asyncio.to_thread(
            self.store.renew_leader,
            self._lease,
            now_ms,
            self.settings.leader_lease_seconds * 1000,
        )
        self._lease = renewed
        self._last_heartbeat = now_monotonic
        if renewed is None:
            self._last_error_code = "CRON_LEADER_LOST"

    async def _refresh_catalog(self, now_ms: int) -> None:
        try:
            entries = await self.ctx.api.list()
            if not isinstance(entries, list):
                raise TypeError("api.list 未返回列表")
            definitions = self._build_definitions(entries, now_ms)
            await asyncio.to_thread(self.store.reconcile_jobs, definitions, now_ms)
            self._last_error_code = ""
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error_code = "CRON_DISCOVERY_FAILED"
            self.ctx.logger.warning("YesMai Cron catalog 刷新失败，保留最后成功 snapshot：%s", exc)

    def _build_definitions(self, entries: list[Any], now_ms: int) -> list[dict[str, Any]]:
        by_owner: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("public") is not True or entry.get("enabled", True) is not True:
                continue
            metadata = entry.get("metadata")
            if not isinstance(metadata, dict) or metadata.get("yesmai_protocol") != _CRON_PROTOCOL:
                continue
            owner = str(entry.get("plugin_id") or "").strip()
            full_name = str(entry.get("full_name") or "").strip()
            if not owner or owner == self.leader_id or not full_name:
                continue
            by_owner.setdefault(owner, []).append(entry)

        accepted: list[dict[str, Any]] = []
        active_cache_keys: set[str] = set()
        for owner in sorted(by_owner):
            owner_entries = sorted(by_owner[owner], key=lambda value: str(value.get("full_name") or ""))
            identity_counts: dict[tuple[str, str], int] = {}
            for entry in owner_entries:
                metadata = entry.get("metadata") or {}
                identity = (
                    str(entry.get("full_name") or "").strip(),
                    str(metadata.get("stable_name") or "").strip(),
                )
                identity_counts[identity] = identity_counts.get(identity, 0) + 1
            eligible_entries: list[dict[str, Any]] = []
            for entry in owner_entries:
                metadata = entry.get("metadata") or {}
                identity = (
                    str(entry.get("full_name") or "").strip(),
                    str(metadata.get("stable_name") or "").strip(),
                )
                if identity_counts[identity] > 1:
                    self.ctx.logger.warning(
                        "YesMai Cron Job 稳定身份冲突，已拒绝全部冲突定义：owner=%s handler=%s stable_name=%s",
                        owner,
                        identity[0],
                        identity[1],
                    )
                    continue
                eligible_entries.append(entry)
            for entry in eligible_entries[: self.settings.owner_job_limit]:
                if len(accepted) >= self.settings.global_job_limit:
                    self._definition_cache = {
                        key: value for key, value in self._definition_cache.items() if key in active_cache_keys
                    }
                    return accepted
                try:
                    cache_payload = {
                        "plugin_id": entry.get("plugin_id"),
                        "full_name": entry.get("full_name"),
                        "version": entry.get("version"),
                        "metadata": entry.get("metadata"),
                    }
                    cache_key = hashlib.sha256(
                        json.dumps(cache_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                            "utf-8"
                        )
                    ).hexdigest()
                    active_cache_keys.add(cache_key)
                    definition = self._definition_cache.get(cache_key)
                    if definition is None:
                        definition = self._definition_from_entry(entry, now_ms)
                        self._definition_cache[cache_key] = definition
                    accepted.append(dict(definition))
                except (CronCalendarError, TypeError, ValueError) as exc:
                    self.ctx.logger.warning(
                        "YesMai Cron Job 定义无效，已禁用：%s：%s",
                        entry.get("full_name") or "<unknown>",
                        exc,
                    )
        self._definition_cache = {
            key: value for key, value in self._definition_cache.items() if key in active_cache_keys
        }
        return accepted

    def _definition_from_entry(self, entry: dict[str, Any], now_ms: int) -> dict[str, Any]:
        metadata = entry["metadata"]
        serialized = json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(serialized.encode("utf-8")) > 8192:
            raise ValueError("metadata 超过 8 KiB")
        owner = str(entry["plugin_id"]).strip()
        full_name = str(entry["full_name"]).strip()
        stable_name = str(metadata.get("stable_name") or "").strip()
        if not stable_name:
            raise ValueError("stable_name 不能为空")
        schedule = normalize_schedule(
            metadata.get("schedule"),
            default_timezone=self.settings.default_timezone,
        )
        interval = minimum_interval_seconds(schedule, after=datetime.fromtimestamp(now_ms / 1000, UTC))
        if interval is None:
            raise ValueError("schedule 不可满足")
        if interval < self.settings.minimum_interval_seconds:
            raise ValueError(f"schedule 最短间隔不能小于 {self.settings.minimum_interval_seconds} 秒")
        execution = metadata.get("execution")
        if not isinstance(execution, dict):
            execution = {}
        misfire = int(execution.get("misfire_grace_seconds", 60))
        max_instances = int(execution.get("max_instances", 1))
        timeout_seconds = int(execution.get("timeout_seconds", 3600))
        if misfire < 0:
            raise ValueError("misfire_grace_seconds 不能为负数")
        if not 1 <= max_instances <= 4:
            raise ValueError("max_instances 必须在 1..4")
        if not 1 <= timeout_seconds <= self.settings.maximum_timeout_seconds:
            raise ValueError(f"timeout_seconds 必须在 1..{self.settings.maximum_timeout_seconds}")
        next_fire = next_occurrence(schedule, datetime.fromtimestamp(now_ms / 1000, UTC))
        if next_fire is None:
            raise ValueError("schedule 没有未来 occurrence")
        fingerprint_payload = {
            "handler": full_name,
            "version": str(entry.get("version") or "1"),
            "stable_name": stable_name,
            "schedule": schedule.to_metadata(),
            "execution": {
                "misfire_grace_seconds": misfire,
                "coalesce": bool(execution.get("coalesce", True)),
                "max_instances": max_instances,
                "timeout_seconds": timeout_seconds,
            },
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {
            "job_id": CronStore.job_id(owner, full_name, stable_name),
            "owner_plugin_id": owner,
            "handler_full_name": full_name,
            "api_version": str(entry.get("version") or "1"),
            "stable_name": stable_name,
            "catalog_fingerprint": fingerprint,
            "schedule": schedule.to_metadata(),
            "timezone": schedule.timezone,
            "misfire_grace_seconds": misfire,
            "coalesce": bool(execution.get("coalesce", True)),
            "max_instances": max_instances,
            "timeout_seconds": timeout_seconds,
            "next_fire_at_ms": int(next_fire.scheduled_for_utc.timestamp() * 1000),
        }

    async def _tick(self, now_ms: int) -> None:
        lease = self._lease
        if lease is None:
            return
        await asyncio.to_thread(self.store.recover, lease.epoch, now_ms)
        await self._materialize_due(now_ms)
        await asyncio.to_thread(self.store.mark_overlap_expired, now_ms)
        if lease.lease_expires_at_ms - now_ms < 10000:
            return
        owner_counts: dict[str, int] = {}
        for task in self._dispatch_tasks:
            owner = getattr(task, "_yesmai_cron_owner", "")
            owner_counts[owner] = owner_counts.get(owner, 0) + 1
        for occurrence in await asyncio.to_thread(self.store.list_ready_occurrences, now_ms):
            owner = str(occurrence["owner_plugin_id"])
            if owner_counts.get(owner, 0) >= self.settings.owner_dispatch_limit:
                continue
            attempt = await asyncio.to_thread(
                self.store.claim_occurrence,
                str(occurrence["occurrence_id"]),
                lease,
                now_ms,
                self.settings.token_ttl_seconds * 1000,
            )
            if attempt is None:
                continue
            task = asyncio.create_task(self._dispatch(attempt), name=f"yesmai-cron-{attempt.run_id}")
            task._yesmai_cron_owner = owner
            self._dispatch_tasks.add(task)
            task.add_done_callback(self._dispatch_tasks.discard)
            owner_counts[owner] = owner_counts.get(owner, 0) + 1

    async def _materialize_due(self, now_ms: int) -> None:
        for job in await asyncio.to_thread(self.store.list_active_jobs):
            next_fire_ms = job.get("next_fire_at_ms")
            if next_fire_ms is None or int(next_fire_ms) > now_ms:
                continue
            schedule = normalize_schedule(job["schedule"], default_timezone=self.settings.default_timezone)
            cursor_ms = int(next_fire_ms)
            grace_ms = int(job["misfire_grace_seconds"]) * 1000
            cutoff_ms = now_ms - grace_ms
            if cursor_ms < cutoff_ms:
                resume = next_occurrence(
                    schedule,
                    datetime.fromtimestamp((cutoff_ms - 1) / 1000, UTC),
                )
                if resume is None:
                    continue
                resume_ms = int(resume.scheduled_for_utc.timestamp() * 1000)
                compacted = await asyncio.to_thread(
                    self.store.compact_expired_backlog,
                    str(job["job_id"]),
                    int(job["job_revision"]),
                    cursor_ms,
                    resume_ms,
                    cutoff_ms,
                    self._lease,
                    now_ms,
                )
                if not compacted:
                    continue
                cursor_ms = resume_ms
                if cursor_ms > now_ms:
                    continue
            due: list[Any] = []
            for _ in range(100):
                if cursor_ms > now_ms:
                    break
                local = datetime.fromtimestamp(cursor_ms / 1000, UTC).astimezone(ZoneInfo(schedule.timezone))
                due.append(
                    {
                        "scheduled_for_ms": cursor_ms,
                        "local_scheduled_text": local.isoformat(),
                        "fold": local.fold,
                        "utc_offset_seconds": int((local.utcoffset() or UTC.utcoffset(local)).total_seconds()),
                        "tzdata_version": self._tzdata_version(),
                    }
                )
                following = next_occurrence(schedule, datetime.fromtimestamp(cursor_ms / 1000, UTC))
                if following is None:
                    cursor_ms = 0
                    break
                cursor_ms = int(following.scheduled_for_utc.timestamp() * 1000)
            has_more_due = bool(cursor_ms and cursor_ms <= now_ms)
            for index, occurrence in enumerate(due):
                scheduled = int(occurrence["scheduled_for_ms"])
                late = now_ms - scheduled
                coalesced = bool(job["coalesce"]) and (index < len(due) - 1 or has_more_due)
                status = "MISFIRED" if coalesced or late > int(job["misfire_grace_seconds"]) * 1000 else "READY"
                following = next_occurrence(schedule, datetime.fromtimestamp(scheduled / 1000, UTC))
                following_ms = int(following.scheduled_for_utc.timestamp() * 1000) if following else None
                changed = await asyncio.to_thread(
                    self.store.materialize,
                    str(job["job_id"]),
                    int(job["job_revision"]),
                    scheduled,
                    occurrence,
                    status,
                    following_ms,
                    now_ms,
                )
                if not changed:
                    break

    @staticmethod
    def _tzdata_version() -> str:
        try:
            return importlib.metadata.version("tzdata")
        except importlib.metadata.PackageNotFoundError:
            return "system"

    async def _dispatch(self, attempt: DispatchAttempt) -> None:
        timeout = attempt.timeout_seconds
        api_name = f"{attempt.handler_full_name}@{attempt.api_version}"
        scheduled = datetime.fromtimestamp(attempt.scheduled_for_ms / 1000, UTC).isoformat()
        deadline_ms = self._now_ms() + timeout * 1000
        try:
            result = await asyncio.wait_for(
                self.ctx.api.call(
                    api_name,
                    run_id=attempt.run_id,
                    token=attempt.token,
                    scheduled_for_utc=scheduled,
                    deadline_utc=datetime.fromtimestamp(deadline_ms / 1000, UTC).isoformat(),
                    idempotency_key=attempt.occurrence_id,
                    rpc_timeout_ms=(timeout + 5) * 1000,
                ),
                timeout=timeout,
            )
            status = await asyncio.to_thread(self.store.attempt_status, attempt.run_id)
            if status != "AUTHORIZED":
                return
            succeeded = isinstance(result, dict) and result.get("ok") is True
            code = (
                str(result.get("code") or ("OK" if succeeded else "CRON_HANDLER_FAILED"))
                if isinstance(result, dict)
                else "CRON_HANDLER_RESULT_INVALID"
            )
            message = str(result.get("message") or "") if isinstance(result, dict) else "Cron handler 返回格式无效"
            await asyncio.to_thread(
                self.store.finish,
                attempt.run_id,
                succeeded=succeeded,
                code=code,
                message=message,
                now_ms=self._now_ms(),
            )
        except asyncio.CancelledError:
            await self._mark_unknown_if_authorized(attempt, "CRON_CORE_STOPPED")
            raise
        except TimeoutError:
            await self._mark_unknown_if_authorized(attempt, "CRON_EXECUTION_TIMEOUT")
        except Exception as exc:
            self.ctx.logger.warning("YesMai Cron handler 调用失败：%s：%s", api_name, exc)
            await self._mark_unknown_if_authorized(attempt, "CRON_EXECUTION_UNAVAILABLE")

    async def _mark_unknown_if_authorized(self, attempt: DispatchAttempt, code: str) -> None:
        status = await asyncio.to_thread(self.store.attempt_status, attempt.run_id)
        if status != "AUTHORIZED":
            return
        now_ms = self._now_ms()
        await asyncio.to_thread(
            self.store.mark_unknown,
            attempt.run_id,
            code,
            now_ms,
            now_ms + (attempt.timeout_seconds + 60) * 1000,
        )


__all__ = ["CronService", "CronSettings"]
