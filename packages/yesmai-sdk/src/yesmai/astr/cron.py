"""Restricted Astr scheduler facade backed by owner-bound dynamic APIs."""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import threading
from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..core import AsyncCoreClient
from ..cron import CronTrigger, CronUnsupportedError
from .star_tools import activate_star, deactivate_star


@dataclass(slots=True)
class CronJob:
    id: str
    func: Callable[..., Any]
    trigger: CronTrigger
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    name: str
    misfire_grace_time: int
    coalesce: bool
    max_instances: int
    timeout_seconds: int
    api_name: str
    handler_name: str

    @property
    def pending(self) -> bool:
        return False


@dataclass(slots=True)
class _CatalogBatch:
    scheduler: Any
    jobs: dict[str, CronJob]
    mutations: list[tuple[str, CronJob | str, bool]]
    depth: int = 1
    closed: bool = False


_CURRENT_BATCH: contextvars.ContextVar[_CatalogBatch | None] = contextvars.ContextVar(
    "yesmai_astr_cron_batch",
    default=None,
)


class AstrCronScheduler:
    """Synchronous APScheduler-like catalog facade with asynchronous batch flush."""

    def __init__(self, star: Any, *, default_timezone: str | None = None) -> None:
        self.star = star
        self.default_timezone = default_timezone
        self._jobs: dict[str, CronJob] = {}
        self._api_names: set[str] = set()
        self._lock = threading.RLock()
        self._flush_lock = asyncio.Lock()
        self._dirty_revision = 0
        self._synced_revision = 0
        self._closed = False

    def add_job(
        self,
        func: Callable[..., Any],
        trigger: CronTrigger,
        args: list[Any] | tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        id: str | None = None,
        name: str | None = None,
        misfire_grace_time: int = 60,
        coalesce: bool = True,
        max_instances: int = 1,
        replace_existing: bool = False,
        **options: Any,
    ) -> CronJob:
        if self._closed:
            raise RuntimeError("Astr Cron scheduler 已关闭")
        if options:
            unsupported = ", ".join(sorted(options))
            raise CronUnsupportedError(f"astr-calendar@1 add_job 暂不支持参数：{unsupported}")
        if not callable(func):
            raise TypeError("Cron func 必须可调用")
        if not isinstance(trigger, CronTrigger):
            raise CronUnsupportedError("第一阶段只支持 yesmai.astr.CronTrigger")
        job_id = str(id or "").strip()
        if not job_id:
            raise CronUnsupportedError("持久 Cron Job 必须提供 id")
        with self._lock:
            batch = self._active_batch()
            jobs = batch.jobs if batch is not None else self._jobs
            if job_id in jobs and not replace_existing:
                raise ValueError(f"Cron Job 已存在：{job_id}")
            identity = f"{type(self.star).__module__}:{type(self.star).__qualname__}:{job_id}"
            digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
            job = CronJob(
                id=job_id,
                func=func,
                trigger=trigger,
                args=tuple(args or ()),
                kwargs=dict(kwargs or {}),
                name=str(name or job_id),
                misfire_grace_time=int(misfire_grace_time),
                coalesce=bool(coalesce),
                max_instances=int(max_instances),
                timeout_seconds=3600,
                api_name=f"yesmai.astr.cron.{digest}",
                handler_name=f"yesmai_astr_cron__{digest}",
            )
            jobs[job_id] = job
            if batch is not None:
                batch.mutations.append(("add", job, replace_existing))
            else:
                self._register_job(job)
                self._dirty_revision += 1
            return job

    def get_job(self, job_id: str, jobstore: str | None = None) -> CronJob | None:
        if jobstore not in (None, "default"):
            raise CronUnsupportedError("第一阶段只支持 default jobstore")
        with self._lock:
            batch = self._active_batch()
            jobs = batch.jobs if batch is not None else self._jobs
            return jobs.get(str(job_id))

    def get_jobs(self, jobstore: str | None = None, pending: Any = None) -> list[CronJob]:
        del pending
        if jobstore not in (None, "default"):
            raise CronUnsupportedError("第一阶段只支持 default jobstore")
        with self._lock:
            batch = self._active_batch()
            jobs = batch.jobs if batch is not None else self._jobs
            return list(jobs.values())

    def remove_job(self, job_id: str, jobstore: str | None = None) -> None:
        if jobstore not in (None, "default"):
            raise CronUnsupportedError("第一阶段只支持 default jobstore")
        with self._lock:
            batch = self._active_batch()
            jobs = batch.jobs if batch is not None else self._jobs
            job = jobs.pop(str(job_id), None)
            if job is None:
                raise KeyError(f"Cron Job 不存在：{job_id}")
            if batch is not None:
                batch.mutations.append(("remove", job.id, False))
            else:
                self.star.unregister_dynamic_api(job.api_name, version="1")
                self._api_names.discard(job.api_name)
                self._dirty_revision += 1

    def remove_all_jobs(self, jobstore: str | None = None) -> None:
        for job in list(self.get_jobs(jobstore)):
            self.remove_job(job.id, jobstore)

    def _active_batch(self) -> _CatalogBatch | None:
        batch = _CURRENT_BATCH.get()
        if batch is None or batch.scheduler is not self:
            return None
        if batch.closed:
            raise CronUnsupportedError("Cron catalog transaction 已结束")
        return batch

    def begin_batch(self) -> tuple[_CatalogBatch, contextvars.Token[_CatalogBatch | None] | None]:
        with self._lock:
            current = self._active_batch()
            if current is not None:
                current.depth += 1
                return current, None
            batch = _CatalogBatch(self, dict(self._jobs), [])
            token = _CURRENT_BATCH.set(batch)
            return batch, token

    async def finish_batch(
        self,
        batch: _CatalogBatch,
        token: contextvars.Token[_CatalogBatch | None] | None,
        *,
        success: bool,
    ) -> None:
        should_flush = False
        try:
            with self._lock:
                if batch.closed:
                    return
                batch.depth -= 1
                if batch.depth > 0:
                    return
                if success and batch.mutations:
                    candidate = dict(self._jobs)
                    for operation, payload, replace_existing in batch.mutations:
                        if operation == "add":
                            assert isinstance(payload, CronJob)
                            if payload.id in candidate and not replace_existing:
                                raise ValueError(f"Cron Job 并发冲突：{payload.id}")
                            candidate[payload.id] = payload
                        else:
                            job_id = str(payload)
                            if job_id not in candidate:
                                raise KeyError(f"Cron Job 并发移除冲突：{job_id}")
                            candidate.pop(job_id)
                    self._jobs = candidate
                    self._rebuild_dynamic_apis()
                    self._dirty_revision += 1
                should_flush = success and self._dirty_revision != self._synced_revision
                batch.closed = True
        finally:
            if token is not None:
                _CURRENT_BATCH.reset(token)
        if should_flush:
            await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            while True:
                with self._lock:
                    target_revision = self._dirty_revision
                    if target_revision == self._synced_revision:
                        return
                accepted = await self.star.sync_dynamic_apis(offline_reason="Cron Job 已移除或插件已重载")
                if accepted is not True:
                    raise RuntimeError("CRON_CATALOG_SYNC_FAILED: Host 未接受动态 Cron catalog")
                with self._lock:
                    self._synced_revision = target_revision
                    if self._dirty_revision == target_revision:
                        return

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._jobs.clear()
            for api_name in list(self._api_names):
                self.star.unregister_dynamic_api(api_name, version="1")
            self._api_names.clear()

    def _rebuild_dynamic_apis(self) -> None:
        for api_name in list(self._api_names):
            self.star.unregister_dynamic_api(api_name, version="1")
        self._api_names.clear()
        for job in self._jobs.values():
            self._register_job(job)

    def _register_job(self, job: CronJob) -> None:
        self.star.register_dynamic_api(
            job.api_name,
            self._build_endpoint(job),
            description=f"Astr Cron：{job.name}",
            version="1",
            public=True,
            handler_name=job.handler_name,
            timeout_ms=(job.timeout_seconds + 5) * 1000,
            yesmai_protocol="cron.handler@1",
            stable_name=job.id,
            schedule=job.trigger.to_schedule(self.default_timezone),
            execution={
                "misfire_grace_seconds": job.misfire_grace_time,
                "coalesce": job.coalesce,
                "max_instances": job.max_instances,
                "timeout_seconds": job.timeout_seconds,
            },
        )
        self._api_names.add(job.api_name)

    def _build_endpoint(self, job: CronJob) -> Callable[..., Any]:
        async def endpoint(
            run_id: str = "",
            token: str = "",
            scheduled_for_utc: str = "",
            deadline_utc: str = "",
            idempotency_key: str = "",
            **extra: Any,
        ) -> dict[str, Any]:
            del scheduled_for_utc, idempotency_key, extra
            core = AsyncCoreClient(self.star.ctx)
            try:
                authorization = await core.cron.authorize(str(run_id), str(token))
            except Exception:
                return {
                    "ok": False,
                    "code": "CRON_AUTHORIZATION_REJECTED",
                    "message": "Cron 执行授权失败。",
                    "data": None,
                    "retryable": False,
                }
            if not isinstance(authorization, dict) or authorization.get("ok") is not True:
                return {
                    "ok": False,
                    "code": "CRON_AUTHORIZATION_REJECTED",
                    "message": "Cron 执行授权失败。",
                    "data": None,
                    "retryable": False,
                }
            timeout = job.timeout_seconds
            if deadline_utc:
                try:
                    deadline = datetime.fromisoformat(str(deadline_utc).replace("Z", "+00:00"))
                    timeout = max(
                        0.001,
                        min(timeout, (deadline.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()),
                    )
                except ValueError:
                    pass
            from . import _CURRENT_CORE

            core_token = _CURRENT_CORE.set(core)
            star_token = activate_star(self.star)
            try:
                async with asyncio.timeout(timeout):
                    if inspect.iscoroutinefunction(job.func):
                        result = await job.func(*job.args, **job.kwargs)
                    else:
                        result = await asyncio.to_thread(job.func, *job.args, **job.kwargs)
                        if inspect.isawaitable(result):
                            result = await result
                    if inspect.isasyncgen(result) or inspect.isgenerator(result):
                        raise CronUnsupportedError("Cron callback 不支持 generator")
            except TimeoutError:
                return {
                    "ok": False,
                    "code": "CRON_HANDLER_TIMEOUT",
                    "message": "Cron handler 执行超时。",
                    "data": None,
                    "retryable": False,
                }
            except Exception as exc:
                self.star.ctx.logger.error("Astr Cron %s 执行失败：%s", job.id, exc, exc_info=True)
                return {
                    "ok": False,
                    "code": "CRON_HANDLER_FAILED",
                    "message": "Cron handler 执行失败。",
                    "data": None,
                    "retryable": False,
                }
            finally:
                deactivate_star(star_token)
                _CURRENT_CORE.reset(core_token)
            return {"ok": True, "code": "OK", "message": "Cron handler 执行完成。", "data": None, "retryable": False}

        return endpoint


class CronManager:
    def __init__(self, star: Any) -> None:
        self.scheduler = AstrCronScheduler(star)


@asynccontextmanager
async def cron_catalog_batch(instance: Any) -> Iterator[None]:
    manager = getattr(getattr(instance, "context", None), "cron_manager", None)
    scheduler = getattr(manager, "scheduler", None)
    if not isinstance(scheduler, AstrCronScheduler):
        yield
        return
    batch, token = scheduler.begin_batch()
    try:
        yield
    except BaseException:
        await scheduler.finish_batch(batch, token, success=False)
        raise
    else:
        await scheduler.finish_batch(batch, token, success=True)


__all__ = ["AstrCronScheduler", "CronJob", "CronManager", "CronTrigger", "CronUnsupportedError", "cron_catalog_batch"]
