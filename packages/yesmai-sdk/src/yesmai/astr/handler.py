"""Astr synchronous, coroutine and async-generator handler execution bridge."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..core import AsyncCoreClient, SyncCoreClient
from ..event import EventResult
from .cron import cron_catalog_batch
from .event_types import MessageEventResult, ResultContentType
from .star_tools import activate_star, deactivate_star


class AstrStreamingUnsupportedError(RuntimeError):
    """Raised when an Astr true-streaming result reaches the execution bridge."""


def _reject_unsupported_streaming(result: Any) -> None:
    if not isinstance(result, MessageEventResult):
        return
    if result.async_stream is not None or result.result_content_type is ResultContentType.STREAMING_RESULT:
        raise AstrStreamingUnsupportedError(
            "MessageEventResult.async_stream true streaming is unsupported; "
            "async-generator handlers use ordered snapshot delivery instead"
        )


def result_is_stopped(result: Any, event: Any) -> bool:
    if isinstance(result, MessageEventResult):
        return bool(result.blocked or event.is_stopped())
    if isinstance(result, EventResult):
        return bool(result.blocked or not result.continue_processing or event.is_stopped())
    return bool(event.is_stopped())


@dataclass(frozen=True, slots=True)
class _HandlerResultSnapshot:
    result: Any
    sent_during_handler: bool
    stopped: bool


def _snapshot_result(result: Any, event: Any) -> _HandlerResultSnapshot:
    _reject_unsupported_streaming(result)
    return _HandlerResultSnapshot(
        result=result,
        sent_during_handler=bool(event._has_send_oper),
        stopped=result_is_stopped(result, event),
    )


async def execute_astr_handler(
    instance: Any,
    handler: Callable[..., Any],
    event: Any,
    *,
    async_core: AsyncCoreClient,
    sync_core: SyncCoreClient,
    current_core: Any,
) -> list[_HandlerResultSnapshot]:
    """Execute a handler and atomically flush any staged Cron catalog changes."""

    async with cron_catalog_batch(instance):
        yielded: list[_HandlerResultSnapshot] = []
        if inspect.isasyncgenfunction(handler):
            event._core = async_core
            token = current_core.set(async_core)
            star_token = activate_star(instance)
            generator = handler(instance, event)
            try:
                async for item in generator:
                    resolved = event.get_result() if item is None else item
                    snapshot = _snapshot_result(resolved, event)
                    yielded.append(snapshot)
                    if snapshot.stopped:
                        break
            finally:
                try:
                    await generator.aclose()
                finally:
                    deactivate_star(star_token)
                    current_core.reset(token)
            return yielded or [_snapshot_result(event.get_result(), event)]

        if inspect.iscoroutinefunction(handler):
            event._core = async_core
            token = current_core.set(async_core)
            star_token = activate_star(instance)
            try:
                result = await handler(instance, event)
            finally:
                deactivate_star(star_token)
                current_core.reset(token)
            return [_snapshot_result(event.get_result() if result is None else result, event)]

        def invoke() -> Any:
            event._core = sync_core
            token = current_core.set(sync_core)
            star_token = activate_star(instance)
            try:
                return handler(instance, event)
            finally:
                deactivate_star(star_token)
                current_core.reset(token)

        result = await asyncio.to_thread(invoke)
        if inspect.isawaitable(result):
            event._core = async_core
            token = current_core.set(async_core)
            star_token = activate_star(instance)
            try:
                result = await result
            finally:
                deactivate_star(star_token)
                current_core.reset(token)
        return [_snapshot_result(event.get_result() if result is None else result, event)]


__all__ = ["AstrStreamingUnsupportedError", "execute_astr_handler", "result_is_stopped"]
