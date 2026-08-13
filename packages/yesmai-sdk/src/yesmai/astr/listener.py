"""Astr 独立消息 listener 的动态 API 桥接。"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from typing import Any

from ..chain import MessageChain as NativeMessageChain
from ..core import AsyncCoreClient, SyncCoreClient
from ..event import EventResult, YesMaiEvent
from .event_types import MessageEventResult
from .filters import (
    describe_filter_rules,
    filters_match,
    is_independent_listener,
    resolve_filter_permissions,
)
from .handler import execute_astr_handler

_LISTENER_PROTOCOL = "astr.listener@1"


def iter_independent_listeners(plugin_class: type[Any]) -> list[tuple[str, Callable[..., Any]]]:
    """按类定义顺序返回当前类直接声明的独立 listener。"""

    return [
        (name, member)
        for name, member in plugin_class.__dict__.items()
        if is_independent_listener(member)
    ]


def build_listener_api_name(plugin_class: type[Any], method_name: str) -> str:
    identity = f"{plugin_class.__module__}:{plugin_class.__qualname__}:{method_name}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"yesmai.astr.listener.{digest}"


def build_listener_handler_name(plugin_class: type[Any], method_name: str) -> str:
    identity = f"{plugin_class.__module__}:{plugin_class.__qualname__}:{method_name}:handler"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"yesmai_astr_listener__{digest}"


def register_independent_listeners(instance: Any) -> None:
    for method_name, handler in iter_independent_listeners(type(instance)):
        instance.register_dynamic_api(
            build_listener_api_name(type(instance), method_name),
            _build_listener_endpoint(instance, method_name, handler),
            description=f"Astr 独立消息监听器：{type(instance).__name__}.{method_name}",
            version="1",
            public=True,
            handler_name=build_listener_handler_name(type(instance), method_name),
            yesmai_protocol=_LISTENER_PROTOCOL,
            yesmai_handler=f"{type(instance).__name__}.{method_name}",
            yesmai_filters=describe_filter_rules(handler),
        )


def _build_listener_endpoint(
    instance: Any,
    method_name: str,
    handler: Callable[..., Any],
) -> Callable[..., Any]:
    async def endpoint(
        event_id: str = "",
        message: Any = None,
        hook: str = "",
        deadline_ms: int = 0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        try:
            return await _invoke_listener(
                instance,
                method_name,
                handler,
                event_id=event_id,
                message=message,
                hook=hook,
                deadline_ms=deadline_ms,
                extra=kwargs,
            )
        except Exception as exc:
            instance.ctx.logger.error(
                "Astr 独立监听器 %s.%s 执行失败：%s",
                type(instance).__name__,
                method_name,
                exc,
                exc_info=True,
            )
            return _response(
                False,
                "ASTR_LISTENER_FAILED",
                "Astr 消息监听器执行失败。",
                None,
                retryable=False,
            )

    return endpoint


async def _invoke_listener(
    instance: Any,
    method_name: str,
    handler: Callable[..., Any],
    *,
    event_id: str,
    message: Any,
    hook: str,
    deadline_ms: int,
    extra: dict[str, Any],
) -> dict[str, Any]:
    from . import _CURRENT_CORE, AstrMessageEvent

    if not isinstance(message, dict):
        return _response(False, "ASTR_LISTENER_MESSAGE_INVALID", "监听事件缺少有效消息对象。", None)

    loop = asyncio.get_running_loop()
    async_core = AsyncCoreClient(instance.ctx)
    sync_core = SyncCoreClient(async_core, loop)
    payload: dict[str, Any] = {
        "event_id": str(event_id),
        "message": message,
        "hook": str(hook),
        "deadline_ms": int(deadline_ms or 0),
        **extra,
    }
    base_event = YesMaiEvent.from_kwargs(
        instance.ctx,
        component_type="ASTR_LISTENER",
        component_name=method_name,
        kwargs=payload,
    )
    event = AstrMessageEvent(base_event, sync_core)
    await resolve_filter_permissions(handler, event, async_core)
    if not filters_match(handler, event):
        return _response(
            True,
            "ASTR_LISTENER_NOT_MATCHED",
            "监听条件未匹配。",
            {
                "handled": False,
                "matched": False,
                "stop_yesmai_propagation": False,
                "segments": [],
                "sent_during_handler": False,
            },
        )

    results = await execute_astr_handler(
        instance,
        handler,
        event,
        async_core=async_core,
        sync_core=sync_core,
        current_core=_CURRENT_CORE,
    )
    serialized_results = [
        _serialize_result(
            snapshot.result,
            sent_during_handler=snapshot.sent_during_handler,
            stopped=snapshot.stopped,
        )["data"]
        for snapshot in results
    ]
    if len(serialized_results) == 1:
        return _response(True, "OK", "操作成功", serialized_results[0])
    stop = any(bool(item and item.get("stop_yesmai_propagation")) for item in serialized_results)
    return _response(
        True,
        "OK",
        "操作成功",
        {
            "handled": any(bool(item and item.get("handled")) for item in serialized_results),
            "matched": True,
            "stop_yesmai_propagation": stop,
            "segments": [],
            "sent_during_handler": any(bool(item and item.get("sent_during_handler")) for item in serialized_results),
            "results": serialized_results,
        },
    )


def _serialize_result(
    result: Any,
    *,
    sent_during_handler: bool,
    stopped: bool,
) -> dict[str, Any]:
    handled = result is not None or sent_during_handler or stopped
    segments: list[dict[str, Any]] = []
    custom_result: Any = None

    if isinstance(result, MessageEventResult):
        segments = _safe_segments(result.chain)
        stopped = result.blocked or stopped
        custom_result = _msgpack_safe(result.custom_result)
    elif isinstance(result, EventResult):
        segments = _safe_segments(result.chain)
        stopped = result.blocked or not result.continue_processing or stopped
        custom_result = _msgpack_safe(result.custom_result)
    elif isinstance(result, str):
        segments = NativeMessageChain.text(result).to_segments()
    elif result is not None:
        return _response(
            False,
            "ASTR_LISTENER_RESULT_INVALID",
            f"监听器返回了不支持的结果类型：{type(result).__name__}",
            None,
        )

    if sent_during_handler:
        segments = []
    data = {
        "handled": handled,
        "matched": True,
        "stop_yesmai_propagation": bool(stopped),
        "segments": segments,
        "sent_during_handler": bool(sent_during_handler),
    }
    if custom_result is not None:
        data["custom_result"] = custom_result
    return _response(True, "OK", "操作成功", data)


def _safe_segments(chain: Any) -> list[dict[str, Any]]:
    if not hasattr(chain, "to_segments"):
        raise TypeError("监听器结果缺少有效消息链")
    segments = chain.to_segments()
    if not isinstance(segments, list):
        raise TypeError("监听器消息链必须序列化为列表")
    return [_msgpack_safe(segment) for segment in segments]


def _msgpack_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, list | tuple):
        return [_msgpack_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _msgpack_safe(item) for key, item in value.items()}
    return str(value)


def _response(
    ok: bool,
    code: str,
    message: str,
    data: Any,
    *,
    retryable: bool = False,
) -> dict[str, Any]:
    return {
        "ok": bool(ok),
        "code": str(code),
        "message": str(message),
        "data": data,
        "retryable": bool(retryable),
    }


__all__ = [
    "build_listener_api_name",
    "build_listener_handler_name",
    "iter_independent_listeners",
    "register_independent_listeners",
]
