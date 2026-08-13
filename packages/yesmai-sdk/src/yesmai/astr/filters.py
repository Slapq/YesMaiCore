"""AstrBot 风格过滤器的严格限定兼容实现。"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from enum import Flag, auto
from functools import wraps
from typing import Any

from maibot_sdk import HookHandler, Tool
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder, ToolParameterInfo

from ..core import AsyncCoreClient
from ..event import YesMaiEvent
from .cron import cron_catalog_batch
from .provider import LLMResponse, ProviderRequest

_FILTERS_ATTR = "__yesmai_astr_filters__"
_COMPONENT_INFO_ATTR = "__maibot_component_info__"
_COMMAND_ADMIN_PERMISSION = "yesmai.bot.command_admin"
_COMMAND_ADMIN_SOURCE = "yesmai-core-config@1"


class AstrFilterUnsupportedError(RuntimeError):
    """请求的 Astr 过滤能力在当前 MaiBot 映射中无法可靠实现。"""


class EventMessageType(Flag):
    GROUP_MESSAGE = auto()
    PRIVATE_MESSAGE = auto()
    OTHER_MESSAGE = auto()
    ALL = GROUP_MESSAGE | PRIVATE_MESSAGE | OTHER_MESSAGE


class PermissionType(Flag):
    """AstrBot 权限类型；MEMBER 不排除管理员。"""

    ADMIN = auto()
    MEMBER = auto()


class PlatformAdapterType(Flag):
    AIOCQHTTP = auto()
    QQOFFICIAL = auto()
    QQOFFICIAL_WEBHOOK = auto()
    TELEGRAM = auto()
    WECOM = auto()
    WECOM_AI_BOT = auto()
    LARK = auto()
    DINGTALK = auto()
    DISCORD = auto()
    SLACK = auto()
    KOOK = auto()
    VOCECHAT = auto()
    WEIXIN_OFFICIAL_ACCOUNT = auto()
    SATORI = auto()
    MISSKEY = auto()
    LINE = auto()
    MATRIX = auto()
    WEIXIN_OC = auto()
    MATTERMOST = auto()
    WEBCHAT = auto()
    ALL = auto()


_PLATFORM_NAME_TO_TYPE: dict[str, PlatformAdapterType] = {
    "aiocqhttp": PlatformAdapterType.AIOCQHTTP,
    "qq_official": PlatformAdapterType.QQOFFICIAL,
    "qq_official_webhook": PlatformAdapterType.QQOFFICIAL_WEBHOOK,
    "telegram": PlatformAdapterType.TELEGRAM,
    "wecom": PlatformAdapterType.WECOM,
    "wecom_ai_bot": PlatformAdapterType.WECOM_AI_BOT,
    "lark": PlatformAdapterType.LARK,
    "dingtalk": PlatformAdapterType.DINGTALK,
    "discord": PlatformAdapterType.DISCORD,
    "slack": PlatformAdapterType.SLACK,
    "kook": PlatformAdapterType.KOOK,
    "vocechat": PlatformAdapterType.VOCECHAT,
    "weixin_official_account": PlatformAdapterType.WEIXIN_OFFICIAL_ACCOUNT,
    "satori": PlatformAdapterType.SATORI,
    "misskey": PlatformAdapterType.MISSKEY,
    "line": PlatformAdapterType.LINE,
    "matrix": PlatformAdapterType.MATRIX,
    "weixin_oc": PlatformAdapterType.WEIXIN_OC,
    "mattermost": PlatformAdapterType.MATTERMOST,
    "webchat": PlatformAdapterType.WEBCHAT,
}


@dataclass(frozen=True, slots=True)
class _FilterRule:
    kind: str
    value: Any

    def matches(self, event: Any) -> bool:
        if self.kind == "message_type":
            actual = {
                "group_message": EventMessageType.GROUP_MESSAGE,
                "friend_message": EventMessageType.PRIVATE_MESSAGE,
                "other_message": EventMessageType.OTHER_MESSAGE,
            }.get(str(getattr(event.get_message_type(), "value", event.get_message_type())))
            return actual is not None and bool(actual & self.value)
        if self.kind == "platform":
            if self.value & PlatformAdapterType.ALL:
                return True
            actual = _PLATFORM_NAME_TO_TYPE.get(event.get_platform_name().strip().lower())
            return actual is not None and bool(actual & self.value)
        if self.kind == "permission":
            if self.value == PermissionType.MEMBER:
                return True
            return self.value == PermissionType.ADMIN and bool(event.is_admin())
        return False


def _append_rule(handler: Callable[..., Any], rule: _FilterRule) -> Callable[..., Any]:
    rules = tuple(getattr(handler, _FILTERS_ATTR, ())) + (rule,)
    setattr(handler, _FILTERS_ATTR, rules)
    return handler


def get_filter_rules(handler: Callable[..., Any]) -> tuple[_FilterRule, ...]:
    return tuple(getattr(handler, _FILTERS_ATTR, ()))


def is_independent_listener(handler: Any) -> bool:
    return callable(handler) and bool(get_filter_rules(handler)) and not hasattr(handler, _COMPONENT_INFO_ATTR)


def describe_filter_rules(handler: Callable[..., Any]) -> list[dict[str, Any]]:
    descriptions: list[dict[str, Any]] = []
    for rule in get_filter_rules(handler):
        value = rule.value
        if isinstance(value, Flag):
            names = [member.name for member in type(value) if member in value]
            serialized: Any = names
        else:
            serialized = str(value)
        descriptions.append({"kind": rule.kind, "value": serialized})
    return descriptions


def requires_admin_permission(handler: Callable[..., Any]) -> bool:
    return any(
        rule.kind == "permission" and rule.value == PermissionType.ADMIN
        for rule in get_filter_rules(handler)
    )


async def resolve_filter_permissions(
    handler: Callable[..., Any],
    event: Any,
    core: AsyncCoreClient,
) -> None:
    if not requires_admin_permission(handler):
        return
    platform_name = str(event.get_platform_name() or "").strip().lower()
    user_id = str(event.get_sender_id() or "").strip()
    if not platform_name or not user_id:
        return
    try:
        result = await core.permission.resolve(
            _COMMAND_ADMIN_PERMISSION,
            platform_name,
            user_id,
        )
    except Exception:
        return
    value = result
    if isinstance(value, dict) and value.get("success") is True and isinstance(value.get("result"), dict):
        value = value["result"]
    data = value.get("data") if isinstance(value, dict) and value.get("ok") is True else None
    identity = data.get("identity") if isinstance(data, dict) else None
    if (
        isinstance(data, dict)
        and data.get("permission") == _COMMAND_ADMIN_PERMISSION
        and data.get("decision") == "allow"
        and data.get("verified") is True
        and data.get("source") == _COMMAND_ADMIN_SOURCE
        and isinstance(identity, dict)
        and str(identity.get("platform") or "").strip().lower() == platform_name
        and str(identity.get("user_id") or "").strip() == user_id
    ):
        event._grant_verified_permission(_COMMAND_ADMIN_PERMISSION)


def filters_match(handler: Callable[..., Any], event: Any) -> bool:
    return all(rule.matches(event) for rule in get_filter_rules(handler))


def validate_star_class(plugin_class: type[Any]) -> None:
    """校验独立 Astr listener 能被当前同步桥接可靠执行。"""

    for name, member in plugin_class.__dict__.items():
        if not is_independent_listener(member):
            continue
        if inspect.isgeneratorfunction(member):
            raise AstrFilterUnsupportedError(
                f"Astr 独立监听器 {plugin_class.__name__}.{name} 当前不支持同步 generator；"
                "请使用 async generator 或普通同步/异步函数。"
            )
        parameters = list(inspect.signature(member).parameters.values())
        positional = [
            parameter
            for parameter in parameters
            if parameter.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        ]
        if len(positional) < 2:
            raise AstrFilterUnsupportedError(
                f"Astr 独立监听器 {plugin_class.__name__}.{name} 必须使用 (self, event) 参数。"
            )


def _invoke_astr_hook(handler: Callable[..., Any], instance: Any, payload: Any, kwargs: dict[str, Any]) -> Any:
    parameters = list(inspect.signature(handler).parameters.values())[1:]
    accepts_var_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters)
    positional = [
        parameter for parameter in parameters
        if parameter.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
    ]
    if len(positional) == 1 and not accepts_var_kwargs:
        return handler(instance, payload)
    return handler(instance, **kwargs)


def _hook_result(result: Any, original: dict[str, Any], *, writable: set[str]) -> dict[str, Any]:
    if result is None:
        return {"action": "continue"}
    if isinstance(result, (ProviderRequest, LLMResponse)):
        candidate = {name: getattr(result, name) for name in writable if hasattr(result, name)}
    elif isinstance(result, dict):
        candidate = dict(result)
    else:
        candidate = {"response": str(result)} if "response" in writable else {}
    modified = {key: value for key, value in candidate.items() if key in writable}
    if not modified:
        return {"action": "continue"}
    merged = dict(original)
    merged.update(modified)
    return {"action": "continue", "modified_kwargs": merged}


def _make_hook_decorator(hook_name: str, payload_type: type[Any], writable: set[str], *, name: str) -> Callable[..., Any]:
    def decorator_factory(*args: Any, priority: int = 0, **kwargs: Any) -> Any:
        del priority
        if args:
            if len(args) != 1 or not callable(args[0]):
                raise TypeError(f"{name} 只接受处理器函数或关键字参数")
            return decorator_factory()(args[0])
        metadata = dict(kwargs)
        metadata.setdefault("yesmai_protocol", "astr.llm.hook@1")
        metadata.setdefault("yesmai_writable", sorted(writable))

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            def build_payload(hook_kwargs: dict[str, Any]) -> Any:
                fields = getattr(payload_type, "__dataclass_fields__", {})
                return payload_type(**{key: value for key, value in hook_kwargs.items() if key in fields})

            if inspect.iscoroutinefunction(handler):
                async def wrapped(instance: Any, **hook_kwargs: Any) -> dict[str, Any]:
                    result = await _invoke_astr_hook(handler, instance, build_payload(hook_kwargs), hook_kwargs)
                    return _hook_result(result, hook_kwargs, writable=writable)
            else:
                def wrapped(instance: Any, **hook_kwargs: Any) -> dict[str, Any]:
                    result = _invoke_astr_hook(handler, instance, build_payload(hook_kwargs), hook_kwargs)
                    return _hook_result(result, hook_kwargs, writable=writable)

            wrapped.__name__ = getattr(handler, "__name__", name)
            wrapped.__doc__ = getattr(handler, "__doc__", None)
            return HookHandler(
                hook_name,
                name=name,
                description="AstrBot 兼容的受限 LLM Hook；失败和 abort 由 Host 隔离。",
                mode=HookMode.BLOCKING,
                order=HookOrder.NORMAL,
                timeout_ms=6000,
                error_policy=ErrorPolicy.SKIP,
                **metadata,
            )(wrapped)
        return decorator
    return decorator_factory


class AstrFilter:
    """由 ``yesmai.astr.filter`` 暴露的严格限定过滤器集合。"""

    def on_llm_request(self, *args: Any, **kwargs: Any) -> Any:
        return _make_hook_decorator(
            "maisaka.replyer.before_request", ProviderRequest,
            {"task_name", "model_name", "extra_prompt"}, name="astr-on-llm-request"
        )(*args, **kwargs)

    def on_llm_response(self, *args: Any, **kwargs: Any) -> Any:
        return _make_hook_decorator(
            "maisaka.replyer.after_response", LLMResponse,
            {"response"}, name="astr-on-llm-response"
        )(*args, **kwargs)

    def llm_tool(
        self,
        name: str,
        description: str = "",
        *,
        parameters: list[ToolParameterInfo] | dict[str, Any] | None = None,
        brief_description: str = "",
        detailed_description: str = "",
        **metadata: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """将 AstrBot llm_tool 映射为 MaiBot 原生 Tool 组件。"""

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            @wraps(handler)
            async def wrapped(instance: Star, **kwargs: Any) -> Any:
                from . import _CURRENT_CORE, AstrMessageEvent
                from .star_tools import activate_star, deactivate_star

                async_core = AsyncCoreClient(instance.ctx)
                tool_event = None
                if "event" in inspect.signature(handler).parameters:
                    base_event = YesMaiEvent.from_kwargs(
                        instance.ctx,
                        component_type="TOOL",
                        component_name=name,
                        kwargs=kwargs,
                    )
                    tool_event = AstrMessageEvent(base_event, async_core)
                call_kwargs = dict(kwargs)
                if tool_event is not None:
                    call_kwargs["event"] = tool_event
                signature = inspect.signature(handler)
                accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
                if not accepts_kwargs:
                    allowed = {
                        parameter.name
                        for parameter in signature.parameters.values()
                        if parameter.name != "self"
                        and parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
                    }
                    call_kwargs = {key: value for key, value in call_kwargs.items() if key in allowed}
                async with cron_catalog_batch(instance):
                    core_token = _CURRENT_CORE.set(async_core)
                    star_token = activate_star(instance)
                    try:
                        result = handler(instance, **call_kwargs)
                        if inspect.isasyncgen(result):
                            text_parts: list[str] = []
                            media_sent = False
                            async for item in result:
                                if item is None:
                                    continue
                                if isinstance(item, str):
                                    text_parts.append(item)
                                    continue
                                chain = getattr(item, "chain", None)
                                if chain is None:
                                    raise TypeError(f"Astr llm_tool 返回了不支持的结果类型：{type(item).__name__}")
                                if all(segment.type == "text" for segment in chain.segments):
                                    text_parts.append(chain.plain_text())
                                    continue
                                if tool_event is None or not tool_event.session_id:
                                    raise RuntimeError("Astr llm_tool 媒体结果缺少当前会话，无法发送")
                                send_result = await tool_event.send(chain)
                                if not isinstance(send_result, dict) or send_result.get("ok") is not True:
                                    code = send_result.get("code") if isinstance(send_result, dict) else "SEND_FAILED"
                                    message = send_result.get("message") if isinstance(send_result, dict) else "媒体结果发送失败"
                                    raise RuntimeError(f"Astr llm_tool 媒体发送失败（{code}）：{message}")
                                media_sent = True
                            if media_sent:
                                text_parts.append("媒体结果已发送到当前会话。")
                            return "\n".join(part for part in text_parts if part)
                        return await result if inspect.isawaitable(result) else result
                    finally:
                        deactivate_star(star_token)
                        _CURRENT_CORE.reset(core_token)

            return Tool(
                name,
                description=description,
                brief_description=brief_description,
                detailed_description=detailed_description,
                parameters=parameters,
                yesmai_astr=True,
                **metadata,
            )(wrapped)

        return decorator

    EventMessageType = EventMessageType
    PermissionType = PermissionType
    PlatformAdapterType = PlatformAdapterType

    def event_message_type(
        self, event_message_type: EventMessageType, *, priority: int = 0
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if not isinstance(event_message_type, EventMessageType):
            raise TypeError("event_message_type 必须使用 filter.EventMessageType")

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            decorated = _append_rule(handler, _FilterRule("message_type", event_message_type))
            decorated.__yesmai_astr_priority__ = int(priority)
            return decorated

        return decorator

    def platform_adapter_type(
        self, platform_adapter_type: PlatformAdapterType | str, *, priority: int = 0
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if isinstance(platform_adapter_type, str):
            resolved = _PLATFORM_NAME_TO_TYPE.get(platform_adapter_type.strip().lower())
            if resolved is None:
                raise AstrFilterUnsupportedError(f"无法识别 Astr 平台适配器：{platform_adapter_type}")
            platform_adapter_type = resolved
        if not isinstance(platform_adapter_type, PlatformAdapterType):
            raise TypeError("platform_adapter_type 必须使用 filter.PlatformAdapterType 或明确的平台名称")

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            decorated = _append_rule(handler, _FilterRule("platform", platform_adapter_type))
            decorated.__yesmai_astr_priority__ = int(priority)
            return decorated

        return decorator

    def permission_type(
        self, permission_type: PermissionType, raise_error: bool = True, *, priority: int = 0
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        del raise_error
        if not isinstance(permission_type, PermissionType):
            raise TypeError("permission_type 必须使用 filter.PermissionType")

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            decorated = _append_rule(handler, _FilterRule("permission", permission_type))
            decorated.__yesmai_astr_priority__ = int(priority)
            return decorated

        return decorator


filter_support = AstrFilter()


__all__ = [
    "AstrFilter",
    "AstrFilterUnsupportedError",
    "EventMessageType",
    "PermissionType",
    "PlatformAdapterType",
    "filter_support",
    "describe_filter_rules",
    "filters_match",
    "get_filter_rules",
    "is_independent_listener",
    "validate_star_class",
]
