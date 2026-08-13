"""AstrBot 风格的最小兼容层。"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import time
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import Any

from maibot_sdk import Command, MaiBotPlugin, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

from ..core import AsyncCoreClient, SyncCoreClient
from ..event import EventResult, YesMaiEvent
from .command_parser import CommandParserMixin, CommandTokens
from .config import AstrBotConfig, AstrConfigPersistenceUnsupportedError
from .cron import CronManager, CronTrigger, CronUnsupportedError, cron_catalog_batch
from .event_types import EventResultType, MessageEventResult, MessageType, PlatformMetadata, ResultContentType
from .filters import (
    AstrFilter,
    AstrFilterUnsupportedError,
    EventMessageType,
    PermissionType,
    PlatformAdapterType,
    filters_match,
    resolve_filter_permissions,
    validate_star_class,
)
from .handler import AstrStreamingUnsupportedError, execute_astr_handler
from .listener import register_independent_listeners
from .logging import logger
from .message_chain import MessageChain
from .message_components import AstrComponent, At, File, Image, Plain, Record, Reply, Video
from .provider import (
    AstrLLMArgumentUnsupportedError,
    AstrLLMProviderUnsupportedError,
    LLMResponse,
    ProviderRequest,
)
from .star_tools import StarTools, activate_star, deactivate_star

_CURRENT_CORE: contextvars.ContextVar[SyncCoreClient | AsyncCoreClient | None] = contextvars.ContextVar(
    "yesmai_astr_core", default=None
)


class AstrMessageEvent:
    """AstrBot 常用消息事件 API 的轻量兼容对象。"""

    def __init__(self, event: YesMaiEvent, core: SyncCoreClient) -> None:
        self._event = event
        self._core = core
        self.message_obj = event.message
        self.platform_meta = PlatformMetadata(name=event.platform.name, id=event.platform.name)
        self.platform = self.platform_meta
        self.role = "member"
        self._verified_permissions: frozenset[str] = frozenset()
        self.is_wake = bool(event.extra.get("is_wake", False))
        self.is_at_or_wake_command = bool(event.extra.get("is_at_or_wake_command", False))
        self.created_at = float(event.extra.get("timestamp") or time.time())
        self.call_llm = False
        self.plugins_name: list[str] | None = None
        self._extras: dict[str, Any] = {}
        self._result: MessageEventResult | None = None
        self._force_stopped = False
        self._has_send_oper = False

    @property
    def unified_msg_origin(self) -> str:
        return self._event.unified_msg_origin

    @property
    def message_str(self) -> str:
        return self._event.plain_text

    def get_sender_id(self) -> str:
        return self._event.user_id

    def get_sender_name(self) -> str:
        message = self._event.message
        if isinstance(message, dict):
            info = message.get("message_info")
            if isinstance(info, dict):
                user = info.get("user_info")
                if isinstance(user, dict):
                    return str(user.get("user_nickname") or user.get("user_id") or "朋友")
        return self._event.user_id or "朋友"

    def get_group_id(self) -> str:
        return self._event.group_id

    def get_platform_name(self) -> str:
        return self._event.platform.name

    def get_message_str(self) -> str:
        return self._event.plain_text

    @property
    def session_id(self) -> str:
        return self._event.stream_id

    def get_platform_id(self) -> str:
        return self.platform_meta.id

    def get_message_type(self) -> MessageType:
        if self._event.platform.is_group:
            return MessageType.GROUP_MESSAGE
        if self._event.platform.is_private:
            return MessageType.FRIEND_MESSAGE
        return MessageType.OTHER_MESSAGE

    def get_session_id(self) -> str:
        return self.session_id

    def get_self_id(self) -> str:
        return self._event.platform.account_id

    def is_private_chat(self) -> bool:
        return self._event.platform.is_private

    def is_group_chat(self) -> bool:
        return self._event.platform.is_group

    def is_wake_up(self) -> bool:
        return self.is_wake

    def is_admin(self) -> bool:
        return "yesmai.bot.command_admin" in self._verified_permissions

    def _grant_verified_permission(self, permission: str) -> None:
        normalized = str(permission or "").strip()
        if normalized:
            self._verified_permissions = self._verified_permissions | {normalized}
            if normalized == "yesmai.bot.command_admin":
                self.role = "admin"

    def get_messages(self) -> list[AstrComponent]:
        raw_message = self.message_obj.get("raw_message", []) if isinstance(self.message_obj, dict) else []
        if not isinstance(raw_message, list):
            return []
        components: list[AstrComponent] = []
        for item in raw_message:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").strip().lower()
            data = item.get("data", item.get("content"))
            if item_type == "text":
                components.append(Plain(str(data or "")))
            elif item_type == "image":
                components.append(Image(str(item.get("binary_data_base64") or data or "")))
            elif item_type == "at":
                descriptor = data if isinstance(data, dict) else {}
                components.append(At(descriptor.get("target_user_id", ""), descriptor.get("target_user_nickname", "")))
            elif item_type == "reply":
                descriptor = data if isinstance(data, dict) else {}
                components.append(Reply(descriptor.get("target_message_id", "")))
            else:
                components.append(AstrComponent(item_type or "custom", data))
        return components

    def get_message_outline(self) -> str:
        parts: list[str] = []
        for component in self.get_messages():
            if isinstance(component, Plain):
                parts.append(component.text)
            elif isinstance(component, Image):
                parts.append("[图片]")
            elif isinstance(component, At):
                parts.append(f"[At:{component.qq}]")
            elif isinstance(component, Reply):
                parts.append("[引用消息]")
            else:
                parts.append(f"[{component.type}]")
        return " ".join(parts)

    def set_extra(self, key: str, value: Any) -> None:
        self._extras[str(key)] = value

    def get_extra(self, key: str | None = None, default: Any = None) -> Any:
        return self._extras if key is None else self._extras.get(key, default)

    def clear_extra(self) -> None:
        self._extras.clear()

    def set_result(self, result: MessageEventResult | EventResult | str) -> None:
        if isinstance(result, str):
            result = self.plain_result(result)
        if isinstance(result, EventResult):
            result = MessageEventResult(
                chain=MessageChain.from_segments(result.chain.to_segments()),
                blocked=result.blocked or not result.continue_processing,
                custom_result=result.custom_result,
            )
        if not isinstance(result, MessageEventResult):
            raise TypeError("set_result 仅接受 MessageEventResult、EventResult 或字符串")
        self._result = result

    def get_result(self) -> MessageEventResult | None:
        return self._result

    def clear_result(self) -> None:
        self._result = None

    def stop_event(self) -> None:
        self._force_stopped = True
        if self._result is None:
            self._result = MessageEventResult().stop_event()
        else:
            self._result.stop_event()

    def continue_event(self) -> None:
        self._force_stopped = False
        if self._result is None:
            self._result = MessageEventResult().continue_event()
        else:
            self._result.continue_event()

    def is_stopped(self) -> bool:
        return self._force_stopped or bool(self._result and self._result.is_stopped())

    def should_call_llm(self, call_llm: bool) -> None:
        self.call_llm = bool(call_llm)

    def make_result(self) -> MessageEventResult:
        return MessageEventResult()

    def plain_result(self, text: str) -> MessageEventResult:
        return MessageEventResult().message(text)

    def image_result(self, url_or_path: str) -> MessageEventResult:
        result = MessageEventResult()
        source = str(url_or_path or "").strip()
        if source.startswith(("http://", "https://")):
            return result.url_image(source)
        if source.startswith("data:"):
            return result.base64_image(source)
        return result.file_image(source)

    def chain_result(self, chain: MessageChain | list[AstrComponent]) -> MessageEventResult:
        return MessageEventResult(chain=chain if isinstance(chain, MessageChain) else MessageChain(chain))

    def send(self, content: str | MessageChain) -> Any:
        if isinstance(self._core, AsyncCoreClient):
            return self._send_async(content)
        if isinstance(content, MessageChain):
            if not all(segment.type == "text" for segment in content.segments):
                result = self._core.call("send.chain", stream_id=self._event.stream_id, segments=content.to_segments())
            else:
                result = self._core.send.text(self._event.stream_id, content.plain_text())
        else:
            result = self._core.send.text(self._event.stream_id, content)
        self._has_send_oper = bool(result.get("ok", False)) if isinstance(result, dict) else bool(result)
        return result

    async def _send_async(self, content: str | MessageChain) -> Any:
        if isinstance(content, MessageChain):
            if not all(segment.type == "text" for segment in content.segments):
                result = await self._core.call(
                    "send.chain", stream_id=self._event.stream_id, segments=content.to_segments()
                )
            else:
                result = await self._core.send.text(self._event.stream_id, content.plain_text())
        else:
            result = await self._core.send.text(self._event.stream_id, content)
        self._has_send_oper = bool(result.get("ok", False)) if isinstance(result, dict) else bool(result)
        return result


class Context:
    """AstrBot Context 的受限配置 facade；业务能力通过 ``Star.core`` 使用。"""

    def __init__(self, config: dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        self._star: Star | None = None
        self._config = AstrBotConfig(config or {})

    def _bind_star(self, star: Star) -> None:
        self._star = star
        self.cron_manager = CronManager(star)

    def get_config(self) -> AstrBotConfig:
        """返回当前插件的可变配置 facade。"""

        return self._star.config if self._star is not None else self._config

    def _async_core(self) -> AsyncCoreClient:
        active = _CURRENT_CORE.get()
        if isinstance(active, AsyncCoreClient):
            return active
        if self._star is None:
            raise RuntimeError("Astr Context 尚未绑定插件")
        return AsyncCoreClient(self._star.ctx)

    async def llm_generate(
        self,
        *,
        prompt: Any,
        task: str = "utils",
        chat_provider_id: str = "",
        image_urls: list[str] | None = None,
        audio_urls: list[str] | None = None,
        tools: Any = None,
        system_prompt: str | None = None,
        contexts: list[Any] | None = None,
        stream: bool = False,
        temperature: float | None = None,
        max_tokens: int | None = None,
        **kwargs: Any,
    ) -> LLMResponse:
        """通过 MaiBot model task 执行一次非流式 LLM 生成。"""

        if str(chat_provider_id or "").strip():
            raise AstrLLMProviderUnsupportedError(
                "YesMai 不能把 Astr chat_provider_id 映射为 MaiBot model task；请显式传入 task。"
            )
        unsupported = {
            "image_urls": image_urls,
            "audio_urls": audio_urls,
            "tools": tools,
            "system_prompt": system_prompt,
            "contexts": contexts,
            "stream": stream,
            **kwargs,
        }
        used_unsupported = [
            name
            for name, value in unsupported.items()
            if value not in (None, False, [], {}, "")
        ]
        if used_unsupported:
            raise AstrLLMArgumentUnsupportedError(
                "当前 task-only LLM bridge 不支持参数：" + ", ".join(sorted(used_unsupported))
            )
        normalized_task = str(task or "").strip()
        if not normalized_task:
            raise ValueError("MaiBot model task 不能为空")
        if not isinstance(prompt, (str, list)) or not prompt:
            raise ValueError("prompt 必须是非空字符串或消息对象列表")
        options: dict[str, Any] = {}
        if temperature is not None:
            options["temperature"] = float(temperature)
        if max_tokens is not None:
            options["max_tokens"] = int(max_tokens)
        result = await self._async_core().llm.generate(prompt, task=normalized_task, **options)
        if not isinstance(result, dict) or result.get("ok") is not True:
            code = str(result.get("code") or "LLM_GENERATE_FAILED") if isinstance(result, dict) else "LLM_GENERATE_FAILED"
            message = str(result.get("message") or "LLM 生成失败") if isinstance(result, dict) else "LLM 生成失败"
            raise RuntimeError(f"{code}: {message}")
        data = result.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("LLM_RESULT_INVALID: Core 未返回有效 data")
        text = str(data.get("text") or data.get("response") or "")
        usage = {
            "prompt_tokens": int(data.get("prompt_tokens") or 0),
            "completion_tokens": int(data.get("completion_tokens") or 0),
            "total_tokens": int(data.get("total_tokens") or 0),
        }
        return LLMResponse(
            response=text,
            task_name=normalized_task,
            requested_model_name=str(data.get("model") or data.get("model_name") or ""),
            prompt_tokens=usage["prompt_tokens"],
            completion_tokens=usage["completion_tokens"],
            total_tokens=usage["total_tokens"],
            reasoning_content=str(data.get("reasoning") or "") or None,
            raw_completion=data,
            usage=usage,
        )


class Star(CommandParserMixin, MaiBotPlugin):
    """AstrBot 风格同步插件基类。"""

    def __init__(self, context: Context | None = None, config: dict[str, Any] | None = None) -> None:
        super().__init__()
        validate_star_class(type(self))
        self._astr_config = AstrBotConfig(config or {})
        self.context = context or Context()
        self.context._bind_star(self)
        self._astr_kv_lock = asyncio.Lock()
        register_independent_listeners(self)

    @property
    def config(self) -> AstrBotConfig:
        """返回与 AstrBot 字典语义兼容的配置对象。"""

        return self._astr_config

    @config.setter
    def config(self, value: Any) -> None:
        if value is None:
            normalized: dict[str, Any] = {}
        elif isinstance(value, AstrBotConfig):
            self._astr_config = value
            return
        elif isinstance(value, dict):
            normalized = dict(value)
        else:
            raise TypeError("Astr 插件 config 必须是字典或 None")
        if hasattr(self, "_astr_config"):
            self._astr_config.replace(normalized)
        else:
            self._astr_config = AstrBotConfig(normalized)

    def set_plugin_config(self, config: dict[str, Any]) -> None:
        """接收 MaiBot Runner 配置注入并更新同一个 Astr 配置对象。"""

        super().set_plugin_config(config)
        self._astr_config.replace(self.get_plugin_config_data())

    async def get_kv_data(self, key: str, default: Any = None) -> Any:
        """Read Astr-compatible plugin state from the MaiBot plugin data dir."""

        async with self._astr_kv_lock:
            path = Path(self.ctx.paths.data_dir) / "astr-kv.json"
            if not path.is_file():
                return default
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                logger.warning("Astr KV 文件不可读，返回默认值: %s", path)
                return default
            return payload.get(str(key), default) if isinstance(payload, dict) else default

    async def put_kv_data(self, key: str, value: Any) -> None:
        """Atomically write or delete Astr-compatible plugin state."""

        async with self._astr_kv_lock:
            data_dir = Path(self.ctx.paths.data_dir)
            data_dir.mkdir(parents=True, exist_ok=True)
            path = data_dir / "astr-kv.json"
            payload: dict[str, Any] = {}
            if path.is_file():
                try:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        payload = loaded
                except (OSError, json.JSONDecodeError):
                    logger.warning("Astr KV 文件不可读，将重建: %s", path)
            if value is None:
                payload.pop(str(key), None)
            else:
                payload[str(key)] = value
            temporary = path.with_suffix(".json.tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temporary.replace(path)

    @property
    def core(self) -> SyncCoreClient | AsyncCoreClient:
        client = _CURRENT_CORE.get()
        if client is None:
            raise RuntimeError("self.core 只能在 Astr 风格处理器执行期间使用")
        return client

    async def html_render(
        self,
        template: str,
        data: dict[str, Any] | None = None,
        return_url: bool = True,
        options: dict[str, Any] | None = None,
    ) -> str:
        """兼容 AstrBot HTML 渲染签名，稳定返回可发送的 Data URI。

        ``return_url`` 为签名兼容参数；跨 Runner 场景不返回私有文件路径。
        """

        del return_url

        client = _CURRENT_CORE.get()
        if not isinstance(client, AsyncCoreClient):
            raise RuntimeError("html_render 只能在异步 Astr 处理器执行期间使用")
        try:
            from jinja2 import Environment

            rendered_html = Environment(autoescape=True).from_string(str(template or "")).render(**(data or {}))
        except Exception as exc:
            raise RuntimeError(f"HTML 模板渲染失败：{exc}") from exc
        result = await client.render.html2png(rendered_html, **(options or {}))
        if not isinstance(result, dict) or result.get("ok") is not True:
            message = result.get("message") if isinstance(result, dict) else "未知错误"
            raise RuntimeError(f"HTML 图片渲染失败：{message}")
        payload = result.get("data")
        if not isinstance(payload, dict) or not payload.get("data_uri"):
            raise RuntimeError("HTML 图片渲染没有返回有效图片")
        return str(payload["data_uri"])

    async def initialize(self) -> None:
        return None

    async def terminate(self) -> None:
        return None

    async def on_load(self) -> None:
        async with cron_catalog_batch(self):
            token = activate_star(self)
            try:
                StarTools.initialize(self.context)
                await self.initialize()
                callbacks: list[Callable[..., Any]] = []
                for cls in type(self).__mro__:
                    for member in cls.__dict__.values():
                        if getattr(member, "__yesmai_astr_platform_loaded__", False):
                            if callable(member) and member not in callbacks:
                                callbacks.append(member)
                for callback in callbacks:
                    result = callback(self)
                    if inspect.isawaitable(result):
                        await result
            finally:
                deactivate_star(token)

    async def on_unload(self) -> None:
        token = activate_star(self)
        try:
            await self.terminate()
        finally:
            deactivate_star(token)
            self.context.cron_manager.scheduler.close()

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        del scope, config_data, version


class _AstrFilter(AstrFilter):
    def on_platform_loaded(self) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Mark an Astr platform-ready lifecycle callback.

        MaiBot has no platform-loaded event envelope, so the Star bridge invokes
        this callback after ``on_load``. It is deliberately not a message filter.
        """

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            handler.__yesmai_astr_platform_loaded__ = True
            return handler

        return decorator

    def command(
        self,
        name: str,
        *,
        pattern: str = "",
        description: str = "",
        aliases: list[str] | None = None,
        alias: set[str] | list[str] | None = None,
        priority: int = 0,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if aliases is None and alias is not None:
            aliases = list(alias)
        resolved_pattern = pattern or rf"^/{name}(?:\s|$)"

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            @wraps(handler)
            async def wrapped(instance: Star, **kwargs: Any) -> Any:
                loop = asyncio.get_running_loop()
                async_core = AsyncCoreClient(instance.ctx)
                sync_core = SyncCoreClient(async_core, loop)
                base_event = YesMaiEvent.from_kwargs(
                    instance.ctx,
                    component_type="COMMAND",
                    component_name=name,
                    kwargs=kwargs,
                )
                event = AstrMessageEvent(base_event, sync_core)
                await resolve_filter_permissions(wrapped, event, async_core)
                if not filters_match(wrapped, event):
                    return False, "", False

                results = await execute_astr_handler(
                    instance,
                    handler,
                    event,
                    async_core=async_core,
                    sync_core=sync_core,
                    current_core=_CURRENT_CORE,
                )
                for snapshot in results[:-1]:
                    await _send_intermediate_command_result(
                        async_core,
                        base_event.stream_id,
                        snapshot.result,
                        sent_during_handler=snapshot.sent_during_handler,
                    )
                final_snapshot = results[-1]
                result = final_snapshot.result
                stopped = final_snapshot.stopped
                sent_during_handler = final_snapshot.sent_during_handler
                if isinstance(result, MessageEventResult):
                    blocked = result.blocked or stopped
                    if sent_during_handler:
                        return True, "", blocked
                    if result.chain and all(segment.type == "text" for segment in result.chain.segments):
                        return True, result.chain.plain_text(), blocked
                    if result.chain:
                        await async_core.call(
                            "send.chain",
                            stream_id=base_event.stream_id,
                            segments=result.chain.to_segments(),
                        )
                    return True, "", blocked
                if isinstance(result, EventResult):
                    blocked = result.blocked or not result.continue_processing or stopped
                    if sent_during_handler:
                        return True, "", blocked
                    if result.chain and all(segment.type == "text" for segment in result.chain.segments):
                        return True, result.chain.plain_text(), blocked
                    if result.chain:
                        await async_core.call(
                            "send.chain",
                            stream_id=base_event.stream_id,
                            segments=result.chain.to_segments(),
                        )
                    return True, "", blocked
                if isinstance(result, str):
                    return True, "" if sent_during_handler else result, stopped
                if result is None:
                    return True, "", stopped
                return result

            wrapped.__yesmai_astr_priority__ = int(priority)
            return Command(
                name,
                description=description,
                pattern=resolved_pattern,
                aliases=aliases,
                yesmai_astr=True,
            )(wrapped)

        return decorator


filter = _AstrFilter()
permission_type = filter.permission_type
llm_tool = filter.llm_tool
on_llm_request = filter.on_llm_request
on_llm_response = filter.on_llm_response


async def _send_intermediate_command_result(
    async_core: AsyncCoreClient,
    stream_id: str,
    result: Any,
    *,
    sent_during_handler: bool,
) -> None:
    if result is None or sent_during_handler:
        return
    if isinstance(result, str):
        await async_core.call("send.text", stream_id=stream_id, text=result)
        return
    if isinstance(result, MessageEventResult):
        if result.chain:
            await async_core.call("send.chain", stream_id=stream_id, segments=result.chain.to_segments())
        return
    if isinstance(result, EventResult):
        if result.chain:
            await async_core.call("send.chain", stream_id=stream_id, segments=result.chain.to_segments())
        return
    raise TypeError(f"Astr 异步处理器 yield 了不支持的结果类型：{type(result).__name__}")


def register(
    name: str,
    author: str,
    description: str = "",
    version: str = "",
    repo: str | None = None,
    *,
    desc: str | None = None,
) -> Callable[[type[Star]], type[Star]]:
    """保存 AstrBot 风格元数据；实际插件身份仍由 Manifest 决定。"""

    resolved_description = str(desc if desc is not None else description)

    def decorator(plugin_class: type[Star]) -> type[Star]:
        metadata = {
            "name": str(name),
            "author": str(author),
            "description": resolved_description,
            "version": str(version),
        }
        if repo is not None:
            metadata["repo"] = str(repo)
        plugin_class.__yesmai_astr_metadata__ = metadata
        return plugin_class

    return decorator


__all__ = [
    "AstrMessageEvent",
    "AstrBotConfig",
    "AstrConfigPersistenceUnsupportedError",
    "AstrFilterUnsupportedError",
    "AstrLLMArgumentUnsupportedError",
    "AstrLLMProviderUnsupportedError",
    "AstrStreamingUnsupportedError",
    "At",
    "CommandParserMixin",
    "CommandTokens",
    "Context",
    "CronTrigger",
    "CronUnsupportedError",
    "EventMessageType",
    "EventResultType",
    "File",
    "Image",
    "LLMResponse",
    "MessageChain",
    "MessageEventResult",
    "MessageType",
    "Plain",
    "PlatformAdapterType",
    "PlatformMetadata",
    "PermissionType",
    "ProviderRequest",
    "Record",
    "Reply",
    "ResultContentType",
    "Star",
    "StarTools",
    "Video",
    "filter",
    "llm_tool",
    "logger",
    "on_llm_request",
    "on_llm_response",
    "permission_type",
    "register",
]
