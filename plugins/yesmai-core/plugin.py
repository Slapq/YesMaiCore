"""YesMaiCore 运行时插件。"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import mimetypes
import platform
import sys
import time
import urllib.error
import urllib.request
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import unquote_to_bytes, urlparse

from maibot_sdk import API, Field, HomeCard, HookHandler, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder
from pydantic import BaseModel

try:
    from .cron_service import CronService, CronSettings
    from .model_config import ModelConfigError, ModelConfigManager
    from .web_integration import CoreWebIntegration, WebIntegrationError
except ImportError:  # 兼容测试和 Runner 以文件模块方式加载 plugin.py
    plugin_directory = str(Path(__file__).resolve().parent)
    if plugin_directory not in sys.path:
        sys.path.insert(0, plugin_directory)
    from cron_service import CronService, CronSettings
    from model_config import ModelConfigError, ModelConfigManager
    from web_integration import CoreWebIntegration, WebIntegrationError

PLUGIN_ID = "com.yesmai.core"
PROTOCOL_VERSION = "1"
PLUGIN_VERSION = "0.1.3"

_DEFAULT_FEATURES: dict[str, bool] = {
    "remote_config": False,
    "leaderboard": False,
    "platform_registry": True,
}

_DEFAULT_PLATFORM_CAPABILITIES: dict[str, dict[str, bool]] = {
    "qq": {"text": True, "image": True, "emoji": True, "forward": True, "hybrid": True, "custom": True},
    "discord": {"text": True, "image": True, "emoji": True, "forward": False, "hybrid": True, "custom": True},
    "telegram": {"text": True, "image": True, "emoji": True, "forward": False, "hybrid": True, "custom": False},
    "kook": {"text": True, "image": True, "emoji": True, "forward": False, "hybrid": True, "custom": False},
}


_MEDIA_DOWNLOAD_TIMEOUT_SECONDS = 20
_MEDIA_CHAIN_DEADLINE_SECONDS = 30
_MEDIA_MAX_BYTES = 32 * 1024 * 1024
_MEDIA_CHAIN_MAX_BYTES = 64 * 1024 * 1024
_MEDIA_CHAIN_MAX_SEGMENTS = 8
_MEDIA_SEGMENT_TYPES = frozenset({"image", "voice", "file"})

_HOOK_NAME = "chat.receive.after_process"
_HOOK_LISTENER_PROTOCOL = "astr.listener@1"
_HOOK_QUEUE_MAX = 128
_HOOK_WORKER_COUNT = 2
_HOOK_DIRECTORY_TTL_SECONDS = 30.0
_HOOK_DEDUP_TTL_SECONDS = 30.0
_HOOK_LISTENER_TIMEOUT_MS = 3000
_HOOK_EVENT_DEADLINE_SECONDS = 10.0
_HOOK_WARNING_INTERVAL_SECONDS = 30.0
_COMMAND_ADMIN_PERMISSION = "yesmai.bot.command_admin"
_PERMISSION_SOURCE = "yesmai-core-config@1"


class _PluginConfigSection(BaseModel):
    config_version: str = "1"


class _CronConfigSection(BaseModel):
    enabled: bool = True
    default_timezone: str = "Asia/Shanghai"
    catalog_refresh_seconds: float = Field(default=5.0, ge=1.0, le=300.0)
    owner_job_limit: int = Field(default=32, ge=1, le=128)
    global_job_limit: int = Field(default=256, ge=1, le=1024)
    owner_dispatch_limit: int = Field(default=4, ge=1, le=16)
    minimum_interval_seconds: int = Field(default=60, ge=60, le=86400)
    maximum_timeout_seconds: int = Field(default=7200, ge=1, le=86400)


class _PermissionConfigSection(BaseModel):
    command_admins: list[str] = Field(default_factory=list)


class YesMaiCoreConfig(PluginConfigBase):
    plugin: _PluginConfigSection = Field(default_factory=_PluginConfigSection)
    cron: _CronConfigSection = Field(default_factory=_CronConfigSection)
    permission: _PermissionConfigSection = Field(default_factory=_PermissionConfigSection)
    web_url: str = ""
    web_poll_interval_seconds: float = Field(default=5.0, ge=1.0, le=300.0)


def _ok(data: Any = None, message: str = "操作成功") -> dict[str, Any]:
    return {"ok": True, "code": "OK", "message": message, "data": data, "retryable": False}


def _error(code: str, message: str, *, data: Any = None, retryable: bool = False) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message, "data": data, "retryable": retryable}


def _normalize_command_admin(value: Any) -> tuple[str, str] | None:
    normalized = str(value or "").strip()
    if normalized.count(":") != 1:
        return None
    platform_name, user_id = normalized.split(":", 1)
    platform_name = platform_name.strip().lower()
    user_id = user_id.strip()
    if not platform_name or not user_id:
        return None
    return platform_name, user_id


def _lookup_config_value(config: dict[str, Any], key: str, default: Any) -> Any:
    normalized = str(key or "").strip()
    if not normalized:
        return deepcopy(config)
    current: Any = config
    for segment in normalized.split("."):
        if not isinstance(current, dict) or segment not in current:
            return default
        current = current[segment]
    return deepcopy(current)


def _load_media_source(source: str) -> tuple[str, str, int]:
    normalized = str(source or "").strip()
    if not normalized:
        raise ValueError("媒体来源不能为空")
    if normalized.startswith("data:"):
        header, separator, payload = normalized.partition(",")
        if not separator:
            raise ValueError("无效的 data URI")
        mime_type = header[5:].split(";", 1)[0] or "application/octet-stream"
        try:
            raw = base64.b64decode(payload, validate=True) if ";base64" in header else unquote_to_bytes(payload)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("无效的 Base64 媒体数据") from exc
        if len(raw) > _MEDIA_MAX_BYTES:
            raise ValueError("媒体文件超过大小限制")
        return base64.b64encode(raw).decode("ascii"), mime_type, len(raw)
    parsed = urlparse(normalized)
    if parsed.scheme in {"https", "http"}:
        request = urllib.request.Request(normalized, headers={"User-Agent": f"YesMaiCore/{PLUGIN_VERSION}"})
        try:
            with urllib.request.urlopen(request, timeout=_MEDIA_DOWNLOAD_TIMEOUT_SECONDS) as response:
                raw = response.read(_MEDIA_MAX_BYTES + 1)
                mime_type = response.headers.get_content_type()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ValueError(f"无法下载媒体：{exc}") from exc
        if len(raw) > _MEDIA_MAX_BYTES:
            raise ValueError("媒体文件超过大小限制")
        return base64.b64encode(raw).decode("ascii"), mime_type, len(raw)
    candidate = Path(normalized).expanduser()
    if candidate.is_symlink():
        raise ValueError("媒体文件不存在或不是安全的普通文件")
    path = candidate.resolve()
    if not path.is_file():
        raise ValueError("媒体文件不存在或不是安全的普通文件")
    with path.open("rb") as file:
        raw = file.read(_MEDIA_MAX_BYTES + 1)
    if len(raw) > _MEDIA_MAX_BYTES:
        raise ValueError("媒体文件超过大小限制")
    mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return base64.b64encode(raw).decode("ascii"), mime_type, len(raw)


class YesMaiCorePlugin(MaiBotPlugin):
    """提供稳定、可序列化的跨插件公共 API。"""

    config_model = YesMaiCoreConfig

    def __init__(self) -> None:
        super().__init__()
        self._features = dict(_DEFAULT_FEATURES)
        self._features["astr_listener_bridge"] = True
        self._features["model_config_mvp"] = True
        self._features["model_directory"] = True
        self._features["chat_resolve"] = True
        self._features["script"] = False
        self._features["cron"] = False
        self._platform_capabilities = deepcopy(_DEFAULT_PLATFORM_CAPABILITIES)
        self._hook_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_HOOK_QUEUE_MAX)
        self._hook_workers: list[asyncio.Task[None]] = []
        self._hook_seen: dict[str, float] = {}
        self._hook_directory: list[dict[str, Any]] = []
        self._hook_directory_expires_at = 0.0
        self._hook_directory_lock = asyncio.Lock()
        self._hook_last_warning_at = 0.0
        self._model_config = ModelConfigManager()
        self._model_config_lock = asyncio.Lock()
        self._web_integration: CoreWebIntegration | None = None
        self._web_worker: asyncio.Task[None] | None = None
        self._cron_service: CronService | None = None

    async def on_load(self) -> None:
        if not self._hook_workers:
            self._hook_workers = [
                asyncio.create_task(self._hook_worker(index), name=f"yesmai-core-hook-{index}")
                for index in range(_HOOK_WORKER_COUNT)
            ]
        await self._restart_cron_service()
        await self._restart_web_integration()
        self.ctx.logger.info("YesMaiCore %s 已加载", PLUGIN_VERSION)

    async def on_unload(self) -> None:
        await self._stop_cron_service()
        await self._stop_web_integration()
        workers, self._hook_workers = self._hook_workers, []
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._hook_seen.clear()
        self._hook_directory.clear()
        self._hook_directory_expires_at = 0.0
        while not self._hook_queue.empty():
            try:
                self._hook_queue.get_nowait()
                self._hook_queue.task_done()
            except asyncio.QueueEmpty:
                break
        self.ctx.logger.info("YesMaiCore 已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        if scope == "self":
            self.set_plugin_config(config_data)
            await self._restart_cron_service()
            await self._restart_web_integration()
            self.ctx.logger.info("YesMaiCore 配置已更新: version=%s", version)

    async def _restart_cron_service(self) -> None:
        await self._stop_cron_service()
        config_data = self.get_plugin_config_data()
        raw_cron = config_data.get("cron") if isinstance(config_data.get("cron"), dict) else {}
        settings = CronSettings(
            enabled=bool(raw_cron.get("enabled", True)),
            default_timezone=str(raw_cron.get("default_timezone") or "Asia/Shanghai"),
            catalog_refresh_seconds=float(raw_cron.get("catalog_refresh_seconds") or 5.0),
            owner_job_limit=int(raw_cron.get("owner_job_limit") or 32),
            global_job_limit=int(raw_cron.get("global_job_limit") or 256),
            owner_dispatch_limit=int(raw_cron.get("owner_dispatch_limit") or 4),
            minimum_interval_seconds=int(raw_cron.get("minimum_interval_seconds") or 60),
            maximum_timeout_seconds=int(raw_cron.get("maximum_timeout_seconds") or 7200),
        )
        if not settings.enabled:
            return
        service = CronService(self.ctx, Path(self.ctx.paths.data_dir), settings)
        try:
            await service.start()
        except Exception as exc:
            self.ctx.logger.error("YesMai Cron 初始化失败，已保持 fail-closed：%s", exc, exc_info=True)
            return
        self._cron_service = service
        self._features["cron"] = True

    async def _stop_cron_service(self) -> None:
        service, self._cron_service = self._cron_service, None
        self._features["cron"] = False
        if service is not None:
            await service.stop()

    async def _restart_web_integration(self) -> None:
        await self._stop_web_integration()
        config_data = self.get_plugin_config_data()
        web_url = str(config_data.get("web_url") or "").strip()
        if not web_url:
            self._features["remote_config"] = False
            return
        try:
            integration = CoreWebIntegration(
                web_url=web_url,
                data_dir=Path(self.ctx.paths.data_dir),
                logger=self.ctx.logger,
                operation_handler=self._execute_web_operation,
                capabilities_provider=self._web_capabilities,
                poll_interval_seconds=float(config_data.get("web_poll_interval_seconds") or 5.0),
            )
        except Exception as exc:
            self._features["remote_config"] = False
            self.ctx.logger.error("YesMaiWeb 实例通道初始化失败：%s", exc)
            return
        self._web_integration = integration
        self._web_worker = asyncio.create_task(integration.run(), name="yesmai-core-web")
        self._features["remote_config"] = True

    async def _stop_web_integration(self) -> None:
        worker, self._web_worker = self._web_worker, None
        self._web_integration = None
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    def _web_capabilities(self) -> dict[str, Any]:
        operations = [
            "model.config.get@1",
            "device.info.get@1",
            "model.config.validate@1",
            "model.config.patch@1",
            "model.config.restore@1",
            "plugin.status.get@1",
            "plugin.enable@1",
            "plugin.reload@1",
            "group.summary.config.get@1",
            "group.summary.config.update@1",
            "group.summary.preview@1",
            "daily.analysis.config.get@1",
            "daily.analysis.config.update@1",
            "daily.analysis.preview@1",
        ]
        return {
            "core_version": PLUGIN_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "operations": operations,
            "supported_operations": operations,
        }

    async def _execute_web_operation(
        self,
        operation: str,
        payload: dict[str, Any],
        remote_context: dict[str, Any],
    ) -> dict[str, Any]:
        del remote_context
        if operation == "device.info.get@1":
            identity = self._web_integration.identity if self._web_integration is not None else None
            return _ok(
                {
                    "core_version": PLUGIN_VERSION,
                    "protocol_version": PROTOCOL_VERSION,
                    "python_version": platform.python_version(),
                    "platform": platform.platform(),
                    "instance_uuid": identity.instance_uuid if identity is not None else "",
                    "web_enabled": self._web_worker is not None,
                }
            )
        if operation == "model.config.get@1":
            return await self._run_model_config(self._model_config.get_plaintext)
        if operation == "model.config.validate@1":
            result = await self._run_model_config(self._model_config.validate, payload.get("config"))
            return result if result.get("ok") is not True else _ok({"valid": True}, "模型配置有效")
        if operation in {"model.config.patch@1", "model.config.restore@1"}:
            manager_operation = (
                self._model_config.patch
                if operation == "model.config.patch@1"
                else self._model_config.restore
            )
            args = (payload.get("patch"),) if operation == "model.config.patch@1" else ()
            async with self._model_config_lock:
                result = await self._run_model_config(manager_operation, *args)
                if result.get("ok") is not True:
                    return result
                plaintext_result = await self._run_model_config(self._model_config.get_plaintext)
                if plaintext_result.get("ok") is not True:
                    return plaintext_result
                if self._web_integration is not None:
                    try:
                        cached = self._web_integration.get_cached_instance_config(PLUGIN_ID)
                        base_revision = cached.revision if cached is not None else 0
                        new_config = plaintext_result.get("data")
                        if not isinstance(new_config, dict):
                            return _error("EXECUTION_FAILED", "获取最新模型配置失败")
                        await self._web_integration.write_back_instance_config(
                            PLUGIN_ID, base_revision, new_config
                        )
                    except WebIntegrationError as exc:
                        if exc.code == "CONFLICT":
                            return _error(
                                "CONFLICT",
                                f"本地模型配置已应用，但与 Web 配置发生 revision 冲突：{exc.message}",
                                data={
                                    "resource": "instance_config",
                                    "plugin_id": PLUGIN_ID,
                                    "base_revision": base_revision,
                                    "local_applied": True,
                                },
                            )
                        self.ctx.logger.warning(f"写回 Web 配置失败但继续：{exc}")
                    except Exception as exc:
                        self.ctx.logger.warning(f"写回 Web 配置失败但继续：{exc}")
                return plaintext_result
        if operation == "daily.analysis.config.get@1":
            return _ok(await self.config_get("", {}, namespace="com.yesmai.qq-group-daily-analysis"))
        if operation == "daily.analysis.config.update@1":
            integration = self._web_integration
            candidate = payload.get("config")
            base_revision = payload.get("base_revision")
            if integration is None or not isinstance(candidate, dict):
                return _error("INVALID_PAYLOAD", "需要 Web 连接和有效的 config 对象")
            try:
                base = int(base_revision)
            except (TypeError, ValueError):
                return _error("INVALID_PAYLOAD", "base_revision 必须是非负整数")
            if base < 0:
                return _error("INVALID_PAYLOAD", "base_revision 必须是非负整数")
            try:
                current = await integration.fetch_instance_config("com.yesmai.qq-group-daily-analysis")
                current_revision = current.revision if current is not None else 0
                if base != current_revision:
                    return _error("CONFIG_CONFLICT", f"Web 配置 revision 已是 {current_revision}，提交的是 {base}")
                revision = await integration.write_back_instance_config(
                    "com.yesmai.qq-group-daily-analysis", base, dict(candidate)
                )
                return _ok({"plugin_id": "com.yesmai.qq-group-daily-analysis", "config": dict(candidate), "revision": revision})
            except Exception as exc:
                if "CONFLICT" in str(exc).upper():
                    return _error("CONFIG_CONFLICT", f"日报配置冲突：{exc}")
                return _error("CONFIG_UPDATE_FAILED", f"日报配置更新失败：{exc}", retryable=True)
        if operation == "daily.analysis.preview@1":
            stream_id = str(payload.get("stream_id") or "").strip()
            if not stream_id:
                return _error("INVALID_PAYLOAD", "缺少 stream_id")
            try:
                result = await self.ctx.api.call(
                    "com.yesmai.qq-group-daily-analysis.preview@1",
                    stream_id=stream_id,
                    days=payload.get("days"),
                )
                return _ok(result)
            except Exception as exc:
                return _error("PREVIEW_FAILED", f"日报预览失败：{exc}", retryable=True)
        if operation == "group.summary.config.get@1":
            return _ok(await self.config_get("", {}, namespace="com.yesmai.group-summary"))
        if operation == "group.summary.config.update@1":
            integration = self._web_integration
            candidate = payload.get("config")
            base_revision = payload.get("base_revision")
            if integration is None or not isinstance(candidate, dict):
                return _error("INVALID_PAYLOAD", "需要 Web 连接和有效的 config 对象")
            try:
                base = int(base_revision)
            except (TypeError, ValueError):
                return _error("INVALID_PAYLOAD", "base_revision 必须是非负整数")
            if base < 0:
                return _error("INVALID_PAYLOAD", "base_revision 必须是非负整数")
            try:
                current = await integration.fetch_instance_config("com.yesmai.group-summary")
                current_revision = current.revision if current is not None else 0
                if base != current_revision:
                    return _error("CONFIG_CONFLICT", f"Web 配置 revision 已是 {current_revision}，提交的是 {base}")
                revision = await integration.write_back_instance_config(
                    "com.yesmai.group-summary", base, dict(candidate)
                )
                return _ok({"plugin_id": "com.yesmai.group-summary", "config": dict(candidate), "revision": revision})
            except Exception as exc:
                if "CONFLICT" in str(exc).upper():
                    return _error("CONFIG_CONFLICT", f"群总结配置冲突：{exc}")
                return _error("CONFIG_UPDATE_FAILED", f"群总结配置更新失败：{exc}", retryable=True)
        if operation == "group.summary.preview@1":
            stream_id = str(payload.get("stream_id") or "").strip()
            if not stream_id:
                return _error("INVALID_PAYLOAD", "缺少 stream_id")
            try:
                result = await self.ctx.api.call(
                    "com.yesmai.group-summary.preview@1",
                    stream_id=stream_id,
                    hours=payload.get("hours"),
                )
                return _ok(result)
            except Exception as exc:
                return _error("PREVIEW_FAILED", f"群总结预览失败：{exc}", retryable=True)
        if operation == "plugin.status.get@1":
            try:
                loaded, registered = await asyncio.gather(
                    self.ctx.component.list_loaded_plugins(),
                    self.ctx.component.list_registered_plugins(),
                )
            except Exception as exc:
                return _error("EXECUTION_FAILED", f"读取插件状态失败：{exc}", retryable=True)
            return _ok({"loaded": loaded, "registered": registered})
        if operation in {"plugin.enable@1", "plugin.reload@1"}:
            plugin_id = str(payload.get("plugin_id") or "").strip()
            if not plugin_id:
                return _error("INVALID_PAYLOAD", "缺少 plugin_id")
            if operation == "plugin.reload@1" and plugin_id == PLUGIN_ID:
                return _error("VALIDATION_FAILED", "不允许通过当前控制任务重载 YesMaiCore 自身")
            capability = {
                "plugin.enable@1": self.ctx.component.load_plugin,
                "plugin.reload@1": self.ctx.component.reload_plugin,
            }[operation]
            try:
                host_result = await capability(plugin_id)
            except Exception as exc:
                return _error("EXECUTION_FAILED", f"插件运行态操作失败：{exc}", retryable=True)
            if isinstance(host_result, dict) and host_result.get("success") is False:
                return _error(
                    "EXECUTION_FAILED",
                    str(host_result.get("error") or host_result.get("message") or "Host 拒绝插件运行态操作"),
                )
            return _ok({"plugin_id": plugin_id, "operation": operation, "host_result": host_result})
        return _error("UNSUPPORTED_OPERATION", f"当前 Core 不支持控制任务：{operation}")

    @HookHandler(
        _HOOK_NAME,
        name="yesmai-astr-listener-bridge",
        description="将麦麦入站消息安全转发到 YesMai Astr listener",
        mode=HookMode.OBSERVE,
        order=HookOrder.NORMAL,
        timeout_ms=1000,
        error_policy=ErrorPolicy.SKIP,
        yesmai_protocol="astr.listener.bridge@1",
    )
    async def observe_inbound_message(self, message: Any = None, **kwargs: Any) -> dict[str, str]:
        del kwargs
        if not isinstance(message, dict):
            return {"action": "continue"}
        event_id = self._build_hook_event_id(message)
        now = time.monotonic()
        self._prune_hook_seen(now)
        if event_id in self._hook_seen:
            return {"action": "continue"}
        event = {
            "event_id": event_id,
            "message": dict(message),
            "hook": _HOOK_NAME,
            "deadline_ms": int(_HOOK_EVENT_DEADLINE_SECONDS * 1000),
        }
        try:
            self._hook_queue.put_nowait(event)
        except asyncio.QueueFull:
            self._warn_hook_limited("YesMaiCore 消息监听队列已满，已丢弃最新事件。")
            return {"action": "continue"}
        self._hook_seen[event_id] = now
        return {"action": "continue"}

    @staticmethod
    def _build_hook_event_id(message: dict[str, Any]) -> str:
        identity = {
            "message_id": message.get("message_id"),
            "timestamp": message.get("timestamp"),
            "platform": message.get("platform"),
            "session_id": message.get("session_id"),
            "raw_message": message.get("raw_message"),
        }
        payload = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _prune_hook_seen(self, now: float) -> None:
        expired = [event_id for event_id, seen_at in self._hook_seen.items() if now - seen_at >= _HOOK_DEDUP_TTL_SECONDS]
        for event_id in expired:
            self._hook_seen.pop(event_id, None)

    def _warn_hook_limited(self, message: str) -> None:
        now = time.monotonic()
        if now - self._hook_last_warning_at >= _HOOK_WARNING_INTERVAL_SECONDS:
            self._hook_last_warning_at = now
            self.ctx.logger.warning(message)

    async def _hook_worker(self, worker_id: int) -> None:
        del worker_id
        while True:
            event = await self._hook_queue.get()
            try:
                async with asyncio.timeout(_HOOK_EVENT_DEADLINE_SECONDS):
                    listeners = await self._get_hook_listeners()
                    for listener in listeners:
                        should_stop = await self._call_hook_listener(listener, event)
                        if should_stop:
                            break
            except TimeoutError:
                self._warn_hook_limited("YesMaiCore 消息监听事件处理超时，已停止本次分发。")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ctx.logger.warning("YesMaiCore 消息监听分发失败：%s", exc)
            finally:
                self._hook_queue.task_done()

    async def _get_hook_listeners(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        if now < self._hook_directory_expires_at:
            return list(self._hook_directory)
        async with self._hook_directory_lock:
            now = time.monotonic()
            if now < self._hook_directory_expires_at:
                return list(self._hook_directory)
            old_directory = list(self._hook_directory)
            try:
                entries = await self.ctx.api.list()
                if not isinstance(entries, list):
                    raise TypeError("api.list 未返回 API 列表")
                directory = [entry for entry in entries if self._is_hook_listener(entry)]
                directory.sort(key=lambda item: (str(item.get("plugin_id") or ""), str(item.get("full_name") or ""), str(item.get("version") or "1")))
                self._hook_directory = directory
                self._hook_directory_expires_at = now + _HOOK_DIRECTORY_TTL_SECONDS
                return list(directory)
            except Exception as exc:
                self._warn_hook_limited(f"YesMaiCore 无法刷新消息监听目录，将使用离线缓存：{exc}")
                if old_directory:
                    self._hook_directory_expires_at = now + min(5.0, _HOOK_DIRECTORY_TTL_SECONDS)
                return old_directory

    @staticmethod
    def _is_hook_listener(entry: Any) -> bool:
        if not isinstance(entry, dict):
            return False
        metadata = entry.get("metadata")
        plugin_id = str(entry.get("plugin_id") or "")
        return (
            plugin_id.startswith("com.yesmai.")
            and plugin_id != PLUGIN_ID
            and entry.get("public") is True
            and entry.get("enabled", True) is True
            and isinstance(metadata, dict)
            and metadata.get("yesmai_protocol") == _HOOK_LISTENER_PROTOCOL
            and bool(str(entry.get("full_name") or "").strip())
        )

    async def _call_hook_listener(self, listener: dict[str, Any], event: dict[str, Any]) -> bool:
        full_name = str(listener.get("full_name") or "").strip()
        version = str(listener.get("version") or "1").strip() or "1"
        api_name = f"{full_name}@{version}"
        try:
            result = await asyncio.wait_for(
                self.ctx.api.call(api_name, **event),
                timeout=_HOOK_LISTENER_TIMEOUT_MS / 1000.0,
            )
        except TimeoutError:
            self.ctx.logger.warning("YesMai listener 调用超时：%s", api_name)
            return False
        except Exception as exc:
            self.ctx.logger.warning("YesMai listener 调用失败：%s：%s", api_name, exc)
            return False
        parsed = self._parse_hook_listener_result(result, api_name)
        if parsed is None:
            return False
        message = event.get("message")
        stream_id = str(message.get("session_id") or "") if isinstance(message, dict) else ""
        results = parsed.get("results") or [parsed]
        for item in results:
            segments = item["segments"]
            if not segments or item["sent_during_handler"]:
                continue
            if not stream_id:
                self.ctx.logger.warning("YesMai listener 返回了消息，但原事件缺少 session_id：%s", api_name)
                continue
            try:
                send_result = await self.send_chain(stream_id, segments)
            except Exception as exc:
                self.ctx.logger.warning("YesMai listener 自动发送异常：%s：%s", api_name, exc)
            else:
                if not send_result.get("ok", False):
                    self.ctx.logger.warning(
                        "YesMai listener 自动发送失败：%s：%s",
                        api_name,
                        send_result.get("message") or "未知错误",
                    )
        return any(item["stop_yesmai_propagation"] for item in results)

    def _parse_hook_listener_result(self, result: Any, api_name: str) -> dict[str, Any] | None:
        if not isinstance(result, dict):
            self.ctx.logger.warning("YesMai listener 返回格式无效：%s", api_name)
            return None
        if result.get("ok") is not True:
            self.ctx.logger.warning(
                "YesMai listener 返回失败：%s：%s",
                api_name,
                result.get("message") or result.get("error") or "未知错误",
            )
            return None
        data = result.get("data")
        if not isinstance(data, dict):
            self.ctx.logger.warning("YesMai listener 缺少有效 data：%s", api_name)
            return None
        parsed = self._parse_hook_listener_item(data, api_name)
        if parsed is None:
            return None
        raw_results = data.get("results")
        if raw_results is None:
            return parsed
        if not isinstance(raw_results, list) or not raw_results:
            self.ctx.logger.warning("YesMai listener results 类型无效：%s", api_name)
            return None
        results: list[dict[str, Any]] = []
        for item in raw_results:
            normalized = self._parse_hook_listener_item(item, api_name)
            if normalized is None:
                return None
            results.append(normalized)
        parsed["results"] = results
        return parsed

    def _parse_hook_listener_item(self, data: Any, api_name: str) -> dict[str, Any] | None:
        if not isinstance(data, dict):
            self.ctx.logger.warning("YesMai listener 结果项格式无效：%s", api_name)
            return None
        if not isinstance(data.get("stop_yesmai_propagation"), bool):
            self.ctx.logger.warning("YesMai listener stop_yesmai_propagation 类型无效：%s", api_name)
            return None
        if not isinstance(data.get("sent_during_handler"), bool):
            self.ctx.logger.warning("YesMai listener sent_during_handler 类型无效：%s", api_name)
            return None
        raw_segments = data.get("segments")
        if not isinstance(raw_segments, list) or not all(isinstance(segment, dict) for segment in raw_segments):
            self.ctx.logger.warning("YesMai listener segments 类型无效：%s", api_name)
            return None
        return {
            "segments": list(raw_segments),
            "sent_during_handler": data["sent_during_handler"],
            "stop_yesmai_propagation": data["stop_yesmai_propagation"],
        }

    @API(
        "cron.execution.authorize",
        description="原子消费 YesMai Cron 单次执行授权",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def cron_execution_authorize(self, run_id: str, token: str) -> dict[str, Any]:
        service = self._cron_service
        if service is None:
            return _error("CRON_AUTHORIZATION_REJECTED", "Cron 执行授权失败。")
        try:
            authorized = await service.authorize(str(run_id), str(token))
        except Exception:
            self.ctx.logger.error("Cron 执行授权存储失败", exc_info=True)
            return _error("CRON_AUTHORIZATION_REJECTED", "Cron 执行授权失败。")
        if not authorized:
            return _error("CRON_AUTHORIZATION_REJECTED", "Cron 执行授权失败。")
        return _ok({"authorized": True}, "Cron 执行已授权。")

    @API(
        "cron.status",
        description="读取不含 handler payload 与 token 的 Cron 状态摘要",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def cron_status(self) -> dict[str, Any]:
        service = self._cron_service
        if service is None:
            return _ok(
                {
                    "enabled": False,
                    "running": False,
                    "is_leader": False,
                    "dispatching": 0,
                    "jobs": {},
                    "occurrences": {},
                    "protocol": "cron.handler@1",
                }
            )
        try:
            return _ok(await service.status())
        except Exception as exc:
            return _error("CRON_STATUS_UNAVAILABLE", f"Cron 状态不可用：{exc}", retryable=True)

    @API(
        "script.validate",
        description="YesMaiScript is temporarily disabled",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def script_validate(self, source: str) -> dict[str, Any]:
        del source
        return _error("FEATURE_DISABLED", "YesMaiScript 功能已暂时禁用")

    @API(
        "script.compile",
        description="YesMaiScript is temporarily disabled",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def script_compile(self, source: str) -> dict[str, Any]:
        del source
        return _error("FEATURE_DISABLED", "YesMaiScript 功能已暂时禁用")

    @API(
        "script.install",
        description="YesMaiScript is temporarily disabled",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def script_install(self, source: str, replace: bool = False) -> dict[str, Any]:
        del source, replace
        return _error("FEATURE_DISABLED", "YesMaiScript 功能已暂时禁用")

    @API(
        "health",
        description="查询 YesMaiCore 状态和协议版本",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "plugin_id": PLUGIN_ID,
            "plugin_version": PLUGIN_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "features": dict(self._features),
        }

    @API(
        "capabilities",
        description="查询指定平台的保守能力矩阵",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def capabilities(self, platform: str = "") -> dict[str, Any]:
        normalized_platform = str(platform or "unknown").strip().lower() or "unknown"
        capabilities = self._platform_capabilities.get(
            normalized_platform,
            {"text": True, "image": False, "emoji": False, "forward": False, "hybrid": False, "custom": False},
        )
        return {"platform": normalized_platform, "capabilities": dict(capabilities)}

    @API(
        "send.text",
        description="通过 YesMaiCore 发送文本消息",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def send_text(self, stream_id: str, text: str) -> dict[str, Any]:
        if not str(stream_id or "").strip():
            return _error("STREAM_ID_REQUIRED", "缺少 stream_id，无法发送消息。")
        sent = bool(await self.ctx.send.text(str(text), str(stream_id)))
        if not sent:
            return _error("SEND_FAILED", "MaiBot 未能发送文本消息。", retryable=True)
        return _ok({"sent": True})

    @API(
        "send.chain",
        description="通过 YesMaiCore 发送可序列化消息链",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def send_chain(self, stream_id: str, segments: list[dict[str, Any]]) -> dict[str, Any]:
        if not str(stream_id or "").strip():
            return _error("STREAM_ID_REQUIRED", "缺少 stream_id，无法发送消息链。")
        valid_segments = [segment for segment in segments if isinstance(segment, dict)]
        media_count = sum(
            1 for segment in valid_segments if str(segment.get("type") or "").strip().lower() in _MEDIA_SEGMENT_TYPES
        )
        if media_count > _MEDIA_CHAIN_MAX_SEGMENTS:
            return _error("MESSAGE_CHAIN_MEDIA_LIMIT", f"单条消息最多包含 {_MEDIA_CHAIN_MAX_SEGMENTS} 个媒体段。")
        try:
            async with asyncio.timeout(_MEDIA_CHAIN_DEADLINE_SECONDS):
                normalized: list[dict[str, Any]] = []
                total_media_bytes = 0
                for segment in valid_segments:
                    normalized_segment, media_bytes = await self._normalize_chain_segment(segment)
                    total_media_bytes += media_bytes
                    if total_media_bytes > _MEDIA_CHAIN_MAX_BYTES:
                        raise ValueError("消息链媒体总大小超过限制")
                    normalized.append(normalized_segment)
        except TimeoutError:
            return _error("MESSAGE_CHAIN_TIMEOUT", "处理消息链媒体超时，请稍后重试。", retryable=True)
        except ValueError as exc:
            return _error("MESSAGE_COMPONENT_INVALID", str(exc))
        sent = bool(await self.ctx.send.hybrid(normalized, str(stream_id)))
        if not sent:
            return _error("SEND_FAILED", "MaiBot 未能发送消息链。", retryable=True)
        return _ok({"sent": True})

    @staticmethod
    async def _normalize_chain_segment(segment: dict[str, Any]) -> tuple[dict[str, Any], int]:
        segment_type = str(segment.get("type") or "").strip().lower()
        content = segment.get("content")
        if segment_type == "text":
            return {"type": "text", "content": str(content or "")}, 0
        if segment_type == "image":
            descriptor = content if isinstance(content, dict) else {"source": content}
            source = str(descriptor.get("source") or "")
            if source and not source.startswith(("http://", "https://", "data:")) and "://" not in source:
                try:
                    raw = base64.b64decode(source, validate=True)
                    source = f"data:image/png;base64,{base64.b64encode(raw).decode('ascii')}"
                except (ValueError, binascii.Error):
                    pass
            encoded, _mime_type, byte_count = await asyncio.to_thread(_load_media_source, source)
            return {"type": "image", "binary_data_base64": encoded, "data": ""}, byte_count
        if segment_type == "voice":
            descriptor = content if isinstance(content, dict) else {"source": content}
            encoded, _mime_type, byte_count = await asyncio.to_thread(
                _load_media_source, str(descriptor.get("source") or "")
            )
            return {"type": "voice", "binary_data_base64": encoded, "data": ""}, byte_count
        if segment_type == "file":
            descriptor = content if isinstance(content, dict) else {"source": content}
            encoded, mime_type, byte_count = await asyncio.to_thread(
                _load_media_source, str(descriptor.get("source") or "")
            )
            return {
                "type": "file",
                "data": {
                    "name": str(descriptor.get("name") or ""),
                    "mime_type": mime_type,
                    "base64": encoded,
                },
            }, byte_count
        if segment_type == "video":
            raise ValueError("当前 MaiBot Host 尚无标准视频消息组件；Video 已保留但暂不能发送")
        if segment_type == "at":
            descriptor = content if isinstance(content, dict) else {"target_user_id": content}
            return {"type": "at", "data": dict(descriptor)}, 0
        if segment_type == "reply":
            descriptor = content if isinstance(content, dict) else {"target_message_id": content}
            return {"type": "reply", "data": dict(descriptor)}, 0
        raise ValueError(f"不支持的消息组件类型：{segment_type or '空'}")

    @API(
        "llm.generate",
        description="通过 YesMaiCore 调用 MaiBot LLM",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def llm_generate(self, prompt: Any, **options: Any) -> dict[str, Any]:
        normalized_options = dict(options)
        requested_task = str(normalized_options.pop("task", "") or "").strip()
        if requested_task:
            normalized_options["model"] = requested_task
        result = await self.ctx.llm.generate(prompt, **normalized_options)
        if not isinstance(result, dict) or not result.get("success", False):
            return _error("LLM_GENERATE_FAILED", str(result.get("error") or "LLM 生成失败") if isinstance(result, dict) else "LLM 生成失败", retryable=True)
        normalized = dict(result)
        normalized["text"] = str(result.get("text") or result.get("response") or "")
        normalized["model"] = str(result.get("model") or result.get("model_name") or "")
        normalized["task"] = requested_task or str(options.get("model") or "")
        return _ok(normalized)

    @API(
        "chat.resolve",
        description="将平台目标严格解析为已存在的 MaiBot 聊天流",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def chat_resolve(
        self,
        platform: str,
        chat_type: str,
        target_id: str,
        account_id: str = "",
        scope: str = "",
        expected_stream_id: str = "",
    ) -> dict[str, Any]:
        normalized_platform = str(platform or "").strip()
        normalized_chat_type = str(chat_type or "").strip().lower()
        normalized_target_id = str(target_id or "").strip()
        normalized_account_id = str(account_id or "").strip()
        normalized_scope = str(scope or "").strip()
        expected = str(expected_stream_id or "").strip()
        if not normalized_platform or normalized_chat_type not in {"group", "private"} or not normalized_target_id:
            return _error(
                "CHAT_RESOLVE_ARGUMENT_INVALID",
                "platform、chat_type(group/private) 和 target_id 均为必填参数。",
            )
        try:
            result = (
                await self.ctx.chat.get_group_streams(normalized_platform)
                if normalized_chat_type == "group"
                else await self.ctx.chat.get_private_streams(normalized_platform)
            )
        except Exception as exc:
            return _error("CHAT_HOST_UNAVAILABLE", f"MaiBot 聊天流查询失败：{exc}", retryable=True)
        if isinstance(result, dict) and result.get("success") is False:
            return _error(
                "CHAT_HOST_UNAVAILABLE",
                str(result.get("error") or "MaiBot 聊天流查询失败。"),
                retryable=True,
            )
        if not isinstance(result, list):
            return _error("CHAT_RESULT_INVALID", "MaiBot 聊天流列表格式无效。", retryable=True)
        streams = result

        target_key = "group_id" if normalized_chat_type == "group" else "user_id"
        candidates: dict[str, dict[str, Any]] = {}
        for raw_stream in streams:
            if not isinstance(raw_stream, dict):
                continue
            if str(raw_stream.get("platform") or "").strip() != normalized_platform:
                continue
            if str(raw_stream.get(target_key) or "").strip() != normalized_target_id:
                continue
            if normalized_account_id and str(raw_stream.get("account_id") or "").strip() != normalized_account_id:
                continue
            if normalized_scope and str(raw_stream.get("scope") or "").strip() != normalized_scope:
                continue
            stream_id = str(raw_stream.get("stream_id") or raw_stream.get("session_id") or "").strip()
            if not stream_id:
                return _error("CHAT_RESULT_INVALID", "匹配的 MaiBot 聊天流缺少 stream_id。", retryable=True)
            candidates[stream_id] = dict(raw_stream)

        if not candidates:
            return _error("CHAT_NOT_FOUND", "没有找到符合身份约束的已存在聊天流。")
        if len(candidates) > 1:
            return _error("CHAT_AMBIGUOUS", "找到多个聊天流，请补充 account_id 或 scope。")
        stream_id, stream = next(iter(candidates.items()))
        if expected and expected != stream_id:
            return _error("CHAT_IDENTITY_MISMATCH", "聊天流已变化，与 expected_stream_id 不一致。")
        return _ok(
            {
                "stream_id": stream_id,
                "platform": normalized_platform,
                "chat_type": normalized_chat_type,
                "target_id": normalized_target_id,
                "group_id": str(stream.get("group_id") or ""),
                "user_id": str(stream.get("user_id") or ""),
                "account_id": str(stream.get("account_id") or ""),
                "scope": str(stream.get("scope") or ""),
                "verified": True,
                "source": "host.chat",
            }
        )

    @API(
        "permission.resolve",
        description="解析 YesMai Bot 命令管理员权限",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def permission_resolve(
        self,
        permission: str,
        platform: str,
        user_id: str,
    ) -> dict[str, Any]:
        normalized_permission = str(permission or "").strip()
        if normalized_permission != _COMMAND_ADMIN_PERMISSION:
            return _error("PERMISSION_UNSUPPORTED", "当前权限类型不受支持。")
        identity = _normalize_command_admin(f"{platform}:{user_id}")
        if identity is None:
            return _error("PERMISSION_IDENTITY_MISSING", "权限解析缺少有效的平台或用户身份。")
        configured = self.get_plugin_config_data().get("permission", {})
        raw_admins = configured.get("command_admins", []) if isinstance(configured, dict) else []
        allowed_identities = {
            parsed
            for value in raw_admins if (parsed := _normalize_command_admin(value)) is not None
        }
        decision = "allow" if identity in allowed_identities else "deny"
        return _ok(
            {
                "permission": _COMMAND_ADMIN_PERMISSION,
                "decision": decision,
                "verified": True,
                "identity": {"platform": identity[0], "user_id": identity[1]},
                "source": _PERMISSION_SOURCE,
            }
        )

    @API(
        "render.html2png",
        description="通过 YesMaiCore 透明代理 MaiBot 原生 HTML 转 PNG 能力",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def render_html2png(self, html: str, options: Any = None) -> dict[str, Any]:
        rendered_html = str(html or "")
        if not rendered_html.strip():
            return _error("RENDER_HTML_REQUIRED", "缺少需要渲染的 HTML。")
        if options is not None and not isinstance(options, dict):
            return _error("RENDER_OPTIONS_INVALID", "渲染参数必须是字典。")
        raw_options = dict(options or {})
        viewport = raw_options.get("viewport")
        if viewport is None and ("viewport_width" in raw_options or "viewport_height" in raw_options):
            viewport = {
                "width": raw_options.get("viewport_width", 900),
                "height": raw_options.get("viewport_height", 500),
            }
        try:
            result = await self.ctx.render.html2png(
                rendered_html,
                selector=str(raw_options.get("selector", "body")),
                viewport=viewport if isinstance(viewport, dict) else None,
                device_scale_factor=raw_options.get("device_scale_factor", 2.0),
                full_page=raw_options.get("full_page", False),
                omit_background=raw_options.get("omit_background", False),
                wait_until=str(raw_options.get("wait_until", "load")),
                wait_for_selector=str(raw_options.get("wait_for_selector", "")),
                wait_for_timeout_ms=raw_options.get("wait_for_timeout_ms", 0),
                render_timeout_ms=raw_options.get("render_timeout_ms", 0),
                allow_network=raw_options.get("allow_network", False),
            )
        except Exception as exc:
            return _error("RENDER_FAILED", f"MaiBot HTML 渲染失败：{exc}", retryable=True)
        if not isinstance(result, dict):
            return _error("RENDER_RESULT_INVALID", "MaiBot HTML 渲染返回格式无效。", retryable=True)
        if result.get("success") is False:
            return _error("RENDER_FAILED", str(result.get("error") or "MaiBot HTML 渲染失败。"), retryable=True)
        image_base64 = str(result.get("image_base64") or result.get("base64") or "")
        try:
            image_bytes = base64.b64decode(image_base64, validate=True)
        except (ValueError, binascii.Error):
            return _error("RENDER_RESULT_INVALID", "MaiBot HTML 渲染没有返回有效图片。", retryable=True)
        if not image_bytes:
            return _error("RENDER_RESULT_INVALID", "MaiBot HTML 渲染返回了空图片。", retryable=True)
        mime_type = str(result.get("mime_type") or "image/png")
        normalized = dict(result)
        normalized.update({
            "base64": image_base64,
            "data_uri": f"data:{mime_type};base64,{image_base64}",
            "mime_type": mime_type,
        })
        return _ok(normalized)

    @API(
        "message.recent",
        description="通过 YesMaiCore 查询最近消息",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def message_recent(
        self,
        stream_id: str,
        limit: int = 100,
        since_timestamp: float | None = None,
        hours: float = 24.0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> dict[str, Any]:
        normalized_stream_id = str(stream_id or "").strip()
        if not normalized_stream_id:
            return _error("STREAM_ID_REQUIRED", "缺少 stream_id，无法查询消息。")
        try:
            normalized_limit = int(limit)
            normalized_hours = float(hours)
            cutoff = float(since_timestamp) if since_timestamp is not None else None
        except (TypeError, ValueError):
            return _error("MESSAGE_RECENT_ARGUMENT_INVALID", "limit、hours 和 since_timestamp 必须是数字。")
        if normalized_limit < 0 or normalized_hours < 0:
            return _error("MESSAGE_RECENT_ARGUMENT_INVALID", "limit 和 hours 不能是负数。")
        end_time = time.time()
        start_time = cutoff if cutoff is not None else end_time - normalized_hours * 3600
        try:
            messages = await self.ctx.message.get_by_time_in_chat(
                normalized_stream_id,
                str(start_time),
                str(end_time),
                limit=normalized_limit,
                limit_mode=str(limit_mode or "latest"),
                filter_mai=bool(filter_mai),
                filter_command=bool(filter_command),
                include_binary_data=bool(include_binary_data),
            )
        except Exception as exc:
            return _error("MESSAGE_QUERY_FAILED", f"MaiBot 最近消息查询失败：{exc}", retryable=True)
        if isinstance(messages, dict) and messages.get("success") is False:
            return _error("MESSAGE_QUERY_FAILED", str(messages.get("error") or "MaiBot 最近消息查询失败。"), retryable=True)
        if not isinstance(messages, list):
            return _error("MESSAGE_RESULT_INVALID", "MaiBot 最近消息查询返回格式无效。", retryable=True)
        return _ok(self._normalize_messages(messages))

    @API(
        "message.by_time",
        description="通过 YesMaiCore 按时间范围查询指定聊天流消息",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def message_by_time(
        self,
        stream_id: str,
        start_timestamp: float,
        end_timestamp: float | None = None,
        limit: int = 0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> dict[str, Any]:
        normalized_stream_id = str(stream_id or "").strip()
        if not normalized_stream_id:
            return _error("STREAM_ID_REQUIRED", "缺少 stream_id，无法查询消息。")
        try:
            start_time = float(start_timestamp)
            end_time = time.time() if end_timestamp is None else float(end_timestamp)
            normalized_limit = int(limit)
        except (TypeError, ValueError):
            return _error("MESSAGE_TIME_RANGE_INVALID", "消息时间范围和 limit 必须是数字。")
        if start_time > end_time or normalized_limit < 0:
            return _error("MESSAGE_TIME_RANGE_INVALID", "消息起始时间不能晚于结束时间，limit 不能为负数。")
        try:
            messages = await self.ctx.message.get_by_time_in_chat(
                normalized_stream_id,
                str(start_time),
                str(end_time),
                limit=normalized_limit,
                limit_mode=str(limit_mode or "latest"),
                filter_mai=bool(filter_mai),
                filter_command=bool(filter_command),
                include_binary_data=bool(include_binary_data),
            )
        except Exception as exc:
            return _error("MESSAGE_QUERY_FAILED", f"MaiBot 消息查询失败：{exc}", retryable=True)
        if isinstance(messages, dict) and messages.get("success") is False:
            return _error("MESSAGE_QUERY_FAILED", str(messages.get("error") or "MaiBot 消息查询失败。"), retryable=True)
        if not isinstance(messages, list):
            return _error("MESSAGE_RESULT_INVALID", "MaiBot 消息查询返回格式无效。", retryable=True)
        return _ok(self._normalize_messages(messages))

    @staticmethod
    def _normalize_messages(messages: Any) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        if not isinstance(messages, list):
            return normalized
        for raw_message in messages:
            if not isinstance(raw_message, dict):
                continue
            message = dict(raw_message)
            message_info = message.get("message_info") if isinstance(message.get("message_info"), dict) else {}
            user_info = message_info.get("user_info") if isinstance(message_info.get("user_info"), dict) else {}
            sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
            message["sender"] = {
                "user_id": str(sender.get("user_id") or user_info.get("user_id") or ""),
                "nickname": str(sender.get("nickname") or user_info.get("user_nickname") or ""),
                "card": str(sender.get("card") or user_info.get("user_cardname") or ""),
            }
            text = message.get("processed_plain_text")
            if not isinstance(text, str):
                segments = message.get("raw_message")
                text = "".join(
                    str(segment.get("data") or "")
                    for segment in segments
                    if isinstance(segment, dict) and str(segment.get("type") or "").lower() == "text"
                ) if isinstance(segments, list) else str(segments or "")
            message["text"] = text
            normalized.append(message)
        return normalized

    @API(
        "person.resolve",
        description="通过 YesMaiCore 解析 MaiBot 人物信息",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def person_resolve(self, platform: str, user_id: str) -> dict[str, Any]:
        person_id = await self.ctx.person.get_id(str(platform), str(user_id))
        if not person_id:
            return _error("PERSON_NOT_FOUND", "没有找到对应人物信息。")
        return _ok({"person_id": person_id, "platform": str(platform), "user_id": str(user_id)})

    @API(
        "model.config.get",
        description="读取经过密钥脱敏的 MaiBot 模型配置",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def model_config_get(self) -> dict[str, Any]:
        return await self._run_model_config(self._model_config.get_redacted)

    @API(
        "model.directory.get",
        description="读取不含凭据的 Provider、Model 与 Task 目录",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def model_directory_get(self) -> dict[str, Any]:
        return await self._run_model_config(self._model_config.get_directory)

    @API(
        "model.config.validate",
        description="使用当前 MaiBot 模型规则验证候选配置，不写入文件",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def model_config_validate(self, config: Any) -> dict[str, Any]:
        result = await self._run_model_config(self._model_config.validate, config)
        if result.get("ok") is not True:
            return result
        return _ok({"valid": True}, "模型配置有效")

    @API(
        "model.config.patch",
        description="递归更新 MaiBot 模型配置并创建单份自动备份",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def model_config_patch(self, patch: Any) -> dict[str, Any]:
        return await self._run_model_config(self._model_config.patch, patch, exclusive=True)

    @API(
        "model.config.restore",
        description="恢复 YesMaiCore 最近一次模型配置备份",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def model_config_restore(self) -> dict[str, Any]:
        return await self._run_model_config(self._model_config.restore, exclusive=True)

    async def _run_model_config(self, operation: Any, *args: Any, exclusive: bool = False) -> dict[str, Any]:
        async def invoke() -> dict[str, Any]:
            try:
                data = await asyncio.to_thread(operation, *args)
            except ModelConfigError as exc:
                return _error(exc.code, exc.message)
            except Exception as exc:
                return _error("MODEL_CONFIG_INTERNAL_UNAVAILABLE", f"模型配置功能暂不可用：{exc}", retryable=True)
            return _ok(data)

        if exclusive:
            async with self._model_config_lock:
                return await invoke()
        return await invoke()

    @API(
        "config.get",
        description="读取 YesMai 集中配置；远程服务未启用时返回调用方提供的默认值",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def config_get(self, key: str, default: Any = None, namespace: str = "default") -> dict[str, Any]:
        normalized_namespace = str(namespace or "default").strip() or "default"
        normalized_key = str(key)
        integration = self._web_integration
        if integration is not None:
            stale = False
            source = "web"
            try:
                record = await integration.fetch_instance_config(normalized_namespace)
            except Exception as exc:
                self.ctx.logger.warning("YesMaiWeb 实例配置同步失败，使用离线缓存：%s", exc)
                record = integration.get_cached_instance_config(normalized_namespace)
                stale = record is not None
                source = "cache"
            if record is not None:
                return {
                    "namespace": normalized_namespace,
                    "key": normalized_key,
                    "value": _lookup_config_value(record.config, normalized_key, default),
                    "source": source,
                    "stale": stale,
                    "revision": record.revision,
                }
        return {
            "namespace": normalized_namespace,
            "key": normalized_key,
            "value": default,
            "source": "default",
            "stale": False,
        }

    @API(
        "leaderboard.list",
        description="读取 YesMai 排行榜；服务未启用时返回空榜单",
        version=PROTOCOL_VERSION,
        public=True,
    )
    async def leaderboard_list(self, board: str, limit: int = 10) -> dict[str, Any]:
        normalized_limit = max(1, min(int(limit), 100))
        return {
            "board": str(board),
            "items": [],
            "limit": normalized_limit,
            "available": False,
            "reason": "排行榜服务尚未启用",
        }

    @HomeCard(
        "yesmai-core-status",
        title="YesMaiCore",
        description="YesMai 系列插件核心服务状态",
        content=[
            {"type": "markdown", "content": "**运行中** · 当前处于本地优先模式。"},
            {"type": "key_value", "entries": {"协议版本": PROTOCOL_VERSION, "插件版本": PLUGIN_VERSION}},
        ],
        icon="boxes",
        width="medium",
        order=200,
    )
    async def home_card_marker(self) -> None:
        return None


def create_plugin() -> YesMaiCorePlugin:
    return YesMaiCorePlugin()
