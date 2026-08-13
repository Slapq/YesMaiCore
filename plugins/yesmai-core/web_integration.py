"""YesMaiCore 主动连接 YesMaiWeb 的实例控制客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import tempfile
import urllib.error
import urllib.request
import uuid
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

RequestCallable = Callable[[str, str, dict[str, Any] | None, str | None], Awaitable[dict[str, Any]]]
OperationHandler = Callable[[str, dict[str, Any], dict[str, Any]], Awaitable[dict[str, Any]]]
CapabilitiesProvider = Callable[[], dict[str, Any]]


class WebIntegrationError(RuntimeError):
    """YesMaiWeb 请求失败。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class InstanceIdentity:
    instance_uuid: str
    secret: str

    @property
    def bearer(self) -> str:
        return f"{self.instance_uuid}.{self.secret}"


@dataclass(frozen=True)
class CachedInstanceConfig:
    plugin_id: str
    revision: int
    config: dict[str, Any]


class InstanceConfigCache:
    """保存最近一次成功从 Web 读取的实例配置。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._records = self._load()

    def get(self, plugin_id: str) -> CachedInstanceConfig | None:
        value = self._records.get(plugin_id)
        if not isinstance(value, dict):
            return None
        revision = value.get("revision")
        config = value.get("config")
        if not isinstance(revision, int) or revision < 1 or not isinstance(config, dict):
            return None
        return CachedInstanceConfig(plugin_id=plugin_id, revision=revision, config=deepcopy(config))

    def put(self, record: CachedInstanceConfig) -> None:
        self._records[record.plugin_id] = {"revision": record.revision, "config": deepcopy(record.config)}
        self._write()

    def remove(self, plugin_id: str) -> None:
        if self._records.pop(plugin_id, None) is not None:
            self._write()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        if not self.path.is_file() or self.path.is_symlink():
            raise WebIntegrationError("INSTANCE_CONFIG_CACHE_INVALID", "实例配置缓存不是安全的普通文件")
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WebIntegrationError("INSTANCE_CONFIG_CACHE_INVALID", f"实例配置缓存损坏：{exc}") from exc
        if not isinstance(value, dict):
            raise WebIntegrationError("INSTANCE_CONFIG_CACHE_INVALID", "实例配置缓存顶层必须是对象")
        return value

    def _write(self) -> None:
        _atomic_write_json(self.path, self._records)


class JsonWebClient:
    """使用标准库发送 JSON 请求，阻塞 I/O 由调用方放入线程。"""

    def __init__(self, web_url: str, *, timeout_seconds: float = 20.0) -> None:
        normalized = str(web_url or "").strip().rstrip("/")
        if not normalized.startswith(("https://", "http://")):
            raise ValueError("web_url 必须是 http:// 或 https:// URL")
        self.base_url = normalized
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    async def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        bearer: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._request_sync, method, path, payload, bearer)

    def _request_sync(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        bearer: str | None,
    ) -> dict[str, Any]:
        headers = {"Accept": "application/json", "User-Agent": "YesMaiCore/0.1.3"}
        body: bytes | None = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            envelope = self._decode_envelope(raw)
            raise WebIntegrationError(
                str(envelope.get("code") or f"HTTP_{exc.code}"),
                str(envelope.get("message") or f"YesMaiWeb 返回 HTTP {exc.code}"),
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise WebIntegrationError("WEB_UNAVAILABLE", f"无法连接 YesMaiWeb：{exc}") from exc
        envelope = self._decode_envelope(raw)
        if envelope.get("ok") is not True:
            raise WebIntegrationError(
                str(envelope.get("code") or "WEB_RESPONSE_INVALID"),
                str(envelope.get("message") or "YesMaiWeb 请求失败"),
            )
        data = envelope.get("data")
        if not isinstance(data, dict):
            raise WebIntegrationError("WEB_RESPONSE_INVALID", "YesMaiWeb 响应 data 必须是对象")
        return data

    @staticmethod
    def _decode_envelope(raw: bytes) -> dict[str, Any]:
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebIntegrationError("WEB_RESPONSE_INVALID", "YesMaiWeb 返回了无效 JSON") from exc
        if not isinstance(value, dict):
            raise WebIntegrationError("WEB_RESPONSE_INVALID", "YesMaiWeb 响应必须是对象")
        return value


class CoreWebIntegration:
    """维护实例身份并串行执行 YesMaiWeb 控制任务。"""

    def __init__(
        self,
        *,
        web_url: str,
        data_dir: Path,
        logger: logging.Logger,
        operation_handler: OperationHandler,
        capabilities_provider: CapabilitiesProvider,
        poll_interval_seconds: float = 5.0,
        request: RequestCallable | None = None,
    ) -> None:
        self.web_url = str(web_url).strip().rstrip("/")
        self.data_dir = Path(data_dir)
        self.logger = logger
        self.operation_handler = operation_handler
        self.capabilities_provider = capabilities_provider
        self.poll_interval_seconds = max(1.0, float(poll_interval_seconds))
        self.identity = load_or_create_identity(self.data_dir / "web-instance.json")
        self.config_cache = InstanceConfigCache(self.data_dir / "instance-config-cache.json")
        self.process_nonce = str(uuid.uuid4())
        self._request = request or JsonWebClient(self.web_url).request

    async def run(self) -> None:
        backoff_seconds = 1.0
        while True:
            try:
                await self.bootstrap()
                backoff_seconds = 1.0
                while True:
                    handled = await self.poll_once()
                    if not handled:
                        await asyncio.sleep(self.poll_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("YesMaiWeb 实例通道暂不可用：%s", exc)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2.0, 60.0)

    async def bootstrap(self) -> None:
        await self._request(
            "POST",
            "/api/v1/instances/register",
            {
                "instance_uuid": self.identity.instance_uuid,
                "secret": self.identity.secret,
                "display_name": "MaiBot",
            },
            None,
        )
        await self._request(
            "POST",
            "/api/v1/instances/capabilities",
            {"process_nonce": self.process_nonce, "capabilities": self.capabilities_provider()},
            self.identity.bearer,
        )

    async def fetch_instance_config(self, plugin_id: str) -> CachedInstanceConfig | None:
        normalized = str(plugin_id or "").strip()
        if not normalized:
            raise ValueError("plugin_id 不能为空")
        response = await self._request(
            "GET",
            f"/api/v1/instances/configs/{quote(normalized, safe='')}",
            None,
            self.identity.bearer,
        )
        if response.get("exists") is not True:
            self.config_cache.remove(normalized)
            return None
        revision = response.get("revision")
        config = response.get("config")
        if not isinstance(revision, int) or revision < 1 or not isinstance(config, dict):
            raise WebIntegrationError("WEB_RESPONSE_INVALID", "YesMaiWeb 实例配置响应无效")
        record = CachedInstanceConfig(plugin_id=normalized, revision=revision, config=deepcopy(config))
        self.config_cache.put(record)
        return record

    def get_cached_instance_config(self, plugin_id: str) -> CachedInstanceConfig | None:
        return self.config_cache.get(str(plugin_id or "").strip())

    async def write_back_instance_config(self, plugin_id: str, base_revision: int, config: dict[str, Any]) -> int:
        """Core 本地修改成功后主动写回 Web，如果 CAS 冲突则抛异常。

        返回 Web 保存后的新 revision。
        """
        normalized = str(plugin_id or "").strip()
        if not normalized:
            raise ValueError("plugin_id 不能为空")
        if not isinstance(base_revision, int) or base_revision < 0:
            raise ValueError("base_revision 必须是非负整数")
        if not isinstance(config, dict):
            raise ValueError("config 必须是字典")
        response = await self._request(
            "POST",
            f"/api/v1/instances/configs/{quote(normalized, safe='')}/write-back",
            {"base_revision": base_revision, "config": deepcopy(config)},
            self.identity.bearer,
        )
        new_revision = response.get("revision")
        if not isinstance(new_revision, int) or new_revision < 1:
            raise WebIntegrationError("WEB_RESPONSE_INVALID", "Web 写回响应 revision 无效")
        record = CachedInstanceConfig(plugin_id=normalized, revision=new_revision, config=deepcopy(config))
        self.config_cache.put(record)
        return new_revision

    async def poll_once(self) -> bool:
        capabilities = self.capabilities_provider()
        operations = capabilities.get("supported_operations", capabilities.get("operations"))
        supported_operations = [str(item) for item in operations] if isinstance(operations, list) else []
        if not supported_operations:
            return False
        response = await self._request(
            "POST",
            "/api/v1/instances/tasks/poll",
            {"process_nonce": self.process_nonce, "supported_operations": supported_operations},
            self.identity.bearer,
        )
        task = response.get("task")
        if not isinstance(task, dict):
            if response.get("capabilities_stale") is True:
                await self._request(
                    "POST",
                    "/api/v1/instances/capabilities",
                    {"process_nonce": self.process_nonce, "capabilities": capabilities},
                    self.identity.bearer,
                )
            return False
        await self._execute_task(task)
        return True

    async def _execute_task(self, task: dict[str, Any]) -> None:
        task_id = str(task.get("id") or "")
        operation = str(task.get("operation") or "")
        lease_token = str(task.get("lease_token") or "")
        payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
        if not task_id or not operation or not lease_token:
            raise WebIntegrationError("WEB_TASK_INVALID", "YesMaiWeb 返回的控制任务缺少必要字段")

        lease = await self._request(
            "POST",
            f"/api/v1/instances/tasks/{task_id}/lease",
            {"process_nonce": self.process_nonce, "lease_token": lease_token},
            self.identity.bearer,
        )
        if lease.get("status") == "cancel_requested":
            await self._report_result(
                task_id,
                lease_token,
                "cancelled",
                {"code": "CANCELLED", "message": "任务已取消"},
            )
            return

        requested_by = task.get("requested_by") if isinstance(task.get("requested_by"), dict) else {}
        remote_context = {
            "remote_context_version": 1,
            "origin": "yesmai_web",
            "task_id": task_id,
            "instance_uuid": self.identity.instance_uuid,
            "requested_by_type": str(requested_by.get("type") or "system"),
            "requested_by_id": str(requested_by.get("id") or ""),
            "authorization_basis": str(requested_by.get("authorization_basis") or "system_maintenance"),
        }
        renewer = asyncio.create_task(
            self._renew_lease(task_id, lease_token),
            name=f"yesmai-web-lease-{task_id}",
        )
        try:
            try:
                result = await self.operation_handler(operation, payload, remote_context)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result = {"ok": False, "code": "EXECUTION_FAILED", "message": str(exc), "data": None}
        finally:
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)
        status = "succeeded" if result.get("ok") is True else "failed"
        await self._report_result(task_id, lease_token, status, result)

    async def _renew_lease(self, task_id: str, lease_token: str) -> None:
        while True:
            await asyncio.sleep(20.0)
            lease = await self._request(
                "POST",
                f"/api/v1/instances/tasks/{task_id}/lease",
                {"process_nonce": self.process_nonce, "lease_token": lease_token},
                self.identity.bearer,
            )
            if lease.get("status") == "cancel_requested":
                self.logger.info(
                    "YesMaiWeb 已请求取消控制任务 %s，等待当前 operation 协作结束",
                    task_id,
                )

    async def _report_result(self, task_id: str, lease_token: str, status: str, result: dict[str, Any]) -> None:
        await self._request(
            "POST",
            f"/api/v1/instances/tasks/{task_id}/result",
            {
                "process_nonce": self.process_nonce,
                "lease_token": lease_token,
                "status": status,
                "result": result,
            },
            self.identity.bearer,
        )


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, target)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def load_or_create_identity(path: Path) -> InstanceIdentity:
    """读取或原子创建实例 UUID 与随机密钥。"""

    target = Path(path)
    if target.exists():
        if not target.is_file() or target.is_symlink():
            raise WebIntegrationError("INSTANCE_IDENTITY_INVALID", "实例身份文件不是安全的普通文件")
        try:
            value = json.loads(target.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise ValueError("实例身份必须是对象")
            instance_uuid = str(value.get("instance_uuid") or "")
            secret = str(value.get("secret") or "")
            uuid.UUID(instance_uuid)
            if not secret:
                raise ValueError("实例密钥为空")
            return InstanceIdentity(instance_uuid=instance_uuid, secret=secret)
        except (OSError, ValueError, TypeError, json.JSONDecodeError, AttributeError) as exc:
            raise WebIntegrationError("INSTANCE_IDENTITY_INVALID", f"实例身份文件损坏：{exc}") from exc

    identity = InstanceIdentity(instance_uuid=str(uuid.uuid4()), secret=secrets.token_urlsafe(32))
    _atomic_write_json(target, {"instance_uuid": identity.instance_uuid, "secret": identity.secret})
    return identity


__all__ = [
    "CachedInstanceConfig",
    "CoreWebIntegration",
    "InstanceConfigCache",
    "InstanceIdentity",
    "JsonWebClient",
    "WebIntegrationError",
    "load_or_create_identity",
]
