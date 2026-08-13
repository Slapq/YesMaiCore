"""YesMaiCore 客户端。

原生插件使用 :class:`AsyncCoreClient`；Astr 风格同步插件使用
:class:`SyncCoreClient`。两者调用相同的 Core 公共协议。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, TypeVar

from maibot_sdk import PluginContext

T = TypeVar("T")


class CoreUnavailableError(RuntimeError):
    """YesMaiCore 在运行时调用窗口内不可用。"""


class AsyncCoreClient:
    """面向 YesMai 原生异步插件的 Core 客户端。"""

    def __init__(self, ctx: PluginContext) -> None:
        self._ctx = ctx
        self.send = _AsyncSendAPI(self)
        self.llm = _AsyncLLMAPI(self)
        self.message = _AsyncMessageAPI(self)
        self.chat = _AsyncChatAPI(self)
        self.permission = _AsyncPermissionAPI(self)
        self.cron = _AsyncCronAPI(self)
        self.person = _AsyncPersonAPI(self)
        self.render = _AsyncRenderAPI(self)
        self.model = _AsyncModelAPI(self)

    async def call(self, method: str, *, version: str = "1", **kwargs: Any) -> Any:
        normalized = str(method or "").strip()
        if not normalized:
            raise ValueError("Core API 名称不能为空")
        api_name = normalized if normalized.startswith("com.yesmai.core.") else f"com.yesmai.core.{normalized}"
        try:
            return await self._ctx.api.call(api_name, version=version, **kwargs)
        except Exception as exc:
            raise CoreUnavailableError(f"YesMaiCore API 调用失败：{api_name}") from exc


class SyncCoreClient:
    """在线程中的 Astr 风格处理器里同步调用 Core。"""

    def __init__(self, async_client: AsyncCoreClient, loop: asyncio.AbstractEventLoop) -> None:
        self._async = async_client
        self._loop = loop
        self.send = _SyncSendAPI(self)
        self.llm = _SyncLLMAPI(self)
        self.message = _SyncMessageAPI(self)
        self.chat = _SyncChatAPI(self)
        self.permission = _SyncPermissionAPI(self)
        self.cron = _SyncCronAPI(self)
        self.person = _SyncPersonAPI(self)
        self.render = _SyncRenderAPI(self)
        self.model = _SyncModelAPI(self)

    def wait(self, awaitable: Coroutine[Any, Any, T], *, timeout: float = 60.0) -> T:
        future = asyncio.run_coroutine_threadsafe(awaitable, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise

    def call(self, method: str, *, version: str = "1", timeout: float = 60.0, **kwargs: Any) -> Any:
        return self.wait(self._async.call(method, version=version, **kwargs), timeout=timeout)


@dataclass(slots=True)
class _AsyncSendAPI:
    client: AsyncCoreClient

    async def text(self, stream_id: str, text: str) -> Any:
        return await self.client.call("send.text", stream_id=stream_id, text=text)


@dataclass(slots=True)
class _AsyncLLMAPI:
    client: AsyncCoreClient

    async def generate(self, prompt: Any, *, task: str = "", **options: Any) -> Any:
        if task:
            options["task"] = task
        return await self.client.call("llm.generate", prompt=prompt, **options)


@dataclass(slots=True)
class _AsyncMessageAPI:
    client: AsyncCoreClient

    async def recent(
        self,
        stream_id: str,
        limit: int = 10,
        since_timestamp: float | None = None,
        *,
        hours: float = 24.0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> Any:
        return await self.client.call(
            "message.recent",
            stream_id=stream_id,
            limit=limit,
            since_timestamp=since_timestamp,
            hours=hours,
            limit_mode=limit_mode,
            filter_mai=filter_mai,
            filter_command=filter_command,
            include_binary_data=include_binary_data,
        )

    async def by_time(
        self,
        stream_id: str,
        start_timestamp: float,
        end_timestamp: float | None = None,
        *,
        limit: int = 0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> Any:
        return await self.client.call(
            "message.by_time",
            stream_id=stream_id,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            limit=limit,
            limit_mode=limit_mode,
            filter_mai=filter_mai,
            filter_command=filter_command,
            include_binary_data=include_binary_data,
        )


@dataclass(slots=True)
class _AsyncChatAPI:
    client: AsyncCoreClient

    async def resolve(
        self,
        platform: str,
        chat_type: str,
        target_id: str,
        *,
        account_id: str = "",
        scope: str = "",
        expected_stream_id: str = "",
    ) -> Any:
        return await self.client.call(
            "chat.resolve",
            platform=platform,
            chat_type=chat_type,
            target_id=target_id,
            account_id=account_id,
            scope=scope,
            expected_stream_id=expected_stream_id,
        )


@dataclass(slots=True)
class _AsyncPermissionAPI:
    client: AsyncCoreClient

    async def resolve(self, permission: str, platform: str, user_id: str) -> Any:
        return await self.client.call(
            "permission.resolve",
            permission=permission,
            platform=platform,
            user_id=user_id,
        )


@dataclass(slots=True)
class _AsyncCronAPI:
    client: AsyncCoreClient

    async def authorize(self, run_id: str, token: str) -> Any:
        return await self.client.call("cron.execution.authorize", run_id=run_id, token=token)

    async def status(self) -> Any:
        return await self.client.call("cron.status")


@dataclass(slots=True)
class _AsyncRenderAPI:
    client: AsyncCoreClient

    async def html2png(self, html: str, **options: Any) -> Any:
        return await self.client.call("render.html2png", html=html, options=options)


@dataclass(slots=True)
class _AsyncPersonAPI:
    client: AsyncCoreClient

    async def resolve(self, platform: str, user_id: str) -> Any:
        return await self.client.call("person.resolve", platform=platform, user_id=user_id)


@dataclass(slots=True)
class _AsyncModelConfigAPI:
    client: AsyncCoreClient

    async def get(self) -> Any:
        return await self.client.call("model.config.get")

    async def validate(self, config: dict[str, Any]) -> Any:
        return await self.client.call("model.config.validate", config=config)

    async def patch(self, patch: dict[str, Any]) -> Any:
        return await self.client.call("model.config.patch", patch=patch)

    async def restore(self) -> Any:
        return await self.client.call("model.config.restore")


@dataclass(slots=True)
class _AsyncModelDirectoryAPI:
    client: AsyncCoreClient

    async def get(self) -> Any:
        return await self.client.call("model.directory.get")


class _AsyncModelAPI:
    def __init__(self, client: AsyncCoreClient) -> None:
        self.config = _AsyncModelConfigAPI(client)
        self.directory = _AsyncModelDirectoryAPI(client)


@dataclass(slots=True)
class _SyncSendAPI:
    client: SyncCoreClient

    def text(self, stream_id: str, text: str) -> Any:
        return self.client.call("send.text", stream_id=stream_id, text=text)


@dataclass(slots=True)
class _SyncLLMAPI:
    client: SyncCoreClient

    def generate(self, prompt: Any, *, task: str = "", **options: Any) -> Any:
        if task:
            options["task"] = task
        return self.client.call("llm.generate", prompt=prompt, **options)


@dataclass(slots=True)
class _SyncMessageAPI:
    client: SyncCoreClient

    def recent(
        self,
        stream_id: str,
        limit: int = 10,
        since_timestamp: float | None = None,
        *,
        hours: float = 24.0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> Any:
        return self.client.call(
            "message.recent",
            stream_id=stream_id,
            limit=limit,
            since_timestamp=since_timestamp,
            hours=hours,
            limit_mode=limit_mode,
            filter_mai=filter_mai,
            filter_command=filter_command,
            include_binary_data=include_binary_data,
        )

    def by_time(
        self,
        stream_id: str,
        start_timestamp: float,
        end_timestamp: float | None = None,
        *,
        limit: int = 0,
        limit_mode: str = "latest",
        filter_mai: bool = False,
        filter_command: bool = False,
        include_binary_data: bool = False,
    ) -> Any:
        return self.client.call(
            "message.by_time",
            stream_id=stream_id,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            limit=limit,
            limit_mode=limit_mode,
            filter_mai=filter_mai,
            filter_command=filter_command,
            include_binary_data=include_binary_data,
        )


@dataclass(slots=True)
class _SyncChatAPI:
    client: SyncCoreClient

    def resolve(
        self,
        platform: str,
        chat_type: str,
        target_id: str,
        *,
        account_id: str = "",
        scope: str = "",
        expected_stream_id: str = "",
    ) -> Any:
        return self.client.call(
            "chat.resolve",
            platform=platform,
            chat_type=chat_type,
            target_id=target_id,
            account_id=account_id,
            scope=scope,
            expected_stream_id=expected_stream_id,
        )


@dataclass(slots=True)
class _SyncPermissionAPI:
    client: SyncCoreClient

    def resolve(self, permission: str, platform: str, user_id: str) -> Any:
        return self.client.call(
            "permission.resolve",
            permission=permission,
            platform=platform,
            user_id=user_id,
        )


@dataclass(slots=True)
class _SyncCronAPI:
    client: SyncCoreClient

    def authorize(self, run_id: str, token: str) -> Any:
        return self.client.call("cron.execution.authorize", run_id=run_id, token=token)

    def status(self) -> Any:
        return self.client.call("cron.status")


@dataclass(slots=True)
class _SyncRenderAPI:
    client: SyncCoreClient

    def html2png(self, html: str, **options: Any) -> Any:
        return self.client.call("render.html2png", html=html, options=options)


@dataclass(slots=True)
class _SyncModelConfigAPI:
    client: SyncCoreClient

    def get(self) -> Any:
        return self.client.call("model.config.get")

    def validate(self, config: dict[str, Any]) -> Any:
        return self.client.call("model.config.validate", config=config)

    def patch(self, patch: dict[str, Any]) -> Any:
        return self.client.call("model.config.patch", patch=patch)

    def restore(self) -> Any:
        return self.client.call("model.config.restore")


@dataclass(slots=True)
class _SyncModelDirectoryAPI:
    client: SyncCoreClient

    def get(self) -> Any:
        return self.client.call("model.directory.get")


class _SyncModelAPI:
    def __init__(self, client: SyncCoreClient) -> None:
        self.config = _SyncModelConfigAPI(client)
        self.directory = _SyncModelDirectoryAPI(client)


@dataclass(slots=True)
class _SyncPersonAPI:
    client: SyncCoreClient

    def resolve(self, platform: str, user_id: str) -> Any:
        return self.client.call("person.resolve", platform=platform, user_id=user_id)
