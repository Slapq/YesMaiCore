"""YesMai 插件基类。"""

from __future__ import annotations

from typing import Any

from maibot_sdk import MaiBotPlugin

from .chain import MessageChain
from .core import AsyncCoreClient


class YesMaiPlugin(MaiBotPlugin):
    """YesMai 原生异步插件基类。"""

    def __init__(self) -> None:
        super().__init__()
        self._core_client: AsyncCoreClient | None = None

    @property
    def core(self) -> AsyncCoreClient:
        if self._core_client is None:
            self._core_client = AsyncCoreClient(self.ctx)
        return self._core_client

    async def send(self, stream_id: str, content: str | MessageChain) -> bool:
        if isinstance(content, str):
            result = await self.core.send.text(stream_id, content)
        elif not isinstance(content, MessageChain):
            raise TypeError("content 仅接受字符串或 MessageChain")
        elif all(segment.type == "text" for segment in content.segments):
            result = await self.core.send.text(stream_id, content.plain_text())
        else:
            result = await self.core.call(
                "send.chain",
                stream_id=stream_id,
                segments=content.to_segments(),
            )
        return isinstance(result, dict) and result.get("ok") is True

    async def call_core(self, method: str, *, version: str = "1", default: Any = None, **kwargs: Any) -> Any:
        """调用硬依赖的 YesMaiCore。

        ``default`` 只用于 Core 在重载窗口中暂时离线等防御性场景，不能用来绕过
        Manifest 对 ``com.yesmai.core`` 的正式硬依赖。
        """

        api_name = str(method or "").strip()
        if not api_name:
            return default
        if "." not in api_name:
            api_name = f"com.yesmai.core.{api_name}"
        try:
            return await self.core.call(api_name, version=version, **kwargs)
        except Exception as exc:
            self.ctx.logger.debug("YesMaiCore API 调用失败，已降级: %s", exc)
            return default
