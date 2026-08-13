"""统一事件对象与回复结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, TYPE_CHECKING

from .chain import MessageChain
from .core import AsyncCoreClient
from .platform import PlatformInfo

if TYPE_CHECKING:
    from maibot_sdk import PluginContext


@dataclass(frozen=True, slots=True)
class EventResult:
    """处理器可返回的统一结果。"""

    chain: MessageChain = field(default_factory=MessageChain)
    blocked: bool = False
    continue_processing: bool = True
    custom_result: Any = None


@dataclass(slots=True)
class YesMaiEvent:
    """从 MaiBot 组件 kwargs 归一化得到的事件对象。"""

    ctx: PluginContext
    component_type: str
    component_name: str
    platform: PlatformInfo
    plain_text: str = ""
    message: Any = None
    matched_groups: dict[str, Any] = field(default_factory=dict)
    function_args: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def stream_id(self) -> str:
        return self.platform.stream_id

    @property
    def user_id(self) -> str:
        return self.platform.user_id

    @property
    def group_id(self) -> str:
        return self.platform.group_id

    @property
    def unified_msg_origin(self) -> str:
        """稳定的会话标识；优先使用 Host 提供的 stream_id。"""

        if self.stream_id:
            return f"{self.platform.name}:{self.stream_id}"
        target_type = "group" if self.platform.is_group else "private"
        target_id = self.group_id if self.platform.is_group else self.user_id
        return f"{self.platform.name}:{target_type}:{target_id}"

    @classmethod
    def from_kwargs(
        cls,
        ctx: PluginContext,
        *,
        component_type: str,
        component_name: str,
        kwargs: Mapping[str, Any],
    ) -> YesMaiEvent:
        raw = dict(kwargs)
        matched_groups = raw.get("matched_groups")
        function_args = raw.get("function_args")
        message = raw.get("message", raw.get("raw_message"))
        message_data = message if isinstance(message, Mapping) else {}
        plain_text = str(
            raw.get("plain_text")
            or raw.get("text")
            or raw.get("message_content")
            or message_data.get("processed_plain_text")
            or raw.get("raw_message")
            or ""
        )
        return cls(
            ctx=ctx,
            component_type=component_type,
            component_name=component_name,
            platform=PlatformInfo.from_kwargs(raw),
            plain_text=plain_text,
            message=message,
            matched_groups=dict(matched_groups) if isinstance(matched_groups, Mapping) else {},
            function_args=dict(function_args) if isinstance(function_args, Mapping) else {},
            extra=raw,
        )

    def plain_result(self, text: str) -> EventResult:
        return EventResult(chain=MessageChain.text(text))

    def chain_result(self, chain: MessageChain) -> EventResult:
        if not isinstance(chain, MessageChain):
            raise TypeError("chain_result 仅接受 MessageChain")
        return EventResult(chain=chain)

    def stop_event(self, result: EventResult | None = None) -> EventResult:
        base = result or EventResult()
        return EventResult(
            chain=base.chain,
            blocked=True,
            continue_processing=False,
            custom_result=base.custom_result,
        )

    async def send(self, content: str | MessageChain) -> bool:
        """向当前会话发送文本或消息链。"""

        if not self.stream_id:
            raise RuntimeError("当前事件缺少 stream_id，无法发送消息")
        core = AsyncCoreClient(self.ctx)
        if isinstance(content, str):
            result = await core.send.text(self.stream_id, content)
            return isinstance(result, dict) and result.get("ok") is True
        if not isinstance(content, MessageChain):
            raise TypeError("send 仅接受字符串或 MessageChain")
        if not content:
            return True
        if all(segment.type == "text" for segment in content.segments):
            result = await core.send.text(self.stream_id, content.plain_text())
        else:
            result = await core.call(
                "send.chain",
                stream_id=self.stream_id,
                segments=content.to_segments(),
            )
        return isinstance(result, dict) and result.get("ok") is True
