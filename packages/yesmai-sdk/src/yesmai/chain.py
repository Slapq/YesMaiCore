"""可序列化消息链。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class MessageSegment:
    """单个消息段。"""

    type: str
    content: Any

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "content": self.content}


@dataclass(slots=True)
class MessageChain:
    """用于构建文本、图片和自定义消息段的轻量消息链。"""

    segments: list[MessageSegment] = field(default_factory=list)

    @classmethod
    def text(cls, text: str) -> MessageChain:
        return cls().add_text(text)

    @classmethod
    def from_segments(cls, segments: Iterable[MessageSegment | dict[str, Any]]) -> MessageChain:
        chain = cls()
        for segment in segments:
            if isinstance(segment, MessageSegment):
                chain.segments.append(segment)
            elif isinstance(segment, dict):
                chain.add(str(segment.get("type", "custom")), segment.get("content"))
            else:
                raise TypeError("消息段必须是 MessageSegment 或字典")
        return chain

    def add(self, segment_type: str, content: Any) -> MessageChain:
        normalized_type = str(segment_type or "").strip()
        if not normalized_type:
            raise ValueError("消息段类型不能为空")
        self.segments.append(MessageSegment(normalized_type, content))
        return self

    def add_text(self, text: str) -> MessageChain:
        return self.add("text", str(text))

    def add_image(self, image_base64: str) -> MessageChain:
        return self.add("image", image_base64)

    def add_emoji(self, emoji_base64: str) -> MessageChain:
        return self.add("emoji", emoji_base64)

    def to_segments(self) -> list[dict[str, Any]]:
        return [segment.to_dict() for segment in self.segments]

    def plain_text(self) -> str:
        return "".join(str(segment.content) for segment in self.segments if segment.type == "text")

    def __bool__(self) -> bool:
        return bool(self.segments)
