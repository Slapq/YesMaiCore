"""AstrBot 风格 MessageChain。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from ..chain import MessageChain as NativeMessageChain
from ..chain import MessageSegment
from .message_components import AstrComponent, At, File, Image, Plain, Record, Reply, Video

_COMPONENT_TYPES: dict[str, type[AstrComponent]] = {
    "text": Plain,
    "image": Image,
    "at": At,
    "reply": Reply,
    "voice": Record,
    "video": Video,
    "file": File,
}


class MessageChain(NativeMessageChain):
    """兼容 AstrBot 常见链式构建方式。"""

    def __init__(self, chain: Iterable[AstrComponent | MessageSegment | dict[str, Any] | str] | None = None) -> None:
        super().__init__()
        self.use_t2i_: bool | None = None
        self.use_markdown_: bool | None = None
        self.type: str | None = None
        if chain is not None:
            for component in chain:
                self.append(component)

    def append(self, component: AstrComponent | MessageSegment | dict[str, Any] | str) -> MessageChain:
        if isinstance(component, str):
            self.add_text(component)
        elif isinstance(component, AstrComponent):
            self.add(component.type, component.content)
        elif isinstance(component, MessageSegment):
            self.segments.append(component)
        elif isinstance(component, dict):
            self.add(str(component.get("type") or "custom"), component.get("content"))
        else:
            raise TypeError(f"不支持的消息组件：{type(component).__name__}")
        return self

    def message(self, text: str) -> MessageChain:
        return self.append(Plain(text))

    def image(self, source: str) -> MessageChain:
        return self.append(Image(source))

    def url_image(self, url: str) -> MessageChain:
        return self.append(Image.fromURL(url))

    def file_image(self, path: str) -> MessageChain:
        return self.append(Image.fromFileSystem(path))

    def base64_image(self, data: str) -> MessageChain:
        return self.append(Image.fromBase64(data))

    def at(self, user_id: str | int, name: str = "") -> MessageChain:
        return self.append(At(user_id, name))

    def reply(self, message_id: str) -> MessageChain:
        return self.append(Reply(message_id))

    def record(self, source: str) -> MessageChain:
        return self.append(Record(source))

    def video(self, source: str) -> MessageChain:
        return self.append(Video(source))

    def file(self, source: str, name: str = "") -> MessageChain:
        return self.append(File(source, name))

    def derive(
        self,
        chain: Iterable[AstrComponent | MessageSegment | dict[str, Any] | str] | None = None,
    ) -> MessageChain:
        derived = MessageChain(chain)
        derived.use_t2i_ = self.use_t2i_
        derived.use_markdown_ = self.use_markdown_
        derived.type = self.type
        return derived

    def use_t2i(self, enabled: bool) -> MessageChain:
        self.use_t2i_ = bool(enabled)
        return self

    def use_markdown(self, enabled: bool | None = True) -> MessageChain:
        self.use_markdown_ = enabled if enabled is None else bool(enabled)
        return self

    def get_plain_text(self, with_other_comps_mark: bool = False) -> str:
        if not with_other_comps_mark:
            return " ".join(component.text for component in self.chain if isinstance(component, Plain))
        parts: list[str] = []
        for component in self.chain:
            parts.append(component.text if isinstance(component, Plain) else f"[{type(component).__name__}]")
        return " ".join(parts)

    def squash_plain(self) -> MessageChain:
        plain_segments = [segment for segment in self.segments if segment.type == "text"]
        if not plain_segments:
            return self
        combined = "".join(str(segment.content) for segment in plain_segments)
        first_plain_index = next(index for index, segment in enumerate(self.segments) if segment.type == "text")
        self.segments = [segment for segment in self.segments if segment.type != "text"]
        self.segments.insert(first_plain_index, MessageSegment("text", combined))
        return self

    @property
    def chain(self) -> list[AstrComponent]:
        components: list[AstrComponent] = []
        for segment in self.segments:
            component_type = _COMPONENT_TYPES.get(segment.type)
            content = segment.content
            if component_type is Plain:
                components.append(Plain(str(content)))
            elif component_type is Image:
                descriptor = content if isinstance(content, dict) else {"source": content}
                components.append(Image(str(descriptor.get("source") or "")))
            elif component_type is At:
                descriptor = content if isinstance(content, dict) else {"target_user_id": content}
                components.append(At(descriptor.get("target_user_id", ""), descriptor.get("target_user_nickname", "")))
            elif component_type is Reply:
                descriptor = content if isinstance(content, dict) else {"target_message_id": content}
                components.append(Reply(descriptor.get("target_message_id", "")))
            elif component_type is Record:
                descriptor = content if isinstance(content, dict) else {"source": content}
                components.append(Record(str(descriptor.get("source") or "")))
            elif component_type is Video:
                descriptor = content if isinstance(content, dict) else {"source": content}
                components.append(Video(str(descriptor.get("source") or "")))
            elif component_type is File:
                descriptor = content if isinstance(content, dict) else {"source": content}
                components.append(File(str(descriptor.get("source") or ""), str(descriptor.get("name") or "")))
            else:
                components.append(AstrComponent(segment.type, content))
        return components

    def __iter__(self) -> Iterator[AstrComponent]:
        return iter(self.chain)

    def __len__(self) -> int:
        return len(self.segments)


__all__ = ["MessageChain"]
