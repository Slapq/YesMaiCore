"""AstrBot 风格消息组件。"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, BinaryIO


@dataclass(frozen=True, slots=True)
class AstrComponent:
    """可安全序列化的 Astr 风格消息组件基类。"""

    type: str
    content: Any

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "content": self.content}


class Plain(AstrComponent):
    def __init__(self, text: str = "") -> None:
        super().__init__("text", str(text))

    @property
    def text(self) -> str:
        return str(self.content)


class Image(AstrComponent):
    """图片组件；source 可以是 URL、文件路径、data URI 或 Base64。"""

    def __init__(self, source: str = "") -> None:
        super().__init__("image", {"source": str(source)})

    @classmethod
    def fromURL(cls, url: str) -> Image:  # noqa: N802 - AstrBot 兼容命名
        return cls(url)

    @classmethod
    def fromFileSystem(cls, path: str) -> Image:  # noqa: N802 - AstrBot 兼容命名
        return cls(path)

    @classmethod
    def fromBase64(cls, data: str) -> Image:  # noqa: N802 - AstrBot 兼容命名
        normalized = str(data or "").strip()
        if not normalized:
            raise ValueError("Base64 图片内容不能为空")
        if normalized.startswith("data:"):
            return cls(normalized)
        return cls(f"data:image/png;base64,{normalized}")

    @classmethod
    def fromBytes(cls, data: bytes) -> Image:  # noqa: N802 - AstrBot 兼容命名
        if not isinstance(data, bytes):
            raise TypeError("Image.fromBytes 只接受 bytes")
        return cls.fromBase64(base64.b64encode(data).decode("ascii"))

    @classmethod
    def fromIO(cls, stream: BinaryIO) -> Image:  # noqa: N802 - AstrBot 兼容命名
        return cls.fromBytes(stream.read())


class At(AstrComponent):
    def __init__(self, qq: str | int = "", name: str = "") -> None:
        super().__init__("at", {"target_user_id": str(qq), "target_user_nickname": str(name)})

    @property
    def qq(self) -> str:
        return str(self.content.get("target_user_id", ""))


class Reply(AstrComponent):
    def __init__(self, id: str = "") -> None:  # noqa: A002 - AstrBot 兼容参数名
        super().__init__("reply", {"target_message_id": str(id)})

class Record(AstrComponent):
    def __init__(self, source: str = "") -> None:
        super().__init__("voice", {"source": str(source)})

    @classmethod
    def fromURL(cls, url: str) -> Record:  # noqa: N802
        return cls(url)

    @classmethod
    def fromFileSystem(cls, path: str) -> Record:  # noqa: N802
        return cls(path)

    @classmethod
    def fromBase64(cls, data: str) -> Record:  # noqa: N802
        return cls(data)


class Video(AstrComponent):
    def __init__(self, source: str = "") -> None:
        super().__init__("video", {"source": str(source)})

    @classmethod
    def fromURL(cls, url: str) -> Video:  # noqa: N802
        return cls(url)

    @classmethod
    def fromFileSystem(cls, path: str) -> Video:  # noqa: N802
        return cls(path)


class File(AstrComponent):
    def __init__(self, source: str = "", name: str = "", file: str | None = None) -> None:  # noqa: A002
        # AstrBot's internal component historically used ``file=`` while the
        # public YesMai component uses ``source``. Accept both at the boundary.
        resolved_source = source if file is None else file
        super().__init__("file", {"source": str(resolved_source), "name": str(name)})

    @classmethod
    def fromFileSystem(cls, path: str, name: str = "") -> File:  # noqa: N802
        return cls(path, name)


__all__ = ["AstrComponent", "At", "File", "Image", "Plain", "Record", "Reply", "Video"]
