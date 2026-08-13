"""AstrBot 事件相关兼容类型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from .message_chain import MessageChain
from .message_components import Image, Plain


class MessageType(str, Enum):
    """AstrBot 常用消息类型。"""

    GROUP_MESSAGE = "group_message"
    FRIEND_MESSAGE = "friend_message"
    OTHER_MESSAGE = "other_message"


class EventResultType(Enum):
    CONTINUE = auto()
    STOP = auto()


class ResultContentType(Enum):
    LLM_RESULT = auto()
    AGENT_RUNNER_ERROR = auto()
    GENERAL_RESULT = auto()
    STREAMING_RESULT = auto()
    STREAMING_FINISH = auto()


@dataclass(frozen=True, slots=True)
class PlatformMetadata:
    """平台名称与唯一标识。"""

    name: str
    id: str


@dataclass(slots=True)
class MessageEventResult:
    """AstrBot 风格的可链式消息事件结果。"""

    chain: MessageChain = field(default_factory=MessageChain)
    blocked: bool = False
    custom_result: Any = None
    result_type: EventResultType = EventResultType.CONTINUE
    result_content_type: ResultContentType = ResultContentType.GENERAL_RESULT
    async_stream: Any = None
    lock_result: bool = False

    def __post_init__(self) -> None:
        if self.blocked or self.result_type is EventResultType.STOP:
            self.blocked = True
            self.result_type = EventResultType.STOP

    def message(self, text: str) -> MessageEventResult:
        self.chain.append(Plain(text))
        return self

    def url_image(self, url: str) -> MessageEventResult:
        self.chain.append(Image.fromURL(url))
        return self

    def file_image(self, path: str) -> MessageEventResult:
        self.chain.append(Image.fromFileSystem(path))
        return self

    def base64_image(self, data: str) -> MessageEventResult:
        self.chain.append(Image.fromBase64(data))
        return self

    def stop_event(self) -> MessageEventResult:
        self.blocked = True
        self.result_type = EventResultType.STOP
        return self

    def continue_event(self) -> MessageEventResult:
        self.blocked = False
        self.result_type = EventResultType.CONTINUE
        return self

    def is_stopped(self) -> bool:
        return self.blocked or self.result_type is EventResultType.STOP

    def set_result_type(self, result_type: Any) -> MessageEventResult:
        if isinstance(result_type, EventResultType):
            self.result_type = result_type
        else:
            normalized = str(getattr(result_type, "name", result_type) or "").strip().lower()
            self.result_type = (
                EventResultType.STOP
                if normalized in {"stop", "stopped", "block", "blocked"}
                else EventResultType.CONTINUE
            )
        self.blocked = self.result_type is EventResultType.STOP
        return self

    def set_async_stream(self, stream: Any) -> MessageEventResult:
        self.async_stream = stream
        return self

    def set_result_content_type(self, result_type: ResultContentType) -> MessageEventResult:
        if not isinstance(result_type, ResultContentType):
            raise TypeError("result_content_type 必须使用 ResultContentType")
        self.result_content_type = result_type
        return self

    def is_llm_result(self) -> bool:
        return self.result_content_type is ResultContentType.LLM_RESULT

    def is_model_result(self) -> bool:
        return self.result_content_type in {
            ResultContentType.LLM_RESULT,
            ResultContentType.AGENT_RUNNER_ERROR,
        }

    def set_custom_result(self, result: Any) -> MessageEventResult:
        self.custom_result = result
        return self

    def set_console_log(self, message: str, level: Any = None) -> MessageEventResult:
        del level
        self.custom_result = {"console_log": str(message)}
        return self


__all__ = [
    "EventResultType",
    "MessageEventResult",
    "MessageType",
    "PlatformMetadata",
    "ResultContentType",
]
