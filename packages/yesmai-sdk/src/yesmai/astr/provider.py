"""AstrBot ProviderRequest/LLMResponse 的受限、可序列化兼容模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class AstrLLMProviderUnsupportedError(RuntimeError):
    """Astr Provider identity cannot be mapped to a MaiBot model task."""


class AstrLLMArgumentUnsupportedError(RuntimeError):
    """The current task-only LLM bridge cannot preserve an Astr argument."""


@dataclass(slots=True)
class ProviderRequest:
    """Maisaka replyer.before_request 的字段快照。"""

    session_id: str = ""
    request_type: str = ""
    task_name: str = ""
    model_name: str = ""
    extra_prompt: str = ""
    attempt: int = 1
    retry_count: int = 0
    max_retries: int = 0
    reply_message_id: str = ""
    reply_reason: str = ""
    selected_expression_ids: list[str] = field(default_factory=list)
    reply_tool_args: dict[str, Any] = field(default_factory=dict)
    prompt: str | None = None
    image_urls: list[str] = field(default_factory=list)
    audio_urls: list[str] = field(default_factory=list)
    extra_user_content_parts: list[Any] = field(default_factory=list)
    func_tool: Any = None
    contexts: list[dict[str, Any]] = field(default_factory=list)
    system_prompt: str | None = None
    conversation: Any = None
    tool_calls_result: Any = None
    model: str | None = None


@dataclass(slots=True)
class LLMResponse:
    """Maisaka replyer.after_response 的字段快照；response 是可回写正文。"""

    response: str = ""
    session_id: str = ""
    request_type: str = ""
    task_name: str = ""
    requested_model_name: str = ""
    attempt: int = 1
    retry_count: int = 0
    max_retries: int = 0
    reply_message_id: str = ""
    selected_expression_ids: list[str] = field(default_factory=list)
    reply_tool_args: dict[str, Any] = field(default_factory=dict)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    retry: bool = False
    retry_reason: str = ""
    role: str = "assistant"
    reasoning_content: str | None = None
    reasoning_signature: str | None = None
    tools_call_args: list[dict[str, Any]] = field(default_factory=list)
    tools_call_name: list[str] = field(default_factory=list)
    tools_call_ids: list[str] = field(default_factory=list)
    raw_completion: Any = None
    is_chunk: bool = False
    id: str = ""
    usage: dict[str, Any] | None = None

    @property
    def completion_text(self) -> str:
        """AstrBot 常用别名；与 Host response 保持同步。"""
        return self.response

    @completion_text.setter
    def completion_text(self, value: str) -> None:
        self.response = str(value)


__all__ = [
    "AstrLLMArgumentUnsupportedError",
    "AstrLLMProviderUnsupportedError",
    "LLMResponse",
    "ProviderRequest",
]
