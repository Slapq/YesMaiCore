"""AstrBot 风格的纯本地命令解析辅助类型。"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(slots=True)
class CommandTokens:
    """按空白拆分后的命令 token。"""

    tokens: list[str] = field(default_factory=list)

    @property
    def len(self) -> int:
        return len(self.tokens)

    def get(self, index: int) -> str | None:
        return self.tokens[index] if 0 <= index < len(self.tokens) else None


class CommandParserMixin:
    """与 AstrBot 常用 CommandParserMixin 等价的纯 Python 辅助方法。"""

    def parse_commands(self, message: str) -> CommandTokens:
        normalized = str(message or "").strip()
        return CommandTokens(re.split(r"\s+", normalized) if normalized else [])

    def regex_match(self, message: str, command: str) -> bool:
        return re.search(str(command), str(message or ""), re.MULTILINE) is not None


__all__ = ["CommandParserMixin", "CommandTokens"]
