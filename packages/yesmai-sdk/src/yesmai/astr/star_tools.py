"""AstrBot StarTools 的严格限定兼容入口。"""

from __future__ import annotations

import contextvars
from pathlib import Path
from typing import Any

_CURRENT_STAR: contextvars.ContextVar[Any | None] = contextvars.ContextVar("yesmai_astr_star", default=None)


def activate_star(plugin: Any) -> contextvars.Token[Any | None]:
    return _CURRENT_STAR.set(plugin)


def deactivate_star(token: contextvars.Token[Any | None]) -> None:
    _CURRENT_STAR.reset(token)


class StarTools:
    """当前只提供执行窗口内的本插件持久化数据目录。"""

    @classmethod
    def initialize(cls, context: Any) -> None:
        del context

    @classmethod
    def get_data_dir(cls, plugin_name: str | None = None) -> Path:
        plugin = _CURRENT_STAR.get()
        if plugin is None:
            raise RuntimeError("StarTools.get_data_dir 只能在 initialize 或 Astr 处理器执行期间使用")
        current_plugin_id = str(plugin.ctx.plugin_id)
        requested = str(plugin_name or "").strip()
        if requested and requested not in {current_plugin_id, type(plugin).__name__}:
            raise RuntimeError("StarTools.get_data_dir 当前只允许访问本插件的数据目录")
        path = Path(plugin.ctx.paths.data_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path


__all__ = ["StarTools"]
