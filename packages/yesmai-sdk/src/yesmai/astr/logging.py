"""AstrBot 风格的标准日志入口。

该模块只提供 Python 标准库 ``logging.Logger``，不创建独立日志系统，也不绕过
MaiBot 的日志处理。Runner 环境会按其标准 logging 配置处理这些记录。

限制：模块级 logger 使用固定名称 ``yesmai.astr``，不会自动携带具体插件 ID。
需要插件级名称或上下文信息时，应使用 ``self.ctx.logger``。
"""

from __future__ import annotations

import logging


logger = logging.getLogger("yesmai.astr")


__all__ = ["logger"]
