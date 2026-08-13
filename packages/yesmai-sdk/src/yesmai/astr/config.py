"""AstrBot 常用配置对象的受限兼容实现。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class AstrConfigPersistenceUnsupportedError(RuntimeError):
    """MaiBot 普通插件尚无 AstrBot 配置写回能力。"""


class AstrBotConfig(dict[str, Any]):
    """AstrBotConfig 的字典兼容面；配置持久化明确不伪成功。"""

    def __getattr__(self, key: str) -> Any:
        return self.get(key)

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            object.__setattr__(self, key, value)
        else:
            self[key] = value

    def replace(self, data: Mapping[str, Any]) -> None:
        self.clear()
        self.update(dict(data))

    def save_config(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AstrConfigPersistenceUnsupportedError(
            "当前 MaiBot SDK 未向普通插件开放 AstrBotConfig.save_config() 配置写回能力。"
        )

    async def save_config_async(self, *args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        raise AstrConfigPersistenceUnsupportedError(
            "当前 MaiBot SDK 未向普通插件开放 AstrBotConfig.save_config_async() 配置写回能力。"
        )


__all__ = ["AstrBotConfig", "AstrConfigPersistenceUnsupportedError"]
