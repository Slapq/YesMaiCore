"""平台信息与能力矩阵。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PlatformCapabilities:
    """某个平台的常见消息能力。未知平台采用保守默认值。"""

    text: bool = True
    image: bool = False
    emoji: bool = False
    forward: bool = False
    hybrid: bool = False
    custom: bool = False


_CAPABILITY_MATRIX: dict[str, PlatformCapabilities] = {
    "qq": PlatformCapabilities(text=True, image=True, emoji=True, forward=True, hybrid=True, custom=True),
    "discord": PlatformCapabilities(text=True, image=True, emoji=True, hybrid=True, custom=True),
    "telegram": PlatformCapabilities(text=True, image=True, emoji=True, hybrid=True),
    "kook": PlatformCapabilities(text=True, image=True, emoji=True, hybrid=True),
}


@dataclass(frozen=True, slots=True)
class PlatformInfo:
    """归一化的平台与会话信息。"""

    name: str = "unknown"
    stream_id: str = ""
    user_id: str = ""
    group_id: str = ""
    account_id: str = ""
    is_group: bool = False
    is_private: bool = False

    @property
    def capabilities(self) -> PlatformCapabilities:
        return _CAPABILITY_MATRIX.get(self.name, PlatformCapabilities())

    @classmethod
    def from_kwargs(cls, kwargs: dict[str, object]) -> PlatformInfo:
        message = kwargs.get("message")
        message_data = message if isinstance(message, dict) else {}
        message_info = message_data.get("message_info")
        message_info_data = message_info if isinstance(message_info, dict) else {}
        user_info = message_info_data.get("user_info")
        user_info_data = user_info if isinstance(user_info, dict) else {}
        group_info = message_info_data.get("group_info")
        group_info_data = group_info if isinstance(group_info, dict) else {}

        platform = str(kwargs.get("platform") or message_data.get("platform") or "unknown").strip().lower() or "unknown"
        group_id = str(kwargs.get("group_id") or group_info_data.get("group_id") or "")
        raw_is_group = kwargs.get("is_group", kwargs.get("is_group_message"))
        raw_is_private = kwargs.get("is_private", kwargs.get("is_private_message"))
        is_group = bool(raw_is_group) if raw_is_group is not None else bool(group_id)
        is_private = bool(raw_is_private) if raw_is_private is not None else not is_group
        return cls(
            name=platform,
            stream_id=str(kwargs.get("stream_id") or message_data.get("session_id") or ""),
            user_id=str(kwargs.get("user_id") or user_info_data.get("user_id") or ""),
            group_id=group_id,
            account_id=str(kwargs.get("account_id") or kwargs.get("self_id") or ""),
            is_group=is_group,
            is_private=is_private,
        )
