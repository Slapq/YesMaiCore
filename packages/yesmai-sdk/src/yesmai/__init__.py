"""YesMai 插件开发 API。"""

from .chain import MessageChain, MessageSegment
from .core import AsyncCoreClient, CoreUnavailableError, SyncCoreClient
from .cron import CronTrigger, CronUnsupportedError, cron_job
from .event import EventResult, YesMaiEvent
from .filter import filter
from .platform import PlatformCapabilities, PlatformInfo
from .plugin import YesMaiPlugin

__version__ = "0.1.3"

__all__ = [
    "AsyncCoreClient",
    "CoreUnavailableError",
    "CronTrigger",
    "CronUnsupportedError",
    "EventResult",
    "MessageChain",
    "MessageSegment",
    "PlatformCapabilities",
    "PlatformInfo",
    "SyncCoreClient",
    "YesMaiEvent",
    "YesMaiPlugin",
    "cron_job",
    "filter",
]
