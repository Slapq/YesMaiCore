"""Owner-bound Cron declarations shared by native and Astr-style plugins."""

from __future__ import annotations

import calendar
import hashlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import Any

from maibot_sdk import API

from .core import AsyncCoreClient

_MONTH_NAMES = {name.lower(): index for index, name in enumerate(calendar.month_abbr) if name}
_WEEKDAY_NAMES = {name.lower(): index for index, name in enumerate(calendar.day_abbr)}


class CronUnsupportedError(RuntimeError):
    """The requested scheduler behavior is outside astr-calendar@1."""


def _atom(value: str, names: dict[str, int] | None) -> int:
    normalized = value.strip().lower()
    if names and normalized in names:
        return names[normalized]
    try:
        return int(normalized)
    except ValueError as exc:
        raise ValueError(f"无法解析 Cron 字段值：{value}") from exc


def _field(raw: Any, minimum: int, maximum: int, *, names: dict[str, int] | None = None) -> list[int]:
    if raw in (None, "*"):
        return list(range(minimum, maximum + 1))
    if isinstance(raw, int):
        values = {raw}
    elif isinstance(raw, (list, tuple, set, frozenset)):
        values: set[int] = set()
        for value in raw:
            values.update(_field(value, minimum, maximum, names=names))
    elif isinstance(raw, str):
        values = set()
        for part in raw.split(","):
            base, slash, raw_step = part.strip().partition("/")
            step = int(raw_step) if slash else 1
            if step <= 0:
                raise ValueError("Cron 步长必须大于 0")
            if base == "*":
                start, end = minimum, maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start, end = _atom(start_text, names), _atom(end_text, names)
                if start > end:
                    raise ValueError(f"Cron 不支持反向范围：{base}")
            else:
                start = end = _atom(base, names)
            values.update(range(start, end + 1, step))
    else:
        raise TypeError(f"Cron 字段类型无效：{type(raw).__name__}")
    if not values or min(values) < minimum or max(values) > maximum:
        raise ValueError(f"Cron 字段必须位于 {minimum}..{maximum}")
    return sorted(values)


def _weekday_field(raw: Any) -> list[int]:
    if raw in (None, "*"):
        return list(range(1, 8))
    values = _field(raw, 0, 6, names=_WEEKDAY_NAMES)
    return sorted({value + 1 for value in values})


@dataclass(frozen=True, slots=True)
class CronTrigger:
    """Minute-resolution subset of APScheduler's CronTrigger."""

    minute: Any = "*"
    hour: Any = "*"
    day: Any = "*"
    month: Any = "*"
    day_of_week: Any = "*"
    timezone: Any = None

    def __init__(
        self,
        year: Any = None,
        month: Any = None,
        day: Any = None,
        week: Any = None,
        day_of_week: Any = None,
        hour: Any = None,
        minute: Any = None,
        second: Any = None,
        start_date: Any = None,
        end_date: Any = None,
        timezone: Any = None,
        jitter: Any = None,
    ) -> None:
        unsupported = {
            "year": year,
            "week": week,
            "start_date": start_date,
            "end_date": end_date,
            "jitter": jitter,
        }
        used = [name for name, value in unsupported.items() if value is not None]
        if used:
            raise CronUnsupportedError("astr-calendar@1 暂不支持：" + ", ".join(used))
        if second not in (None, 0, "0"):
            raise CronUnsupportedError("astr-calendar@1 只支持 second=0")
        object.__setattr__(self, "minute", "*" if minute is None else minute)
        object.__setattr__(self, "hour", "*" if hour is None else hour)
        object.__setattr__(self, "day", "*" if day is None else day)
        object.__setattr__(self, "month", "*" if month is None else month)
        object.__setattr__(self, "day_of_week", "*" if day_of_week is None else day_of_week)
        object.__setattr__(self, "timezone", timezone)

    def to_schedule(self, default_timezone: str | None = None) -> dict[str, Any]:
        schedule = {
            "dialect": "astr-calendar@1",
            "minute": _field(self.minute, 0, 59),
            "hour": _field(self.hour, 0, 23),
            "day_of_month": _field(self.day, 1, 31),
            "month": _field(self.month, 1, 12, names=_MONTH_NAMES),
            "day_of_week": _weekday_field(self.day_of_week),
        }
        timezone_value = self.timezone if self.timezone is not None else default_timezone
        if timezone_value is not None:
            timezone_name = str(getattr(timezone_value, "key", timezone_value)).strip()
            if not timezone_name:
                raise ValueError("Cron timezone 不能为空")
            schedule["timezone"] = timezone_name
        return schedule


async def authorize_cron_run(instance: Any, run_id: str, token: str) -> None:
    result = await AsyncCoreClient(instance.ctx).cron.authorize(run_id, token)
    if not isinstance(result, dict) or result.get("ok") is not True:
        raise PermissionError("CRON_AUTHORIZATION_REJECTED")


def cron_job(
    stable_name: str,
    trigger: CronTrigger,
    *,
    description: str = "",
    misfire_grace_time: int = 60,
    coalesce: bool = True,
    max_instances: int = 1,
    timeout_seconds: int = 3600,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Declare a native owner-bound Cron handler."""

    normalized_name = str(stable_name or "").strip()
    if not normalized_name:
        raise ValueError("Cron stable_name 不能为空")
    if not isinstance(trigger, CronTrigger):
        raise TypeError("trigger 必须是 yesmai.CronTrigger")

    def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
        identity = f"{handler.__module__}:{handler.__qualname__}:{normalized_name}"
        api_name = "yesmai.cron." + hashlib.sha256(identity.encode()).hexdigest()[:16]

        @wraps(handler)
        async def endpoint(
            instance: Any,
            run_id: str = "",
            token: str = "",
            scheduled_for_utc: str = "",
            deadline_utc: str = "",
            idempotency_key: str = "",
            **kwargs: Any,
        ) -> dict[str, Any]:
            del scheduled_for_utc, deadline_utc, idempotency_key, kwargs
            try:
                await authorize_cron_run(instance, run_id, token)
            except Exception:
                return {
                    "ok": False,
                    "code": "CRON_AUTHORIZATION_REJECTED",
                    "message": "Cron 执行授权失败。",
                    "data": None,
                    "retryable": False,
                }
            try:
                result = handler(instance)
                if inspect.isasyncgen(result) or inspect.isgenerator(result):
                    raise CronUnsupportedError("Cron handler 不支持 generator")
                if inspect.isawaitable(result):
                    result = await result
            except Exception as exc:
                instance.ctx.logger.error("Cron handler %s 执行失败：%s", normalized_name, exc, exc_info=True)
                return {
                    "ok": False,
                    "code": "CRON_HANDLER_FAILED",
                    "message": "Cron handler 执行失败。",
                    "data": None,
                    "retryable": False,
                }
            return {"ok": True, "code": "OK", "message": "Cron handler 执行完成。", "data": None, "retryable": False}

        return API(
            api_name,
            description=description or f"YesMai Cron：{normalized_name}",
            version="1",
            public=True,
            timeout_ms=(int(timeout_seconds) + 5) * 1000,
            yesmai_protocol="cron.handler@1",
            stable_name=normalized_name,
            schedule=trigger.to_schedule(),
            execution={
                "misfire_grace_seconds": int(misfire_grace_time),
                "coalesce": bool(coalesce),
                "max_instances": int(max_instances),
                "timeout_seconds": int(timeout_seconds),
            },
        )(endpoint)

    return decorator


__all__ = ["CronTrigger", "CronUnsupportedError", "authorize_cron_run", "cron_job"]
