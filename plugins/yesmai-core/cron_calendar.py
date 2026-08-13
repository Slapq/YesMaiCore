"""Deterministic minute-resolution calendar schedules for YesMai Cron."""

from __future__ import annotations

import calendar
import importlib.metadata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC = timezone.utc
_MONTH_NAMES = {name.lower(): index for index, name in enumerate(calendar.month_abbr) if name}
_WEEKDAY_NAMES = {name.lower(): index + 1 for index, name in enumerate(calendar.day_abbr)}


class CronCalendarError(ValueError):
    """The calendar definition is invalid or cannot produce an occurrence."""


@dataclass(frozen=True, slots=True)
class CalendarOccurrence:
    scheduled_for_utc: datetime
    local_scheduled: datetime
    fold: int
    utc_offset_seconds: int
    timezone: str
    tzdata_version: str


@dataclass(frozen=True, slots=True)
class CalendarSchedule:
    minute: tuple[int, ...]
    hour: tuple[int, ...]
    day_of_month: tuple[int, ...]
    month: tuple[int, ...]
    day_of_week: tuple[int, ...]
    timezone: str

    def to_metadata(self) -> dict[str, Any]:
        return {
            "dialect": "astr-calendar@1",
            "minute": list(self.minute),
            "hour": list(self.hour),
            "day_of_month": list(self.day_of_month),
            "month": list(self.month),
            "day_of_week": list(self.day_of_week),
            "timezone": self.timezone,
        }


def _tzdata_version() -> str:
    try:
        return importlib.metadata.version("tzdata")
    except importlib.metadata.PackageNotFoundError:
        return "system"


def _parse_atom(value: str, names: dict[str, int] | None) -> int:
    normalized = value.strip().lower()
    if names and normalized in names:
        return names[normalized]
    try:
        return int(normalized)
    except ValueError as exc:
        raise CronCalendarError(f"无法解析日历字段值：{value}") from exc


def _expand_field(
    raw: Any,
    minimum: int,
    maximum: int,
    *,
    field_name: str,
    names: dict[str, int] | None = None,
) -> tuple[int, ...]:
    if raw in (None, "*"):
        return tuple(range(minimum, maximum + 1))
    if isinstance(raw, int):
        values = {raw}
    elif isinstance(raw, (list, tuple, set, frozenset)):
        values = set()
        for item in raw:
            values.update(_expand_field(item, minimum, maximum, field_name=field_name, names=names))
    elif isinstance(raw, str):
        values: set[int] = set()
        expression = raw.strip()
        if not expression:
            raise CronCalendarError(f"{field_name} 不能为空")
        for part in expression.split(","):
            base, separator, step_text = part.strip().partition("/")
            if separator:
                try:
                    step = int(step_text)
                except ValueError as exc:
                    raise CronCalendarError(f"{field_name} 步长无效：{step_text}") from exc
                if step <= 0:
                    raise CronCalendarError(f"{field_name} 步长必须大于 0")
            else:
                step = 1
            if base == "*":
                start, end = minimum, maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start = _parse_atom(start_text, names)
                end = _parse_atom(end_text, names)
                if start > end:
                    raise CronCalendarError(f"{field_name} 不支持反向范围：{base}")
            else:
                start = end = _parse_atom(base, names)
            values.update(range(start, end + 1, step))
    else:
        raise CronCalendarError(f"{field_name} 类型无效：{type(raw).__name__}")
    if not values:
        raise CronCalendarError(f"{field_name} 不能为空集合")
    invalid = sorted(value for value in values if value < minimum or value > maximum)
    if invalid:
        raise CronCalendarError(f"{field_name} 超出范围 {minimum}..{maximum}：{invalid}")
    return tuple(sorted(values))


def normalize_schedule(raw: dict[str, Any], *, default_timezone: str = "Asia/Shanghai") -> CalendarSchedule:
    if not isinstance(raw, dict):
        raise CronCalendarError("schedule 必须是字典")
    dialect = str(raw.get("dialect") or "astr-calendar@1").strip()
    if dialect != "astr-calendar@1":
        raise CronCalendarError(f"不支持的日历 dialect：{dialect}")
    timezone_name = str(raw.get("timezone") or default_timezone).strip()
    if not timezone_name:
        raise CronCalendarError("timezone 不能为空")
    try:
        ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise CronCalendarError(f"无效的 IANA timezone：{timezone_name}") from exc
    return CalendarSchedule(
        minute=_expand_field(raw.get("minute", "*"), 0, 59, field_name="minute"),
        hour=_expand_field(raw.get("hour", "*"), 0, 23, field_name="hour"),
        day_of_month=_expand_field(raw.get("day_of_month", raw.get("day", "*")), 1, 31, field_name="day_of_month"),
        month=_expand_field(raw.get("month", "*"), 1, 12, field_name="month", names=_MONTH_NAMES),
        day_of_week=_expand_field(raw.get("day_of_week", "*"), 1, 7, field_name="day_of_week", names=_WEEKDAY_NAMES),
        timezone=timezone_name,
    )


def resolve_local_time(local_time: datetime, timezone_name: str) -> list[CalendarOccurrence]:
    if local_time.tzinfo is not None:
        raise CronCalendarError("resolve_local_time 需要 naive local datetime")
    zone = ZoneInfo(timezone_name)
    resolved: dict[datetime, CalendarOccurrence] = {}
    tz_version = _tzdata_version()
    for fold in (0, 1):
        aware = local_time.replace(tzinfo=zone, fold=fold)
        instant = aware.astimezone(UTC)
        round_trip = instant.astimezone(zone)
        if round_trip.replace(tzinfo=None) != local_time or round_trip.fold != fold:
            continue
        offset = round_trip.utcoffset()
        occurrence = CalendarOccurrence(
            scheduled_for_utc=instant,
            local_scheduled=round_trip,
            fold=fold,
            utc_offset_seconds=int(offset.total_seconds()) if offset is not None else 0,
            timezone=timezone_name,
            tzdata_version=tz_version,
        )
        resolved[instant] = occurrence
    return [resolved[key] for key in sorted(resolved)]


def _matching_dates(schedule: CalendarSchedule, start_year: int) -> Iterable[tuple[int, int, int]]:
    for year in range(start_year, min(start_year + 400, 10000)):
        for month in schedule.month:
            _, days_in_month = calendar.monthrange(year, month)
            for day in schedule.day_of_month:
                if day > days_in_month:
                    continue
                if datetime(year, month, day).isoweekday() in schedule.day_of_week:
                    yield year, month, day


def next_occurrence(schedule: CalendarSchedule, after: datetime) -> CalendarOccurrence | None:
    if after.tzinfo is None:
        raise CronCalendarError("after 必须包含 timezone")
    after_utc = after.astimezone(UTC)
    zone = ZoneInfo(schedule.timezone)
    local_after = after_utc.astimezone(zone)
    local_floor = local_after.replace(second=0, microsecond=0, tzinfo=None)
    first_date = (local_after.year, local_after.month, local_after.day)
    current_resolutions = resolve_local_time(local_floor, schedule.timezone)
    inside_first_fold = local_after.fold == 0 and len(current_resolutions) == 2

    for year, month, day in _matching_dates(schedule, local_after.year):
        candidate_date = (year, month, day)
        if candidate_date < first_date:
            continue
        scan_entire_date = candidate_date == first_date and inside_first_fold
        best: CalendarOccurrence | None = None
        for hour in schedule.hour:
            for minute in schedule.minute:
                local_candidate = datetime(year, month, day, hour, minute)
                if candidate_date == first_date and not scan_entire_date and local_candidate <= local_floor:
                    continue
                for candidate in resolve_local_time(local_candidate, schedule.timezone):
                    if candidate.scheduled_for_utc <= after_utc:
                        continue
                    if not scan_entire_date:
                        return candidate
                    if best is None or candidate.scheduled_for_utc < best.scheduled_for_utc:
                        best = candidate
        if best is not None:
            return best
    return None


def _date_matches(schedule: CalendarSchedule, value: date) -> bool:
    return (
        value.month in schedule.month
        and value.day in schedule.day_of_month
        and value.isoweekday() in schedule.day_of_week
    )


@lru_cache(maxsize=128)
def _transition_dates(timezone_name: str, start_year: int) -> tuple[date, ...]:
    zone = ZoneInfo(timezone_name)
    cursor = date(start_year, 1, 1)
    end_year = min(start_year + 400, 10000)
    transitions: list[date] = []
    while cursor.year < end_year:
        try:
            following = cursor + timedelta(days=1)
        except OverflowError:
            break
        current_offset = datetime(cursor.year, cursor.month, cursor.day, tzinfo=zone).utcoffset()
        following_offset = datetime(following.year, following.month, following.day, tzinfo=zone).utcoffset()
        if current_offset != following_offset:
            transitions.append(cursor)
        cursor = following
    return tuple(transitions)


def _local_minimum_interval(schedule: CalendarSchedule) -> int:
    minutes = sorted({hour * 60 + minute for hour in schedule.hour for minute in schedule.minute})
    if len(minutes) == 1:
        return 86400
    gaps = [(right - left) * 60 for left, right in zip(minutes, minutes[1:], strict=False)]
    gaps.append((1440 - minutes[-1] + minutes[0]) * 60)
    return min(gaps)


def minimum_interval_seconds(schedule: CalendarSchedule, *, after: datetime | None = None) -> int | None:
    cursor = after or datetime(2024, 1, 1, tzinfo=UTC)
    first = next_occurrence(schedule, cursor)
    if first is None:
        return None

    minimum = _local_minimum_interval(schedule)
    if minimum <= 60:
        return minimum

    start_year = cursor.astimezone(ZoneInfo(schedule.timezone)).year
    candidate_dates: set[date] = set()
    for transition_date in _transition_dates(schedule.timezone, start_year):
        for offset in (-1, 0, 1):
            try:
                candidate = transition_date + timedelta(days=offset)
            except OverflowError:
                continue
            if _date_matches(schedule, candidate):
                candidate_dates.add(candidate)

    instants: list[datetime] = []
    for candidate_date in sorted(candidate_dates):
        for hour in schedule.hour:
            for minute in schedule.minute:
                local_candidate = datetime(
                    candidate_date.year,
                    candidate_date.month,
                    candidate_date.day,
                    hour,
                    minute,
                )
                instants.extend(
                    occurrence.scheduled_for_utc
                    for occurrence in resolve_local_time(local_candidate, schedule.timezone)
                )
    ordered = sorted(set(instants))
    for left, right in zip(ordered, ordered[1:], strict=False):
        interval = int((right - left).total_seconds())
        if 0 < interval < minimum:
            minimum = interval
    return minimum


__all__ = [
    "CalendarOccurrence",
    "CalendarSchedule",
    "CronCalendarError",
    "minimum_interval_seconds",
    "next_occurrence",
    "normalize_schedule",
    "resolve_local_time",
]
