"""类似 AstrBot 的简洁组件装饰器。"""

from __future__ import annotations

from functools import wraps
from inspect import isawaitable
from typing import Any, Callable, Iterable

from maibot_sdk import Command, EventHandler
from maibot_sdk.types import EventType

from .event import EventResult, YesMaiEvent

Handler = Callable[..., Any]


async def _invoke(handler: Handler, instance: object, event: YesMaiEvent) -> Any:
    result = handler(instance, event)
    if isawaitable(result):
        return await result
    return result


def _normalize_platforms(platforms: str | Iterable[str] | None) -> frozenset[str]:
    if platforms is None:
        return frozenset()
    values = [platforms] if isinstance(platforms, str) else list(platforms)
    return frozenset(str(value).strip().lower() for value in values if str(value).strip())


class _Filter:
    """生成 MaiBot 标准组件声明，并在调用时注入 :class:`YesMaiEvent`。"""

    def command(
        self,
        name: str,
        *,
        pattern: str = "",
        aliases: list[str] | None = None,
        description: str = "",
        platforms: str | Iterable[str] | None = None,
        intercept: bool = False,
    ) -> Callable[[Handler], Handler]:
        allowed_platforms = _normalize_platforms(platforms)

        def decorator(handler: Handler) -> Handler:
            @wraps(handler)
            async def wrapped(instance: object, **kwargs: Any) -> Any:
                event = YesMaiEvent.from_kwargs(
                    instance.ctx,
                    component_type="COMMAND",
                    component_name=name,
                    kwargs=kwargs,
                )
                if allowed_platforms and event.platform.name not in allowed_platforms:
                    return False, "", False
                result = await _invoke(handler, instance, event)
                if isinstance(result, EventResult):
                    if not result.chain:
                        return True, "", intercept
                    if all(segment.type == "text" for segment in result.chain.segments):
                        return True, result.chain.plain_text(), intercept
                    await event.send(result.chain)
                    return True, "", intercept
                if isinstance(result, str):
                    return True, result, intercept
                if result is None:
                    return True, "", intercept
                return result

            metadata = {"yesmai": True, "platforms": sorted(allowed_platforms)}
            return Command(
                name,
                description=description,
                pattern=pattern,
                aliases=aliases,
                **metadata,
            )(wrapped)

        return decorator

    def event(
        self,
        name: str = "",
        *,
        event_type: EventType = EventType.ON_MESSAGE,
        description: str = "",
        intercept_message: bool = False,
        weight: int = 0,
        platforms: str | Iterable[str] | None = None,
    ) -> Callable[[Handler], Handler]:
        allowed_platforms = _normalize_platforms(platforms)

        def decorator(handler: Handler) -> Handler:
            component_name = name or handler.__name__

            @wraps(handler)
            async def wrapped(instance: object, **kwargs: Any) -> Any:
                event = YesMaiEvent.from_kwargs(
                    instance.ctx,
                    component_type="EVENT_HANDLER",
                    component_name=component_name,
                    kwargs=kwargs,
                )
                if allowed_platforms and event.platform.name not in allowed_platforms:
                    return None
                result = await _invoke(handler, instance, event)
                if not isinstance(result, EventResult):
                    return result
                if result.chain:
                    await event.send(result.chain)
                if result.blocked or not result.continue_processing:
                    return {
                        "blocked": result.blocked,
                        "continue_processing": result.continue_processing,
                        "custom_result": result.custom_result,
                    }
                return None

            metadata = {"yesmai": True, "platforms": sorted(allowed_platforms)}
            return EventHandler(
                component_name,
                description=description,
                event_type=event_type,
                intercept_message=intercept_message,
                weight=weight,
                **metadata,
            )(wrapped)

        return decorator


filter = _Filter()
