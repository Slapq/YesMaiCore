# yesmai-sdk

`yesmai-sdk` 是 YesMai 面向 MaiBot 插件开发者的轻量包装层。

## 安装

推荐由 [YesMaiInstaller](https://github.com/Slapq/YesMaiInstaller) 自动安装与 Core 匹配的 SDK。开发环境也可以从 `YesMaiCore` checkout 安装：

```bash
python -m pip install ./packages/yesmai-sdk
```

要求 Python `>=3.10`、MaiBot Plugin SDK `>=2.7.0,<3.0.0`。业务插件仍需在 Manifest 中硬依赖 `com.yesmai.core`；单独安装 SDK 不会提供或伪装 Core 运行时。

许可证：GPL-3.0-only，见 `LICENSE`。

## 原生示例

```python
from yesmai import YesMaiPlugin, filter

class HelloPlugin(YesMaiPlugin):
    @filter.command("hello", pattern=r"^/hello$")
    async def hello(self, event):
        return event.plain_result("你好！")
```

它不绕过 MaiBot Runner，也不会把 Python 回调传给 YesMaiCore。装饰器最终仍生成 MaiBot SDK 能识别的标准组件声明。原生 `YesMaiEvent.send()`、`YesMaiPlugin.send()` 与 Astr `event.send()` 均通过 `com.yesmai.core.send.*@1`，普通插件不会直接调用业务型 `ctx.send.*`。

## AstrBot 风格严格兼容入口

```python
from yesmai.astr import AstrMessageEvent, Star, filter

class GroupHello(Star):
    @filter.command("hello")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.permission_type(filter.PermissionType.MEMBER)
    def hello(self, event: AstrMessageEvent):
        return event.plain_result("群聊你好！")
```

当前过滤器第一阶段支持与 `@filter.command` 组合，也支持独立同步消息监听器。多个过滤器使用 AND 逻辑。支持 `EventMessageType`、`PlatformAdapterType` Flag 组合和 `PermissionType.MEMBER`。

独立监听器示例：

```python
class Listener(Star):
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    def on_group(self, event: AstrMessageEvent):
        return event.plain_result(f"收到：{event.message_str}")
```

SDK 会自动把该方法注册为 `astr.listener@1` 动态 public API，由 YesMaiCore 的 Observe Hook 调用。返回文本或消息链会由 Core 统一送入 MaiBot SendService；`event.stop_event()` 会停止同一消息剩余的 YesMai listener，但不会中止 MaiBot 主消息链。

当前处理器支持同步普通函数、async coroutine 和 async generator/yield。同步处理器在线程执行；async 处理器运行于 Runner 事件循环。async generator 的多次 yield 会按顺序通过 YesMaiCore 发送。同步 generator 暂不支持并会明确报错。

兼容工具包括 `CommandParserMixin`、`CommandTokens`、`StarTools.get_data_dir()`、`Image.fromBytes()` / `Image.fromIO()`，以及 AstrBot 风格的 `EventResultType`、`ResultContentType` 和 MessageChain 元数据/派生辅助方法。`StarTools.get_data_dir()` 零配置使用 MaiBot 授予的本插件持久化目录，仅在插件生命周期或处理器执行期间可用，不允许访问其他插件目录。

`Star.config` 是可写的 `AstrBotConfig` facade，兼容真实 AstrBot 插件在构造函数中执行 `self.config = config or {}`。MaiBot Runner 注入或热更新配置时会原地更新同一对象；`self.context.get_config()` 返回该 facade。配置持久化写回仍明确不支持，不伪造成功。

`await self.context.llm_generate(prompt=..., task="utils")` 提供 task-only、非流式 LLM 映射并返回 `LLMResponse` 快照。Astr Provider ID、Provider 活对象、会话 Provider、流式、tools/system/context 和 response_format 不会被猜测或伪造。原生和 Astr 插件也可使用 `core.model.directory.get()` 查询脱敏模型目录、使用 `core.chat.resolve()` 严格解析已存在 Host stream。

`filter.llm_tool` 会注册为 MaiBot 原生 Tool；Tool 内可通过 AstrBot 常见签名 `await self.html_render(template, data, return_url=True, options=...)` 在兼容层完成 Jinja2 渲染，再由 YesMaiCore 透明代理 MaiBot 原生 HTML 转图片能力。返回的 Data URI 可直接用于 `event.image_result()`。Tool 的 async generator 若 yield 媒体结果，会通过当前会话的 Core SendService 发送；只有 Core 确认发送成功后，才向 Maisaka 返回媒体已发送状态。`allow_network` 等 Host 渲染参数由下游插件明确选择，Core 不强制覆盖。

## Owner-Bound Cron

原生插件可用 `CronTrigger` 与 `@cron_job(...)` 声明自己的 Job；Astr 插件使用受限 facade：

```python
from yesmai.astr import CronTrigger, Star

class DailyFixture(Star):
    async def initialize(self):
        self.context.cron_manager.scheduler.add_job(
            self.run_daily,
            CronTrigger(hour=9, minute=0, timezone="Asia/Shanghai"),
            id="daily",
            replace_existing=True,
            misfire_grace_time=60,
            coalesce=True,
            max_instances=1,
        )

    async def run_daily(self):
        ...
```

支持 `add_job/get_job/get_jobs/remove_job/remove_all_jobs`。Job 必须有稳定 `id`；第一阶段只接受 `CronTrigger`、minute resolution 和 `second=0`。不支持 year/week/start_date/end_date/jitter、任意 jobstore、interval/date trigger 或 APScheduler 活对象；这些请求会明确抛 `CronUnsupportedError`。

SDK 把 callable 保留在 owner Runner 内，只向 Host 发布结构化 `cron.handler@1` API。每次执行必须先由 wrapper 消费 Core 一次性 token；授权失败不会进入 callable。生命周期、Command/listener/Tool handler 中的 Job 变更按 batch staged，并只在正常结束后同步动态 catalog。

严格限制：

- 同步 generator 暂不支持；
- `PermissionType.ADMIN` 只匹配 YesMaiCore 配置的 `platform:user_id` Bot 命令管理员；普通事件/invoke 中的 `role` 不参与授权，Core 不可用时 fail-closed；
- 平台会话群主/群管理员当前没有可靠 Host 身份字段，不会自动获得上述命令权限；
- `telegram`、`discord`、`kook` 等明确平台可匹配；不会把 `qq` 或 `napcat` 猜测为 AIOCQHTTP/QQOFFICIAL；
- Command 过滤失败不会执行处理器或调用 Core，但 MaiBot Host 已选中该 Command，不会回退尝试其他同名 Command，并可能继续普通消息处理。
