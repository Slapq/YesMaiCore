# YesMaiCore

YesMaiCore 是 YesMai 系列的能力中枢与接口核心，负责以几乎零配置的稳定协议代理 MaiBot Host 能力。

## 当前能力

- `com.yesmai.core.health@1`：核心状态与协议版本。
- `com.yesmai.core.capabilities@1`：平台能力矩阵。
- `com.yesmai.core.config.get@1`：集中配置读取协议；远程服务未启用时返回调用方默认值。
- `com.yesmai.core.send.text@1` / `send.chain@1`：统一经 MaiBot SendService 发送文本和消息链。
- `com.yesmai.core.llm.generate@1`：调用 MaiBot 模型任务配置，并提供稳定 `text` / `model` / `task` 字段；推荐用 `task="utils"` 执行总结整理。
- `com.yesmai.core.chat.resolve@1`：按平台、会话类型、目标、账号和 scope 严格解析已存在 Host stream；歧义时拒绝，不创建会话。
- `com.yesmai.core.permission.resolve@1`：按 Core 配置精确解析 `platform:user_id` Bot 命令管理员；不信任调用方 role。
- `com.yesmai.core.message.recent@1`：查询聊天流最近消息，支持时间窗口、数量、过滤和二进制参数，不额外缩小 Host 查询范围。
- `com.yesmai.core.message.by_time@1`：按时间范围查询聊天流消息，并补充稳定 `sender` / `text` 兼容字段。
- `com.yesmai.core.person.resolve@1`：解析平台用户对应的 MaiBot 人物信息。
- `com.yesmai.core.model.config.get@1`：零配置读取并脱敏返回 MaiBot 模型配置。
- `com.yesmai.core.model.directory.get@1`：返回严格脱敏、只读的 Provider/Model/Task 引用目录；不承诺与 Host 内存强一致。
- `com.yesmai.core.model.config.validate@1`：用当前 MaiBot 模型规则验证候选配置。
- `com.yesmai.core.model.config.patch@1`：递归更新模型配置，自动创建单份备份并原子提交。
- `com.yesmai.core.model.config.restore@1`：恢复最近一次 YesMai 模型配置备份。
- `com.yesmai.core.leaderboard.list@1`：排行榜读取协议；远程服务未启用时返回空榜单。
- `com.yesmai.core.cron.execution.authorize@1`：原子消费 Core 签发的一次性 Cron token；只供 owner wrapper 使用。
- `com.yesmai.core.cron.status@1`：返回 leader、Job/Occurrence 状态计数和服务状态，不返回 token 或业务 payload。
- `com.yesmai.core.render.html2png@1`：透明代理 MaiBot 原生 HTML 转 PNG 能力，支持下游选择 `allow_network` 等 Host 参数。

## 安装

将 `plugins/yesmai-core/` 复制到 MaiBot 的 `plugins/` 目录。基础能力不需要用户配置，模型配置路径会自动发现。`web_url` 默认为空，此时 Core 不创建实例身份，也不产生任何 YesMaiWeb 网络请求。

## Bot 命令管理员

Astr `PermissionType.ADMIN` 使用 Core 配置的 Bot 命令管理员：

```toml
[permission]
command_admins = ["qq:123456789"]
```

platform 归一化为小写，user ID 原样精确匹配。同一 ID 不跨平台继承。普通事件或 invoke 参数中的 `role="admin"`、平台群管理员、MaiBot 插件操作者、Web 管理员和发布用户均不会自动获得该权限。Core 查询异常时 SDK fail-closed。

MaiBot 1.1.3 尚无可信统一的平台会话成员角色能力，所以平台群主/群管理员自动授权当前不支持。

## Owner-Bound Cron

Core 从 Host API registry 发现 metadata 协议为 `cron.handler@1` 的 public owner API。owner 只取 Runner/Manifest 绑定的 registry 字段，不接受业务参数自报。Schedule 使用 minute-resolution `astr-calendar@1`、日期字段 AND、IANA timezone；默认 `Asia/Shanghai`。DST gap 跳过，fold 生成两个不同 UTC Occurrence。

状态持久化于插件数据目录的 `cron-v1.sqlite3`，包括 Job revision、Occurrence、Run Attempt、leader lease 和 fencing epoch。默认限制为每 owner 32 Job、全局 256 Job、最短 60 秒、每 owner 4 个 dispatch。未消费 token 到期后可重新尝试；token 已授权后的 timeout 或断连写 `UNKNOWN`，同一 Occurrence 不自动重试。该语义不承诺 exactly-once，也不能强制取消 Runner 中已经开始的 callable。

配置位于 `[cron]`：

```toml
[cron]
enabled = true
default_timezone = "Asia/Shanghai"
catalog_refresh_seconds = 5.0
owner_job_limit = 32
global_job_limit = 256
owner_dispatch_limit = 4
minimum_interval_seconds = 60
maximum_timeout_seconds = 7200
```

Core 不提供通用 `cron.create/update/cancel/trigger`，也不持久化 Python callable。日分析和真实消息发送不属于当前阶段。

## YesMaiWeb 实例通道

设置 `web_url` 后，Core 在插件数据目录持久保存 `web-instance.json`，主动向 YesMaiWeb 注册、上报能力并轮询控制任务。可用 `web_poll_interval_seconds` 调整空闲轮询间隔。身份文件损坏时 Core 明确报错，不会静默生成新 UUID。`config.get@1` 从 Web 读取按插件 ID 隔离的实例配置，并将最近成功版本明文缓存到 `instance-config-cache.json`；Web 暂不可用时返回该缓存并标记 stale，不影响消息、LLM、渲染等本地 Core 能力。

当前 Core operation：

- `device.info.get@1`：读取 Core/Python/平台和实例通道状态；
- `model.config.get@1`：向已授权 Web 实例控制返回完整明文模型配置；
- `model.config.validate@1`：校验候选模型配置；
- `model.config.patch@1`：校验、备份并原子更新模型配置，结果返回明文配置；
- `model.config.restore@1`：恢复最近备份，结果返回明文配置；
- `plugin.status.get@1`：读取 Host 当前已加载和已注册插件状态；
- `plugin.enable@1` / `plugin.reload@1`：调用 Host 原生插件加载和重载能力。MaiBot 新运行时不支持单独卸载插件，因此 Core 不广告 `plugin.disable@1`；Core 不允许通过自身任务自重载。

现有本地 public API `com.yesmai.core.model.config.get@1` 仍返回脱敏配置，不因 Web operation 改变语义。

## 设计说明

Core 的公共 API 只接受和返回可被 msgpack 序列化的数据，不跨插件传递 Python 回调、类实例或装饰器。正式 YesMai 功能插件必须硬依赖 Core；高级能力不可绕过 Core 直接操作宿主。Core 不替下游插件强制内容审查、网络控制或资源配额；需要这些策略时应安装独立可选插件。

模型配置 MVP 会隐藏 API Key 等敏感值，并依赖 MaiBot 文件 watcher 自动热重载。当前只保留一份 `.yesmai.bak`，不等待 Host 返回最终重载结果。ADR 0005 的远程控制语义要求后续为已授权实例操作者提供完整明文模型配置读取；公开目录 API 和未授权用户不得访问该能力。

## License

GPL-3.0-only，见 `LICENSE`。
