# YesMaiCore 0.1.3

## Changes in 0.1.3

- Temporarily disable `script.validate/compile/install@1` with stable `FEATURE_DISABLED` responses.
- Advertise `features.script=false` and remove the `yesmai-script` runtime dependency.
- Retain compatibility endpoints and dormant source without permitting compilation or installation.

## Changes in 0.1.2

- Add bounded YesMaiScript validate, compile, and durable install APIs.
- Require explicit restart instead of claiming hot discovery after script installation.
- Declare the managed `yesmai-script` runtime dependency.

## Changes in 0.1.1

- Add fenced long-stop Cron backlog compaction and schema v3 audit records.
- Remove unsupported remote plugin disable/unload advertising.
- Return structured model-config CAS conflicts without claiming Core stopped.
- Align current YesMai SDK, Astr listener, and formal plugin compatibility.

YesMaiCore is the YesMai runtime plugin for MaiBot. It provides versioned Core APIs for messaging, model tasks, history, rendering, chat resolution, permissions, and owner-bound Cron.

## Package Contents

This package contains only the Core plugin. It does not include runtime data, SQLite databases, Web instance identity files, credentials, or the YesMai SDK source package.

## Requirements

- MaiBot `1.0.0` through `1.99.99`
- MaiBot plugin SDK `2.7.0` through `2.99.99`
- Python packages declared in `_manifest.json`: `tomlkit>=0.13.3,<1.0.0` and `tzdata>=2024.1`
- YesMai SDK installed separately when required by dependent plugins

## Installation

Copy the extracted `yesmai-core/` directory into MaiBot's `plugins/` directory and restart or reload MaiBot through its supported plugin lifecycle. Review `config.toml` before enabling optional integrations.

YesMaiWeb integration is disabled by default with `web_url = ""`. Core does not create a Web instance identity or start a Web polling worker until a trusted HTTP(S) URL is configured.

## Security Boundaries

- Bot command administrators use exact `platform:user_id` entries under `[permission].command_admins`.
- Event or invoke `role="admin"` values are not trusted.
- Platform conversation owner/admin roles are a separate, currently unsupported permission domain.
- Cron does not claim exactly-once external side effects.

## License

GPL-3.0-only. See `LICENSE`.
