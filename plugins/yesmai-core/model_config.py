"""YesMaiCore 模型配置 MVP。

直接管理 MaiBot 的 ``config/model_config.toml``，提供读取、校验、递归补丁和
单备份恢复。该模块不修改 Host 内存；提交后由 MaiBot 文件 watcher 完成热重载。
"""

from __future__ import annotations

import copy
import importlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable

import tomlkit

Validator = Callable[[dict[str, Any]], None]
_SENSITIVE_KEYS = frozenset({"api_key", "token", "secret", "password", "authorization", "cookie", "session_token"})


class ModelConfigError(RuntimeError):
    """带稳定错误码的模型配置操作错误。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _redact(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for item_key, item_value in value.items():
            normalized_key = str(item_key).strip().lower()
            if normalized_key in _SENSITIVE_KEYS:
                raw = str(item_value or "")
                redacted[str(item_key)] = "***" if raw else ""
                if normalized_key == "api_key":
                    redacted["has_api_key"] = bool(raw)
            else:
                redacted[str(item_key)] = _redact(item_value, str(item_key))
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if key.strip().lower() in _SENSITIVE_KEYS:
        return "***" if str(value or "") else ""
    return value


def _has_sensitive_value(value: Any, key: str = "") -> bool:
    normalized_key = key.strip().lower()
    if normalized_key in _SENSITIVE_KEYS:
        return bool(str(value or ""))
    if isinstance(value, dict):
        return any(_has_sensitive_value(item, str(item_key)) for item_key, item in value.items())
    if isinstance(value, list):
        return any(_has_sensitive_value(item) for item in value)
    return False


def _directory_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _merge_patch(target: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(target)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_patch(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _default_validator(config: dict[str, Any]) -> None:
    """使用当前 MaiBot 内部模型校验器；不可用时明确失败。"""

    try:
        config_module = importlib.import_module("src.config.config")
        base_module = importlib.import_module("src.config.config_base")
        model_class = getattr(config_module, "ModelConfig")
        attribute_data = getattr(base_module, "AttributeData")
    except Exception as exc:
        raise ModelConfigError(
            "MODEL_CONFIG_INTERNAL_UNAVAILABLE", f"当前 MaiBot 模型配置校验器不可用：{exc}"
        ) from exc
    try:
        model_class.from_dict(attribute_data(), copy.deepcopy(config))
    except Exception as exc:
        raise ModelConfigError("MODEL_CONFIG_INVALID", f"模型配置校验失败：{exc}") from exc


def discover_model_config_path() -> Path:
    """零配置发现 MaiBot 模型配置，只接受固定的标准相对路径。"""

    plugin_root = Path(__file__).resolve().parent
    candidates = [
        Path.cwd().resolve() / "config" / "model_config.toml",
        plugin_root.parent.parent.resolve() / "config" / "model_config.toml",
    ]
    for candidate in candidates:
        if candidate.is_file() and not candidate.is_symlink():
            return candidate.resolve()
    return candidates[0].resolve()


class ModelConfigManager:
    """模型配置文件的同步事务管理器；调用方应通过 ``asyncio.to_thread`` 使用。"""

    def __init__(self, path: Path | None = None, validator: Validator | None = None) -> None:
        self.path = (path or discover_model_config_path()).resolve()
        self.backup_path = self.path.with_name(f"{self.path.name}.yesmai.bak")
        self._validator = validator or _default_validator

    def _read(self, path: Path | None = None) -> dict[str, Any]:
        target = (path or self.path).resolve()
        if not target.is_file() or target.is_symlink():
            code = "MODEL_CONFIG_BACKUP_NOT_FOUND" if target == self.backup_path else "MODEL_CONFIG_NOT_FOUND"
            message = "没有可恢复的 YesMai 模型配置备份。" if target == self.backup_path else "未找到 MaiBot 模型配置文件。"
            raise ModelConfigError(code, message)
        try:
            document = tomlkit.parse(target.read_text(encoding="utf-8"))
            data = document.unwrap()
        except Exception as exc:
            raise ModelConfigError("MODEL_CONFIG_READ_FAILED", f"读取模型配置失败：{exc}") from exc
        if not isinstance(data, dict):
            raise ModelConfigError("MODEL_CONFIG_READ_FAILED", "模型配置顶层必须是表结构。")
        return data

    def get_plaintext(self) -> dict[str, Any]:
        return copy.deepcopy(self._read())

    def get_redacted(self) -> dict[str, Any]:
        return _redact(self._read())

    def get_directory(self) -> dict[str, Any]:
        """Project model configuration into a credential-free read-only directory."""

        config = self._read()
        raw_providers = config.get("api_providers", [])
        raw_models = config.get("models", [])
        raw_tasks = config.get("model_task_config", {})
        if not isinstance(raw_providers, list) or not isinstance(raw_models, list) or not isinstance(raw_tasks, dict):
            raise ModelConfigError(
                "MODEL_DIRECTORY_INVALID",
                "模型目录要求 api_providers/models 为列表且 model_task_config 为对象。",
            )

        warnings: list[dict[str, str]] = []
        providers: list[dict[str, Any]] = []
        provider_ids: set[str] = set()
        for index, raw_provider in enumerate(raw_providers):
            if not isinstance(raw_provider, dict):
                warnings.append({"code": "PROVIDER_INVALID", "reference": str(index)})
                continue
            provider_id = str(raw_provider.get("name") or "").strip()
            if not provider_id:
                warnings.append({"code": "PROVIDER_ID_MISSING", "reference": str(index)})
                continue
            provider_ids.add(provider_id)
            providers.append(
                {
                    "id": provider_id,
                    "id_source": "config_name",
                    "stable": False,
                    "client_type": str(raw_provider.get("client_type") or ""),
                    "auth_type": str(raw_provider.get("auth_type") or ""),
                    "has_credentials": _has_sensitive_value(raw_provider),
                }
            )

        models: list[dict[str, Any]] = []
        model_ids: set[str] = set()
        for index, raw_model in enumerate(raw_models):
            if not isinstance(raw_model, dict):
                warnings.append({"code": "MODEL_INVALID", "reference": str(index)})
                continue
            model_id = str(raw_model.get("name") or "").strip()
            if not model_id:
                warnings.append({"code": "MODEL_ID_MISSING", "reference": str(index)})
                continue
            provider_id = str(raw_model.get("api_provider") or "").strip()
            if provider_id and provider_id not in provider_ids:
                warnings.append({"code": "MODEL_PROVIDER_NOT_FOUND", "reference": f"{model_id}:{provider_id}"})
            model_ids.add(model_id)
            models.append(
                {
                    "id": model_id,
                    "id_source": "config_name",
                    "stable": False,
                    "model_identifier": str(raw_model.get("model_identifier") or ""),
                    "provider_id": provider_id,
                    "visual": bool(raw_model.get("visual", False)),
                    "force_stream_mode": bool(raw_model.get("force_stream_mode", False)),
                    "cache": bool(raw_model.get("cache", False)),
                }
            )

        tasks: list[dict[str, Any]] = []
        for task_id, raw_task in raw_tasks.items():
            normalized_task_id = str(task_id).strip()
            if not normalized_task_id or not isinstance(raw_task, dict):
                warnings.append({"code": "TASK_INVALID", "reference": str(task_id)})
                continue
            raw_model_ids = raw_task.get("model_list", [])
            if not isinstance(raw_model_ids, list):
                warnings.append({"code": "TASK_MODEL_LIST_INVALID", "reference": normalized_task_id})
                raw_model_ids = []
            task_model_ids = [str(item).strip() for item in raw_model_ids if str(item).strip()]
            for model_id in task_model_ids:
                if model_id not in model_ids:
                    warnings.append({"code": "TASK_MODEL_NOT_FOUND", "reference": f"{normalized_task_id}:{model_id}"})
            tasks.append(
                {
                    "id": normalized_task_id,
                    "id_source": "config_field",
                    "stable": False,
                    "model_ids": task_model_ids,
                    "max_tokens": _directory_scalar(raw_task.get("max_tokens")),
                    "temperature": _directory_scalar(raw_task.get("temperature")),
                    "slow_threshold": _directory_scalar(raw_task.get("slow_threshold")),
                    "selection_strategy": str(raw_task.get("selection_strategy") or ""),
                    "hard_timeout": _directory_scalar(raw_task.get("hard_timeout")),
                }
            )

        inner = config.get("inner") if isinstance(config.get("inner"), dict) else {}
        return {
            "schema_version": 1,
            "source": "config/model_config.toml",
            "config_version": str(inner.get("version") or ""),
            "consistent_with_host_runtime": None,
            "providers": providers,
            "models": models,
            "tasks": tasks,
            "warnings": warnings,
        }

    def validate(self, candidate: Any) -> None:
        if not isinstance(candidate, dict):
            raise ModelConfigError("MODEL_CONFIG_INVALID", "模型配置必须是对象。")
        try:
            self._validator(copy.deepcopy(candidate))
        except ModelConfigError:
            raise
        except Exception as exc:
            raise ModelConfigError("MODEL_CONFIG_INVALID", f"模型配置校验失败：{exc}") from exc

    @staticmethod
    def _reject_reserved_patch(patch: dict[str, Any]) -> None:
        inner = patch.get("inner")
        if isinstance(inner, dict) and "version" in inner:
            raise ModelConfigError("MODEL_CONFIG_PATCH_INVALID", "不允许通过补丁修改 inner.version。")

    def patch(self, patch: Any) -> dict[str, Any]:
        if not isinstance(patch, dict) or not patch:
            raise ModelConfigError("MODEL_CONFIG_PATCH_INVALID", "模型配置补丁必须是非空对象。")
        self._reject_reserved_patch(patch)
        current = self._read()
        candidate = _merge_patch(current, patch)
        self.validate(candidate)
        try:
            self.backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.path, self.backup_path)
            self._atomic_write(candidate)
        except ModelConfigError:
            raise
        except Exception as exc:
            raise ModelConfigError("MODEL_CONFIG_WRITE_FAILED", f"写入模型配置失败：{exc}") from exc
        return _redact(candidate)

    def restore(self) -> dict[str, Any]:
        candidate = self._read(self.backup_path)
        try:
            self.validate(candidate)
            self._atomic_write(candidate)
        except ModelConfigError as exc:
            if exc.code == "MODEL_CONFIG_BACKUP_NOT_FOUND":
                raise
            raise ModelConfigError("MODEL_CONFIG_RESTORE_FAILED", exc.message) from exc
        except Exception as exc:
            raise ModelConfigError("MODEL_CONFIG_RESTORE_FAILED", f"恢复模型配置失败：{exc}") from exc
        return _redact(candidate)

    def _atomic_write(self, config: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self.path.name}.yesmai-",
                suffix=".tmp",
                dir=self.path.parent,
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(tomlkit.dumps(config))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self.path)
            temporary_path = None
        except Exception as exc:
            raise ModelConfigError("MODEL_CONFIG_WRITE_FAILED", f"原子写入模型配置失败：{exc}") from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)


__all__ = ["ModelConfigError", "ModelConfigManager", "discover_model_config_path"]
