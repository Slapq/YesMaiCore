"""Bounded YesMaiScript validation, compilation, and durable installation."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

_MAX_SOURCE_BYTES = 256 * 1024
_MAX_COMPILED_BYTES = 512 * 1024


class ScriptServiceError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ScriptService:
    def __init__(self, plugins_root: Path, data_dir: Path) -> None:
        self.plugins_root = Path(plugins_root).resolve()
        self.data_dir = Path(data_dir).resolve()
        self._lock = asyncio.Lock()
        self._lock_path = self.data_dir / "script-install.lock"
        self._backup_root = self.data_dir / "script-backups"
        self._state_path = self.data_dir / "script-installs.json"

    @staticmethod
    def _compiler() -> Any:
        try:
            from yesmai_script import YesMaiScriptCompiler, YesMaiScriptError
        except ImportError as exc:
            raise ScriptServiceError(
                "SCRIPT_RUNTIME_UNAVAILABLE",
                "yesmai-script is not installed; install the declared Core dependency and restart MaiBot",
            ) from exc
        return YesMaiScriptCompiler(), YesMaiScriptError

    @staticmethod
    def _bounded_source(source: Any) -> str:
        content = str(source or "")
        if not content.strip():
            raise ScriptServiceError("SCRIPT_SOURCE_REQUIRED", "YesMaiScript source is required")
        if len(content.encode("utf-8")) > _MAX_SOURCE_BYTES:
            raise ScriptServiceError("SCRIPT_SOURCE_TOO_LARGE", "YesMaiScript source exceeds 256 KiB")
        return content

    def validate(self, source: Any) -> dict[str, Any]:
        content = self._bounded_source(source)
        compiler, script_error = self._compiler()
        try:
            model = compiler.parser.parse_string(content, source="<core-api>")
        except script_error as exc:
            raise ScriptServiceError("SCRIPT_INVALID", str(exc)) from exc
        return {
            "valid": True,
            "plugin_id": model["plugin"]["id"],
            "version": model["plugin"]["version"],
            "commands": [command["name"] for command in model["commands"]],
        }

    def compile(self, source: Any) -> dict[str, Any]:
        content = self._bounded_source(source)
        compiler, script_error = self._compiler()
        try:
            compiled = compiler.compile_string(content, source="<core-api>")
        except script_error as exc:
            raise ScriptServiceError("SCRIPT_INVALID", str(exc)) from exc
        total_size = sum(len(value.encode("utf-8")) for value in compiled.files.values())
        if total_size > _MAX_COMPILED_BYTES:
            raise ScriptServiceError("SCRIPT_COMPILED_TOO_LARGE", "compiled plugin exceeds 512 KiB")
        try:
            compile(compiled.files["plugin.py"], "plugin.py", "exec")
            manifest = json.loads(compiled.files["_manifest.json"])
        except (KeyError, SyntaxError, json.JSONDecodeError) as exc:
            raise ScriptServiceError("SCRIPT_COMPILE_FAILED", f"compiled plugin is invalid: {exc}") from exc
        if manifest.get("id") != compiled.plugin_id:
            raise ScriptServiceError("SCRIPT_COMPILE_FAILED", "compiled Manifest identity mismatch")
        return {
            "plugin_id": compiled.plugin_id,
            "directory_name": compiled.directory_name,
            "files": dict(compiled.files),
            "total_bytes": total_size,
        }

    async def install(self, source: Any, *, replace: bool = False) -> dict[str, Any]:
        compiled = self.compile(source)
        async with self._lock:
            return await asyncio.to_thread(self._install_sync, compiled, bool(replace))

    def _install_sync(self, compiled: dict[str, Any], replace: bool) -> dict[str, Any]:
        self.plugins_root.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        descriptor: int | None = None
        try:
            descriptor = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ScriptServiceError("SCRIPT_INSTALL_BUSY", "another script installation is active") from exc
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = None
                handle.write(str(os.getpid()))
            return self._commit(compiled, replace)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            self._lock_path.unlink(missing_ok=True)

    def _commit(self, compiled: dict[str, Any], replace: bool) -> dict[str, Any]:
        directory_name = str(compiled["directory_name"])
        target = (self.plugins_root / directory_name).resolve()
        try:
            target.relative_to(self.plugins_root)
        except ValueError as exc:
            raise ScriptServiceError("SCRIPT_PATH_ESCAPE", "compiled plugin path escapes plugins root") from exc
        if target.is_symlink():
            raise ScriptServiceError("SCRIPT_TARGET_UNSAFE", "script target cannot be a symlink")
        if target.exists() and not target.is_dir():
            raise ScriptServiceError("SCRIPT_TARGET_UNSAFE", "script target is not a directory")
        if target.exists() and not replace:
            raise ScriptServiceError("SCRIPT_ALREADY_INSTALLED", "script plugin already exists; set replace=true")
        if target.exists():
            manifest_path = target / "_manifest.json"
            try:
                existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ScriptServiceError("SCRIPT_TARGET_UNSAFE", "existing target Manifest is invalid") from exc
            if existing.get("id") != compiled["plugin_id"]:
                raise ScriptServiceError("SCRIPT_TARGET_UNSAFE", "existing target identity mismatch")

        staged = Path(tempfile.mkdtemp(prefix=f".{directory_name}-", dir=self.plugins_root))
        backup = self._backup_root / directory_name
        backup_created = False
        try:
            for relative, content in compiled["files"].items():
                destination = staged / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(content, encoding="utf-8")
            if backup.exists():
                shutil.rmtree(backup)
            if target.exists():
                self._backup_root.mkdir(parents=True, exist_ok=True)
                target.rename(backup)
                backup_created = True
            staged.rename(target)
        except Exception as exc:
            if target.exists() and backup_created:
                shutil.rmtree(target, ignore_errors=True)
            if backup_created and backup.exists() and not target.exists():
                backup.rename(target)
            raise ScriptServiceError(
                "SCRIPT_INSTALL_FAILED",
                f"script install failed and was rolled back: {exc}",
            ) from exc
        finally:
            if staged.exists():
                shutil.rmtree(staged, ignore_errors=True)

        state = self._read_state()
        state[str(compiled["plugin_id"])] = {
            "directory": directory_name,
            "version": json.loads(compiled["files"]["_manifest.json"])["version"],
            "restart_required": True,
            "backup_available": backup_created,
        }
        self._write_state(state)
        return {
            "installed": True,
            "plugin_id": compiled["plugin_id"],
            "directory_name": directory_name,
            "loaded": False,
            "restart_required": True,
            "backup_available": backup_created,
        }

    def _read_state(self) -> dict[str, Any]:
        if not self._state_path.is_file():
            return {}
        try:
            value = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _write_state(self, state: dict[str, Any]) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self._state_path)


__all__ = ["ScriptService", "ScriptServiceError"]
