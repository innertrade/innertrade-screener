from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = Path(os.getenv("PUSH_STATE_FILE", BASE_DIR / "state" / "push_enabled.json"))
STATUS_FILE = Path(os.getenv("PUSH_STATUS_FILE", BASE_DIR / "runtime" / "push_status.json"))
LOG_FILE = Path(os.getenv("PUSH_LOG_FILE", BASE_DIR / "push_signals.log"))

ISO_FMT = "%Y-%m-%dT%H:%M:%S%z"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(tmp_path, path)


def _append_log(message: str, level: str = "INFO") -> None:
    _ensure_parent(LOG_FILE)
    timestamp = datetime.now(timezone.utc).strftime(ISO_FMT)
    line = f"[{level}] {timestamp} {message}"
    with open(LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def log_event(message: str, level: str = "INFO") -> None:
    """Public helper for writing to the push log."""
    _append_log(message, level=level)


def get_push_enabled(default: bool = True) -> bool:
    try:
        data = _read_json(STATE_FILE)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        _append_log("Failed to decode push state file, defaulting to %s" % default, level="WARNING")
        return default
    except Exception as exc:  # pragma: no cover - defensive
        _append_log(f"Unexpected error reading push state: {exc}", level="ERROR")
        return default
    return bool(data.get("enabled", default))


def set_push_enabled(enabled: bool, source: str) -> Dict[str, Any]:
    enabled_bool = bool(enabled)
    previous_state: bool | None = None
    try:
        data = _read_json(STATE_FILE)
        previous_state = bool(data.get("enabled"))
    except FileNotFoundError:
        previous_state = None
    except Exception:
        previous_state = None

    payload = {
        "enabled": enabled_bool,
        "updated_at": datetime.now(timezone.utc).strftime(ISO_FMT),
        "source": source,
    }
    _write_json(STATE_FILE, payload)
    if previous_state is None or previous_state != enabled_bool:
        log_event(f"PUSH_ENABLED changed to {'ON' if enabled_bool else 'OFF'} by {source}")
    else:
        log_event(f"PUSH_ENABLED already {'ON' if enabled_bool else 'OFF'} (confirmed by {source})", level="DEBUG")
    write_runtime_status(enabled=enabled_bool, source=source, meta={"event": "toggle"})
    return payload


def write_runtime_status(*, enabled: bool, source: str, meta: Dict[str, Any] | None = None) -> None:
    payload: Dict[str, Any] = {
        "enabled": bool(enabled),
        "source": source,
        "timestamp": datetime.now(timezone.utc).strftime(ISO_FMT),
    }
    if meta:
        payload.update(meta)
    try:
        _write_json(STATUS_FILE, payload)
    except Exception as exc:  # pragma: no cover - defensive
        _append_log(f"Failed to write runtime status: {exc}", level="ERROR")

