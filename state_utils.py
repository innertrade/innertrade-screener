"""Shared helpers for toggleable push/trend state flags."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

DEFAULT_STATE = {"push_enabled": True, "trend_enabled": True}
_BASE_DIR = Path(__file__).resolve().parent


def _state_path() -> Path:
    configured = os.getenv("PUSH_STATE_FILE", "state/push_enabled.json")
    path = Path(configured)
    if not path.is_absolute():
        path = (_BASE_DIR / path).resolve()
    return path


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_state() -> Dict[str, bool]:
    path = _state_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            return {
                "push_enabled": bool(data.get("push_enabled", DEFAULT_STATE["push_enabled"])),
                "trend_enabled": bool(data.get("trend_enabled", DEFAULT_STATE["trend_enabled"]))
            }
    except FileNotFoundError:
        save_state(DEFAULT_STATE)
    except json.JSONDecodeError:
        # Corrupted file – overwrite with defaults to keep scripts operational.
        save_state(DEFAULT_STATE)
    return DEFAULT_STATE.copy()


def save_state(state: Dict[str, bool]) -> None:
    path = _state_path()
    _ensure_parent(path)
    payload = {
        "push_enabled": bool(state.get("push_enabled", DEFAULT_STATE["push_enabled"])),
        "trend_enabled": bool(state.get("trend_enabled", DEFAULT_STATE["trend_enabled"]))
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def get_push_enabled() -> bool:
    return load_state()["push_enabled"]


def set_push_enabled(value: bool) -> bool:
    state = load_state()
    state["push_enabled"] = bool(value)
    save_state(state)
    return state["push_enabled"]


def toggle_push_enabled() -> bool:
    state = load_state()
    state["push_enabled"] = not state.get("push_enabled", DEFAULT_STATE["push_enabled"])
    save_state(state)
    return state["push_enabled"]


def get_trend_enabled() -> bool:
    return load_state()["trend_enabled"]


def set_trend_enabled(value: bool) -> bool:
    state = load_state()
    state["trend_enabled"] = bool(value)
    save_state(state)
    return state["trend_enabled"]


def toggle_trend_enabled() -> bool:
    state = load_state()
    state["trend_enabled"] = not state.get("trend_enabled", DEFAULT_STATE["trend_enabled"])
    save_state(state)
    return state["trend_enabled"]
