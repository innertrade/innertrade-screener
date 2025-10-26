"""State management helpers for push and trend toggles."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

PUSH_STATE_FILE = os.getenv("PUSH_STATE_FILE", "state/push_enabled.json")

_DEFAULT_STATE = {"push": True, "trend": True}


def _state_path() -> Path:
    path = Path(PUSH_STATE_FILE)
    if not path.is_absolute():
        base_dir = Path(__file__).resolve().parent
        path = base_dir / path
    return path


def ensure_state_file() -> Dict[str, bool]:
    path = _state_path()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_state(_DEFAULT_STATE)
        return _DEFAULT_STATE.copy()
    return _read_state()


def _read_state() -> Dict[str, bool]:
    path = _state_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except FileNotFoundError:
        return ensure_state_file()
    except Exception:
        data = {}
    state = _DEFAULT_STATE.copy()
    for key in state:
        if key in data:
            state[key] = bool(data[key])
    return state


def _write_state(state: Dict[str, bool]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _update_state(**kwargs: bool) -> Dict[str, bool]:
    state = _read_state()
    changed = False
    for key, value in kwargs.items():
        if key in state:
            bool_value = bool(value)
            if state[key] != bool_value:
                state[key] = bool_value
                changed = True
    if changed:
        _write_state(state)
    return state


def get_push_enabled() -> bool:
    return _read_state()["push"]


def set_push_enabled(enabled: bool) -> bool:
    return _update_state(push=enabled)["push"]


def toggle_push_enabled() -> bool:
    current = get_push_enabled()
    return set_push_enabled(not current)


def get_trend_enabled() -> bool:
    return _read_state()["trend"]


def set_trend_enabled(enabled: bool) -> bool:
    return _update_state(trend=enabled)["trend"]


def toggle_trend_enabled() -> bool:
    current = get_trend_enabled()
    return set_trend_enabled(not current)


def load_state() -> Dict[str, bool]:
    return _read_state()
