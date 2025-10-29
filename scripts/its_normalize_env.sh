#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/var/log/its_normalize.log"
mkdir -p "$(dirname "$LOG_FILE")"

log_msg() {
  local level="$1"
  shift
  local ts
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  local message="$*"
  echo "$ts | $level | $message"
  echo "$ts | $level | $message" >>"$LOG_FILE"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

log_msg INFO "its_normalize_env start | root=$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  log_msg INFO "Loading environment from $ROOT_DIR/.env"
  set -a
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env"
  set +a
fi

STATE_FILE="${PUSH_STATE_FILE:-state/push_enabled.json}"
if [[ "${STATE_FILE}" != /* ]]; then
  STATE_FILE="$ROOT_DIR/$STATE_FILE"
fi
export STATE_FILE

log_msg INFO "Normalizing push state at $STATE_FILE"

PY_OUT="$(python3 - <<'PY'
import json
import os
import pathlib

state_path = pathlib.Path(os.environ["STATE_FILE"]).resolve()
default = {"tvoi": True, "trnd": False}


def normalize(data):
    state = default.copy()
    migrated = False
    if data is None:
        migrated = True
    elif isinstance(data, bool):
        state = {"tvoi": bool(data), "trnd": bool(data)}
        migrated = True
    elif isinstance(data, dict):
        if "tvoi" in data or "trnd" in data:
            state["tvoi"] = bool(data.get("tvoi", state["tvoi"]))
            state["trnd"] = bool(data.get("trnd", state["trnd"]))
        elif "enabled" in data:
            val = bool(data.get("enabled"))
            state = {"tvoi": val, "trnd": val}
            migrated = True
        else:
            if "push_enabled" in data:
                state["tvoi"] = bool(data.get("push_enabled"))
                migrated = True
            if "trend_enabled" in data:
                state["trnd"] = bool(data.get("trend_enabled"))
                migrated = True
    else:
        migrated = True
    return state, migrated


def read_existing(path: pathlib.Path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def atomic_write(path: pathlib.Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp.replace(path)


current = read_existing(state_path)
state, migrated = normalize(current)
if current is None or migrated or state != current:
    atomic_write(state_path, state)
    status = "updated"
else:
    status = "unchanged"
print(f"{status}|{state_path}|{json.dumps(state)}")
PY
)"

if [[ -z "$PY_OUT" ]]; then
  log_msg ERROR "State migration script produced no output"
  exit 1
fi

status="${PY_OUT%%|*}"
rest="${PY_OUT#*|}"
path_part="${rest%%|*}"
json_state="${rest#*|}"
log_msg INFO "State ${status} at ${path_part} => ${json_state}"

missing=()
[[ -z "${MENU_BOT_TOKEN:-}" ]] && missing+=(MENU_BOT_TOKEN)
[[ -z "${TELEGRAM_BOT_TOKEN:-}" ]] && missing+=(TELEGRAM_BOT_TOKEN)
[[ -z "${TELEGRAM_CHAT_ID:-}" ]] && missing+=(TELEGRAM_CHAT_ID)
if (( ${#missing[@]} )); then
  log_msg WARN "Missing environment variables: ${missing[*]}"
fi

restart_if_exists() {
  local unit="$1"
  if ! command -v systemctl >/dev/null 2>&1; then
    log_msg WARN "systemctl unavailable, skip $unit"
    return
  fi
  if ! systemctl list-unit-files | awk '{print $1}' | grep -Fxq "$unit"; then
    log_msg INFO "Unit $unit not found, skipping"
    return
  fi
  if systemctl try-restart "$unit"; then
    log_msg INFO "try-restart $unit succeeded"
  else
    log_msg WARN "try-restart $unit failed"
  fi
}

for unit in menu_bot.service push_signals.service; do
  restart_if_exists "$unit"
done

log_msg INFO "its_normalize_env complete"
