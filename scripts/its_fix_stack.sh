#!/usr/bin/env bash
set -euo pipefail

LOG_PREFIX="[its_fix_stack]"

log() {
    printf '%s %s\n' "$LOG_PREFIX" "$1"
}

warn() {
    printf '%s WARNING: %s\n' "$LOG_PREFIX" "$1" >&2
}

fatal() {
    printf '%s ERROR: %s\n' "$LOG_PREFIX" "$1" >&2
    exit 1
}

# Kill processes whose cwd points to a deleted directory and match target names.
kill_deleted_processes() {
    log "Scanning for python/gunicorn/push_trend processes with deleted CWDs"
    local killed_any=0
    local cwd_paths pid cmd
    mapfile -t cwd_paths < <(find /proc -maxdepth 2 -type l -lname '* (deleted)' -path '/proc/[0-9]*/cwd' 2>/dev/null || true)

    for cwd_path in "${cwd_paths[@]}"; do
        pid="$(basename "$(dirname "$cwd_path")")"
        [[ "$pid" =~ ^[0-9]+$ ]] || continue
        if [[ ! -r "/proc/$pid/cmdline" ]]; then
            continue
        fi
        cmd="$(tr '\0' ' ' <"/proc/$pid/cmdline")"
        if [[ -z "$cmd" ]]; then
            cmd="$(<"/proc/$pid/comm" 2>/dev/null)"
        fi
        if [[ ! "$cmd" =~ (python|gunicorn|push_trend) ]]; then
            continue
        fi

        if [[ ! -e "/proc/$pid" ]]; then
            continue
        fi

        log "Terminating PID $pid ($cmd)"
        killed_any=1
        kill -TERM "$pid" 2>/dev/null || true
        for _ in {1..10}; do
            if [[ ! -e "/proc/$pid" ]]; then
                break
            fi
            sleep 0.5
        done
        if [[ -e "/proc/$pid" ]]; then
            warn "PID $pid did not exit after SIGTERM, sending SIGKILL"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done

    if [[ $killed_any -eq 0 ]]; then
        log "No matching zombie processes were found"
    fi
}

# Disable and stop legacy units if they exist.
disable_legacy_unit() {
    local unit="$1"
    if systemctl list-unit-files "$unit" >/dev/null 2>&1; then
        log "Disabling legacy unit $unit"
        systemctl disable --now "$unit" >/dev/null 2>&1 || warn "Failed to disable $unit"
    else
        log "Legacy unit $unit not present"
    fi
}

restart_unit() {
    local unit="$1"
    if systemctl list-unit-files "$unit" >/dev/null 2>&1; then
        log "Restarting $unit"
        if ! systemctl restart "$unit"; then
            fatal "Failed to restart $unit"
        fi
    else
        fatal "Required unit $unit not found"
    fi
}

check_no_deleted_cwds() {
    if find /proc -maxdepth 2 -type l -lname '* (deleted)' -path '/proc/[0-9]*/cwd' 2>/dev/null | grep -q .; then
        fatal "Found processes with deleted CWD after cleanup"
    fi
    log "No deleted CWD entries remaining"
}

check_listener() {
    local listener_line pid owner
    listener_line="$(ss -H -ltnp | awk '/127\.0\.0\.1:8088/ {print; exit}')"
    if [[ -z "$listener_line" ]]; then
        fatal "Listener on 127.0.0.1:8088 not found"
    fi
    pid="$(awk 'match($0, /pid=([0-9]+)/, m) {print m[1]}' <<<"$listener_line")"
    if [[ -z "$pid" ]]; then
        warn "Unable to determine PID for 127.0.0.1:8088"
        return
    fi
    if [[ -e "/proc/$pid" ]]; then
        owner="$(stat -c '%U' "/proc/$pid" 2>/dev/null || echo unknown)"
        if [[ "$owner" != "deploy" ]]; then
            warn "Listener PID $pid is owned by $owner instead of deploy"
        else
            log "Listener PID $pid is owned by deploy"
        fi
        local comm
        comm="$(<"/proc/$pid/comm" 2>/dev/null || echo unknown)"
        log "Listener PID $pid command name: $comm"
    else
        warn "PID $pid for 127.0.0.1:8088 disappeared during check"
    fi
}

check_health() {
    local response
    response="$(curl -fsS 127.0.0.1:8088/health 2>/dev/null || true)"
    if [[ -z "$response" ]]; then
        fatal "Failed to fetch /health"
    fi
    if ! python3 - <<'PY' >/dev/null 2>&1 <<<"$response"; then
import json, sys
try:
    payload = json.load(sys.stdin)
except json.JSONDecodeError:
    raise SystemExit(1)
if payload.get("status") != "ok":
    raise SystemExit(1)
PY
        fatal "Health endpoint returned unexpected payload: $response"
    fi
    log "Health check successful"
}

main() {
    if [[ $EUID -ne 0 ]]; then
        fatal "Run this script as root"
    fi

    kill_deleted_processes

    disable_legacy_unit "screener.service"
    disable_legacy_unit "push_trend.service"
    disable_legacy_unit "menu_bot.service"
    disable_legacy_unit "pre_forwarder.service"

    restart_unit "innertrade-api.service"
    restart_unit "tvoi_gateway.service"
    restart_unit "tvoi_consumer.service"
    restart_unit "push_signals.service"

    check_listener
    check_health
    check_no_deleted_cwds

    log "Stack fix completed successfully"
}

main "$@"

