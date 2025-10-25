#!/usr/bin/env bash
# its_fix_stack.sh — normalize the InnerTrade screener stack on the VPS.
#
# Usage: sudo bash /root/its_fix_stack.sh
# The script is idempotent and can be safely re-run.

set -euo pipefail

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_command() {
  local cmd=$1
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log "ERROR: required command '$cmd' is missing."
    exit 1
  fi
}

kill_stale_processes() {
  log 'Scanning for python/gunicorn/push_trend processes with deleted cwd...'
  mapfile -t candidate_pids < <(
    ps -eo pid=,comm=,args= |
      awk '/python/ || /gunicorn/ || /push_trend/ {print $1}' |
      sort -u
  )

  if ((${#candidate_pids[@]} == 0)); then
    log 'No matching processes found.'
    return
  fi

  local killed=()
  for pid in "${candidate_pids[@]}"; do
    [[ -d "/proc/$pid" ]] || continue
    local cwd
    cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null || true)
    if [[ "${cwd:-}" == *'(deleted)'* ]]; then
      local cmdline
      cmdline=$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || echo 'unknown command')
      log "Terminating PID $pid with deleted cwd ($cmdline)"
      kill -TERM "$pid" 2>/dev/null || true
      killed+=("$pid")
    fi
  done

  if ((${#killed[@]} == 0)); then
    log 'No stale processes required termination.'
    return
  fi

  sleep 3

  for pid in "${killed[@]}"; do
    if [[ -d "/proc/$pid" ]]; then
      log "Force killing PID $pid"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
}

disable_legacy_units() {
  local legacy_units=(screener.service push_trend.service)
  for unit in "${legacy_units[@]}"; do
    if systemctl list-unit-files "$unit" >/dev/null 2>&1; then
      log "Disabling and stopping legacy unit $unit"
      systemctl disable --now "$unit" >/dev/null 2>&1 || true
    else
      log "Legacy unit $unit not present; skipping"
    fi
  done
}

enable_valid_units() {
  local valid_units=(innertrade-screener.service menu_bot.service push_signals.service)
  log 'Ensuring valid units are enabled and restarted'
  systemctl daemon-reload
  for unit in "${valid_units[@]}"; do
    if systemctl list-unit-files "$unit" >/dev/null 2>&1; then
      log "Enabling and restarting $unit"
      systemctl enable --now "$unit"
    else
      log "WARNING: Expected unit $unit is missing"
    fi
  done
}

check_listener() {
  log 'Verifying gunicorn listener on 127.0.0.1:8088'
  if ! ss -H -ltnp | grep -F '127.0.0.1:8088' | grep -q 'gunicorn'; then
    log 'ERROR: gunicorn listener on 127.0.0.1:8088 not found'
    exit 1
  fi
}

check_health() {
  log 'Requesting /health endpoint'
  local response
  response=$(curl -fsS 127.0.0.1:8088/health)
  if ! grep -q '"status"[[:space:]]*:[[:space:]]*"ok"' <<<"$response"; then
    log "ERROR: /health check did not return status=ok (response: $response)"
    exit 1
  fi
}

check_deleted_processes() {
  log 'Ensuring no python/gunicorn/push_trend process has a deleted cwd'
  mapfile -t remaining < <(
    ps -eo pid=,comm= |
      awk '/python/ || /gunicorn/ || /push_trend/ {print $1}'
  )

  for pid in "${remaining[@]}"; do
    [[ -d "/proc/$pid" ]] || continue
    local cwd
    cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null || true)
    if [[ "${cwd:-}" == *'(deleted)'* ]]; then
      log "ERROR: PID $pid still has cwd marked as deleted"
      exit 1
    fi
  done
}

main() {
  require_command ps
  require_command systemctl
  require_command ss
  require_command curl

  kill_stale_processes
  disable_legacy_units
  enable_valid_units

  check_listener
  check_health
  check_deleted_processes

  log 'Stack normalized successfully.'
}

main "$@"
