#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_APP="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP="${APP:-${DEFAULT_APP}}"
APP_USER="${APP_USER:-deploy}"
APP_GROUP="${APP_GROUP:-${APP_USER}}"
VENV="${VENV:-${APP}/.venv}"
QUEUE_DIRS=(inbox processed failed)
SERVICES=(innertrade-api tvoi_gateway tvoi_consumer pre_forwarder)

log() {
  printf '%s | %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

trap 'log "deploy failed at line $LINENO: $BASH_COMMAND" >&2' ERR

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "missing required command: $1" >&2
    exit 1
  fi
}

for cmd in install systemctl curl python3; do
  require_cmd "$cmd"
done

if [ ! -x "${VENV}/bin/python" ]; then
  log "virtualenv not found at ${VENV}; create it before deploying" >&2
  exit 1
fi

log "stopping managed services before restart"
for svc in "${SERVICES[@]}"; do
  systemctl stop "${svc}.service" 2>/dev/null || true
done

log "installing systemd units"
APP_ROOT="${APP}" APP_USER="${APP_USER}" APP_GROUP="${APP_GROUP}" "${APP}/scripts/units.sh"

log "ensuring queue directories"
for dir in "${QUEUE_DIRS[@]}"; do
  install -o "${APP_USER}" -g "${APP_GROUP}" -m 0770 -d "${APP}/${dir}"
  if [[ "$dir" == inbox ]]; then
    install -o "${APP_USER}" -g "${APP_GROUP}" -m 0770 -d "${APP}/${dir}/.tmp"
  fi
done

log "freeing TCP port 8088"
fuser -k 8088/tcp 2>/dev/null || true
sleep 1

log "enabling and starting services in order"
for svc in "${SERVICES[@]}"; do
  systemctl enable "${svc}.service"
  systemctl start "${svc}.service"
  systemctl is-active --quiet "${svc}.service" || {
    log "service ${svc}.service failed to start" >&2
    systemctl --no-pager --full status "${svc}.service" || true
    exit 1
  }
  log "service ${svc}.service is active"
  sleep 1
done

log "smoke: checking API"
curl -fsS http://127.0.0.1:8088/health >/tmp/innertrade-health.json
curl -fsS http://127.0.0.1:8088/signals | head -c 300 >/tmp/innertrade-signals.txt

log "smoke: pushing synthetic TVOI signal"
SAMPLE='{"type":"PRE","symbol":"TESTUSDT","tf":"5m","price":123,"link":"http://example.com"}'
RESPONSE=$(echo "${SAMPLE}" | curl -fsS -X POST http://127.0.0.1:8787/tvoi -H 'Content-Type: application/json' -d @-)
log "gateway response: ${RESPONSE}"

FILENAME=$(printf '%s' "${RESPONSE}" | python3 - <<'PY'
import json, sys
try:
    data = json.loads(sys.stdin.read())
except Exception:
    data = {}
print(data.get('file', ''))
PY
)

if [ -z "${FILENAME}" ]; then
  log "gateway did not return file name" >&2
  exit 1
fi

log "waiting for consumer to process ${FILENAME}"
max_wait=20
while (( max_wait > 0 )); do
  if [ -f "${APP}/processed/${FILENAME}" ]; then
    log "processed/${FILENAME} present"
    break
  fi
  if [ -f "${APP}/failed/${FILENAME}" ]; then
    log "file moved to failed/${FILENAME}" >&2
    exit 1
  fi
  sleep 1
  ((max_wait--))
done

if (( max_wait == 0 )); then
  log "timeout waiting for processed file" >&2
  exit 1
fi

log "smoke check complete"
log "services status summary"
systemctl --no-pager --full status innertrade-api tvoi_gateway tvoi_consumer pre_forwarder | sed -n '1,120p' || true

log "deploy complete"
