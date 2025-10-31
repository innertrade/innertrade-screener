#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

APP_ROOT=${APP:-${APP_ROOT:-${REPO_ROOT}}}
APP_USER=${APP_USER:-deploy}
APP_GROUP=${APP_GROUP:-${APP_USER}}
VENV=${VENV:-${APP_ROOT}/.venv}
QUEUE_DIRS=(inbox processed failed)
SERVICES=(innertrade-api tvoi_gateway tvoi_consumer pre_forwarder)
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}

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

if [[ ! -x "${VENV}/bin/python" ]]; then
  log "virtualenv not found at ${VENV}; create it before deploying" >&2
  exit 1
fi

log "stopping legacy screener.service"
systemctl stop screener.service 2>/dev/null || true
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true
rm -f "${SYSTEMD_DIR}/screener.service" 2>/dev/null || true

log "stopping managed services"
for svc in "${SERVICES[@]}"; do
  systemctl stop "${svc}.service" 2>/dev/null || true
  systemctl disable "${svc}.service" 2>/dev/null || true
  systemctl reset-failed "${svc}.service" 2>/dev/null || true
done

log "installing systemd units from repository"
APP_ROOT="${APP_ROOT}" SERVICE_USER="${APP_USER}" SERVICE_GROUP="${APP_GROUP}" SYSTEMD_DIR="${SYSTEMD_DIR}" \
  "${APP_ROOT}/scripts/units.sh"

log "ensuring queue directories and permissions"
for dir in "${QUEUE_DIRS[@]}"; do
  install -o "${APP_USER}" -g "${APP_GROUP}" -m 0770 -d "${APP_ROOT}/${dir}"
  if [[ "${dir}" == inbox ]]; then
    install -o "${APP_USER}" -g "${APP_GROUP}" -m 0770 -d "${APP_ROOT}/${dir}/.tmp"
  fi
done

log "ensuring queue directory ownership"
chown -R "${APP_USER}:${APP_GROUP}" "${APP_ROOT}/inbox" "${APP_ROOT}/processed" "${APP_ROOT}/failed"

log "freeing TCP port 8088"
fuser -k 8088/tcp 2>/dev/null || true
sleep 1

log "starting services"
for svc in "${SERVICES[@]}"; do
  systemctl enable "${svc}.service"
  systemctl start "${svc}.service"
  if ! systemctl is-active --quiet "${svc}.service"; then
    log "service ${svc}.service failed to start" >&2
    systemctl --no-pager --full status "${svc}.service" || true
    exit 1
  fi
  log "service ${svc}.service is active"
  sleep 1
done

log "smoke: checking API health"
HEALTH_FILE=$(mktemp)
SIGNALS_FILE=$(mktemp)
export HEALTH_FILE SIGNALS_FILE
curl -fsS http://127.0.0.1:8088/health >"${HEALTH_FILE}"
curl -fsS http://127.0.0.1:8088/signals >"${SIGNALS_FILE}"

python3 - <<'PY'
import json, os, sys
health_file = os.environ["HEALTH_FILE"]
signals_file = os.environ["SIGNALS_FILE"]
with open(health_file, "r", encoding="utf-8") as handle:
    data = json.load(handle)
if data.get("status") != "ok":
    print("health endpoint did not return ok", file=sys.stderr)
    sys.exit(1)
with open(signals_file, "r", encoding="utf-8") as handle:
    signals_payload = json.load(handle)
if not isinstance(signals_payload, dict) or not isinstance(signals_payload.get("data"), list):
    print("signals endpoint did not return JSON with data[] list", file=sys.stderr)
    sys.exit(1)
PY

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

if [[ -z "${FILENAME}" ]]; then
  log "gateway did not return file name" >&2
  exit 1
fi

log "waiting for consumer to process ${FILENAME}"
max_wait=30
while (( max_wait > 0 )); do
  if [[ -f "${APP_ROOT}/processed/${FILENAME}" ]]; then
    log "processed/${FILENAME} present"
    break
  fi
  if [[ -f "${APP_ROOT}/failed/${FILENAME}" ]]; then
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
systemctl --no-pager --full status "${SERVICES[@]}" | sed -n '1,160p' || true

log "deploy complete"
