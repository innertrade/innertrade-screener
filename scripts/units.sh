#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

APP_ROOT=${APP_ROOT:-${REPO_ROOT}}
SERVICE_USER=${SERVICE_USER:-${APP_USER:-deploy}}
SERVICE_GROUP=${SERVICE_GROUP:-${APP_GROUP:-${SERVICE_USER}}}
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
UNITS=(innertrade-api tvoi_gateway tvoi_consumer push_signals)

if [[ ! -d "${APP_ROOT}" ]]; then
  echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | APP_ROOT not found: ${APP_ROOT}" >&2
  exit 1
fi

log() {
  printf '%s | %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

install_unit() {
  local unit_name=$1
  local src="${REPO_ROOT}/deploy/systemd/${unit_name}.service"
  local dst="${SYSTEMD_DIR}/${unit_name}.service"

  if [[ ! -f "${src}" ]]; then
    log "unit template missing: ${src}" >&2
    exit 1
  fi

  log "installing ${unit_name}.service -> ${dst}"
  local tmp
  tmp=$(mktemp)
  sed \
    -e "s#@APP_ROOT@#${APP_ROOT}#g" \
    -e "s#@USER@#${SERVICE_USER}#g" \
    -e "s#@GROUP@#${SERVICE_GROUP}#g" \
    "${src}" >"${tmp}"
  install -m 0644 -D "${tmp}" "${dst}"
  rm -f "${tmp}"
}

for unit in "${UNITS[@]}"; do
  install_unit "${unit}"
done

log "daemon-reload"
systemctl daemon-reload

for unit in "${UNITS[@]}"; do
  log "enabling ${unit}.service"
  systemctl enable "${unit}.service"
done

log "disabling legacy screener.service"
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true
rm -f "${SYSTEMD_DIR}/screener.service" 2>/dev/null || true
