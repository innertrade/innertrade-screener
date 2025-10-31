#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD_DIR="${REPO_ROOT}/systemd"
INSTALL_ROOT="/etc/systemd/system"
LEGACY_SERVICE="screener.service"
APP_ROOT="${APP_ROOT:-${REPO_ROOT}}"
APP_USER="${APP_USER:-deploy}"
APP_GROUP="${APP_GROUP:-${APP_USER}}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "scripts/units.sh must be executed as root" >&2
  exit 1
fi

if [[ ! -d "${SYSTEMD_DIR}" ]]; then
  echo "systemd directory not found at ${SYSTEMD_DIR}" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to render unit templates" >&2
  exit 1
fi

install -d -m 0755 "${INSTALL_ROOT}"

for unit in "${SYSTEMD_DIR}"/*.service; do
  base="$(basename "${unit}")"
  tmp_file="$(mktemp)"
  UNIT_SRC="${unit}" UNIT_TMP="${tmp_file}" APP_ROOT="${APP_ROOT}" APP_USER="${APP_USER}" APP_GROUP="${APP_GROUP}" python3 - <<'PY'
import os
from pathlib import Path

template = Path(os.environ['UNIT_SRC']).read_text(encoding='utf-8')
for key, value in {
    '@APP_ROOT@': os.environ['APP_ROOT'],
    '@APP_USER@': os.environ['APP_USER'],
    '@APP_GROUP@': os.environ['APP_GROUP'],
}.items():
    template = template.replace(key, value)
Path(os.environ['UNIT_TMP']).write_text(template, encoding='utf-8')
PY
  install -m 0644 -o root -g root "${tmp_file}" "${INSTALL_ROOT}/${base}"
  rm -f "${tmp_file}"
  echo "installed ${base}"
done

systemctl daemon-reload

if systemctl list-unit-files | grep -q "^${LEGACY_SERVICE}"; then
  systemctl disable --now "${LEGACY_SERVICE}" 2>/dev/null || true
  systemctl mask "${LEGACY_SERVICE}" 2>/dev/null || true
fi

rm -f "${INSTALL_ROOT}/${LEGACY_SERVICE}" 2>/dev/null || true
