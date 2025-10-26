#!/usr/bin/env bash
set -euo pipefail
APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$APP/.venv/bin/python"

# Файл окружения должен быть тут (на сервере)
ENV_FILE="$APP/.env"

install_unit () {
  local name="$1" entry="$2"
  cat >/etc/systemd/system/${name}.service <<EOF
[Unit]
Description=Innertrade ${name}
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=deploy
WorkingDirectory=${APP}
EnvironmentFile=${ENV_FILE}
ExecStart=${PY} ${APP}/${entry}
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
}

install_unit push_signals push_signals.py
install_unit menu_bot     menu_bot.py

systemctl daemon-reload
systemctl enable push_signals.service menu_bot.service
systemctl restart push_signals.service menu_bot.service || true
