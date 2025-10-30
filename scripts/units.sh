#!/usr/bin/env bash
set -euo pipefail

APP="/home/deploy/apps/innertrade-screener"
PY="${APP}/.venv/bin/python"

# === 1) API, который отдаёт /signals (слушает 127.0.0.1:8088) ===
# ВАЖНО: это не «толстый» screener.service, это именно API.
# Толстый сервис не создаём и не трогаем.
cat >/etc/systemd/system/innertrade-api.service <<'UNIT'
[Unit]
Description=InnerTrade API (/signals via gunicorn)
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
Environment="PORT=8088"
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/gunicorn -b 127.0.0.1:8088 main:app
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

# === 2) HTTP-шлюз /tvoi (127.0.0.1:80/nginx -> 127.0.0.1:8787) ===
# Сам скрипт tvoi_gateway.py уже положили в /usr/local/bin ранее.
cat >/etc/systemd/system/tvoi_gateway.service <<'UNIT'
[Unit]
Description=InnerTrade TVOI HTTP gateway (POST /tvoi -> queue file)
After=network.target

[Service]
Type=simple
User=root
Group=root
Environment=APP=/home/deploy/apps/innertrade-screener
ExecStart=/usr/bin/env python3 /usr/local/bin/tvoi_gateway.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

# === 3) Консюмер очереди (читает APP/inbox и шлёт в Telegram) ===
cat >/etc/systemd/system/tvoi_consumer.service <<'UNIT'
[Unit]
Description=InnerTrade TVOI consumer (file inbox -> Telegram)
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/python /home/deploy/apps/innertrade-screener/consumers/tvoi_consumer.py
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
UNIT

# === 4) Страховка: если где-то остался старый screener.service — выключаем ===
# (Мы его больше НЕ создаём.)
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true

# Применяем
systemctl daemon-reload
