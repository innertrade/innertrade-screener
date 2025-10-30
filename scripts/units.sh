#!/usr/bin/env bash
set -euo pipefail

APP="/home/deploy/apps/innertrade-screener"

cat >/etc/systemd/system/innertrade-api.service <<'UNIT'
[Unit]
Description=InnerTrade API (/health, /signals)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
Environment="APP_ROOT=/home/deploy/apps/innertrade-screener"
Environment="PORT=8088"
ExecStartPre=/usr/bin/env bash -c 'fuser -k 8088/tcp 2>/dev/null || true'
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/gunicorn --workers 1 --bind 127.0.0.1:8088 --pid /run/innertrade-api/gunicorn.pid --access-logfile - --error-logfile - main:app
Restart=always
RestartSec=2
KillSignal=SIGINT
RuntimeDirectory=innertrade-api
RuntimeDirectoryMode=0750

[Install]
WantedBy=multi-user.target
UNIT

cat >/etc/systemd/system/tvoi_gateway.service <<'UNIT'
[Unit]
Description=InnerTrade TVOI HTTP gateway (POST /tvoi -> queue file)
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
Environment="APP_ROOT=/home/deploy/apps/innertrade-screener"
Environment="QUEUE_DIR=/home/deploy/apps/innertrade-screener/inbox"
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/gunicorn --workers 1 --bind 127.0.0.1:8787 --access-logfile - --error-logfile - services.tvoi_gateway:app
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

cat >/etc/systemd/system/tvoi_consumer.service <<'UNIT'
[Unit]
Description=InnerTrade TVOI consumer (file inbox -> Telegram)
After=network.target

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
Environment="APP_ROOT=/home/deploy/apps/innertrade-screener"
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/python -u consumers/tvoi_consumer.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

cat >/etc/systemd/system/pre_forwarder.service <<'UNIT'
[Unit]
Description=InnerTrade PRE forwarder (/signals -> /tvoi)
After=network.target innertrade-api.service tvoi_gateway.service

[Service]
Type=simple
User=deploy
Group=deploy
WorkingDirectory=/home/deploy/apps/innertrade-screener
Environment="HOST=http://127.0.0.1:8088"
Environment="TVOI_URL=http://127.0.0.1:8787/tvoi"
Environment="MIN_Z=2.0"
Environment="MIN_VOL_X=1.5"
Environment="MIN_V24_USD=15000000"
Environment="MIN_OI_Z=0.5"
ExecStart=/home/deploy/apps/innertrade-screener/.venv/bin/python /home/deploy/apps/innertrade-screener/pre_forwarder.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true
