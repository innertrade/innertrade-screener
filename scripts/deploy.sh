#!/usr/bin/env bash
set -euo pipefail

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "$(ts) | $*"; }

APP="/home/deploy/apps/innertrade-screener"
cd "$APP"

SERVICES=(
  innertrade-api.service
  tvoi_gateway.service
  tvoi_consumer.service
  push_signals.service
)

# 0) выключаем и выпиливаем легаси
log "stopping legacy screener.service"
systemctl disable --now screener.service 2>/dev/null || true
rm -f /etc/systemd/system/screener.service || true

log "stopping legacy pre_forwarder.service"
systemctl disable --now pre_forwarder.service 2>/dev/null || true
rm -f /etc/systemd/system/pre_forwarder.service || true

log "stopping legacy menu_bot.service"
systemctl disable --now menu_bot.service 2>/dev/null || true
rm -f /etc/systemd/system/menu_bot.service || true

# 1) останавливаем наши юниты
log "stopping managed services"
for s in "${SERVICES[@]}"; do
  systemctl disable --now "$s" 2>/dev/null || true
done

# 2) ставим systemd-юниты из корня репозитория (если лежат рядом)
log "installing systemd units from repository"
for s in "${SERVICES[@]}"; do
  if [ -f "$APP/deploy/systemd/$s" ]; then
    log "installing $s -> /etc/systemd/system/$s"
    install -m 0644 "$APP/deploy/systemd/$s" "/etc/systemd/system/$s"
  fi
done

log "daemon-reload"
systemctl daemon-reload

# 3) включаем юниты
for s in "${SERVICES[@]}"; do
  log "enabling $s"
  systemctl enable "$s"
done

# 4) директории очереди (не в гите)
log "ensuring queue directories and permissions"
mkdir -p "$APP/inbox" "$APP/processed" "$APP/failed"
chmod 0777 "$APP/inbox" "$APP/processed" "$APP/failed"

# 5) освобождаем порт 8088 (если что-то повисло)
log "freeing TCP port 8088"
pids=$(lsof -nP -iTCP:8088 -sTCP:LISTEN -t 2>/dev/null || true)
if [ -n "${pids:-}" ]; then
  kill -TERM $pids || true
  sleep 1
fi

# 6) запускаем
log "starting services"
for s in "${SERVICES[@]}"; do
  systemctl start "$s"
done

# 7) статусы
for s in "${SERVICES[@]}"; do
  if systemctl is-active --quiet "$s"; then
    log "service $s is active"
  else
    log "service $s failed to start"
  fi
done

# 8) лёгкий smoke
log "smoke: API"
curl -fsS "http://127.0.0.1:8088/health" || true
curl -fsS "http://127.0.0.1:8088/signals" | head -c 300 || true; echo

log "smoke: POST -> gateway"
printf '%s' '{"type":"PRE","symbol":"TESTUSDT","tf":"5m","price":123,"link":"http://x"}' \
 | curl -sS -X POST "http://127.0.0.1:8787/tvoi" -H 'Content-Type: application/json' -d @- ; echo

log "done"
