#!/usr/bin/env bash
set -euo pipefail

APP="/home/deploy/apps/innertrade-screener"

# 0) Код/зависимости (по желанию)
# git pull --rebase || true
# "${APP}/.venv/bin/pip" install -r "${APP}/requirements.txt"

# 1) Нормализуем окружение (если есть)
if [ -x "${APP}/scripts/its_normalize_env.sh" ]; then
  bash "${APP}/scripts/its_normalize_env.sh"
fi

# 2) Ставим/обновляем systemd-юниты тонкого контура
install -m 0644 -D "${APP}/deploy/systemd/tvoi_gateway.service"   /etc/systemd/system/tvoi_gateway.service
install -m 0644 -D "${APP}/deploy/systemd/tvoi_consumer.service"  /etc/systemd/system/tvoi_consumer.service
install -m 0644 -D "${APP}/deploy/systemd/pre_forwarder.service"  /etc/systemd/system/pre_forwarder.service
# innertrade-api.service предполагается уже установлен (как и раньше).
systemctl daemon-reload

# 3) Глушим «толстый» на всякий случай
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true
rm -f /etc/systemd/system/screener.service || true

# 4) Запускаем нужные сервисы (тонкий контур)
systemctl enable --now innertrade-api.service
systemctl enable --now tvoi_gateway.service
systemctl enable --now tvoi_consumer.service
systemctl enable --now pre_forwarder.service

# 5) Смоук-чек
echo "--- smoke ---"
curl -fsS http://127.0.0.1:8088/health || true
curl -fsS http://127.0.0.1:8088/signals | head -c 300; echo || true
echo '{"type":"PRE","symbol":"TESTUSDT","tf":"5m","price":123,"link":"http://x"}' \
 | curl -sS -X POST http://127.0.0.1/tvoi -H 'Content-Type: application/json' -d @- || true

echo "--- statuses ---"
systemctl --no-pager --full status innertrade-api tvoi_gateway tvoi_consumer pre_forwarder | sed -n '1,120p' || true
