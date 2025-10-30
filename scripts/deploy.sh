#!/usr/bin/env bash
set -euo pipefail

APP="/home/deploy/apps/innertrade-screener"

# 0) Обновляем код / зависимости по твоей схеме (опционально)
# git pull --rebase
# ${APP}/.venv/bin/pip install -r requirements.txt

# 1) Нормализуем окружение (если у тебя есть такой скрипт)
if [ -x "${APP}/scripts/its_normalize_env.sh" ]; then
  bash "${APP}/scripts/its_normalize_env.sh"
fi

# 2) Ставим/обновляем юниты (создаёт API, gateway, consumer; глушит screener.service)
bash "${APP}/scripts/units.sh"

# 3) Стартуем нужные сервисы
systemctl enable --now innertrade-api.service
systemctl enable --now tvoi_gateway.service
systemctl enable --now tvoi_consumer.service

# 4) На всякий случай ещё раз выключим толстый (если всплыл где-то)
systemctl disable --now screener.service 2>/dev/null || true
systemctl mask screener.service 2>/dev/null || true

# 5) Быстрая проверка
echo "--- smoke ---"
curl -fsS http://127.0.0.1:8088/health || true
curl -fsS http://127.0.0.1:8088/signals | head -c 300; echo || true
echo '{"type":"PRE","symbol":"TESTUSDT","tf":"5m","price":123,"link":"http://x"}' \
 | curl -sS -X POST http://127.0.0.1/tvoi -H 'Content-Type: application/json' -d @-
