#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/deploy/apps/innertrade-screener"
STATE_DIR="$APP_DIR/state"
STATE_FILE="$STATE_DIR/push_enabled.json"
LOG_FILE="/var/log/its_normalize.log"

mkdir -p "$(dirname "$LOG_FILE")" || true
exec >>"$LOG_FILE" 2>&1

ts() { date +"%Y-%m-%d %H:%M:%S"; }

echo "$(ts) === its_normalize_env.sh start ==="

# 1) проверим .env (только предупреждаем — не валим пайплайн)
if [ -f "$APP_DIR/.env" ]; then
  # shellcheck disable=SC2046
  export $(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$APP_DIR/.env" | xargs -d '\n')
else
  echo "$(ts) WARN: .env not found at $APP_DIR/.env"
fi

# 2) создадим/мигрируем state
mkdir -p "$STATE_DIR"
if [ ! -f "$STATE_FILE" ]; then
  echo '{"tvoi": true, "trnd": false}' > "$STATE_FILE"
  echo "$(ts) State initialized: $STATE_FILE"
else
  # миграция форматов: bool или {"enabled": bool}
  CONTENT="$(cat "$STATE_FILE")"
  if [ "$CONTENT" = "true" ] || [ "$CONTENT" = "false" ]; then
    if [ "$CONTENT" = "true" ]; then
      echo '{"tvoi": true, "trnd": true}' > "$STATE_FILE"
    else
      echo '{"tvoi": false, "trnd": false}' > "$STATE_FILE"
    fi
    echo "$(ts) State migrated from boolean → dict"
  else
    # если нет ключей — добавим по умолчанию
    if ! grep -q '"tvoi"' "$STATE_FILE"; then
      TMP="$(mktemp)"
      jq '. + {"tvoi": true}' "$STATE_FILE" > "$TMP" || echo '{"tvoi": true}' > "$TMP"
      mv "$TMP" "$STATE_FILE"
      echo "$(ts) Added missing key: tvoi"
    fi
    if ! grep -q '"trnd"' "$STATE_FILE"; then
      TMP="$(mktemp)"
      jq '. + {"trnd": false}' "$STATE_FILE" > "$TMP" || echo '{"trnd": false}' > "$TMP"
      mv "$TMP" "$STATE_FILE"
      echo "$(ts) Added missing key: trnd"
    fi
  fi
fi

echo "$(ts) Current state:"
cat "$STATE_FILE" || true

# 3) sanity for tokens (только предупреждаем)
if [ -z "${MENU_BOT_TOKEN:-}" ]; then
  echo "$(ts) WARN: MENU_BOT_TOKEN is not set"
fi
if [ -z "${TELEGRAM_BOT_TOKEN:-}" ]; then
  echo "$(ts) WARN: TELEGRAM_BOT_TOKEN is not set"
fi
if [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
  echo "$(ts) WARN: TELEGRAM_CHAT_ID is not set"
fi

# 4) перезапуск сервисов, если есть
reload_if_exists() {
  local svc="$1"
  if systemctl list-unit-files | grep -q "^${svc}"; then
    echo "$(ts) restarting $svc"
    systemctl --no-pager --full restart "$svc" || true
    sleep 1
    systemctl --no-pager --full status "$svc" | sed -n '1,20p' || true
  else
    echo "$(ts) INFO: systemd unit not found: $svc"
  fi
}

reload_if_exists "menu_bot.service"
reload_if_exists "push_signals.service"

echo "$(ts) === its_normalize_env.sh done ==="
