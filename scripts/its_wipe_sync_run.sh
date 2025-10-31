#!/usr/bin/env bash
# Complete wipe & sync of Innertrade Screener on VPS.
set -euo pipefail

log() {
  printf '%s | %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

REPO_URL=${REPO_URL:-"https://github.com/innertrade/innertrade-screener.git"}
BRANCH=${BRANCH:-"main"}
APP_DIR=${APP_DIR:-"/home/deploy/apps/innertrade-screener"}
SERVICES=${SERVICES:-"screener.service push_signals.service menu_bot.service"}
PYTHON_BIN=${PYTHON_BIN:-"python3"}
PORT=${PORT:-"8088"}
DEPLOY_USER=${DEPLOY_USER:-"deploy"}
ENV_FILE=${ENV_FILE:-"$APP_DIR/.env"}

TMP_ENV=""

cleanup() {
  if [[ -n "$TMP_ENV" && -f "$TMP_ENV" ]]; then
    rm -f "$TMP_ENV"
  fi
}

trap cleanup EXIT

backup_env_if_present() {
  if [[ -f "$ENV_FILE" ]]; then
    TMP_ENV=$(mktemp)
    cp "$ENV_FILE" "$TMP_ENV"
    chmod 600 "$TMP_ENV"
    log "backed up existing .env"
  fi
}

restore_env() {
  if [[ -n "$TMP_ENV" && -f "$TMP_ENV" ]]; then
    install -m 600 "$TMP_ENV" "$ENV_FILE"
  else
    install -m 600 /dev/null "$ENV_FILE"
  fi
  chown "$DEPLOY_USER":"$DEPLOY_USER" "$ENV_FILE"
}

apply_env_vars() {
  local key value
  for key in TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID; do
    value=${!key-}
    if [[ -n "$value" ]]; then
      APP_DIR="$APP_DIR" ENV_KEY="$key" ENV_VALUE="$value" python3 - <<'PY'
import os
from pathlib import Path

app_dir = Path(os.environ["APP_DIR"])
env_path = app_dir / ".env"
env_path.parent.mkdir(parents=True, exist_ok=True)

lines = []
if env_path.exists():
    with env_path.open("r", encoding="utf-8") as fh:
        lines = [line.rstrip("\n") for line in fh]

key = os.environ["ENV_KEY"]
value = os.environ["ENV_VALUE"]

out_lines = []
updated = False
for line in lines:
    if line.startswith(f"{key}="):
        out_lines.append(f"{key}={value}")
        updated = True
    else:
        out_lines.append(line)

if not updated:
    out_lines.append(f"{key}={value}")

with env_path.open("w", encoding="utf-8") as fh:
    fh.write("\n".join(out_lines).rstrip("\n"))
    if out_lines:
        fh.write("\n")
PY
    fi
  done
  chown "$DEPLOY_USER":"$DEPLOY_USER" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}

run_as_deploy() {
  local cmd=$1
  if [[ $(id -un) == "$DEPLOY_USER" ]]; then
    bash -lc "$cmd"
  else
    sudo -u "$DEPLOY_USER" bash -lc "$cmd"
  fi
}

log "[1/8] stopping systemd service(s) if running"
for svc in $SERVICES; do
  if systemctl list-units --full --all | grep -Fq "$svc"; then
    systemctl stop "$svc" || true
  else
    log "service $svc not found, skipping stop"
  fi
done

log "[2/8] backing up configuration and removing old application directory"
backup_env_if_present
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR"
chown "$DEPLOY_USER":"$DEPLOY_USER" "$APP_DIR"

log "[3/8] cloning $REPO_URL@$BRANCH"
run_as_deploy "git clone --branch $BRANCH --depth 1 $REPO_URL $APP_DIR"

log "[4/8] restoring .env and applying secrets if provided"
restore_env
apply_env_vars

log "[5/8] creating virtualenv and installing requirements"
run_as_deploy "cd $APP_DIR && $PYTHON_BIN -m venv .venv"
run_as_deploy "cd $APP_DIR && ./.venv/bin/pip install --upgrade pip wheel"
run_as_deploy "cd $APP_DIR && ./.venv/bin/pip install -r requirements.txt"
run_as_deploy "cd $APP_DIR && ./.venv/bin/pip install certifi"

PYTHON_BIN_PATH="$APP_DIR/.venv/bin/python"
if [[ -x "$PYTHON_BIN_PATH" ]]; then
  CERT_PATH="$($PYTHON_BIN_PATH -c 'import certifi, sys; sys.stdout.write(certifi.where())')"
  ln -sf "$CERT_PATH" "$APP_DIR/cacert.pem"
  chown -h "$DEPLOY_USER":"$DEPLOY_USER" "$APP_DIR/cacert.pem"
fi

log "[6/8] verifying main.py integrity"
run_as_deploy "cd $APP_DIR && ./.venv/bin/python scripts/detect_main_drift.py --ref HEAD"
run_as_deploy "cd $APP_DIR && ./.venv/bin/python -m compileall main.py"

log "[7/8] installing systemd units"
sudo APP_ROOT="$APP_DIR" APP_USER="$DEPLOY_USER" APP_GROUP="$DEPLOY_USER" "$APP_DIR"/scripts/units.sh

log "[8/8] starting service(s) and smoke-check"
sudo systemctl daemon-reload
for svc in $SERVICES; do
  sudo systemctl enable "$svc" || true
  sudo systemctl restart "$svc" || true
done

sleep 5
curl -fsS "http://127.0.0.1:${PORT}/health" || { log "health endpoint failed"; exit 1; }
curl -fsS "http://127.0.0.1:${PORT}/signals" | head -c 600 || true

echo
log "wipe & sync complete"
