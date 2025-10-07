#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Можно передать архив явно: ./bin/restore-working.sh backups/working_YYYY-MM-DD_HHMMSS.tgz
TGZ="${1:-backups/$(readlink -f backups/LATEST_WORKING_TGZ >/dev/null 2>&1 && basename "$(readlink -f backups/LATEST_WORKING_TGZ)" || echo LATEST_WORKING_TGZ)}"

# Если ссылок нет — попробуем напрямую
if [[ ! -f "$TGZ" ]]; then
  if [[ -L backups/LATEST_WORKING_TGZ ]]; then TGZ="$(readlink -f backups/LATEST_WORKING_TGZ)"; fi
fi

if [[ ! -f "$TGZ" ]]; then
  echo "❌ Archive not found: $TGZ"
  echo "Hint: run ./bin/backup-working.sh first."
  exit 1
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
tar -xzf "$TGZ" -C "$TMPDIR"
SNAPDIR="$(find "$TMPDIR" -maxdepth 2 -type d -printf "%p\n" | grep -E '/[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6}$' | head -1)"

echo "Restoring from: $TGZ"
cp -v "${SNAPDIR}/.env" ./.env
cp -v "${SNAPDIR}/push_signals.py" ./push_signals.py

# Подхватим .env и перезапустим форвардер (тихо, идемпотентно)
set +e
pkill -f push_signals.py >/dev/null 2>&1 || true
set -e
set -a; . ./.env; set +a
nohup ./venv/bin/python push_signals.py >/tmp/push_signals.out 2>&1 & disown
sleep 2
echo "--- tail /tmp/push_signals.out ---"
tail -n 60 /tmp/push_signals.out || true
echo "✅ Restore complete."
