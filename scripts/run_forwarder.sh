#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# подтянем .env, если есть
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

PY="./.venv/bin/python"

# helper: проверка наличия файла
has() { [[ -f "$1" ]]; }

# основной воркер
if has "forwarder.py"; then
  exec "$PY" -u forwarder.py
elif has "main.py"; then
  exec "$PY" -u main.py
elif has "push_signals.py"; then
  # fallback на старую схему, если confirmed внутри него
  exec "$PY" -u push_signals.py
else
  echo "ERROR: entrypoint not found (forwarder.py/main.py/push_signals.py)" >&2
  exit 1
fi
