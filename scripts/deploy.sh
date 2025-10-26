#!/usr/bin/env bash
set -euo pipefail

APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP"

# 1) venv + зависимости (идемпотентно)
if [ ! -x ".venv/bin/python" ]; then python3 -m venv .venv; fi
. .venv/bin/activate
pip install -U pip wheel >/dev/null
pip install -r requirements.txt

# 2) sanity: компиляция и сигнатура OI
python -m py_compile main.py push_signals.py menu_bot.py
grep -q 'intervalTime' main.py  # ключ Bybit для OI

# 3) перезапуск сервисов (если уже установлены)
systemctl restart push_signals.service || true
systemctl restart menu_bot.service || true
