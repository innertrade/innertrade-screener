#!/usr/bin/env bash
set -euo pipefail
curl -s http://127.0.0.1:8088/health | head -c 400; echo
curl -s http://127.0.0.1:8088/signals | head -c 400; echo
systemctl is-active --quiet push_signals.service && echo "push_signals: active" || echo "push_signals: not active"
systemctl is-active --quiet menu_bot.service  && echo "menu_bot: active"  || echo "menu_bot: not active"
