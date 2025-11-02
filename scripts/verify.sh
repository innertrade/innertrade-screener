#!/usr/bin/env bash
set -euo pipefail
curl -s http://127.0.0.1:8088/health | head -c 400; echo
curl -s http://127.0.0.1:8088/signals | head -c 400; echo
for svc in innertrade-api.service tvoi_gateway.service tvoi_consumer.service push_signals.service; do
  if systemctl is-active --quiet "$svc"; then
    echo "$svc: active"
  else
    echo "$svc: not active"
  fi
done
