#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

TS="$(date +%F_%H%M%S)"
SNAP_DIR="backups/${TS}"
mkdir -p "$SNAP_DIR"

# Что сохраняем (добавляй файлы по мере надобности)
FILES=(
  ".env"
  "push_signals.py"
)

# Проверим, что файлы существуют (тихо пропускаем отсутствующие)
for f in "${FILES[@]}"; do
  test -f "$f" && cp -v "$f" "$SNAP_DIR"/
done

# Манифест: версии, хэши, пороги
{
  echo "timestamp=${TS}"
  echo "working_dir=$(pwd)"
  echo "python_version=$("./venv/bin/python" -V 2>&1 || echo n/a)"
  echo "commit_hint=$(git rev-parse --short HEAD 2>/dev/null || echo no-git)"
  echo "--- FORWARD_* from .env ---"
  grep -E '^(FORWARD_|HOST=|TELEGRAM_)' .env 2>/dev/null || true
  echo "--- sha256 ---"
  (cd "$SNAP_DIR" && sha256sum ./* 2>/dev/null || true)
} > "${SNAP_DIR}/manifest.txt"

# Упакуем снапшот (самодостаточный архив)
tar -czf "backups/working_${TS}.tgz" -C backups "${TS}"

# Пометим этот снапшот как последний УСПЕШНЫЙ
ln -sfn "${TS}" backups/LATEST_WORKING_DIR
ln -sfn "working_${TS}.tgz" backups/LATEST_WORKING_TGZ

echo "✅ Backup done:"
echo "  - dir: backups/${TS}"
echo "  - tgz: backups/working_${TS}.tgz"
echo "  - latest pointers: backups/LATEST_WORKING_DIR -> ${TS}, LATEST_WORKING_TGZ -> working_${TS}.tgz"
