"""TVOI HTTP gateway.

Accepts POST /tvoi with JSON payloads and enqueues them into the
filesystem inbox directory. The queue is consumed by
``consumers/tvoi_consumer.py``.
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

APP_ROOT = Path(os.environ.get("APP_ROOT", Path(__file__).resolve().parents[1]))
QUEUE_DIR = Path(os.environ.get("QUEUE_DIR", APP_ROOT / "inbox"))
TMP_DIR = QUEUE_DIR / ".tmp"

app = Flask(__name__)


def _ensure_queue_dirs() -> None:
    for path in (QUEUE_DIR, TMP_DIR):
        path.mkdir(parents=True, exist_ok=True)


_ensure_queue_dirs()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@app.get("/health")
def health() -> Any:
    return jsonify({
        "status": "ok",
        "queue": str(QUEUE_DIR),
    })


@app.post("/tvoi")
def enqueue_signal() -> Any:
    try:
        payload = request.get_json(force=True)
    except Exception as exc:  # pragma: no cover - defensive logging
        return jsonify({"ok": False, "error": f"invalid json: {exc}"}), 400

    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "payload must be an object"}), 400

    filename = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex}.json"
    tmp_path = TMP_DIR / filename
    final_path = QUEUE_DIR / filename

    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
        os.replace(tmp_path, final_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        print(f"{_now_iso()} | TVOI gateway enqueue fail | file={filename} | err={exc}", file=sys.stderr, flush=True)
        return jsonify({"ok": False, "error": "failed to write queue file"}), 500

    print(f"{_now_iso()} | TVOI gateway enqueue ok | file={filename}", flush=True)
    return jsonify({"ok": True, "file": filename})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "8787")))
