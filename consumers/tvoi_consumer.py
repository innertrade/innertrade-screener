#!/usr/bin/env python3
"""File-system based TVOI consumer."""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telegram import Bot

APP_ROOT = Path(os.environ.get("APP_ROOT", Path(__file__).resolve().parents[1]))
INBOX = APP_ROOT / "inbox"
PROCESSED = APP_ROOT / "processed"
FAILED = APP_ROOT / "failed"

load_dotenv(dotenv_path=APP_ROOT / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _log(message: str, *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    print(f"{_now_iso()} | {message}", file=stream, flush=True)


def _ensure_dirs() -> None:
    for directory in (INBOX, PROCESSED, FAILED):
        directory.mkdir(parents=True, exist_ok=True)


def _send_pre(bot: Bot, payload: dict) -> None:
    msg_type = payload.get("type", "PRE")
    sym = payload.get("symbol", "?")
    tf = payload.get("tf", "?")
    price = payload.get("price", "?")
    link = payload.get("link", "")
    text = f"🟢 {msg_type} {sym} ({tf}) | price={price}"
    if link:
        text += f"\n{link}"
    bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)


def main() -> int:
    if not BOT_TOKEN or not CHAT_ID:
        _log("TVOI consumer stop | reason=missing TELEGRAM credentials", error=True)
        return 2

    _ensure_dirs()
    bot = Bot(token=BOT_TOKEN)
    _log(f"TVOI consumer start | inbox={INBOX}")

    while True:
        files = sorted(INBOX.glob("*.json"))
        if not files:
            time.sleep(2)
            continue

        for path in files:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                _send_pre(bot, payload)
                shutil.move(str(path), str(PROCESSED / path.name))
                _log(f"TVOI PRE sent=True | file={path.name}")
            except Exception as exc:  # pragma: no cover - runtime guard
                try:
                    shutil.move(str(path), str(FAILED / path.name))
                except Exception:
                    pass
                _log(f"TVOI PRE sent=False | file={path.name} | reason={exc}", error=True)
        time.sleep(0.5)


if __name__ == "__main__":
    sys.exit(main())
