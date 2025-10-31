#!/usr/bin/env python3
import os, time, json, shutil, sys
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

APP = Path("/home/deploy/apps/innertrade-screener")
INBOX = APP / "inbox"
PROCESSED = APP / "processed"
FAILED = APP / "failed"

load_dotenv(dotenv_path=APP / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    print(f"{ts} | {msg}", flush=True)

def send_tg(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log("ERR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": "HTML",
        }, timeout=10)
        return r.ok
    except Exception as e:
        log(f"ERR: send_tg exception: {e}")
        return False

def main():
    INBOX.mkdir(parents=True, exist_ok=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)
    FAILED.mkdir(parents=True, exist_ok=True)

    log(f"TVOI consumer start | inbox={INBOX}")

    while True:
        for f in sorted(INBOX.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                typ = data.get("type", "PRE")
                sym = data.get("symbol", "?")
                tf  = data.get("tf", "?")
                price = data.get("price", "?")
                link = data.get("link", "")

                text = f"🟢 <b>{typ}</b> {sym} ({tf})\nPrice: {price}\n{link}"
                ok = send_tg(text)
                if ok:
                    shutil.move(str(f), PROCESSED / f.name)
                    log(f"TVOI PRE sent=True | file={f.name}")
                else:
                    shutil.move(str(f), FAILED / f.name)
                    log(f"TVOI PRE sent=False | file={f.name}")
            except Exception as e:
                try:
                    shutil.move(str(f), FAILED / f.name)
                except Exception:
                    pass
                log(f"TVOI PRE sent=False | file={f.name} | err={e}")
        time.sleep(1.0)

if __name__ == "__main__":
    main()
