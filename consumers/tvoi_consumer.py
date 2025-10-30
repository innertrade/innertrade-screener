#!/usr/bin/env python3
import os, time, json, shutil, sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from telegram import Bot

APP = Path("/home/deploy/apps/innertrade-screener")
INBOX = APP / "inbox"
PROCESSED = APP / "processed"
FAILED = APP / "failed"

load_dotenv(dotenv_path=APP / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # числовой ID или @channelusername

if not BOT_TOKEN or not CHAT_ID:
    print("ERR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы в .env", file=sys.stderr)
    sys.exit(2)

bot = Bot(token=BOT_TOKEN)

def send_pre(msg: dict):
    t = msg.get("type", "PRE")
    sym = msg.get("symbol", "?")
    tf = msg.get("tf", "?")
    price = msg.get("price", "?")
    link = msg.get("link", "")
    text = f"🟢 {t} {sym} ({tf}) | price={price}"
    if link:
        text += f"\n{link}"
    bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)

def main():
    for d in (INBOX, PROCESSED, FAILED):
        d.mkdir(parents=True, exist_ok=True)
    print(f"{datetime.utcnow().isoformat()}Z | TVOI consumer start | watching {INBOX}", flush=True)
    while True:
        files = sorted(INBOX.glob("*.json"))
        if not files:
            time.sleep(2)
            continue
        for p in files:
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                send_pre(data)
                shutil.move(str(p), str(PROCESSED / p.name))
                print(f"{datetime.utcnow().isoformat()}Z | TVOI PRE sent=True | {p.name}", flush=True)
            except Exception as e:
                try:
                    shutil.move(str(p), str(FAILED / p.name))
                except Exception:
                    pass
                print(f"{datetime.utcnow().isoformat()}Z | TVOI PRE sent=False | file={p.name} | err={e}",
                      file=sys.stderr, flush=True)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
