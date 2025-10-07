#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import logging
import requests

# ---------- Конфигурация из ENV (с дефолтами для локального запуска) ----------
HOST                = os.getenv("HOST", "http://127.0.0.1:8080")  # где крутится /signals
ENGINE_URL          = f"{HOST.rstrip('/')}/signals"

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")

FORWARD_MIN_Z       = float(os.getenv("FORWARD_MIN_Z", "1.8"))
FORWARD_MIN_VOLX    = float(os.getenv("FORWARD_MIN_VOLX", "1.6"))
FORWARD_MIN_VOL24H  = float(os.getenv("FORWARD_MIN_VOL24H", "20000000"))
FORWARD_MIN_OIZ     = float(os.getenv("FORWARD_MIN_OIZ", "0.8"))
FORWARD_POLL_SEC    = int(os.getenv("FORWARD_POLL_SEC", "8"))

# Жёсткая «якорная» анти-дубль логика: одна отправка на символ в рамках 5-мин свечи
# (используем bar_ts из движка; 5 минут = 300000 мс)
FIVE_MIN_MS = 300_000
_last_sent_candle_by_symbol = {}  # sym -> candle_id (bar_ts // 300000)

# ---------- Логирование ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

# ---------- Telegram ----------
def tg_send_text(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID; skip send")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        return resp.ok
    except Exception as e:
        logging.error(f"tg_send_text error: {e}")
        return False

# ---------- Форматирование сообщений ----------
def human_money(n: float) -> str:
    try:
        if n >= 1e9:  return f"${n/1e9:.1f}B"
        if n >= 1e6:  return f"${n/1e6:.1f}M"
        if n >= 1e3:  return f"${n/1e3:.1f}K"
        return f"${n:.0f}"
    except Exception:
        return f"${n}"

def classify(sig) -> tuple[str, str]:
    """
    Возвращает (direction, level)
      direction: 'LONG' | 'SHORT'
      level: 'PRE' | 'CONFIRMED'
    """
    z = float(sig.get("zprice") or 0.0)
    oi_z = sig.get("oi_z", None)
    direction = "LONG" if z >= 0 else "SHORT"
    level = "PRE"
    if oi_z is not None:
        try:
            if float(oi_z) >= FORWARD_MIN_OIZ:
                level = "CONFIRMED"
        except Exception:
            pass
    return direction, level

def format_message(sig) -> str:
    sym = str(sig.get("symbol","")).upper()
    z = float(sig.get("zprice") or 0.0)
    vx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    oi_z = sig.get("oi_z", None)
    direction, level = classify(sig)

    arrow = "⬆️" if direction == "LONG" else "⬇️"
    preface = f"{arrow} <b>{sym}</b>  ({'PRE-' if level=='PRE' else ''}{direction if level=='PRE' else direction+' CONFIRMED'})"

    lines = [
        preface,
        f"{'🟢' if z>=0 else '🔴'} Price Δ={z:+.2f}σ",
        f"🟢 Volume ×{vx:.2f}",
    ]
    if level == "CONFIRMED":
        lines.append(f"{'🟢' if oi_z and oi_z>=0 else '🔴'} OI Δ={float(oi_z):+.2f}σ")
    else:
        lines.append("⏳ Awaiting OI confirmation")
    lines.append(f"24h Volume ≈ {human_money(v24)}")
    return "\n".join(lines)

# ---------- Фильтрация по жёстким порогам ----------
def pass_thresholds(sig) -> bool:
    try:
        z = float(sig.get("zprice") or 0.0)
        vx = float(sig.get("vol_mult") or 0.0)
        v24 = float(sig.get("vol24h_usd") or 0.0)
        if abs(z) < FORWARD_MIN_Z:        return False
        if vx < FORWARD_MIN_VOLX:         return False
        if v24 < FORWARD_MIN_VOL24H:      return False
        return True
    except Exception:
        return False

# ---------- Анти-дубль на уровне свечи (жёсткая норма работы) ----------
def not_sent_this_candle(sig) -> bool:
    sym = str(sig.get("symbol","")).upper()
    bts = int(sig.get("bar_ts") or 0)
    if bts <= 0:
        # без бар-таймстампа мы не знаем свечу — считаем, что нельзя
        return False
    cid = bts // FIVE_MIN_MS
    last = _last_sent_candle_by_symbol.get(sym)
    if last == cid:
        return False
    _last_sent_candle_by_symbol[sym] = cid
    return True

# ---------- Основной цикл ----------
def poll_once():
    try:
        resp = requests.get(ENGINE_URL, timeout=10)
        data = resp.json().get("data", []) if resp.ok else []
    except Exception as e:
        logging.error(f"poll error: {e}")
        return

    sent = 0
    for sig in data:
        if not pass_thresholds(sig):
            continue
        if not not_sent_this_candle(sig):
            continue
        text = format_message(sig)
        ok = tg_send_text(text)
        if ok:
            sym = str(sig.get("symbol","")).upper()
            z   = float(sig.get("zprice") or 0.0)
            vx  = float(sig.get("vol_mult") or 0.0)
            oi_z= sig.get("oi_z", None)
            logging.info(f"sent to TG | {sym} | "
                         f"{'pre_' if 'Awaiting' in text else ''}{'long' if z>=0 else 'short'} | "
                         f"z={z:.2f} volx={vx:.2f} oi_z={oi_z}")
            sent += 1

    if sent == 0:
        logging.info("no matches this tick")

def main():
    logging.info(f"forwarder start | host={ENGINE_URL} "
                 f"thresholds: |z|≥{FORWARD_MIN_Z}, vx≥{FORWARD_MIN_VOLX}, v24≥{FORWARD_MIN_VOL24H:,}, oi≥{FORWARD_MIN_OIZ}")
    while True:
        poll_once()
        time.sleep(FORWARD_POLL_SEC)

if __name__ == "__main__":
    main()
