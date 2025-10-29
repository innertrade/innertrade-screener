#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv
load_dotenv()

# Если используется aiogram/telebot — не трогаем зависимости проекта.
# Ниже простой интерфейс через requests к Telegram, чтобы не конфликтовать с версиями.
import requests

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("push_signals")

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "state" / "push_enabled.json"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# предположим, что ваш сигнальный движок кладёт сюда данные,
# либо этот модуль сам их формирует; оставляем интерфейс функции send_* гибким.
TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

DEFAULT_STATE = {"tvoi": True, "trnd": False}


def _safe_read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return dict(default)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, bool):
            return {"tvoi": bool(data), "trnd": bool(data)}
        if isinstance(data, dict):
            if "enabled" in data and ("tvoi" not in data or "trnd" not in data):
                en = bool(data.get("enabled"))
                data = {"tvoi": en, "trnd": en}
            for k, v in default.items():
                data.setdefault(k, v)
            return data
        return dict(default)
    except Exception as e:
        logger.exception("Failed to read state: %s", e)
        return dict(default)


def get_state() -> Dict[str, Any]:
    return _safe_read_json(STATE_FILE, DEFAULT_STATE)


def _tg_send_message(text: str, parse_mode: Optional[str] = "HTML") -> bool:
    if not TG_API or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not set (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")
        return False
    try:
        r = requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code == 200 and r.json().get("ok"):
            return True
        logger.error("TG send failed: %s", r.text)
        return False
    except Exception as e:
        logger.exception("TG send exception: %s", e)
        return False


def send_tvoi_signal(payload: Dict[str, Any]) -> None:
    """
    Отправка PRE/CONFIRMED по TVOI.
    payload ожидается формата:
      {
        "type": "PRE" | "CONFIRMED",
        "symbol": "BTCUSDT",
        "tf": "5m",
        "price": 67890.1,
        "link": "https://…"
      }
    """
    st = get_state()
    if not st.get("tvoi", True):
        logger.info("TVOI disabled, skip")
        return

    t = payload.get("type", "PRE")
    sym = payload.get("symbol", "?")
    tf = payload.get("tf", "5m")
    price = payload.get("price", "")
    link = payload.get("link", "")
    msg = f"📡 <b>TVOI {t}</b>\n• {sym} ({tf})\n• price: <code>{price}</code>\n{link}"
    ok = _tg_send_message(msg)
    logger.info("TVOI %s sent=%s", t, ok)


def send_trnd_signal(payload: Dict[str, Any]) -> None:
    """
    Отправка сигналов по стратегии тренда (TRND).
    payload аналогичен, допускается доп. поля.
    """
    st = get_state()
    if not st.get("trnd", False):
        logger.info("TRND disabled, skip")
        return

    t = payload.get("type", "SIGNAL")
    sym = payload.get("symbol", "?")
    tf = payload.get("tf", "5m")
    price = payload.get("price", "")
    link = payload.get("link", "")
    msg = f"📈 <b>TRND {t}</b>\n• {sym} ({tf})\n• price: <code>{price}</code>\n{link}"
    ok = _tg_send_message(msg)
    logger.info("TRND %s sent=%s", t, ok)


if __name__ == "__main__":
    logger.info("push_signals boot")
    st = get_state()
    logger.info("Active flags at start: TVOI=%s, TRND=%s", st.get("tvoi"), st.get("trnd"))

    # Пример тестового пинга (не мешает продакшен-логике, можно закомментировать):
    if os.getenv("PUSH_TEST", "0") == "1":
        send_tvoi_signal({"type": "PRE", "symbol": "BTCUSDT", "tf": "5m", "price": 123, "link": "https://example.com"})
        send_trnd_signal({"type": "SIGNAL", "symbol": "ETHUSDT", "tf": "5m", "price": 456, "link": "https://example.com"})
