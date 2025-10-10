#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
InnerTrade Screener — Push Forwarder
- Периодически запрашивает /signals у main.py
- Фильтрует по заданным порогам (z, volx, vol24h)
- (опционально) рассчитывает OI z-score через pre_forwarder.oi_z_score
- Классифицирует pre_* / confirmed_* и шлёт в Telegram

Зависимости: requests
Запуск под systemd, env подхватывается из EnvironmentFile (.env)
"""

import os
import time
import json
import math
import logging
import requests
from typing import Any, Dict, Optional, List, Tuple

# --- OI z-score (готовый модуль) ---
try:
    from pre_forwarder import oi_z_score
except Exception as e:
    oi_z_score = None
    _oi_import_err = e

# ---------- Конфиг из ENV ----------
HOST                 = os.getenv("HOST", "http://127.0.0.1:8080")
HTTP_PORT            = int(os.getenv("HTTP_PORT", "8080"))

FORWARD_POLL_SEC     = int(os.getenv("FORWARD_POLL_SEC", "8"))

FORWARD_MIN_Z        = float(os.getenv("FORWARD_MIN_Z", "1.8"))
FORWARD_MIN_VOLX     = float(os.getenv("FORWARD_MIN_VOLX", "1.6"))
FORWARD_MIN_VOL24H   = float(os.getenv("FORWARD_MIN_VOL24H", "20000000"))
FORWARD_MIN_OIZ      = float(os.getenv("FORWARD_MIN_OIZ", "0.8"))

OI_ENABLE            = os.getenv("OI_ENABLE", "0") == "1"
OI_SOURCE            = os.getenv("OI_SOURCE", "bybit")
FORWARD_OI_INTERVAL  = os.getenv("FORWARD_OI_INTERVAL", "5min")
FORWARD_OI_WINDOW    = int(os.getenv("FORWARD_OI_WINDOW", "48"))

TG_TOKEN             = os.getenv("TG_TOKEN", "")
TG_CHAT_ID           = os.getenv("TG_CHAT_ID", "")
TG_LOG_CHAT          = os.getenv("TG_LOG_CHAT", TG_CHAT_ID)

ADAPTIVE             = os.getenv("ADAPTIVE", "false").lower() in ("1","true","yes")

# ---------- Логирование ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | forwarder | %(levelname)s | %(message)s",
)
logger = logging.getLogger("forwarder")

# ---------- HTTP сессия ----------
http = requests.Session()
http.headers.update({"User-Agent": "innertrade-forwarder/1.0"})

# ---------- Telegram ----------
def tg_send_text(chat_id: str, text: str, buttons: Optional[List[List[Tuple[str,str]]]] = None) -> None:
    if not TG_TOKEN or not chat_id:
        logger.warning("Telegram not configured (missing TG_TOKEN or chat_id)")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if buttons:
        keyboard = [
            [{"text": btxt, "url": burl} for (btxt, burl) in row]
            for row in buttons
        ]
        payload["reply_markup"] = json.dumps({"inline_keyboard": keyboard})
    try:
        r = http.post(url, data=payload, timeout=8)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")

# ---------- Утилиты форматирования ----------
def fmt_sign(x: Optional[float], digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "None"
    s = f"{x:+.{digits}f}"
    return s

def fmt_usd(n: Optional[float]) -> str:
    try:
        n = float(n)
    except:
        return "?"
    if n >= 1_000_000_000:
        return f"${n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n/1_000:.2f}K"
    return f"${n:.0f}"

# ---------- Получение сигналов ----------
def fetch_signals() -> List[Dict[str, Any]]:
    url = f"{HOST.rstrip('/')}/signals"
    try:
        r = http.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
        # Ожидаем формат: { "data": [ {symbol, z, volx, vol24h_usd, direction, price, ...}, ... ] }
        return data.get("data", []) if isinstance(data, dict) else []
    except Exception as e:
        logger.error(f"GET /signals failed: {e}")
        return []

# ---------- Фильтры ----------
def pass_base_filters(item: Dict[str, Any]) -> bool:
    try:
        z = float(item.get("z") or item.get("zprice") or 0.0)
        volx = float(item.get("volx") or item.get("vol_mult") or item.get("volume_mult") or 0.0)
        vol24 = float(item.get("vol24h_usd") or item.get("vol24_usd") or 0.0)
    except Exception:
        return False

    # Лонг/шорт базовые условия
    if z >= FORWARD_MIN_Z and volx >= FORWARD_MIN_VOLX and vol24 >= FORWARD_MIN_VOL24H:
        return True
    if z <= -FORWARD_MIN_Z and volx >= FORWARD_MIN_VOLX and vol24 >= FORWARD_MIN_VOL24H:
        return True
    return False

def direction_of(item: Dict[str, Any]) -> Optional[str]:
    # Пытаемся взять готовое поле, иначе по знаку z:
    d = item.get("direction")
    if isinstance(d, str) and d.lower() in ("long", "short"):
        return d.lower()
    try:
        z = float(item.get("z") or item.get("zprice") or 0.0)
        return "long" if z > 0 else "short"
    except:
        return None

# ---------- OI z-score ----------
def compute_oi_z(symbol: str) -> Optional[float]:
    if not OI_ENABLE or FORWARD_MIN_OIZ <= 0:
        return None
    if oi_z_score is None:
        logger.warning(f"OI disabled: pre_forwarder import failed: {_oi_import_err!r}")
        return None
    try:
        return oi_z_score(
            symbol,
            interval=FORWARD_OI_INTERVAL,
            window=FORWARD_OI_WINDOW
        )
    except Exception as e:
        logger.warning(f"OI z fetch failed for {symbol}: {e}")
        return None

# ---------- Классификация ----------
def classify(item: Dict[str, Any], oi_z: Optional[float]) -> Optional[str]:
    d = direction_of(item)
    if d is None:
        return None
    # pre-метка всегда, если прошли базовые фильтры
    if d == "long":
        status = "pre_long"
        if oi_z is not None and oi_z >= FORWARD_MIN_OIZ:
            status = "confirmed_long"
        return status
    elif d == "short":
        status = "pre_short"
        if oi_z is not None and oi_z <= -FORWARD_MIN_OIZ:
            status = "confirmed_short"
        return status
    return None

# ---------- Кнопки (Bybit / TradingView) ----------
def build_buttons(symbol: str) -> List[List[Tuple[str,str]]]:
    # Простейшие ссылки; при желании подставь свои
    base = symbol.upper().replace("PERP","").replace("-USDT","USDT")
    sym_tv = f"{base}"
    tv_url = f"https://www.tradingview.com/chart/?symbol=BYBIT:{sym_tv}"
    bybit_url = f"https://www.bybit.com/en-US/trade/spot/{base}"
    return [
        [("TradingView", tv_url), ("Bybit", bybit_url)]
    ]

# ---------- Сообщение ----------
def compose_text(item: Dict[str, Any], status: str, oi_z: Optional[float]) -> str:
    sym = str(item.get("symbol") or item.get("sym") or "?").upper()
    price = item.get("price")
    z = None
    volx = None
    vol24 = None
    try:
        z = float(item.get("z") or item.get("zprice"))
    except: pass
    try:
        volx = float(item.get("volx") or item.get("vol_mult") or item.get("volume_mult"))
    except: pass
    try:
        vol24 = float(item.get("vol24h_usd") or item.get("vol24_usd"))
    except: pass

    arrow = "🟢" if "long" in status else "🔴"
    dir_txt = "LONG" if "long" in status else "SHORT"
    conf_badge = "✅ CONFIRMED" if status.startswith("confirmed") else "PRE"

    parts = [
        f"{arrow} <b>{sym}</b>  (<b>{dir_txt}</b>)  <i>{conf_badge}</i>",
        f"z={fmt_sign(z)} | vol×{(f'{volx:.2f}' if isinstance(volx, float) else volx)} | OI z={fmt_sign(oi_z)}",
    ]
    if isinstance(vol24, float):
        parts.append(f"24h volume ≈ {fmt_usd(vol24)}")
    if isinstance(price, (float,int)):
        parts.append(f"Price ≈ {price}")
    return "\n".join(parts)

# ---------- Основной цикл ----------
def main() -> None:
    logger.info("push_forwarder started")
    if OI_ENABLE and oi_z_score is None:
        logger.warning("OI requested but pre_forwarder is unavailable — OI will be None")

    while True:
        try:
            items = fetch_signals()
            if not isinstance(items, list):
                items = []

            for it in items:
                if not pass_base_filters(it):
                    continue

                sym = str(it.get("symbol") or it.get("sym") or "").upper()
                # Расчёт OI z (если включено)
                oi_val = compute_oi_z(sym)

                status = classify(it, oi_val)
                if status is None:
                    continue

                text = compose_text(it, status, oi_val)
                buttons = build_buttons(sym)

                # confirmed — в основной канал; pre — в лог/приват
                dest_chat = TG_CHAT_ID if status.startswith("confirmed") else TG_LOG_CHAT
                tg_send_text(dest_chat, text, buttons=buttons)

                logger.info(f"sent to TG | {sym} | {status} | {text.replace(os.linesep,' ')}")

        except Exception as e:
            logger.error(f"loop error: {e}")

        time.sleep(max(2, FORWARD_POLL_SEC))

# ---------- Entry ----------
if __name__ == "__main__":
    main()
