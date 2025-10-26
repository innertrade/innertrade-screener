from __future__ import annotations
import argparse
import os
import sys
import time
from typing import Optional, Dict, Any

import requests
import requests as _rq
from dotenv import load_dotenv
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

from push_state import get_push_enabled, set_push_enabled, write_runtime_status, log_event

load_dotenv()

HOST = os.getenv("HOST", "http://127.0.0.1:8080").rstrip("/")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

FORWARD_MIN_Z = float(os.getenv("FORWARD_MIN_Z", "1.8"))
FORWARD_MIN_VOLX = float(os.getenv("FORWARD_MIN_VOLX", "1.6"))
FORWARD_MIN_VOL24H = float(os.getenv("FORWARD_MIN_VOL24H", "20000000"))
FORWARD_MIN_OIZ = float(os.getenv("FORWARD_MIN_OIZ", "0.8"))
FORWARD_OI_WINDOW = int(float(os.getenv("FORWARD_OI_WINDOW", "48")))
FORWARD_OI_INTERVAL = os.getenv("FORWARD_OI_INTERVAL", "5min")
FORWARD_POLL_SEC = int(float(os.getenv("FORWARD_POLL_SEC", "8")))
FLAG_REFRESH_SEC = int(float(os.getenv("PUSH_FLAG_REFRESH_SEC", "3")))

_SENT_BARS: Dict[str, int] = {}


def tg_send_http(token: str, chat_id: int, text: str, reply_markup=None):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if reply_markup and hasattr(reply_markup, "to_dict"):
        payload["reply_markup"] = reply_markup.to_dict()
    try:
        _rq.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[tg_send_http] post failed: {e}")

def _map_symbol_to_bybit_linear(sym: str) -> str:
    s = sym.upper()
    return s if s.endswith("USDT") else f"{s}USDT"

def _fetch_signals() -> Dict[str, Any]:
    r = requests.get(f"{HOST}/signals", timeout=10)
    r.raise_for_status()
    return r.json()

def _classify(symbol: str, z: float, volx: float, v24: float, oiz: Optional[float]) -> Dict[str,str]:
    side = "LONG" if z >= 0 else "SHORT"
    pre = f"PRE-{side}"
    conf = f"{side} CONFIRMED"
    if oiz is None or oiz < FORWARD_MIN_OIZ:
        return {"tag": pre, "icon": "⬆️" if z>=0 else "⬇️", "oi_line": "⏳ Awaiting OI confirmation"}
    return {"tag": conf, "icon": "✅", "oi_line": f"{'🟢' if oiz>=0 else '🔴'} OI Δ={abs(oiz):.2f}σ"}

def _format_tv_symbol(symbol: str) -> str:
    s = symbol.upper()
    return f"{s}.P" if s.endswith("USDT") else f"{s}USDT.P"

def _bybit_futures_link(symbol: str) -> str:
    s = symbol.upper()
    if not s.endswith("USDT"):
        s += "USDT"
    return f"https://www.bybit.com/trade/usdt/{s}"

def _get_oi_z(symbol: str) -> Optional[float]:
    try:
        from oi_fallback import oi_z_score_ext
        return oi_z_score_ext(symbol, interval=FORWARD_OI_INTERVAL, window=FORWARD_OI_WINDOW)
    except Exception:
        return None

def _send_telegram(sig: Dict[str, Any], oiz: Optional[float]):
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID == 0:
        return
    symbol = sig["symbol"]
    z = float(sig.get("zprice") or 0.0)
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    klass = _classify(symbol, z, volx, v24, oiz)
    icon = klass["icon"]
    tag  = klass["tag"]
    lines = [
        f"{icon} {symbol}  ({tag})",
        f"{'🟢' if z>=0 else '🔴'} Price Δ={abs(z):.2f}σ",
        f"🟢 Volume ×{volx:.2f}",
        klass["oi_line"] if oiz is None or oiz < FORWARD_MIN_OIZ else "",
        f"24h Volume ≈ ${v24:,.0f}".replace(","," "),
    ]
    text = "\n".join([ln for ln in lines if ln])
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Bybit Futures", url=_bybit_futures_link(symbol)),
         InlineKeyboardButton("TradingView", url=f"https://www.tradingview.com/chart/?symbol=BYBIT%3A{_format_tv_symbol(symbol)}")]
    ])
    tg_send_http(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text, kb)

def _should_forward(sig: Dict[str, Any]) -> bool:
    z = abs(float(sig.get("zprice") or 0.0))
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    return (z >= FORWARD_MIN_Z and volx >= FORWARD_MIN_VOLX and v24 >= FORWARD_MIN_VOL24H)

def _handle_cli(args: argparse.Namespace) -> int:
    if args.status:
        enabled = get_push_enabled()
        state = "true" if enabled else "false"
        print(f"PUSH_ENABLED: {state}")
        write_runtime_status(enabled=enabled, source="cli", meta={"command": "status"})
        return 0

    if args.set is not None:
        enabled = args.set == "on"
        set_push_enabled(enabled, source="cli")
        print(f"PUSH_ENABLED set to {'ON' if enabled else 'OFF'}")
        return 0

    return 1


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push signals dispatcher")
    parser.add_argument("--status", action="store_true", help="Show current PUSH_ENABLED value")
    parser.add_argument("--set", choices=["on", "off"], help="Toggle PUSH_ENABLED state")
    args = parser.parse_args(argv)
    if args.status and args.set is not None:
        parser.error("--status cannot be combined with --set")
    args.loop = not (args.status or args.set is not None)
    return args


def _sleep_interval(enabled: bool, last_fetch: float, poll_interval: int) -> float:
    since_fetch = max(0.0, time.time() - last_fetch)
    until_next_fetch = max(0.0, poll_interval - since_fetch)
    limit = max(1, FLAG_REFRESH_SEC)
    if until_next_fetch <= 0:
        return float(limit)
    return float(max(1, min(limit, until_next_fetch)))


def forward_loop():
    last_flag: Optional[bool] = None
    last_fetch = 0.0
    poll_interval = max(1, FORWARD_POLL_SEC)

    while True:
        enabled = get_push_enabled()
        if enabled != last_flag:
            log_event(f"PUSH_ENABLED observed as {'ON' if enabled else 'OFF'} by forward_loop")
            last_flag = enabled

        write_runtime_status(enabled=enabled, source="forward_loop", meta={"poll_interval": poll_interval})

        now = time.time()
        if now - last_fetch >= poll_interval:
            last_fetch = now
            try:
                data = _fetch_signals()
                items = data.get("data") or []
                for s in items:
                    symbol = s.get("symbol")
                    bar_ts = int(s.get("bar_ts") or 0)
                    if not symbol or bar_ts <= 0:
                        continue
                    last_seen = _SENT_BARS.get(symbol)
                    if last_seen and last_seen >= bar_ts:
                        continue
                    if not _should_forward(s):
                        continue
                    if not enabled:
                        _SENT_BARS[symbol] = bar_ts
                        continue
                    # double-check state before sending to avoid race with CLI/menu updates
                    if not get_push_enabled():
                        enabled = False
                        _SENT_BARS[symbol] = bar_ts
                        continue
                    oiz = _get_oi_z(symbol)
                    _send_telegram(s, oiz)
                    _SENT_BARS[symbol] = bar_ts
            except Exception as exc:
                log_event(f"forward_loop exception: {exc}", level="ERROR")

        time.sleep(_sleep_interval(enabled, last_fetch, poll_interval))


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.loop:
        return _handle_cli(args)

    forward_loop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
