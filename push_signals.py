from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from state_manager import (
    ensure_state_file,
    get_push_enabled,
    get_trend_enabled,
    set_push_enabled,
    set_trend_enabled,
)

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
FORWARD_TIMEOUT = int(float(os.getenv("FORWARD_TIMEOUT", "10")))

logger = logging.getLogger("push_signals")
_session = requests.Session()
_SENT_BARS: Dict[str, int] = {}


def tg_send_http(token: str, chat_id: int, text: str, reply_markup=None) -> bool:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup and hasattr(reply_markup, "to_dict"):
        payload["reply_markup"] = reply_markup.to_dict()
    try:
        response = _session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=FORWARD_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Telegram send failed: %s", exc)
        return False
    return True


def _map_symbol_to_bybit_linear(sym: str) -> str:
    s = sym.upper()
    return s if s.endswith("USDT") else f"{s}USDT"


def _fetch_signals() -> Dict[str, Any]:
    response = _session.get(f"{HOST}/signals", timeout=FORWARD_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _classify(symbol: str, z: float, oiz: Optional[float]) -> Dict[str, str]:
    side = "LONG" if z >= 0 else "SHORT"
    if oiz is None or oiz < FORWARD_MIN_OIZ:
        return {
            "tag": f"PRE-{side}",
            "oi_line": "⏳ Awaiting OI confirmation",
            "header": "⬆️" if z >= 0 else "⬇️",
        }
    return {
        "tag": f"{side} CONFIRMED",
        "oi_line": f"{'🟢' if oiz >= 0 else '🔴'} OI Δ={abs(oiz):.2f}σ",
        "header": "✅",
    }


def _format_tv_symbol(symbol: str) -> str:
    s = symbol.upper()
    return f"{s}.P" if s.endswith("USDT") else f"{s}USDT.P"


def _bybit_futures_link(symbol: str) -> str:
    return f"https://www.bybit.com/trade/usdt/{_map_symbol_to_bybit_linear(symbol)}"


def _get_oi_z(symbol: str) -> Optional[float]:
    try:
        from oi_fallback import oi_z_score_ext

        return oi_z_score_ext(symbol, interval=FORWARD_OI_INTERVAL, window=FORWARD_OI_WINDOW)
    except Exception as exc:  # pragma: no cover - best effort fallback
        logger.debug("oi lookup failed for %s: %s", symbol, exc)
        return None


def _state_allows(sig: Dict[str, Any]) -> bool:
    channel = (sig.get("channel") or sig.get("group") or sig.get("stream") or "").upper()
    if channel in {"TRND", "TVOI", "TREND"}:
        return get_trend_enabled()
    return get_push_enabled()


def _should_forward(sig: Dict[str, Any]) -> bool:
    z = abs(float(sig.get("zprice") or 0.0))
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    return z >= FORWARD_MIN_Z and volx >= FORWARD_MIN_VOLX and v24 >= FORWARD_MIN_VOL24H


def _format_signal(sig: Dict[str, Any], oiz: Optional[float]) -> tuple[str, InlineKeyboardMarkup]:
    symbol = sig["symbol"].upper()
    z = float(sig.get("zprice") or 0.0)
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    klass = _classify(symbol, z, oiz)
    header = klass["header"]
    lines = [
        f"{header} {symbol}  ({klass['tag']})",
        f"{'🟢' if z >= 0 else '🔴'} Price Δ={abs(z):.2f}σ",
        f"🟢 Volume ×{volx:.2f}",
        klass["oi_line"],
        f"24h Volume ≈ ${v24:,.0f}".replace(",", " "),
    ]
    text = "\n".join(filter(None, lines))
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Bybit Futures", url=_bybit_futures_link(symbol)),
                InlineKeyboardButton(
                    "TradingView",
                    url=f"https://www.tradingview.com/chart/?symbol=BYBIT%3A{_format_tv_symbol(symbol)}",
                ),
            ]
        ]
    )
    return text, kb


def _send_signal(sig: Dict[str, Any], oiz: Optional[float]) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.debug("Telegram credentials missing; skipping send")
        return
    text, kb = _format_signal(sig, oiz)
    sent = tg_send_http(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text, kb)
    if not sent:
        logger.warning('telegram delivery failed for %%s', sig.get('symbol'))


def _record_seen(symbol: str, bar_ts: int) -> None:
    prev = _SENT_BARS.get(symbol)
    if prev is None or bar_ts > prev:
        _SENT_BARS[symbol] = bar_ts


def forward_loop() -> None:
    ensure_state_file()
    logger.info(
        "forward loop start | host=%s thresholds: z>=%.2f volx>=%.2f v24>=%s oiz>=%.2f",
        HOST,
        FORWARD_MIN_Z,
        FORWARD_MIN_VOLX,
        f"${FORWARD_MIN_VOL24H:,.0f}".replace(",", " "),
        FORWARD_MIN_OIZ,
    )
    while True:
        try:
            data = _fetch_signals()
        except Exception as exc:
            logger.error("fetch error: %s", exc)
            time.sleep(max(1, FORWARD_POLL_SEC))
            continue

        items = data.get("data") or []
        for sig in items:
            symbol = str(sig.get("symbol") or "").upper()
            bar_ts = int(sig.get("bar_ts") or 0)
            if not symbol or bar_ts <= 0:
                continue
            last_seen = _SENT_BARS.get(symbol)
            if last_seen and last_seen >= bar_ts:
                continue
            if not _should_forward(sig):
                _record_seen(symbol, bar_ts)
                continue
            if not _state_allows(sig):
                _record_seen(symbol, bar_ts)
                continue
            oiz = _get_oi_z(symbol)
            _send_signal(sig, oiz)
            _record_seen(symbol, bar_ts)

        time.sleep(max(1, FORWARD_POLL_SEC))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="InnerTrade push signals forwarder")
    parser.add_argument(
        "--set",
        choices={"on", "off"},
        help="Toggle primary signal stream",
    )
    parser.add_argument(
        "--trend",
        choices={"on", "off"},
        help="Toggle trend signal stream",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print current toggle state and exit",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ensure_state_file()
    args = parse_args()
    if args.set:
        enabled = args.set == "on"
        set_push_enabled(enabled)
        print(f"signals={'on' if enabled else 'off'}")
        return
    if args.trend:
        enabled = args.trend == "on"
        set_trend_enabled(enabled)
        print(f"trend={'on' if enabled else 'off'}")
        return
    if args.status:
        state = {
            "signals": "on" if get_push_enabled() else "off",
            "trend": "on" if get_trend_enabled() else "off",
        }
        print(state)
        return

    forward_loop()


if __name__ == "__main__":
    main()
