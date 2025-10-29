from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("push_signals")

STATE_DEFAULT: Dict[str, bool] = {"tvoi": True, "trnd": False}
BASE_DIR = Path(__file__).resolve().parent
PUSH_STATE_FILE = os.getenv("PUSH_STATE_FILE", "state/push_enabled.json")

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
FORWARD_SEEN_TTL_SEC = int(float(os.getenv("FORWARD_SEEN_TTL_SEC", "1800")))

KEEP_ALIVE_DEFAULT = os.getenv("KEEP_ALIVE", "1").lower() not in {"0", "false", "off", "no"}

_SENT_CACHE: Dict[Tuple[str, int, str], float] = {}


def _state_path() -> Path:
    configured = Path(PUSH_STATE_FILE)
    if not configured.is_absolute():
        configured = (BASE_DIR / configured).resolve()
    return configured


def _normalize_state(data: object) -> Tuple[Dict[str, bool], bool]:
    state = STATE_DEFAULT.copy()
    migrated = False
    if isinstance(data, bool):
        state = {"tvoi": data, "trnd": data}
        migrated = True
    elif isinstance(data, dict):
        if "tvoi" in data or "trnd" in data:
            state["tvoi"] = bool(data.get("tvoi", state["tvoi"]))
            state["trnd"] = bool(data.get("trnd", state["trnd"]))
        elif "enabled" in data:
            val = bool(data.get("enabled"))
            state = {"tvoi": val, "trnd": val}
            migrated = True
        else:
            if "push_enabled" in data:
                state["tvoi"] = bool(data.get("push_enabled"))
                migrated = True
            if "trend_enabled" in data:
                state["trnd"] = bool(data.get("trend_enabled"))
                migrated = True
    else:
        migrated = True
    return state, migrated


def _write_state(state: Dict[str, bool]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"tvoi": bool(state.get("tvoi", False)), "trnd": bool(state.get("trnd", False))}
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def _read_state() -> Dict[str, bool]:
    path = _state_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        LOGGER.info("State file missing, creating default at %s", path)
        _write_state(STATE_DEFAULT)
        return STATE_DEFAULT.copy()
    except json.JSONDecodeError:
        LOGGER.warning("Corrupted state file at %s, resetting to defaults", path)
        _write_state(STATE_DEFAULT)
        return STATE_DEFAULT.copy()
    state, migrated = _normalize_state(data)
    if migrated:
        LOGGER.info("Migrated state format at %s", path)
        _write_state(state)
    return state


def get_state() -> Dict[str, bool]:
    return _read_state()


def set_flag(flag: str, value: bool) -> Dict[str, bool]:
    state = get_state()
    state[flag] = bool(value)
    _write_state(state)
    return state


def toggle_flag(flag: str) -> Tuple[bool, Dict[str, bool]]:
    state = get_state()
    state[flag] = not state.get(flag, False)
    _write_state(state)
    return state[flag], state


def tg_send_http(token: str, chat_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup and hasattr(reply_markup, "to_dict"):
        payload["reply_markup"] = reply_markup.to_dict()
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=10,
        )
        if resp.status_code >= 400:
            LOGGER.error(
                "Telegram send failed | status=%s | body=%s", resp.status_code, resp.text
            )
        return resp
    except Exception as exc:  # pragma: no cover - network exception path
        LOGGER.exception("Telegram send failed: %s", exc)
        return None


def _map_symbol_to_bybit_linear(sym: str) -> str:
    s = sym.upper()
    return s if s.endswith("USDT") else f"{s}USDT"


def _format_tv_symbol(symbol: str) -> str:
    s = symbol.upper()
    return f"{s}.P" if s.endswith("USDT") else f"{s}USDT.P"


def _bybit_futures_link(symbol: str) -> str:
    mapped = _map_symbol_to_bybit_linear(symbol)
    return f"https://www.bybit.com/trade/usdt/{mapped}"


def _compose_keyboard(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
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


def _get_oi_z(symbol: str) -> Optional[float]:
    try:
        from oi_fallback import oi_z_score_ext

        return oi_z_score_ext(symbol, interval=FORWARD_OI_INTERVAL, window=FORWARD_OI_WINDOW)
    except Exception:
        return None


def _classify(symbol: str, z: float, volx: float, v24: float, oiz: Optional[float]) -> Dict[str, str]:
    side = "LONG" if z >= 0 else "SHORT"
    is_confirmed = oiz is not None and oiz >= FORWARD_MIN_OIZ
    tag = f"{side} CONFIRM" if is_confirmed else f"PRE-{side}"
    icon = "✅" if is_confirmed else ("⬆️" if z >= 0 else "⬇️")
    if not is_confirmed:
        oi_line = "⏳ Awaiting OI confirmation"
    else:
        oi_line = f"{'🟢' if (oiz or 0) >= 0 else '🔴'} OI Δ={abs(oiz):.2f}σ"
    return {"tag": tag, "icon": icon, "oi_line": oi_line}


def _compose_message(sig: Dict[str, Any], oiz: Optional[float]) -> Tuple[str, str]:
    symbol = str(sig.get("symbol") or "").upper()
    z = float(sig.get("zprice") or 0.0)
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    klass = _classify(symbol, z, volx, v24, oiz)
    lines = [
        f"{klass['icon']} {symbol}  ({klass['tag']})",
        f"{'🟢' if z >= 0 else '🔴'} Price Δ={abs(z):.2f}σ",
        f"🟢 Volume ×{volx:.2f}",
        klass["oi_line"],
        f"24h Volume ≈ ${v24:,.0f}".replace(",", " "),
    ]
    price = sig.get("close")
    if price is not None:
        lines.append(f"Last price: {price}")
    return "\n".join(lines), klass["tag"]


def _should_forward(sig: Dict[str, Any]) -> bool:
    z = abs(float(sig.get("zprice") or 0.0))
    volx = float(sig.get("vol_mult") or 0.0)
    v24 = float(sig.get("vol24h_usd") or 0.0)
    return z >= FORWARD_MIN_Z and volx >= FORWARD_MIN_VOLX and v24 >= FORWARD_MIN_VOL24H


def _is_trend_signal(sig: Dict[str, Any]) -> bool:
    candidates = [
        str(sig.get("channel", "")),
        str(sig.get("type", "")),
        str(sig.get("tag", "")),
        str(sig.get("name", "")),
    ]
    return any("trnd" in c.lower() or "trend" in c.lower() for c in candidates)


def _send_signal(sig: Dict[str, Any], flag: str, state: Optional[Dict[str, bool]] = None) -> bool:
    state = state or get_state()
    enabled = state.get(flag, False)
    symbol = str(sig.get("symbol") or "").upper()
    if not enabled:
        LOGGER.info("%s disabled, skip | symbol=%s", flag.upper(), symbol or "?")
        return False
    if not symbol:
        LOGGER.debug("Skip signal without symbol: %s", sig)
        return False
    bar_ts = int(sig.get("bar_ts") or 0)
    if bar_ts <= 0:
        LOGGER.debug("Skip signal without bar_ts: %s", sig)
        return False
    if not _should_forward(sig):
        LOGGER.debug("Skip signal below thresholds | symbol=%s", symbol)
        return False
    oiz = _get_oi_z(symbol)
    text, tag = _compose_message(sig, oiz)
    key = (flag, bar_ts, f"{symbol}:{tag}")
    now = time.time()
    if _SENT_CACHE.get(key, 0.0) > now:
        LOGGER.debug("Skip duplicate within TTL | symbol=%s | flag=%s", symbol, flag)
        return False
    markup = _compose_keyboard(symbol)
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID == 0:
        LOGGER.warning("Telegram credentials missing, unable to deliver signal")
        return False
    resp = tg_send_http(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text, markup)
    if resp is None or resp.status_code >= 400:
        return False
    _SENT_CACHE[key] = now + FORWARD_SEEN_TTL_SEC
    LOGGER.info("Delivered %s signal | symbol=%s | tag=%s", flag.upper(), symbol, tag)
    return True


def send_tvoi_signal(payload: Dict[str, Any], state: Optional[Dict[str, bool]] = None) -> bool:
    return _send_signal(payload, "tvoi", state)


def send_trnd_signal(payload: Dict[str, Any], state: Optional[Dict[str, bool]] = None) -> bool:
    return _send_signal(payload, "trnd", state)


def _fetch_signals() -> Dict[str, Any]:
    response = requests.get(f"{HOST}/signals", timeout=10)
    response.raise_for_status()
    return response.json()


def _prune_sent(now: float) -> None:
    expired = [key for key, ttl in _SENT_CACHE.items() if ttl <= now]
    for key in expired:
        _SENT_CACHE.pop(key, None)


def poll_once() -> None:
    try:
        data = _fetch_signals()
    except Exception as exc:  # pragma: no cover - network exception path
        LOGGER.exception("Failed to fetch signals: %s", exc)
        return
    items = data.get("data") or []
    state = get_state()
    now = time.time()
    _prune_sent(now)
    for sig in items:
        try:
            if _is_trend_signal(sig):
                send_trnd_signal(sig, state)
            else:
                send_tvoi_signal(sig, state)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.exception("Failed to process signal: %s", exc)


def forward_loop() -> None:
    LOGGER.info(
        "Forwarder running | host=%s | poll=%ss | z≥%.2f volx≥%.2f vol24h≥%.0f",
        HOST,
        FORWARD_POLL_SEC,
        FORWARD_MIN_Z,
        FORWARD_MIN_VOLX,
        FORWARD_MIN_VOL24H,
    )
    while True:
        poll_once()
        time.sleep(max(1, FORWARD_POLL_SEC))


def keep_alive_loop(interval: int = 30) -> None:
    LOGGER.warning(
        "Entering keep-alive loop | interval=%ss | KEEP_ALIVE can be set to 0 to disable",
        interval,
    )
    while True:
        time.sleep(interval)


def _keep_alive_enabled() -> bool:
    return KEEP_ALIVE_DEFAULT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="InnerTrade push forwarder")
    parser.add_argument("--set", choices=["on", "off"], help="Toggle TVOI feed state")
    parser.add_argument("--trend", choices=["on", "off"], help="Toggle TRND feed state")
    return parser.parse_args()


def main() -> bool:
    state = get_state()
    LOGGER.info("Active flags at start | TVOI=%s | TRND=%s", state["tvoi"], state["trnd"])
    args = parse_args()
    mutated = False
    if args.set:
        value = args.set == "on"
        state = set_flag("tvoi", value)
        mutated = True
        LOGGER.info("TVOI flag updated via CLI | value=%s", state["tvoi"])
    if args.trend:
        value = args.trend == "on"
        state = set_flag("trnd", value)
        mutated = True
        LOGGER.info("TRND flag updated via CLI | value=%s", state["trnd"])
    if mutated:
        return True
    try:
        forward_loop()
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        return True
    except Exception as exc:
        LOGGER.exception("Unhandled error in forward loop: %s", exc)
        if _keep_alive_enabled():
            keep_alive_loop()
            return False
        raise
    return False


if __name__ == "__main__":
    exit_when_done = main()
    if not exit_when_done and _keep_alive_enabled():
        keep_alive_loop()
