#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screener Engine (stable 5m)
- HTTP: /health, /signals
- Mode: signals_5m
- Source: Bybit v5
- Фильтрации по порогам нет — только расчёт метрик (фильтрует push_signals.py)
  ✱ Опционально: можно включить локальную фильтрацию через ENV FILTER_IN_API=1
Зависимости: Flask, requests
"""

import os
import time
import math
import logging
import threading
from typing import Dict, Any, List, Optional, Tuple

import requests
from flask import Flask, jsonify

# -------------------- ENV --------------------

HTTP_PORT           = int(os.getenv("HTTP_PORT", "8088"))
KLINE_SOURCE        = os.getenv("KLINE_SOURCE", "bybit").lower()     # bybit
OI_SOURCE           = os.getenv("OI_SOURCE", "bybit").lower()        # bybit
INTERVAL_MIN        = int(os.getenv("INTERVAL_MIN", "5"))            # 5-мин режим
WINDOW              = int(os.getenv("WINDOW", "48"))                 # глубина окна (48 * 5м ≈ 4ч)
UNIVERSE_ENV        = os.getenv("UNIVERSE", "").strip()              # пусто => авто по Bybit linear USDT
POLL_SEC            = int(os.getenv("POLL_SEC", "8"))                # как часто обновлять кеш
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "8.0"))        # таймаут HTTP
HTTP_RETRIES        = int(os.getenv("HTTP_RETRIES", "2"))            # число повторов при сбое
ADAPTIVE            = os.getenv("ADAPTIVE", "1") == "1"              # просто флаг для /health

# --- Опциональная встроенная фильтрация (для тестов или автономного режима) ---
FILTER_IN_API       = os.getenv("FILTER_IN_API", "0") in ("1", "true", "True", "YES", "yes")

ZPRICE_MIN          = float(os.getenv("ZPRICE_MIN", "1.8"))
VOL_MULT_MIN        = float(os.getenv("VOL_MULT_MIN", "1.6"))
V24H_USD_MIN        = float(os.getenv("V24H_USD_MIN", "20000000"))

# Flat-only по OI: по умолчанию узкий коридор [-0.3; +0.3]
FLAT_ONLY           = os.getenv("FLAT_ONLY", "true").lower() in ("1", "true", "yes")
OI_Z_MIN            = float(os.getenv("OI_Z_MIN", "-0.3"))
OI_Z_MAX            = float(os.getenv("OI_Z_MAX", "0.3"))

# -------------------- LOG --------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------- HTTP -------------------

app = Flask(__name__)

_STATE: Dict[str, Any] = {
    "signals": [],          # кеш последнего расчёта
    "last_update": 0,       # epoch sec
    "universe": [],         # список символов
    "mode": "signals_5m",
    "_bg_started": False,   # защита от повторного старта воркера
}

# ------------------ Helpers ------------------

BYBIT_BASE = "https://api.bybit.com"

_DEFAULT_HEADERS = {
    "User-Agent": f"innertrade-screener/1.0 (+engine; interval={INTERVAL_MIN}m; window={WINDOW})",
    "Accept": "application/json",
}


def _http_get_with_retries(url: str, params: Dict[str, Any], timeout: float, retries: int) -> Optional[requests.Response]:
    """
    Неблокирующие ретраи с экспоненциальной задержкой: 0.5s, 1s, 2s...
    """
    attempt = 0
    delay = 0.5
    last_exc: Optional[Exception] = None
    while attempt <= retries:
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=_DEFAULT_HEADERS)
            return r
        except Exception as e:
            last_exc = e
            if attempt == retries:
                break
            time.sleep(delay)
            delay *= 2
            attempt += 1
    logging.warning(f"HTTP GET fail {url} params={params} err={last_exc}")
    return None


def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = _http_get_with_retries(url, params, timeout=HTTP_TIMEOUT, retries=max(0, HTTP_RETRIES))
    if r is None:
        return None
    if r.ok:
        try:
            return r.json()
        except Exception as e:
            logging.warning(f"HTTP JSON decode fail {url}: {e} body={r.text[:200]}")
            return None
    logging.warning(f"HTTP {r.status_code} for {url} params={params} body={r.text[:200]}")
    return None


def _universe_bybit_linear_usdt() -> List[str]:
    """
    Берём все торгуемые линейные (USDT) фьючерсные контракты с Bybit.
    """
    url = f"{BYBIT_BASE}/v5/market/instruments-info"
    params = {"category": "linear", "status": "Trading"}
    j = _get(url, params)
    out: List[str] = []
    if j and j.get("retCode") == 0 and j.get("result", {}).get("list"):
        for it in j["result"]["list"]:
            if str(it.get("quoteCoin", "")).upper() == "USDT":
                sym = str(it.get("symbol", "")).upper()
                if sym:
                    out.append(sym)
    out = sorted(list(set(out)))
    logging.info(f"universe(bybit linear USDT): {len(out)} symbols")
    return out


def _load_universe() -> List[str]:
    if UNIVERSE_ENV:
        arr = [s.strip().upper() for s in UNIVERSE_ENV.split(",") if s.strip()]
        logging.info(f"universe(from .env UNIVERSE): {len(arr)} symbols")
        return arr
    return _universe_bybit_linear_usdt()


def _kline_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[Dict[str, Any]]]:
    """
    Bybit kline (linear): /v5/market/kline — список свечей по времени (возрастающе).
    """
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": str(interval_min),      # '5'
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
        return None
    rows = j["result"]["list"][::-1]  # в возрастающий порядок

    out: List[Dict[str, Any]] = []
    # row: [startMs, open, high, low, close, volume(base), turnover(quote)]
    for row in rows:
        try:
            out.append({
                "start_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume_base": float(row[5]),
                "turnover_quote": float(row[6]),  # в USDT
            })
        except Exception:
            return None
    return out


def _tickers24h_bybit(symbols: List[str]) -> Dict[str, float]:
    """
    symbol -> vol24h_usd (turnover24h) из /v5/market/tickers?category=linear
    """
    url = f"{BYBIT_BASE}/v5/market/tickers"
    params = {"category": "linear"}
    j = _get(url, params)
    out: Dict[str, float] = {}
    if j and j.get("retCode") == 0 and j.get("result", {}).get("list"):
        symset = set(symbols)
        for it in j["result"]["list"]:
            sym = str(it.get("symbol", "")).upper()
            if sym in symset:
                try:
                    out[sym] = float(it.get("turnover24h") or 0.0)
                except Exception:
                    out[sym] = 0.0
    return out


def _oi_series_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[float]]:
    """
    Ряд Open Interest (возрастающий порядок) из /v5/market/open-interest.
    ВАЖНО: у Bybit здесь ключ называется intervalTime и значение — строка '<N>min'.
    """
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": f"{interval_min}min",  # <-- правильный ключ и формат
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
        logging.warning(f"OI FAIL: params={params} got={j}")
        return None
    rows = j["result"]["list"][::-1]
    out: List[float] = []
    for row in rows:
        try:
            out.append(float(row.get("openInterest") or 0.0))
        except Exception:
            return None
    return out


# ==================== Math ====================

def _mean_std(vals: List[float]) -> Tuple[float, float]:
    n = len(vals)
    if n == 0:
        return 0.0, 0.0
    m = sum(vals) / n
    var = sum((x - m) ** 2 for x in vals) / max(1, (n - 1))
    return m, math.sqrt(var)


def _zscore(curr: float, hist: List[float]) -> Optional[float]:
    if len(hist) < 5:
        return None
    m, s = _mean_std(hist)
    if s <= 0:
        return None
    return (curr - m) / s


# ==================== Metrics ====================

def _calc_metrics_for_symbol(sym: str, interval_min: int, window: int,
                             vol24h_map: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """
    Возвращает одну запись для /signals: ts, symbol, close, zprice, vol_mult, vol24h_usd, bar_ts, oi_z|None
    """
    kl = _kline_bybit(sym, interval_min, limit=window + 1)
    if not kl or len(kl) < (window + 1):
        return None

    curr = kl[-1]
    hist = kl[:-1]

    close = float(curr["close"])
    close_hist = [float(x["close"]) for x in hist]
    zprice = _zscore(close, close_hist)

    turn_curr = float(curr["turnover_quote"])
    turn_hist = [float(x["turnover_quote"]) for x in hist]
    mean_turn, _ = _mean_std(turn_hist)
    vol_mult = (turn_curr / mean_turn) if mean_turn > 0 else None

    vol24h = float(vol24h_map.get(sym, 0.0))

    bar_ts = int(curr["start_ms"])
    ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bar_ts // 1000))

    # OI z-score
    oi_z: Optional[float] = None
    if OI_SOURCE == "bybit":
        oi_series = _oi_series_bybit(sym, interval_min, window + 1)
        if oi_series and len(oi_series) >= (window + 1):
            oi_hist = oi_series[:-1]
            oi_curr = float(oi_series[-1])
            oi_z = _zscore(oi_curr, oi_hist)

    return {
        "ts": ts_str,
        "symbol": sym,
        "close": close,
        "zprice": round(zprice, 3) if zprice is not None else None,
        "vol_mult": round(vol_mult, 2) if vol_mult is not None else None,
        "vol24h_usd": vol24h,
        "bar_ts": bar_ts,
        "oi_z": round(oi_z, 2) if oi_z is not None else None,
    }


def _passes_thresholds(rec: Dict[str, Any]) -> bool:
    """
    Применяет ENV-пороги, если включён FILTER_IN_API.
    """
    try:
        # price Z
        if rec.get("zprice") is None or rec["zprice"] < ZPRICE_MIN:
            return False
        # turnover mult
        if rec.get("vol_mult") is None or rec["vol_mult"] < VOL_MULT_MIN:
            return False
        # 24h volume (USD)
        if float(rec.get("vol24h_usd", 0.0)) < V24H_USD_MIN:
            return False
        # OI flat-only или диапазон
        oi = rec.get("oi_z")
        if FLAT_ONLY:
            # если oi_z отсутствует, считаем что не проходит flat-фильтр
            if oi is None or not (OI_Z_MIN <= oi <= OI_Z_MAX):
                return False
        else:
            # если заданы диапазоны — проверим, но пропускаем None
            if (oi is not None) and not (OI_Z_MIN <= oi <= OI_Z_MAX):
                return False
        return True
    except Exception:
        return False


def _rebuild_signals():
    """
    Пересчитывает кешированный список сигналов. По умолчанию — БЕЗ порогов.
    Если FILTER_IN_API=1 — применяет ENV-фильтры.
    """
    start = time.time()
    universe = _STATE["universe"]
    if not universe:
        return

    vol24h_map = _tickers24h_bybit(universe)

    res_all: List[Dict[str, Any]] = []
    n = 0
    for sym in universe:
        m = _calc_metrics_for_symbol(sym, INTERVAL_MIN, WINDOW, vol24h_map)
        if m:
            res_all.append(m)
        n += 1
        if n % 10 == 0:
            time.sleep(0.2)  # не долбим API

    if FILTER_IN_API:
        res = [r for r in res_all if _passes_thresholds(r)]
    else:
        res = res_all

    _STATE["signals"] = res
    _STATE["last_update"] = int(time.time())

    took = time.time() - start
    logging.info(f"signals rebuilt: {len(res)} rows in {took:.1f}s (universe={len(universe)})")


def _worker_loop():
    logging.info("background worker started")
    while True:
        try:
            _rebuild_signals()
        except Exception as e:
            logging.exception(f"rebuild error: {e}")
        time.sleep(max(2, POLL_SEC))


def _start_background_once():
    if _STATE.get("_bg_started"):
        return
    _STATE["_bg_started"] = True
    # 1) поднимем вселенную
    _STATE["universe"] = _load_universe()
    logging.info(f"runtime init: universe={len(_STATE['universe'])}, interval={INTERVAL_MIN}m, window={WINDOW}")
    # 2) старт воркера
    t = threading.Thread(target=_worker_loop, daemon=True)
    t.start()


# ---- ВАЖНО: запускаем инициализацию ПРИ ИМПОРТЕ (работает под gunicorn) ----
_start_background_once()


# -------------------- HTTP API --------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "mode": _STATE["mode"],
        "adaptive": ADAPTIVE,
        "port": HTTP_PORT,
        "last_update": _STATE["last_update"],
        "universe": len(_STATE["universe"]),
        "interval_min": INTERVAL_MIN,
        "window": WINDOW,
        "source": {"kline": KLINE_SOURCE, "oi": OI_SOURCE},
        "http": {"timeout": HTTP_TIMEOUT, "retries": HTTP_RETRIES},
        "filter": {
            "enabled": FILTER_IN_API,
            "zprice_min": ZPRICE_MIN,
            "vol_mult_min": VOL_MULT_MIN,
            "v24h_usd_min": V24H_USD_MIN,
            "flat_only": FLAT_ONLY,
            "oi_z_min": OI_Z_MIN,
            "oi_z_max": OI_Z_MAX,
        },
    })


@app.route("/signals", methods=["GET"])
def signals():
    return jsonify({
        "data": _STATE["signals"],
        "count": len(_STATE["signals"]),
        "last_update": _STATE["last_update"],
        "interval_min": INTERVAL_MIN,
        "window": WINDOW,
        "filtered": FILTER_IN_API,
    })


# -------------------- Entry (локальный run) -----------------------

def main():
    # при локальном запуске всё уже инициализировано импортом
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
