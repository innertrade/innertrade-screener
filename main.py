#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screener Engine (stable 5m)
- HTTP: /health, /signals
- Mode: signals_5m
- Source: Bybit v5
- Движок только считает метрики (z по цене, vol_mult, OI z). Фильтров нет.
"""

import os
import time
import math
import logging
import threading
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, jsonify

# ==================== ENV ====================

HTTP_PORT     = int(os.getenv("HTTP_PORT", "8080"))
KLINE_SOURCE  = os.getenv("KLINE_SOURCE", "bybit").lower()  # bybit
OI_SOURCE     = os.getenv("OI_SOURCE", "bybit").lower()     # bybit
INTERVAL_MIN  = int(os.getenv("INTERVAL_MIN", "5"))         # 5m
WINDOW        = int(os.getenv("WINDOW", "48"))              # 48*5m ≈ 4h
UNIVERSE_ENV  = os.getenv("UNIVERSE", "").strip()           # пусто => авто по Bybit linear USDT
POLL_SEC      = int(os.getenv("POLL_SEC", "12"))            # период пересчёта кеша
HTTP_TIMEOUT  = float(os.getenv("HTTP_TIMEOUT", "12.0"))
ADAPTIVE      = os.getenv("ADAPTIVE", "1") == "1"

# ==================== LOG ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

UA = "innertrade-screener/1.0 (+bybit v5) Python-requests"

# ==================== APP/STATE ====================

app = Flask(__name__)

_STATE = {
    "signals": [],          # кеш последнего расчёта
    "last_update": 0,       # epoch sec
    "universe": [],         # список символов
    "mode": "signals_5m",
    "started": False,
}

_START_LOCK = threading.Lock()
_WORKER_THREAD: Optional[threading.Thread] = None

BYBIT_BASE = "https://api.bybit.com"

# ==================== HTTP helper ====================

def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT,
                         headers={"User-Agent": UA})
        if r.ok:
            return r.json()
        logging.warning(f"HTTP GET non-OK {url} -> {r.status_code} {r.text[:200]}")
    except Exception as e:
        logging.warning(f"HTTP GET fail {url}: {e}")
    return None

# ==================== Universe ====================

def _universe_bybit_linear_usdt() -> List[str]:
    """
    Берём все торгуемые линейные USDT контракты c Bybit (category=linear, status=Trading).
    Никакого спота.
    """
    url = f"{BYBIT_BASE}/v5/market/instruments-info"
    params = {"category": "linear", "status": "Trading"}
    out: List[str] = []
    j = _get(url, params)
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

# ==================== Data sources ====================

def _kline_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[Dict[str, Any]]]:
    """ Bybit kline linear: /v5/market/kline; возвращаем по времени (возрастающе). """
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": str(interval_min),  # ВАЖНО: строкой '5','15',...
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
        return None
    rows = j["result"]["list"][::-1]  # в возрастающий порядок
    out = []
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
                "turnover_quote": float(row[6]),
            })
        except Exception:
            return None
    return out

def _tickers24h_bybit(symbols: List[str]) -> Dict[str, float]:
    """ symbol -> turnover24h (USD) из /v5/market/tickers?category=linear """
    url = f"{BYBIT_BASE}/v5/market/tickers"
    params = {"category": "linear"}
    out: Dict[str, float] = {}
    j = _get(url, params)
    if j and j.get("retCode") == 0 and j.get("result", {}).get("list"):
        for it in j["result"]["list"]:
            sym = str(it.get("symbol", "")).upper()
            if sym in symbols:
                try:
                    out[sym] = float(it.get("turnover24h") or 0.0)
                except Exception:
                    out[sym] = 0.0
    return out

def _oi_series_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[float]]:
    """
    Ряд Open Interest (возрастающий порядок) из /v5/market/open-interest
    category=linear, interval='5'/'15'/...
    """
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": str(interval_min),  # ВАЖНО: строка
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
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

def _mean_std(vals: List[float]) -> (float, float):
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
    Возвращает:
      ts, symbol, close, zprice, vol_mult, vol24h_usd, bar_ts, oi_z (или None)
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

def _rebuild_signals():
    start = time.time()
    universe = _STATE["universe"]
    if not universe:
        return
    vol24h_map = _tickers24h_bybit(universe)

    res: List[Dict[str, Any]] = []
    for i, sym in enumerate(universe, 1):
        m = _calc_metrics_for_symbol(sym, INTERVAL_MIN, WINDOW, vol24h_map)
        if m:
            res.append(m)
        if i % 10 == 0:
            time.sleep(0.2)  # бережём API

    _STATE["signals"] = res
    _STATE["last_update"] = int(time.time())
    took = time.time() - start
    logging.info(f"signals rebuilt: {len(res)} rows in {took:.1f}s (universe={len(universe)})")

def _worker_loop():
    while True:
        try:
            _rebuild_signals()
        except Exception as e:
            logging.exception(f"rebuild error: {e}")
        time.sleep(max(2, POLL_SEC))

def _start_runtime():
    global _WORKER_THREAD
    with _START_LOCK:
        if _STATE["started"]:
            return
        # 1) вселенная
        _STATE["universe"] = _load_universe()
        logging.info(f"runtime init: universe={len(_STATE['universe'])}, interval={INTERVAL_MIN}m, window={WINDOW}")
        # 2) воркер
        _WORKER_THREAD = threading.Thread(target=_worker_loop, daemon=True, name="signals-worker")
        _WORKER_THREAD.start()
        _STATE["started"] = True
        logging.info("background worker started")

# Стартуем рантайм сразу при импорте модуля (важно для gunicorn main:app).
_start_runtime()

# ==================== HTTP ====================

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
    })

@app.route("/signals", methods=["GET"])
def signals():
    return jsonify({
        "data": _STATE["signals"],
        "count": len(_STATE["signals"]),
        "last_update": _STATE["last_update"],
        "interval_min": INTERVAL_MIN,
        "window": WINDOW,
    })

# Локальный запуск (не нужен под gunicorn, но оставим)
def main():
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, threaded=True)

if __name__ == "__main__":
    main()
