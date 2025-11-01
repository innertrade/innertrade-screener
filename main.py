#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screener Engine (stable 5m)
- HTTP: /health, /signals
- Mode: signals_5m
- Source: Bybit v5
- НИКАКИХ бизнес-порогов/фильтров в коде — только расчёт метрик.
  Любые пороги и правила отбора должны применяться в отдельном процессе/сервисе (форвардере).

Зависимости: Flask, requests
"""

from __future__ import annotations

import os
import time
import math
import logging
import threading
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, jsonify

# -------------------- ENV (только системные настройки) --------------------

HTTP_PORT: int        = int(os.getenv("HTTP_PORT", "8088"))       # порт HTTP
KLINE_SOURCE: str     = os.getenv("KLINE_SOURCE", "bybit").lower()
OI_SOURCE: str        = os.getenv("OI_SOURCE", "bybit").lower()
INTERVAL_MIN: int     = int(os.getenv("INTERVAL_MIN", "5"))       # таймфрейм 5m
WINDOW: int           = int(os.getenv("WINDOW", "48"))            # глубина окна (48*5m≈4h)
UNIVERSE_ENV: str     = os.getenv("UNIVERSE", "").strip()         # список символов через запятую (опц.)
UNIVERSE_FILE: str    = os.getenv("UNIVERSE_FILE", "").strip()    # путь к файлу со списком символов (опц.)
POLL_SEC: int         = int(os.getenv("POLL_SEC", "8"))           # период обновления кеша
HTTP_TIMEOUT: float   = float(os.getenv("HTTP_TIMEOUT", "8.0"))   # таймаут HTTP
ADAPTIVE: bool        = os.getenv("ADAPTIVE", "1") == "1"         # флаг (для /health)
SLEEP_BETWEEN_CALLS: float = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.2"))  # пауза между API-вызовами

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

# ------------------ HTTP client ------------------

BYBIT_BASE = "https://api.bybit.com"

_session = requests.Session()
_session.headers.update({"User-Agent": "InnerTradeScreener/1.0"})
# (без дополнительных зависимостей; retries можно добавить в окружении gunicorn/nginx)

def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.ok:
            return r.json()
        logging.warning(f"HTTP {r.status_code} for {url} params={params} body={r.text[:200]}")
    except Exception as e:
        logging.warning(f"HTTP GET fail {url}: {e}")
    return None

# ------------------ Universe loaders ------------------

def _universe_from_env_var() -> List[str]:
    if not UNIVERSE_ENV:
        return []
    arr = [s.strip().upper() for s in UNIVERSE_ENV.split(",") if s.strip()]
    logging.info(f"universe(from ENV UNIVERSE): {len(arr)} symbols")
    return arr

def _universe_from_file(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = [ln.strip().upper() for ln in f if ln.strip() and not ln.strip().startswith("#")]
        logging.info(f"universe(from file {path}): {len(rows)} symbols")
        return rows
    except Exception as e:
        logging.warning(f"universe file read fail {path}: {e}")
        return []

def _universe_bybit_linear_usdt() -> List[str]:
    """
    Все торгуемые линейные (USDT) фьючерсные контракты на Bybit.
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
    # приоритет: файл -> переменная -> авто
    if UNIVERSE_FILE:
        arr = _universe_from_file(UNIVERSE_FILE)
        if arr:
            return arr
    arr = _universe_from_env_var()
    if arr:
        return arr
    return _universe_bybit_linear_usdt()

# ------------------ Data sources (Bybit) ------------------

def _kline_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[Dict[str, Any]]]:
    """
    Bybit kline (linear): /v5/market/kline — список свечей (возрастающе).
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
    rows = j["result"]["list"][::-1]  # привести к возрастающему времени

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
    Ряд Open Interest (возрастающий) из /v5/market/open-interest.
    Ключ: intervalTime='<N>min'.
    """
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": f"{interval_min}min",
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

# ------------------ Math ------------------

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

# ------------------ Metrics builder ------------------

def _calc_metrics_for_symbol(sym: str,
                             interval_min: int,
                             window: int,
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

    # OI z-score (опционально)
    oi_z: Optional[float] = None
    if OI_SOURCE == "bybit":
        oi_series = _oi_series_bybit(sym, interval_min, window + 1)
        if oi_series and len(oi_series) >= (window + 1):
            oi_hist = oi_series[:-1]
            oi_curr = float(oi_series[-1])
            oi_z_val = _zscore(oi_curr, oi_hist)
            oi_z = round(oi_z_val, 2) if oi_z_val is not None else None

    return {
        "ts": ts_str,
        "symbol": sym,
        "close": close,
        "zprice": round(zprice, 3) if zprice is not None else None,
        "vol_mult": round(vol_mult, 2) if vol_mult is not None else None,
        "vol24h_usd": vol24h,
        "bar_ts": bar_ts,
        "oi_z": oi_z,
    }

# ------------------ Rebuild loop ------------------

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
            # слегка бережём публичный API
            time.sleep(SLEEP_BETWEEN_CALLS)

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
    _STATE["universe"] = _load_universe()
    logging.info(f"runtime init: universe={len(_STATE['universe'])}, interval={INTERVAL_MIN}m, window={WINDOW}")
    t = threading.Thread(target=_worker_loop, daemon=True)
    t.start()

# ---- Важно: инициализация при импорте (работает под gunicorn) ----
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
        "config": {
            "poll_sec": POLL_SEC,
            "http_timeout": HTTP_TIMEOUT,
            "sleep_between_calls": SLEEP_BETWEEN_CALLS,
            "universe_file": UNIVERSE_FILE or None,
            "universe_env": bool(UNIVERSE_ENV),
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
    })

# -------------------- Local entry --------------------

def main():
    # при локальном запуске всё уже инициализировано импортом
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, threaded=True)

if __name__ == "__main__":
    main()
