#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screener Engine (stable 5m)
- HTTP: /health, /signals
- Mode: signals_5m
- Source: Bybit v5
- All thresholds/интервалы берём из .env, но движок сам НИЧЕГО не фильтрует
  (фильтрация/классификация — в push_signals.py). Движок лишь считает метрики.

Зависимости: Flask, requests
"""

import os
import time
import math
import json
import logging
import threading
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, jsonify

# -------------------- ENV --------------------

HTTP_PORT           = int(os.getenv("HTTP_PORT", "8080"))
KLINE_SOURCE        = os.getenv("KLINE_SOURCE", "bybit").lower()  # bybit
OI_SOURCE           = os.getenv("OI_SOURCE", "bybit").lower()     # bybit
INTERVAL_MIN        = int(os.getenv("INTERVAL_MIN", "5"))         # 5-мин режим
WINDOW              = int(os.getenv("WINDOW", "48"))              # глубина окна (48 * 5м ≈ 4ч)
UNIVERSE_ENV        = os.getenv("UNIVERSE", "").strip()           # пусто => авто по Bybit linear USDT
POLL_SEC            = int(os.getenv("POLL_SEC", "8"))             # как часто обновлять кеш
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "8.0"))     # таймаут HTTP
ADAPTIVE            = os.getenv("ADAPTIVE", "1") == "1"           # просто флаг для /health

# -------------------- LOG --------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------- HTTP -------------------

app = Flask(__name__)

_STATE = {
    "signals": [],          # кеш последнего расчёта
    "last_update": 0,       # epoch sec
    "universe": [],         # список символов
    "mode": "signals_5m",
}

# ------------------ Helpers ------------------

BYBIT_BASE = "https://api.bybit.com"


def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.ok:
            return r.json()
    except Exception as e:
        logging.warning(f"HTTP GET fail {url}: {e}")
    return None


def _universe_bybit_linear_usdt() -> List[str]:
    """
    Берём все торгуемые линейные USDT-перпетуалы с Bybit.
    """
    url = f"{BYBIT_BASE}/v5/market/instruments-info"
    # Ключевое: contractType=LinearPerpetual — исключаем dated futures и всё не-перпетуальное
    params = {"category": "linear", "status": "Trading", "contractType": "LinearPerpetual"}
    j = _get(url, params)
    out: List[str] = []
    if j and j.get("retCode") == 0:
        for it in j["result"]["list"]:
            if str(it.get("quoteCoin", "")).upper() == "USDT":
                sym = str(it.get("symbol", "")).upper()
                if sym:
                    out.append(sym)
    out = sorted(list(set(out)))
    logging.info(f"universe(bybit linear USDT perp): {len(out)} symbols")
    return out


def _load_universe() -> List[str]:
    if UNIVERSE_ENV:
        arr = [s.strip().upper() for s in UNIVERSE_ENV.split(",") if s.strip()]
        logging.info(f"universe(from .env UNIVERSE): {len(arr)} symbols")
        return arr
    return _universe_bybit_linear_usdt()


def _kline_bybit(symbol: str, interval_min: int, limit: int) -> Optional[List[Dict[str, Any]]]:
    """
    Bybit kline (linear): /v5/market/kline
    Возвращаем список свечей по времени (возрастающе).
    """
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": str(interval_min),  # '5'
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
        return None
    # Bybit отдаёт в обратном порядке (последняя вперёд); перевернём:
    rows = j["result"]["list"][::-1]

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
                "turnover_quote": float(row[6]),  # в USDT
            })
        except Exception:
            return None
    return out


def _tickers24h_bybit(symbols: List[str]) -> Dict[str, float]:
    """
    Вернём словарь symbol -> vol24h_usd (turnover24h).
    /v5/market/tickers?category=linear
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
    Вернём ряд Open Interest (значения, возрастающая временная ось).
    /v5/market/open-interest?category=linear&symbol=BTCUSDT&interval=5min&limit=xx
    """
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": f"{int(interval_min)}min",  # <-- ключевой фикс: '5min'
        "limit": str(limit),
    }
    j = _get(url, params)
    if not (j and j.get("retCode") == 0 and j.get("result", {}).get("list")):
        return None
    rows = j["result"]["list"][::-1]  # в возрастающий порядок
    out = []
    for row in rows:
        try:
            # row: {'openInterest': 'xxxx', 'timestamp': 'ms'}
            out.append(float(row.get("openInterest") or 0.0))
        except Exception:
            return None
    return out


def _mean_std(vals: List[float]) -> (float, float):
    n = len(vals)
    if n == 0:
        return 0.0, 0.0
    m = sum(vals) / n
    var = sum((x - m) ** 2 for x in vals) / max(1, (n - 1))
    return m, math.sqrt(var)


def _zscore(curr: float, hist: List[float]) -> Optional[float]:
    """
    Z = (curr - mean(hist)) / std(hist), если std>0 и гист длиной >= 5.
    """
    if len(hist) < 5:
        return None
    m, s = _mean_std(hist)
    if s <= 0:
        return None
    return (curr - m) / s


def _calc_metrics_for_symbol(sym: str, interval_min: int, window: int,
                             vol24h_map: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """
    Возвращает одну запись для /signals:
    {
      "ts": "YYYY-mm-dd HH:MM:SS",
      "symbol": "BTCUSDT",
      "close": 61234.5,
      "zprice": 2.31,
      "vol_mult": 1.87,
      "vol24h_usd": 123456789.0,
      "bar_ts": 1759812000000,
      "oi_z": 0.91 | None
    }
    """
    kl = _kline_bybit(sym, interval_min, limit=window + 1)
    if not kl or len(kl) < (window + 1):
        return None

    # Берём последнюю свечу как "текущую"
    curr = kl[-1]
    hist = kl[:-1]  # окно истории без текущей

    # Цена и Z по цене
    close = float(curr["close"])
    close_hist = [float(x["close"]) for x in hist]
    zprice = _zscore(close, close_hist)

    # Объёмы: turnover(quote)
    turn_curr = float(curr["turnover_quote"])
    turn_hist = [float(x["turnover_quote"]) for x in hist]
    mean_turn, _ = _mean_std(turn_hist)
    vol_mult = (turn_curr / mean_turn) if mean_turn > 0 else None

    # vol24h_usd
    vol24h = float(vol24h_map.get(sym, 0.0))

    # ts / bar_ts
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


def _rebuild_signals():
    """
    Переcчитывает кешированный список сигналов.
    НИЧЕГО не фильтруем по порогам — это делает push_signals.py.
    """
    start = time.time()
    universe = _STATE["universe"]
    if not universe:
        return

    # 24h обороты одной пачкой
    vol24h_map = _tickers24h_bybit(universe)

    res: List[Dict[str, Any]] = []
    n = 0
    for sym in universe:
        m = _calc_metrics_for_symbol(sym, INTERVAL_MIN, WINDOW, vol24h_map)
        if m:
            res.append(m)
        n += 1
        # Небольшой троттлинг, чтобы не убить API
        if n % 10 == 0:
            time.sleep(0.2)

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
    })


@app.route("/signals", methods=["GET"])
def signals():
    # Отдаём КЕШ — чтобы /signals был быстрым и стабильным.
    return jsonify({
        "data": _STATE["signals"],
        "count": len(_STATE["signals"]),
        "last_update": _STATE["last_update"],
        "interval_min": INTERVAL_MIN,
        "window": WINDOW,
    })


# -------------------- Entry -----------------------

def main():
    # 1) поднимем вселенную
    _STATE["universe"] = _load_universe()

    # 2) старт воркера
    t = threading.Thread(target=_worker_loop, daemon=True)
    t.start()

    # 3) HTTP
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
