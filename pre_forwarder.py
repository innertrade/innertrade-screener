#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pre_forwarder: library-only
Экспортирует oi_z_score(sym, interval="5min", window=48) без побочных эффектов.
Не печатает и не запускает циклов. Используется из push_signals.py.
"""
import os
import statistics
import requests

_T_CONN = 5
_T_READ = 8

def _map_symbol_to_bybit_linear(symbol: str):
    if not symbol:
        return None
    s = symbol.upper().replace("-", "").replace("PERP", "")
    if s.endswith("USDT") and len(s) >= 7:
        return s
    return None

_session = requests.Session()
_session.headers.update({"User-Agent": "innertrade-pre-forwarder/1.0"})

def _fetch_oi_series_bybit(symbol: str, interval: str, limit: int):
    """Возвращает список openInterest по времени ВПЕРЁД (старое → новое)."""
    bybit_symbol = _map_symbol_to_bybit_linear(symbol)
    if not bybit_symbol:
        return None
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {"category":"linear","symbol":bybit_symbol,"intervalTime":interval,"limit":str(limit)}
    try:
        r = _session.get(url, params=params, timeout=(_T_CONN, _T_READ))
        r.raise_for_status()
        rows = (r.json().get("result") or {}).get("list") or []
        series = [float(x["openInterest"]) for x in reversed(rows) if x.get("openInterest") is not None]
        return series if series else None
    except Exception:
        return None

def oi_z_score(sym: str, interval: str = None, window: int = None):
    """
    Z-скор по ΔOI:
      d_t = OI[t] - OI[t-1]
      oi_z = (d_last - mean(d_tail)) / stdev(d_tail), tail = последние `window` d_t
    Возвращает float или None.
    """
    if interval is None:
        interval = os.getenv("FORWARD_OI_INTERVAL", "5min")
    if window is None:
        window = int(os.getenv("FORWARD_OI_WINDOW", "48"))

    limit = max(window + 2, 10)
    series = _fetch_oi_series_bybit(sym, interval, limit)
    if not series or len(series) < window + 1:
        return None

    diffs = [series[i] - series[i-1] for i in range(1, len(series))]
    tail = diffs[-window:]
    if not tail:
        return None

    mu = statistics.mean(tail)
    sd = statistics.pstdev(tail)
    if not sd:
        return None
    return (diffs[-1] - mu) / sd

# никаких запусков при импорте
