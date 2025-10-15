from __future__ import annotations
import math, requests

def _map_symbol_to_bybit_linear_safe(sym: str):
    try:
        from push_signals import _map_symbol_to_bybit_linear  # type: ignore
        return _map_symbol_to_bybit_linear(sym) or sym
    except Exception:
        return sym

def oi_z_score_ext(symbol: str, interval="5min", window=48):
    try:
        win = int(window)
        if win <= 2:
            win = 3
    except Exception:
        win = 48
    bybit_symbol = _map_symbol_to_bybit_linear_safe(symbol)
    if not bybit_symbol:
        return None
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {"category": "linear", "symbol": bybit_symbol, "intervalTime": str(interval or "5min"), "limit": str(win + 2)}
    r = requests.get(url, params=params, timeout=(5, 10))
    j = r.json()
    if j.get("retCode") != 0:
        return None
    arr = (j.get("result") or {}).get("list") or []
    if len(arr) < win:
        return None
    vals = []
    for x in arr[-win:]:
        try:
            vals.append(float(x["openInterest"]))
        except Exception:
            return None
    if not vals or len(vals) < 2:
        return None
    mu = sum(vals)/len(vals)
    var = sum((v-mu)**2 for v in vals)/(len(vals)-1)
    sd = math.sqrt(max(var, 0.0))
    return float((vals[-1]-mu)/sd) if sd > 0 else 0.0
