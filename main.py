from __future__ import annotations

import os
import time
import json
import math
import threading
from typing import List, Dict, Any, Optional
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests


# =========================
# Конфиг из окружения (.env)
# =========================

def _get_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default))))
    except Exception:
        return default


MODE                 = os.getenv("MODE", "signals_5m")

# Вселенная
UNIVERSE_MODE        = os.getenv("UNIVERSE_MODE", "LIST").upper()   # LIST | ALL
UNIVERSE_LIST        = os.getenv("UNIVERSE_LIST", "")               # CSV тикеров
UNIVERSE_MAX         = _get_int("UNIVERSE_MAX", 500)

# Адаптивные сигналы (5m)
ADAPTIVE_SIGNALS     = os.getenv("ADAPTIVE_SIGNALS", "true").lower() == "true"
BASELINE_5M_WINDOW   = _get_int("BASELINE_5M_WINDOW", 96)     # ~8 часов (96 баров)
PRICE_Z_5M           = _get_float("PRICE_Z_5M", 1.0)          # z-score порог по цене
VOL_MULT_MEDIAN_5M   = _get_float("VOL_MULT_MEDIAN_5M", 1.3)  # кратность объёма к медиане
DEBOUNCE_BARS_5M     = _get_int("DEBOUNCE_BARS_5M", 1)        # антидублирование (минимум баров)

# HTTP/цикл
HTTP_PORT            = _get_int("HTTP_PORT", 8080)
SLEEP_SEC            = _get_int("SLEEP_SEC", 60)               # пауза между сканами в секундах
REQUEST_TIMEOUT      = _get_int("REQUEST_TIMEOUT", 10)


# =========================
# Глобали (память)
# =========================

_SIGNALS: List[Dict[str, Any]] = []
_SIGNALS_LOCK = threading.Lock()
_LAST_SIGNAL_BAR: Dict[str, int] = {}     # symbol -> last 5m bar open time (ms) где мы уже сигналили
_STOP = threading.Event()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _median(xs: List[float]) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    s = sorted(xs)
    m = n // 2
    if n % 2 == 1:
        return s[m]
    return 0.5 * (s[m-1] + s[m])


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mu = _mean(xs)
    var = sum((x - mu) ** 2 for x in xs) / (n - 1)
    return math.sqrt(max(var, 0.0))


# =========================
# Bybit public API (v5)
# =========================

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")

_session = requests.Session()
_session.headers.update({"User-Agent": "screener-bybit-clean/1.0"})


def bybit_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BYBIT_BASE}{path}"
    r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_bybit_universe_usdt(maxn: int = 2000) -> List[str]:
    """
    Получаем все USDT линейные контракты (perp) из instruments-info.
    category=linear
    """
    try:
        data = bybit_get("/v5/market/instruments-info", {"category": "linear"})
        out: List[str] = []
        for it in data.get("result", {}).get("list", []) or []:
            # Поля по v5: symbol, quoteCoin, status (Trading/...)
            sym = (it.get("symbol") or "").upper()
            q = (it.get("quoteCoin") or "").upper()
            st = (it.get("status") or "").lower()
            if not sym or q != "USDT":
                continue
            if st != "trading":
                continue
            if not sym.endswith("USDT"):
                continue
            out.append(sym)
        out = sorted(set(out))
        return out[:maxn]
    except Exception as e:
        print(f"{_now_iso()} | WARNING | [bybit] instruments-info fail: {e}")
        return []


def bybit_klines_5m(symbol: str, limit: int) -> List[Dict[str, Any]]:
    """
    v5 kline:
      GET /v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=200
    Ответ: result.list = [[start, open, high, low, close, volume, turnover], ...]
    """
    params = {"category": "linear", "symbol": symbol, "interval": "5", "limit": str(limit)}
    data = bybit_get("/v5/market/kline", params)
    arr = data.get("result", {}).get("list", []) or []
    # Bybit возвращает newest first — убедимся в порядке по времени возрастания
    # Формат строки: [start, open, high, low, close, volume, turnover]
    rows = []
    for row in arr:
        try:
            rows.append({
                "ts": int(row[0]),          # ms
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),    # базовый объём (контракты/монеты)
                "turnover": float(row[6]) if len(row) > 6 and row[6] is not None else None,
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["ts"])
    return rows


def bybit_ticker_24h(symbol: str) -> Dict[str, Any]:
    """
    v5 tickers:
      GET /v5/market/tickers?category=linear&symbol=BTCUSDT
    Важно: quoteVolume в v5 может называться turnover24h (в USDT).
    """
    params = {"category": "linear", "symbol": symbol}
    data = bybit_get("/v5/market/tickers", params)
    lst = data.get("result", {}).get("list", []) or []
    return lst[0] if lst else {}


# =========================
# Вселенная
# =========================

def get_universe() -> List[str]:
    if UNIVERSE_MODE == "LIST" and UNIVERSE_LIST.strip():
        raw = [x.strip().upper() for x in UNIVERSE_LIST.replace(";", ",").split(",") if x.strip()]
        uniq: List[str] = []
        for s in raw:
            s2 = s if s.endswith("USDT") else (s + "USDT")
            if s2 not in uniq:
                uniq.append(s2)
        return uniq
    # иначе — притягиваем все USDT с Bybit и режем по UNIVERSE_MAX
    return fetch_bybit_universe_usdt(maxn=UNIVERSE_MAX)


# =========================
# Логика сигналов (адаптивная)
# =========================

def check_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Сигнал, если И цена (zprice) >= PRICE_Z_5M, И объём (vol_mult) >= VOL_MULT_MEDIAN_5M.
    """
    try:
        win = max(30, BASELINE_5M_WINDOW)  # чуть больше запаса
        kl = bybit_klines_5m(symbol, limit=win)
        if len(kl) < BASELINE_5M_WINDOW:
            print(f"{_now_iso()} | INFO | [sigskip] {symbol}: klines<window")
            return None

        sample = kl[-BASELINE_5M_WINDOW:]
        closes = [x["close"] for x in sample]
        vols = [x["volume"] for x in sample]

        mu = _mean(closes)
        sd = _stdev(closes)
        vmed = _median(vols)

        last = sample[-1]
        last_ts = last["ts"]
        # антидублирование: не сигналим дважды на один и тот же бар
        if DEBOUNCE_BARS_5M > 0:
            last_seen = _LAST_SIGNAL_BAR.get(symbol)
            if last_seen is not None and last_seen >= last_ts:
                return None

        zprice = (last["close"] - mu) / sd if sd > 0 else 0.0
        vol_mult = (last["volume"] / vmed) if vmed > 0 else 0.0

        if zprice < PRICE_Z_5M:
            print(f"{_now_iso()} | INFO | [sigskip] {symbol}: price_spike<threshold")
            return None
        if vol_mult < VOL_MULT_MEDIAN_5M:
            print(f"{_now_iso()} | INFO | [sigskip] {symbol}: volume_spike<threshold")
            return None

        # справка 24h (по возможности)
        try:
            t24 = bybit_ticker_24h(symbol)
            # в v5 linear turnover24h — суммарный оборот в quote (USDT)
            vol24h_quote = float(t24.get("turnover24h") or 0.0)
        except Exception:
            vol24h_quote = 0.0

        sig = {
            "ts": _now_iso(),
            "symbol": symbol,
            "close": last["close"],
            "zprice": round(zprice, 3),
            "vol_mult": round(vol_mult, 2),
            "vol24h_usd": round(vol24h_quote, 0),
            "bar_ts": last_ts
        }
        _LAST_SIGNAL_BAR[symbol] = last_ts
        return sig

    except Exception as e:
        print(f"{_now_iso()} | WARNING | [sigerr] {symbol}: {e}")
        return None


# =========================
# HTTP
# =========================

class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.startswith("/health"):
            self._json({
                "status": "ok",
                "mode": MODE,
                "adaptive": ADAPTIVE_SIGNALS,
                "port": HTTP_PORT
            })
            return
        if self.path.startswith("/signals"):
            with _SIGNALS_LOCK:
                data = list(_SIGNALS)
            self._json({"kind": "signals", "count": len(data), "data": data})
            return
        self._json({"error": "not found"}, 404)


def _http_thread():
    srv = HTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    print(f"{_now_iso()} | INFO | HTTP on :{HTTP_PORT} (/health /signals)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


# =========================
# Главный цикл
# =========================

def main_loop():
    syms = get_universe()
    print(f"{_now_iso()} | INFO | [universe] n={len(syms)} mode={UNIVERSE_MODE}")

    while not _STOP.is_set():
        t0 = time.time()
        fired = 0
        print(f"{_now_iso()} | INFO | [sigpulse] scan start n={len(syms)}")

        for s in syms:
            sig = check_symbol(s)
            if sig:
                print(f"{_now_iso()} | INFO | [signal] {sig}")
                with _SIGNALS_LOCK:
                    _SIGNALS.append(sig)
                    if len(_SIGNALS) > 200:
                        del _SIGNALS[:-200]
                fired += 1

        dt = time.time() - t0
        print(f"{_now_iso()} | INFO | [sigpulse] scan end fired={fired} dt={dt:.1f}s")
        time.sleep(max(1, SLEEP_SEC))


def main():
    if MODE.lower() != "signals_5m":
        print(f"{_now_iso()} | WARNING | MODE={MODE} — в этом билде доступен только signals_5m")

    th = threading.Thread(target=_http_thread, daemon=True)
    th.start()

    try:
        main_loop()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
