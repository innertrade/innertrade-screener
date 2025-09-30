#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, csv, json, math, signal, sqlite3, threading, argparse, logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse as _urlparse, urlparse, parse_qs

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

AI_TELEGRAM = True
try:
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
except Exception:
    AI_TELEGRAM = False

BUILD_TAG = "screener-5m-signals-2025-09-30-all-usdt-bybit"

# =======================
# Config
# =======================

@dataclass
class Config:
    # Bybit/Binance hosts
    ByBitRestBase: str = "api.bytick.com"
    ByBitRestFallback: str = "api.bybit.com"
    PriceFallbackBinance: str = "api.binance.com"

    # Universe
    UniverseMax: int = 50
    UniverseMode: str = "TOP"          # TOP | ALL | LIST
    UniverseRefreshMin: int = 1
    UniverseList: Optional[List[str]] = None

    # Network
    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"           # linear | inverse | option
    Concurrency: int = 12
    CacheTTL: int = 15

    # Files/logging
    LogFile: str = "bot.log"
    CsvFile: str = "prices.csv"
    DbFile: str = "prices.sqlite3"
    LogLevel: str = "INFO"
    PrintOnly: bool = False

    # HTTP
    HttpPort: int = 8080

    # Modes
    Once: bool = False
    Loop: bool = True

    # Analytics windows (для /volatility /trend)
    VolWindowMin: int = 120
    TrendWindowMin: int = 120

    # --- РЕЖИМЫ ---
    Mode: str = os.getenv("MODE", "signals_5m")  # signals_5m | breakout | activity | volatility | trend

    # --- Breakout v0 (1m на базе цен/объёма снапшота) ---
    BaselineHours: int = int(os.getenv("BASELINE_HOURS", "2"))
    LowVolThresholdPct: float = float(os.getenv("LOW_VOL_THRESHOLD_PCT", "1.2"))
    Min24hVolumeUSD: float = float(os.getenv("MIN_24H_VOLUME_USD", "50000000"))
    SpikeVolRatioMin: float = float(os.getenv("SPIKE_VOL_RATIO_MIN", "3.0"))
    SpikePricePctMin: float = float(os.getenv("SPIKE_PRICE_PCT_MIN", "0.7"))
    MinNotionalUSD: float = float(os.getenv("MIN_NOTIONAL_USD", "100000"))
    CooldownMinutes: int = int(os.getenv("COOLDOWN_MINUTES", "15"))

    # --- Signals 5m thresholds ---
    Baseline5mHours: int = int(os.getenv("BASELINE_5M_HOURS", "1"))
    PriceSpikePct5m: float = float(os.getenv("PRICE_SPIKE_PCT_5M", "0.7"))
    VolumeSpikeX5m: float = float(os.getenv("VOLUME_SPIKE_X_5M", "3.0"))
    OIIncreasePct5m: float = float(os.getenv("OI_INCREASE_PCT_5M", "0.5"))
    KlineSource: str = os.getenv("KLINE_SOURCE", "bybit")   # bybit
    OISource: str = os.getenv("OI_SOURCE", "bybit")         # bybit

    # Telegram
    TelegramPolling: bool = (os.getenv("TELEGRAM_POLLING", "false").lower() == "true")
    TelegramBotToken: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    TelegramAlertChatId: Optional[str] = os.getenv("TELEGRAM_ALERT_CHAT_ID")
    TelegramAllowedChatId: Optional[str] = os.getenv("TELEGRAM_ALLOWED_CHAT_ID")

def env(name, default, cast=None):
    v = os.getenv(name)
    if v is None: return default
    if cast:
        try: return cast(v)
        except Exception: return default
    return v

def auto_port(default: int = 8080) -> int:
    raw = os.getenv("PORT") or os.getenv("HTTP_PORT") or str(default)
    try:
        m = re.search(r"\d+", str(raw))
        return int(m.group()) if m else default
    except Exception:
        return default

def clean_host(v: str) -> str:
    if not v: return v
    v = v.strip()
    if "://" in v:
        parsed = _urlparse(v)
        host = (parsed.netloc or parsed.path or "").strip("/")
    else:
        host = v.strip("/")
    return host.split("/")[0].strip()

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Screener: /health /activity /volatility /trend /signals /ip")

    # Universe
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL","LIST"])
    p.add_argument("--max", type=int, default=env("UNIVERSE_MAX",50,int))
    p.add_argument("--refresh", type=int, default=env("UNIVERSE_REFRESH_MIN",1,int))
    p.add_argument("--list", type=str, default=env("UNIVERSE_LIST",None))

    # Network
    p.add_argument("--timeout", type=int, default=env("REQUEST_TIMEOUT",10,int))
    p.add_argument("--retries", type=int, default=env("MAX_RETRIES",3,int))
    p.add_argument("--backoff", type=float, default=env("BACKOFF_FACTOR",0.6,float))
    p.add_argument("--category", default=env("BYBIT_CATEGORY","linear"), choices=["linear","inverse","option"])
    p.add_argument("--concurrency", type=int, default=env("CONCURRENCY",12,int))

    # Files/logging
    p.add_argument("--log", default=env("LOG_FILE","bot.log"))
    p.add_argument("--csv", default=env("CSV_FILE","prices.csv"))
    p.add_argument("--db", default=env("DB_FILE","prices.sqlite3"))
    p.add_argument("--level", default=env("LOG_LEVEL","INFO"), choices=["DEBUG","INFO","WARNING","ERROR"])
    p.add_argument("--print-only", action="store_true", default=env("PRINT_ONLY","false").lower()=="true")

    # Domains
    p.add_argument("--bybit-base", default=env("BYBIT_REST_BASE","api.bytick.com"))
    p.add_argument("--bybit-fallback", default=env("BYBIT_REST_FALLBACK","api.bybit.com"))
    p.add_argument("--binance", default=env("PRICE_FALLBACK_BINANCE","api.binance.com"))

    # HTTP
    p.add_argument("--http", type=int, default=auto_port(8080))

    # Modes
    p.add_argument("--once", action="store_true")
    p.add_argument("--loop", action="store_true")

    # Analytics windows
    p.add_argument("--vol-window", type=int, default=env("VOL_WINDOW_MIN",120,int))
    p.add_argument("--trend-window", type=int, default=env("TREND_WINDOW_MIN",120,int))

    a = p.parse_args()

    return Config(
        ByBitRestBase=clean_host(a.bybit_base),
        ByBitRestFallback=clean_host(a.bybit_fallback),
        PriceFallbackBinance=clean_host(a.binance),
        UniverseMax=a.max, UniverseMode=a.mode, UniverseRefreshMin=a.refresh,
        UniverseList=[s.strip() for s in a.list.split(",")] if a.list else None,
        RequestTimeout=a.timeout, MaxRetries=a.retries, BackoffFactor=a.backoff, Category=a.category,
        Concurrency=a.concurrency, CacheTTL=15,
        LogFile=a.log, CsvFile=a.csv, DbFile=a.db, LogLevel=a.level, PrintOnly=a.print_only,
        HttpPort=a.http, Once=a.once, Loop=a.loop or (not a.once),
        VolWindowMin=a.vol_window, TrendWindowMin=a.trend_window
    )

# =======================
# Logger
# =======================

def setup_logger(cfg: Config) -> logging.Logger:
    lg = logging.getLogger("bot")
    lg.setLevel(getattr(logging, cfg.LogLevel.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    lg.handlers.clear(); lg.addHandler(sh)
    try:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(cfg.LogFile, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt); lg.addHandler(fh)
    except Exception:
        pass
    return lg

# =======================
# Universe
# =======================

DEFAULT_TOP = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","TONUSDT","TRXUSDT","LINKUSDT",
]

DEFAULT_ALL = DEFAULT_TOP  # будет расширено авто-дискавером

_universe_cache: Dict[str, float] = {}  # sym -> ts_seen
_universe_lock = threading.Lock()

def autodiscover_universe_bybit(session: requests.Session, cfg: Config, logger: logging.Logger) -> List[str]:
    """
    Берём ВСЕ линейные USDT фьючи с Bybit (если доступен).
    """
    try:
        url = f"https://{cfg.ByBitRestBase}/v5/market/instruments-info"
        params = {"category": cfg.Category}
        r = session.get(url, params=params, timeout=cfg.RequestTimeout)
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        syms = []
        for it in lst:
            s = (it.get("symbol") or "").upper()
            if s.endswith("USDT"):
                syms.append(s)
        if not syms:
            raise RuntimeError("empty list from bytick base")
        return sorted(set(syms))
    except Exception as e:
        logger.warning(f"autodiscover(bytick) fail: {e}; try fallback")
        try:
            url = f"https://{cfg.ByBitRestFallback}/v5/market/instruments-info"
            params = {"category": cfg.Category}
            r = session.get(url, params=params, timeout=cfg.RequestTimeout)
            r.raise_for_status()
            data = r.json()
            lst = (((data or {}).get("result") or {}).get("list") or [])
            syms = []
            for it in lst:
                s = (it.get("symbol") or "").upper()
                if s.endswith("USDT"):
                    syms.append(s)
            if syms:
                return sorted(set(syms))
        except Exception as e2:
            logger.warning(f"autodiscover(bybit-fallback) fail: {e2}")
    return []

def get_universe(cfg: Config, session: Optional[requests.Session]=None, logger: Optional[logging.Logger]=None) -> List[str]:
    if cfg.UniverseList:
        return [s.strip().upper() for s in cfg.UniverseList][:cfg.UniverseMax]
    if cfg.UniverseMode.upper() == "TOP":
        return DEFAULT_TOP[:min(cfg.UniverseMax, len(DEFAULT_TOP))]
    if cfg.UniverseMode.upper() == "ALL":
        if session and logger:
            syms = autodiscover_universe_bybit(session, cfg, logger)
            if syms:
                return syms[:cfg.UniverseMax]
        # fallback
        base = list(DEFAULT_ALL)
        return base[:cfg.UniverseMax]
    # LIST без списка -> TOP
    return DEFAULT_TOP[:min(cfg.UniverseMax, len(DEFAULT_TOP))]

# =======================
# DB & CSV
# =======================

class DB:
    def __init__(self, path:str, logger:logging.Logger):
        self.path=path; self.log=logger; self._init()
    def _init(self):
        con=sqlite3.connect(self.path)
        try:
            c=con.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS prices(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL, symbol TEXT NOT NULL, source TEXT NOT NULL,
              price REAL NOT NULL, vol_quote_24h REAL, vol_base_24h REAL)""")
            con.commit()
        finally:
            con.close()
    def insert(self, ts:str, sym:str, src:str, price:float, vq:Optional[float], vb:Optional[float]):
        try:
            con=sqlite3.connect(self.path); c=con.cursor()
            c.execute("INSERT INTO prices(ts,symbol,source,price,vol_quote_24h,vol_base_24h) VALUES(?,?,?,?,?,?)",
                      (ts, sym.upper(), src, float(price),
                       float(vq) if vq is not None else None,
                       float(vb) if vb is not None else None))
            con.commit()
        except Exception as e:
            self.log.error(f"DB insert error {sym}: {e}")
        finally:
            try: con.close()
            except: pass
    def history(self, sym:str, window_min:int)->List[Tuple[str,float]]:
        try:
            con=sqlite3.connect(self.path); c=con.cursor()
            c.execute("""SELECT ts, price FROM prices
                         WHERE symbol=? AND ts >= datetime('now', ?) ORDER BY ts ASC""",
                      (sym.upper(), f"-{int(window_min)} minutes"))
            rows=[(ts,float(p)) for ts,p in c.fetchall()]
            return rows
        except Exception as e:
            self.log.error(f"DB history error {sym}: {e}"); return []
        finally:
            try: con.close()
            except: pass
    def last(self, sym:str):
        try:
            con=sqlite3.connect(self.path); c=con.cursor()
            c.execute("""SELECT ts, price, vol_quote_24h, vol_base_24h
                         FROM prices WHERE symbol=? ORDER BY ts DESC LIMIT 1""", (sym.upper(),))
            r=c.fetchone()
            return (r[0], float(r[1]), (float(r[2]) if r[2] is not None else None),
                    (float(r[3]) if r[3] is not None else None)) if r else None
        except Exception as e:
            self.log.error(f"DB last error {sym}: {e}"); return None
        finally:
            try: con.close()
            except: pass

def ensure_csv(path:str, print_only:bool):
    if print_only: return
    if not os.path.isfile(path):
        with open(path,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp","symbol","source","price","vol_quote_24h","vol_base_24h"])

def append_csv(path:str, row:List[Any], print_only:bool):
    if print_only: return
    with open(path,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow(row)

# =======================
# HTTP session
# =======================

def build_session(cfg: Config) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=cfg.MaxRetries, backoff_factor=cfg.BackoffFactor,
                  status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=100)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    s.trust_env = True
    return s

def n_bybit(sym:str)->str: return sym.replace("-","").upper()
def n_binance(sym:str)->str: return sym.replace("-","").upper()

# =======================
# Price/Volume snapshot fetchers
# =======================

def fetch_bybit(session, cfg, symbol):
    sym = n_bybit(symbol)
    url = f"https://{cfg.ByBitRestBase}/v5/market/tickers"
    r = session.get(url, params={"category": cfg.Category, "symbol": sym}, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    lst = (((data or {}).get("result") or {}).get("list") or [])
    if not lst:
        raise RuntimeError(f"empty result: {data}")
    item = lst[0]
    price = float(item["lastPrice"])
    vq = float(item.get("turnover24h")) if item.get("turnover24h") else None
    vb = float(item.get("volume24h")) if item.get("volume24h") else None
    return price, vq, vb

def fetch_bybit_fb(session, cfg, symbol):
    sym = n_bybit(symbol)
    url = f"https://{cfg.ByBitRestFallback}/v5/market/tickers"
    r = session.get(url, params={"category": cfg.Category, "symbol": sym}, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    lst = (((data or {}).get("result") or {}).get("list") or [])
    if not lst:
        raise RuntimeError(f"empty result: {data}")
    item = lst[0]
    price = float(item["lastPrice"])
    vq = float(item.get("turnover24h")) if item.get("turnover24h") else None
    vb = float(item.get("volume24h")) if item.get("volume24h") else None
    return price, vq, vb

def fetch_binance(session, cfg, symbol):
    sym = n_binance(symbol)
    url = f"https://{cfg.PriceFallbackBinance}/api/v3/ticker/24hr"
    r = session.get(url, params={"symbol": sym}, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    d = r.json()
    if "lastPrice" not in d:
        raise RuntimeError(f"unexpected binance payload: {d}")
    price = float(d["lastPrice"])
    vq = float(d.get("quoteVolume")) if d.get("quoteVolume") else None
    vb = float(d.get("volume")) if d.get("volume") else None
    return price, vq, vb

def fetch_coingecko(session, cfg, symbol):
    # Простая страховка — не используем для сигналов, лишь как последний fallback по цене/24h.
    time.sleep(0.5)
    raise RuntimeError("coingecko disabled for brevity in this build")

def get_snapshot(session, cfg, symbol, logger):
    for fn, name in (
        (fetch_bybit, "bytick"),
        (fetch_bybit_fb, "bybit"),
        (fetch_binance, "binance"),
    ):
        try:
            price, vq, vb = fn(session, cfg, symbol)
            return name, price, vq, vb
        except Exception as e:
            logger.warning(f"[{name}] {symbol} fail: {e}")
    try:
        price, vq_usd, vb_est = fetch_coingecko(session, cfg, symbol)
        return "coingecko", price, vq_usd, vb_est
    except Exception as e:
        logger.warning(f"[coingecko] {symbol} fail: {e}")
    return None

# =======================
# 5m data (klines & OI) from Bybit
# =======================

def bybit_klines_5m(session: requests.Session, cfg: Config, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    v5 /market/kline (interval='5')
    Возвращает список свечей (последняя — текущая/самая свежая).
    """
    sym = n_bybit(symbol)
    params = {"category": cfg.Category, "symbol": sym, "interval": "5", "limit": str(limit)}
    # primary
    url = f"https://{cfg.ByBitRestBase}/v5/market/kline"
    try:
        r = session.get(url, params=params, timeout=cfg.RequestTimeout)
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        # формат: [start, open, high, low, close, volume, turnover]
        out = []
        for row in lst:
            out.append({
                "start": int(row[0])//1000,
                "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]),
                "volume": float(row[5]), "turnover": float(row[6]) if len(row)>6 and row[6] is not None else None
            })
        return out
    except Exception:
        # fallback
        url = f"https://{cfg.ByBitRestFallback}/v5/market/kline"
        r = session.get(url, params=params, timeout=cfg.RequestTimeout)
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        out = []
        for row in lst:
            out.append({
                "start": int(row[0])//1000,
                "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]),
                "volume": float(row[5]), "turnover": float(row[6]) if len(row)>6 and row[6] is not None else None
            })
        return out

def bybit_oi_5m(session: requests.Session, cfg: Config, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    v5 /market/open-interest (interval='5min' у Bybit)
    Возвращает список точек OI (последняя — свежая).
    """
    sym = n_bybit(symbol)
    params = {"category": cfg.Category, "symbol": sym, "interval": "5min", "limit": str(limit)}
    url = f"https://{cfg.ByBitRestBase}/v5/market/open-interest"
    try:
        r = session.get(url, params=params, timeout=cfg.RequestTimeout)
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        out = []
        for row in lst:
            # формат: [timestamp, value]
            out.append({"ts": int(row[0])//1000, "oi": float(row[1])})
        return out
    except Exception:
        url = f"https://{cfg.ByBitRestFallback}/v5/market/open-interest"
        r = session.get(url, params=params, timeout=cfg.RequestTimeout)
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        out = []
        for row in lst:
            out.append({"ts": int(row[0])//1000, "oi": float(row[1])})
        return out

# =======================
# Analytics helpers
# =======================

def realized_vol(prices: List[Tuple[str,float]], window_min: int) -> Optional[float]:
    if len(prices) < 3: return None
    vals = [p for _,p in prices if p>0]
    if len(vals) < 3: return None
    rets = []
    for i in range(1,len(vals)):
        try: rets.append(math.log(vals[i]/vals[i-1]))
        except Exception: pass
    if len(rets) < 2: return None
    mean = sum(rets)/len(rets)
    var = sum((x-mean)**2 for x in rets)/(len(rets)-1)
    std = math.sqrt(var)
    scale = math.sqrt(1440.0/max(1.0, float(window_min)))
    return std*scale*100.0

def linear_trend_pct_day(prices: List[Tuple[str,float]], window_min: int) -> Optional[float]:
    if len(prices) < 3: return None
    ys = [p for _,p in prices]; xs = list(range(len(ys)))
    n = len(xs); sx=sum(xs); sy=sum(ys)
    sxx=sum(x*x for x in xs); sxy=sum(xs[i]*ys[i] for i in range(n))
    denom = n*sxx - sx*sx
    if denom == 0: return None
    slope = (n*sxy - sx*sy)/denom
    last = ys[-1]
    if last <= 0: return None
    steps_per_day = 1440.0/max(1.0, float(window_min))/n
    return (slope/last)*steps_per_day*100.0

def atr_like_pct(prices: List[Tuple[str, float]]) -> Optional[float]:
    if len(prices) < 3: return None
    vals = [p for _, p in prices if p > 0]
    if len(vals) < 3: return None
    acc = 0.0; n = 0
    for i in range(1, len(vals)):
        c0, c1 = vals[i-1], vals[i]
        if c0 > 0:
            acc += abs(c1 - c0) / c0
            n += 1
    if n == 0: return None
    return (acc / n) * 100.0

def pct_change(a: float, b: float) -> Optional[float]:
    if a is None or b is None or b == 0: return None
    return (a / b - 1.0) * 100.0

def approx_notional_1m_from_24h(vol24h_usd: Optional[float]) -> Optional[float]:
    if vol24h_usd is None: return None
    return vol24h_usd / 1440.0

# =======================
# Engines
# =======================

class BreakoutEngineV0:
    def __init__(self, cfg: Config, db: DB, logger: logging.Logger, send_alert_fn):
        self.cfg = cfg
        self.db = db
        self.log = logger
        self.send_alert = send_alert_fn
        self.last_signal_ts: Dict[str, float] = {}

    def _cooldown_ok(self, symbol: str) -> bool:
        cd = max(1, self.cfg.CooldownMinutes) * 60
        t0 = self.last_signal_ts.get(symbol, 0)
        return (time.time() - t0) >= cd

    def _mark_signalled(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

    def check_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        win_min = self.cfg.BaselineHours * 60
        hist = self.db.history(symbol, win_min)
        if len(hist) < max(10, win_min // 2):
            return None
        atr = atr_like_pct(hist)
        if atr is None or atr > self.cfg.LowVolThresholdPct:
            return None
        last_price = hist[-1][1]
        prev_price = hist[-2][1] if len(hist) >= 2 else None
        dprice_pct = pct_change(last_price, prev_price)
        if dprice_pct is None or abs(dprice_pct) < self.cfg.SpikePricePctMin:
            return None
        snap = self.db.last(symbol)
        vol24h_usd = snap[2] if snap else None
        if (vol24h_usd is None) or (vol24h_usd < self.cfg.Min24hVolumeUSD):
            return None
        approx_1m = approx_notional_1m_from_24h(vol24h_usd)
        if (approx_1m is None) or (approx_1m < self.cfg.MinNotionalUSD):
            return None
        if not self._cooldown_ok(symbol):
            return None

        sig = {
            "symbol": symbol,
            "price": last_price,
            "price_change_60s_pct": dprice_pct,
            "atr2h_pct": atr,
            "vol24h_usd": vol24h_usd,
            "notional1m_est_usd": approx_1m,
            "ts": now(),
            "strength": round(min(5.0, abs(dprice_pct) / max(0.1, self.cfg.LowVolThresholdPct) * 1.2), 2),
            "mode": "breakout_v0"
        }
        self._mark_signalled(symbol)
        return sig

class Signals5mEngine:
    """
    Сигнал, если одновременно выполняются:
      - |Δцены за 5м| >= PRICE_SPIKE_PCT_5M
      - объём 5м >= VOLUME_SPIKE_X_5M * средний 5м объём за Baseline5mHours
      - рост OI за 5м >= OI_INCREASE_PCT_5M
      - 24h оборот (USD) >= MIN_24H_VOLUME_USD и мин. «минутный» notional >= MIN_NOTIONAL_USD
    """
    def __init__(self, cfg: Config, db: DB, logger: logging.Logger, session: requests.Session, send_alert_fn):
        self.cfg = cfg
        self.db = db
        self.log = logger
        self.session = session
        self.send_alert = send_alert_fn
        self.last_signal_ts: Dict[str, float] = {}

    def _cooldown_ok(self, symbol: str) -> bool:
        cd = max(1, self.cfg.CooldownMinutes) * 60
        t0 = self.last_signal_ts.get(symbol, 0)
        return (time.time() - t0) >= cd

    def _mark_signalled(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

    def check_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        # 1) Получаем 5м свечи (последние N)
        try:
            kl = bybit_klines_5m(self.session, self.cfg, symbol, limit= max(20, self.cfg.Baseline5mHours*12 + 3))
        except Exception as e:
            self.log.warning(f"klines5m fail {symbol}: {e}")
            return None
        if len(kl) < 6:  # нужно минимум 2 последних + база
            return None

        last = kl[-1]
        prev = kl[-2]
        px_last = float(last["close"])
        px_prev = float(prev["close"])
        dprice_pct = pct_change(px_last, px_prev)
        if dprice_pct is None or abs(dprice_pct) < self.cfg.PriceSpikePct5m:
            return None

        # 2) Объём: last 5m turnover против среднего за baseline
        baseline_candles = max(6, self.cfg.Baseline5mHours * 12)  # 12 пятиминуток в час
        base_slice = kl[-(baseline_candles+1):-1]  # без последней
        vols = [c.get("turnover") if c.get("turnover") is not None else c.get("volume")*c.get("close", px_last) for c in base_slice]
        vols = [float(v) for v in vols if v is not None]
        if len(vols) < max(6, baseline_candles//2):
            return None
        avg5m = sum(vols)/len(vols) if vols else 0.0
        last_vol = float(last.get("turnover") or (last["volume"]*px_last))
        if avg5m <= 0 or last_vol < self.cfg.VolumeSpikeX5m * avg5m:
            return None

        # 3) OI рост за 5м
        try:
            ois = bybit_oi_5m(self.session, self.cfg, symbol, limit=10)
            if len(ois) < 2: 
                return None
            oi_last = float(ois[-1]["oi"])
            oi_prev = float(ois[-2]["oi"])
            oi_chg_pct = pct_change(oi_last, oi_prev) or 0.0
            if oi_chg_pct < self.cfg.OIIncreasePct5m:
                return None
        except Exception as e:
            self.log.warning(f"oi5m fail {symbol}: {e}")
            return None

        # 4) Ликвидность 24h
        snap = self.db.last(symbol)
        vol24h_usd = snap[2] if snap else None
        if (vol24h_usd is None) or (vol24h_usd < self.cfg.Min24hVolumeUSD):
            return None
        approx_1m = approx_notional_1m_from_24h(vol24h_usd)
        if (approx_1m is None) or (approx_1m < self.cfg.MinNotionalUSD):
            return None

        # 5) Cooldown
        if not self._cooldown_ok(symbol):
            return None

        # OK — формируем сигнал
        sig = {
            "symbol": symbol,
            "price": px_last,
            "price_change_5m_pct": dprice_pct,
            "vol5m_usd": last_vol,
            "vol5m_avg_usd": avg5m,
            "vol5m_x": (last_vol/avg5m if avg5m>0 else None),
            "oi_change_5m_pct": oi_chg_pct,
            "vol24h_usd": vol24h_usd,
            "notional1m_est_usd": approx_1m,
            "ts": now(),
            "strength": round(min(5.0,
                                  (abs(dprice_pct)/max(0.1,self.cfg.PriceSpikePct5m)) * 0.7 +
                                  (last_vol/max(1.0,avg5m))/self.cfg.VolumeSpikeX5m * 0.6 +
                                  (oi_chg_pct/max(0.1,self.cfg.OIIncreasePct5m)) * 0.5), 2),
            "mode": "signals_5m"
        }
        self._mark_signalled(symbol)
        return sig

# =======================
# Telegram alerts
# =======================

def format_signal_text(sig: Dict[str, Any]) -> str:
    mode = sig.get("mode","signal").upper()
    if mode == "SIGNALS_5M":
        return (
            f"🚀 [{mode}] {sig['symbol']}\n"
            f"Price: {sig['price']:.8g}\n"
            f"Δ5m: {sig['price_change_5m_pct']:+.2f}% | OI5m:+{sig['oi_change_5m_pct']:.2f}%\n"
            f"Vol5m: ${sig['vol5m_usd']:.0f} (avg ${sig['vol5m_avg_usd']:.0f}, x{sig['vol5m_x']:.2f})\n"
            f"24h Notional≈${sig['vol24h_usd']:.0f} | 1m≈${sig['notional1m_est_usd']:.0f}\n"
            f"Strength: {sig['strength']:.2f}\n"
            f"Time: {sig['ts']}"
        )
    # fallback для breakout_v0
    return (
        f"🚀 [{mode}] {sig['symbol']}\n"
        f"Price: {sig.get('price',0):.8g}\n"
        f"Δ60s: {sig.get('price_change_60s_pct',0):+.2f}% | ATR2h≈{sig.get('atr2h_pct',0):.2f}%\n"
        f"24h Notional≈${sig.get('vol24h_usd',0):.0f} | 1m≈${sig.get('notional1m_est_usd',0):.0f}\n"
        f"Strength: {sig.get('strength',0):.2f}\n"
        f"Time: {sig.get('ts','')}"
    )

def send_signal_alert(sig: Dict[str, Any], logger: logging.Logger):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ALERT_CHAT_ID")
    if not token or not chat_id:
        logger.warning("Telegram alert skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_ALERT_CHAT_ID missing")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        text = format_signal_text(sig)
        requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        logger.error(f"Telegram alert error: {e}")

# =======================
# State + HTTP handler
# =======================

STATE={"ok":0,"fail":0,"last_cycle_start":"","last_cycle_end":""}
_GLOBALS={"cfg":None,"db":None,"signals_lock":threading.Lock(),"signals_buffer":[]}
_SHUTDOWN=False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p=urlparse(self.path); path=p.path; qs=parse_qs(p.query or "")
        if path=="/health":
            self._json(200, {"status":"ok","stats":STATE,"build":BUILD_TAG})
        elif path=="/activity":
            self._json(200, self._activity(qs))
        elif path=="/volatility":
            self._json(200, self._vol(qs))
        elif path=="/trend":
            self._json(200, self._trend(qs))
        elif path=="/signals":
            self._json(200, self._signals(qs))
        elif path=="/ip":
            self._json(200, self._ip())
        else:
            self._raw(404, "not found")
    def _activity(self, qs):
        cfg=_GLOBALS["cfg"]; db=_GLOBALS["db"]
        syms=get_universe(cfg)  # без сессии здесь, чтобы не блокировать
        limit=int(qs.get("limit",[min(20,len(syms))])[0])
        rows=[]
        for s in syms:
            snap=db.last(s)
            if snap:
                ts,price,vq,vb=snap
                act=(vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
                rows.append({"symbol":s,"activity":float(act or 0.0),"price":price,"ts":ts,
                             "note":"24h USD (exchange source)"})
            else:
                rows.append({"symbol":s,"activity":0.0,"price":None,"ts":None})
        rows.sort(key=lambda r: r["activity"], reverse=True)
        return {"kind":"activity","data":rows[:limit]}
    def _vol(self, qs):
        cfg=_GLOBALS["cfg"]; db=_GLOBALS["db"]
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(20,len(syms))])[0])
        win=int(qs.get("window_min",[cfg.VolWindowMin])[0]); out=[]
        for s in syms:
            hist=db.history(s,win); vol=realized_vol(hist,win)
            out.append({"symbol":s,"volatility_pct_day":vol,"last_price":(hist[-1][1] if hist else None)})
        out.sort(key=lambda r: (r["volatility_pct_day"] if r["volatility_pct_day"] is not None else -1), reverse=True)
        return {"kind":"volatility","window_min":win,"data":out[:limit]}
    def _trend(self, qs):
        cfg=_GLOBALS["cfg"]; db=_GLOBALS["db"]
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(20,len(syms))])[0])
        win=int(qs.get("window_min",[cfg.TrendWindowMin])[0]); out=[]
        for s in syms:
            hist=db.history(s,win); tr=linear_trend_pct_day(hist,win)
            out.append({"symbol":s,"trend_pct_day":tr,"last_price":(hist[-1][1] if hist else None)})
        out.sort(key=lambda r: (r["trend_pct_day"] if r["trend_pct_day"] is not None else -1), reverse=True)
        return {"kind":"trend","window_min":win,"data":out[:limit]}
    def _signals(self, qs):
        limit = int(qs.get("limit", [50])[0])
        with _GLOBALS["signals_lock"]:
            data = list(_GLOBALS["signals_buffer"][-limit:])
        return {"kind":"signals", "count": len(data), "data": data}
    def _ip(self):
        try:
            ip = requests.get("https://api.ipify.org", timeout=5).text.strip()
        except Exception as e:
            ip = f"error: {e}"
        return {"public_ip": ip, "build": BUILD_TAG}
    def log_message(self, *a, **k): return
    def _json(self, code:int, obj:Any):
        body=json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def _raw(self, code:int, text:str):
        body=text.encode("utf-8")
        self.send_response(code); self.send_header("Content-Type","text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

# =======================
# HTTP server
# =======================

def run_http(port:int, stop_evt:threading.Event, logger:logging.Logger):
    httpd=HTTPServer(("0.0.0.0",port), Handler)
    httpd.timeout=1.0
    logger.info(f"HTTP on :{port} (/health /activity /volatility /trend /signals /ip)")
    while not stop_evt.is_set():
        httpd.handle_request()
    logger.info("HTTP stopped")

# =======================
# Core loop
# =======================

def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def ensure_csv_header(path:str, print_only:bool):
    ensure_csv(path, print_only)

def run_once(cfg:Config, logger:logging.Logger, sess:requests.Session, db:DB):
    ts=now(); STATE["last_cycle_start"]=ts
    ensure_csv_header(cfg.CsvFile, cfg.PrintOnly)

    syms=get_universe(cfg, sess, logger)
    results:Dict[str,Tuple[str,float,Optional[float],Optional[float]]]={}
    errs:Dict[str,str]={}

    def worker(sym:str):
        snap=get_snapshot(sess,cfg,sym,logger)
        if not snap: return (sym,None,"all sources failed")
        src,price,vq,vb=snap; return (sym,(src,price,vq,vb),None)

    with ThreadPoolExecutor(max_workers=max(1,cfg.Concurrency)) as ex:
        futs={ex.submit(worker,s):s for s in syms}
        for f in as_completed(futs):
            s=futs[f]
            try:
                sym,res,err=f.result()
                if err: errs[sym]=err
                else: results[sym]=res
            except Exception as e:
                errs[s]=str(e)

    ok=0; fail=0
    for s in syms:
        if s in results:
            src,price,vq,vb=results[s]
            logger.info(f"{s}: {price} [{src}] volQ24h={vq} volB24h={vb}")
            if not cfg.PrintOnly:
                append_csv(cfg.CsvFile, [ts,s,src,f"{price:.10g}", vq if vq is not None else "", vb if vb is not None else ""], cfg.PrintOnly)
            db.insert(ts,s,src,price,vq,vb)
            ok+=1
        else:
            logger.warning(f"{s}: нет данных ({errs.get(s,'unknown error')})")
            fail+=1

    STATE["ok"]+=ok; STATE["fail"]+=fail; STATE["last_cycle_end"]=now()

def install_signals(logger):
    def _h(signum, frame):
        global _SHUTDOWN
        _SHUTDOWN=True
        logger.info(f"Signal {signum} -> stop")
    signal.signal(signal.SIGINT,_h)
    signal.signal(signal.SIGTERM,_h)

# =======================
# Telegram bot (optional polling)
# =======================

def build_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Активность")],
            [KeyboardButton(text="Волатильность")],
            [KeyboardButton(text="Тренд")],
            [KeyboardButton(text="Сигналы")],
        ],
        resize_keyboard=True
    )
    return kb

def tg_build_text_activity(cfg: Config, db: DB) -> str:
    syms = get_universe(cfg)
    rows=[]
    for s in syms:
        snap = db.last(s)
        if not snap:
            rows.append((s, 0.0, None))
            continue
        ts, price, vq, vb = snap
        act = (vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
        rows.append((s, float(act or 0.0), price))
    rows.sort(key=lambda r: r[1], reverse=True)
    top = rows[:10]
    lines=["🔥 Топ по активности (24h USD):"]
    for s, act, pr in top:
        lines.append(f"{s}: ${act:,.0f} | px≈{pr}")
    return "\n".join(lines)

def tg_build_text_volatility(cfg: Config, db: DB) -> str:
    syms = get_universe(cfg)
    out=[]
    win = cfg.VolWindowMin
    for s in syms:
        hist=db.history(s,win); vol=realized_vol(hist,win)
        out.append((s, vol, hist[-1][1] if hist else None))
    out.sort(key=lambda r: (r[1] if r[1] is not None else -1), reverse=True)
    top = out[:10]
    lines=[f"📉 Волатильность (дневная, окно {win}m):"]
    for s, vol, px in top:
        vtxt = f"{vol:.2f}%" if vol is not None else "NA"
        lines.append(f"{s}: {vtxt} | px≈{px}")
    return "\n".join(lines)

def tg_build_text_trend(cfg: Config, db: DB) -> str:
    syms = get_universe(cfg)
    out=[]
    win = cfg.TrendWindowMin
    for s in syms:
        hist=db.history(s,win); tr=linear_trend_pct_day(hist,win)
        out.append((s, tr, hist[-1][1] if hist else None))
    out.sort(key=lambda r: (r[1] if r[1] is not None else -1), reverse=True)
    top = out[:10]
    lines=[f"📈 Тренд (дневной, окно {win}m):"]
    for s, tr, px in top:
        ttxt = f"{tr:+.2f}%" if tr is not None else "NA"
        lines.append(f"{s}: {ttxt} | px≈{px}")
    return "\n".join(lines)

def tg_build_text_signals() -> str:
    with _GLOBALS["signals_lock"]:
        data = list(_GLOBALS["signals_buffer"][-10:])
    if not data:
        return "Пока сигналов нет."
    lines=["🚀 Последние сигналы:"]
    for sig in data[::-1]:
        if sig.get("mode") == "signals_5m":
            lines.append(f"{sig['ts']} | {sig['symbol']} | Δ5m={sig['price_change_5m_pct']:+.2f}% | OI5m=+{sig['oi_change_5m_pct']:.2f}% | xVol={sig.get('vol5m_x',0):.2f}")
        else:
            lines.append(f"{sig['ts']} | {sig['symbol']} | Δ60s={sig.get('price_change_60s_pct',0):+.2f}%")
    return "\n".join(lines)

async def telegram_polling_main(cfg: Config, logger: logging.Logger):
    if not AI_TELEGRAM:
        logger.warning("aiogram не установлен — Telegram меню отключено")
        return
    if not cfg.TelegramBotToken:
        logger.warning("TELEGRAM_BOT_TOKEN отсутствует — Telegram меню отключено")
        return

    bot = Bot(cfg.TelegramBotToken)
    dp = Dispatcher()

    try:
        info = await bot.get_webhook_info()
        if getattr(info, "url", ""):
            logger.warning(f"Telegram webhook is SET: {info.url} — polling не получит апдейты")
        else:
            logger.info("Telegram webhook: <empty> (ok for polling)")
    except Exception as e:
        logger.warning(f"GetWebhookInfo failed: {e}")

    @dp.message(commands={"start"})
    async def start_cmd(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        await msg.answer("Привет! Выбери режим:", reply_markup=build_keyboard())

    @dp.message(F.text == "Активность")
    async def on_activity(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        txt = tg_build_text_activity(cfg, _GLOBALS["db"])
        await msg.answer(txt)

    @dp.message(F.text == "Волатильность")
    async def on_vol(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        txt = tg_build_text_volatility(cfg, _GLOBALS["db"])
        await msg.answer(txt)

    @dp.message(F.text == "Тренд")
    async def on_trend(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        txt = tg_build_text_trend(cfg, _GLOBALS["db"])
        await msg.answer(txt)

    @dp.message(F.text == "Сигналы")
    async def on_signals(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        txt = tg_build_text_signals()
        await msg.answer(txt)

    logger.info("Telegram: polling start")
    await dp.start_polling(bot)

# =======================
# Workers bootstrap
# =======================

def start_workers(cfg: Config, logger: logging.Logger) -> Tuple[threading.Event, threading.Thread, threading.Thread, requests.Session, DB, Any]:
    sess=build_session(cfg)
    db=DB(cfg.DbFile, logger)

    _GLOBALS["cfg"] = cfg
    _GLOBALS["db"] = db

    http_stop = threading.Event()
    http_thr = threading.Thread(target=run_http, args=(cfg.HttpPort, http_stop, logger), daemon=True)
    http_thr.start()

    # engines
    breakout_engine = BreakoutEngineV0(cfg, db, logger, send_alert_fn=lambda s: send_signal_alert(s, logger))
    signals5m_engine = Signals5mEngine(cfg, db, logger, sess, send_alert_fn=lambda s: send_signal_alert(s, logger))

    def data_loop():
        try:
            while not http_stop.is_set() and not _SHUTDOWN:
                # 1) Снапшоты цен/24h (минутный цикл)
                run_once(cfg, logger, sess, db)

                # 2) Сигнальный проход:
                mode = cfg.Mode.lower()
                if mode in ("signals_5m", "breakout"):
                    syms = get_universe(cfg, sess, logger)
                    if mode == "signals_5m":
                        eng = signals5m_engine
                    else:
                        eng = breakout_engine
                    for s in syms:
                        try:
                            sig = eng.check_symbol(s)
                            if sig:
                                logger.info(f"[signal] {sig}")
                                with _GLOBALS["signals_lock"]:
                                    _GLOBALS["signals_buffer"].append(sig)
                                    if len(_GLOBALS["signals_buffer"]) > 300:
                                        _GLOBALS["signals_buffer"] = _GLOBALS["signals_buffer"][-300:]
                                eng.send_alert(sig)
                        except Exception as e:
                            logger.warning(f"signal error {s}: {e}")

                # sleep до следующего круга
                sleep_total=max(1, int(cfg.UniverseRefreshMin*60))
                for _ in range(sleep_total):
                    if http_stop.is_set() or _SHUTDOWN:
                        break
                    time.sleep(1)
        finally:
            logger.info("Data loop stopped")

    data_thr = threading.Thread(target=data_loop, daemon=True)
    data_thr.start()

    return http_stop, http_thr, data_thr, sess, db, signals5m_engine

# =======================
# Main
# =======================

def main():
    cfg=parse_args()
    logger=setup_logger(cfg)
    install_signals(logger)

    logger.info(f"CFG: TelegramPolling={cfg.TelegramPolling} | Token={'set' if cfg.TelegramBotToken else 'missing'} | AllowedChatId={cfg.TelegramAllowedChatId} | Mode={cfg.Mode}")
    logger.info(f"BUILD {BUILD_TAG} | hosts: bytick={cfg.ByBitRestBase}, bybit={cfg.ByBitRestFallback}, binance={cfg.PriceFallbackBinance}")

    http_stop, http_thr, data_thr, sess, db, engine = start_workers(cfg, logger)

    if cfg.TelegramPolling and cfg.TelegramBotToken:
        if not AI_TELEGRAM:
            logger.warning("TELEGRAM_POLLING=true, но aiogram не установлен — меню отключено")
        else:
            try:
                import asyncio
                asyncio.run(telegram_polling_main(cfg, logger))
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")

    try:
        while not _SHUTDOWN:
            time.sleep(1)
    finally:
        http_stop.set()
        for _ in range(100):
            if not http_thr.is_alive() and not data_thr.is_alive():
                break
            time.sleep(0.05)
        logger.info("Stopped")

if __name__=="__main__":
    main()
