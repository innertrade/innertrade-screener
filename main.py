#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, csv, json, math, signal, sqlite3, threading, argparse, logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse as _urlparse, urlparse, parse_qs

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# --- aiogram (опционально, только для меню) ---
AI_TELEGRAM = True
try:
    from aiogram import Bot, Dispatcher, F
    if TYPE_CHECKING:
        from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
    AI_TELEGRAM = True
    from aiogram.types import Message
except Exception:
    AI_TELEGRAM = False

BUILD_TAG = "screener-main-5m-signals-oi-2025-09-30"

# =======================
# Config
# =======================

@dataclass
class Config:
    # Bybit/Binance хосты
    ByBitRestBase: str = "api.bytick.com"
    ByBitRestFallback: str = "api.bybit.com"
    PriceFallbackBinance: str = "api.binance.com"

    # Universe
    UniverseMax: int = 50
    UniverseMode: str = "TOP"         # TOP | ALL
    UniverseRefreshMin: int = 1
    UniverseList: Optional[List[str]] = None

    # Network
    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"
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

    # Analytics windows
    VolWindowMin: int = 120
    TrendWindowMin: int = 120

    # ---- Breakout v0 (на 60s; оставляем, но режимом не пользуемся) ----
    Mode: str = os.getenv("MODE", "breakout")

    BaselineHours: int = int(os.getenv("BASELINE_HOURS", "2"))
    LowVolThresholdPct: float = float(os.getenv("LOW_VOL_THRESHOLD_PCT", "1.2"))
    SpikePricePctMin: float = float(os.getenv("SPIKE_PRICE_PCT_MIN", "0.7"))
    Min24hVolumeUSD: float = float(os.getenv("MIN_24H_VOLUME_USD", "50000000"))
    MinNotionalUSD: float = float(os.getenv("MIN_NOTIONAL_USD", "100000"))
    CooldownMinutes: int = int(os.getenv("COOLDOWN_MINUTES", "15"))

    # ---- SIGNALS_5M с OI ----
    Baseline5mHours: int = int(os.getenv("BASELINE_5M_HOURS", "1"))
    PriceSpikePct5m: float = float(os.getenv("PRICE_SPIKE_PCT_5M", "0.7"))
    VolumeSpikeX5m: float = float(os.getenv("VOLUME_SPIKE_X_5M", "3.0"))
    OIIncreasePct5m: float = float(os.getenv("OI_INCREASE_PCT_5M", "0.5"))

    # Источники для 5м
    KlineSource: str = os.getenv("KLINE_SOURCE", "bybit")
    OISource: str = os.getenv("OI_SOURCE", "bybit")

    # Telegram
    TelegramPolling: bool = (os.getenv("TELEGRAM_POLLING", "true").lower() == "true")
    TelegramBotToken: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    TelegramAlertChatId: Optional[str] = os.getenv("TELEGRAM_ALERT_CHAT_ID")
    TelegramAllowedChatId: Optional[str] = os.getenv("TELEGRAM_ALLOWED_CHAT_ID")

def env(name, default, cast=None):
    v = os.getenv(name)
    if v is None:
        return default
    if cast:
        try:
            return cast(v)
        except Exception:
            return default
    return v

def auto_port(default: int = 8080) -> int:
    raw = os.getenv("PORT") or os.getenv("HTTP_PORT") or str(default)
    try:
        m = re.search(r"\d+", str(raw))
        return int(m.group()) if m else default
    except Exception:
        return default

def clean_host(v: str) -> str:
    if not v:
        return v
    v = v.strip()
    if "://" in v:
        parsed = _urlparse(v)
        host = (parsed.netloc or parsed.path or "").strip("/")
    else:
        host = v.strip("/")
    return host.split("/")[0].strip()

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Screener bot: /health /activity /volatility /trend /signals /ip")

    # Universe
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL"])
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
    "APTUSDT","ARBUSDT","OPUSDT","NEARUSDT","SUIUSDT",
    "LTCUSDT","MATICUSDT","ETCUSDT","ATOMUSDT","AAVEUSDT",
]
DEFAULT_ALL = DEFAULT_TOP + [
    "EOSUSDT","XLMUSDT","FILUSDT","INJUSDT","WLDUSDT",
    "PEPEUSDT","SHIBUSDT","FTMUSDT","KASUSDT","RUNEUSDT",
    "SEIUSDT","PYTHUSDT","TIAUSDT","ORDIUSDT","JUPUSDT",
]

def get_universe(cfg: Config) -> List[str]:
    if cfg.UniverseList:
        return [s.strip().upper() for s in cfg.UniverseList][:cfg.UniverseMax]
    base = DEFAULT_TOP if cfg.UniverseMode.upper()=="TOP" else DEFAULT_ALL
    return base[:cfg.UniverseMax]

# =======================
# Cache
# =======================

class TTLCache:
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self.data: Dict[str, Tuple[float,str,float,Optional[float],Optional[float]]] = {}
        self.lock = threading.Lock()
    def get(self, sym: str):
        with self.lock:
            row = self.data.get(sym)
            if not row:
                return None
            price, source, ts, vq, vb = row
            if time.time() - ts <= self.ttl:
                return row
            self.data.pop(sym, None); return None
    def put(self, sym: str, price: float, source: str, vq: Optional[float], vb: Optional[float]):
        with self.lock:
            self.data[sym] = (price, source, time.time(), vq, vb)

# =======================
# DB
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

# =======================
# CSV helpers
# =======================

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
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
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
# CoinGecko symbol -> id (для fallback + активности/меню)
# =======================

COINGECKO_ID = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
    "BNBUSDT": "binancecoin",
    "DOGEUSDT": "dogecoin",
    "ADAUSDT": "cardano",
    "TONUSDT": "toncoin",
    "TRXUSDT": "tron",
    "LINKUSDT": "chainlink",
    "APTUSDT": "aptos",
    "ARBUSDT": "arbitrum",
    "OPUSDT":  "optimism",
    "NEARUSDT":"near",
    "SUIUSDT": "sui",
    "LTCUSDT": "litecoin",
    "MATICUSDT":"matic-network",
    "ETCUSDT": "ethereum-classic",
    "ATOMUSDT":"cosmos",
    "AAVEUSDT":"aave",
    "EOSUSDT": "eos",
    "XLMUSDT": "stellar",
    "FILUSDT": "filecoin",
    "INJUSDT": "injective-protocol",
    "WLDUSDT": "worldcoin-wld",
    "PEPEUSDT":"pepe",
    "SHIBUSDT":"shiba-inu",
    "FTMUSDT": "fantom",
    "KASUSDT": "kaspa",
    "RUNEUSDT":"thorchain",
    "SEIUSDT": "sei-network",
    "PYTHUSDT":"pyth-network",
    "TIAUSDT": "celestia",
    "ORDIUSDT":"ordinals",
    "JUPUSDT": "jupiter-exchange-solana",
}

# =======================
# Fetchers (tickers + kline + OI)
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
    coin_id = COINGECKO_ID.get(symbol.upper())
    if not coin_id:
        raise RuntimeError(f"coingecko id not mapped for {symbol}")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": coin_id, "precision": "full"}
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    arr = r.json()
    if not isinstance(arr, list) or not arr:
        raise RuntimeError(f"coingecko empty for {symbol}: {arr}")
    it = arr[0]
    price = float(it["current_price"])
    vol_usd = float(it.get("total_volume") or 0.0)
    vol_base = (vol_usd / price) if price > 0 else None
    return price, vol_usd, vol_base

def get_snapshot(session, cfg, symbol, logger):
    try:
        price, vq, vb = fetch_bybit(session, cfg, symbol)
        return "bytick", price, vq, vb
    except Exception as e:
        logger.warning(f"[bytick] {symbol} fail: {e}")
    try:
        price, vq, vb = fetch_bybit_fb(session, cfg, symbol)
        return "bybit", price, vq, vb
    except Exception as e:
        logger.warning(f"[bybit-fallback] {symbol} fail: {e}")
    try:
        price, vq, vb = fetch_binance(session, cfg, symbol)
        return "binance", price, vq, vb
    except Exception as e:
        logger.warning(f"[binance] {symbol} fail: {e}")
    logger.info(f"[coingecko] trying {symbol}")
    try:
        price, vq_usd, vb_est = fetch_coingecko(session, cfg, symbol)
        return "coingecko", price, vq_usd, vb_est
    except Exception as e:
        logger.warning(f"[coingecko] {symbol} fail: {e}")
    return None

# --- 5m Kline (Bybit v5) ---
def fetch_bybit_kline_5m(session: requests.Session, cfg: Config, symbol: str, limit: int = 60):
    """
    Возвращает список свечей (от старых к новым): [(open_time_ms, open, high, low, close, volume)]
    interval=5 (минут), category=cfg.Category
    """
    sym = n_bybit(symbol)
    base_urls = [cfg.ByBitRestBase, cfg.ByBitRestFallback]
    for host in base_urls:
        try:
            url = f"https://{host}/v5/market/kline"
            params = {"category": cfg.Category, "symbol": sym, "interval": "5", "limit": str(limit)}
            r = session.get(url, params=params, timeout=cfg.RequestTimeout)
            r.raise_for_status()
            js = r.json()
            lst = (((js or {}).get("result") or {}).get("list") or [])
            if not lst:
                raise RuntimeError(f"kline empty: {js}")
            # Bybit v5 возвращает newest->oldest, перевернём
            rows = []
            for it in reversed(lst):
                # форма: [start, open, high, low, close, volume, turnover] (строки)
                ts = int(it[0]); o=float(it[1]); h=float(it[2]); l=float(it[3]); c=float(it[4]); v=float(it[5])
                rows.append((ts, o, h, l, c, v))
            return rows
        except Exception as e:
            continue
    raise RuntimeError("kline fetch failed")

# --- 5m Open Interest (Bybit v5) ---
def fetch_bybit_oi_5m(session: requests.Session, cfg: Config, symbol: str, limit: int = 2) -> List[Tuple[int,float]]:
    """
    Возвращает [(ts_ms, oi_value)], 5m.
    endpoint: /v5/market/open-interest?category=linear&symbol=BTCUSDT&interval=5min&limit=...
    """
    sym = n_bybit(symbol)
    base_urls = [cfg.ByBitRestBase, cfg.ByBitRestFallback]
    for host in base_urls:
        try:
            url = f"https://{host}/v5/market/open-interest"
            params = {"category": cfg.Category, "symbol": sym, "interval": "5min", "limit": str(limit)}
            r = session.get(url, params=params, timeout=cfg.RequestTimeout)
            r.raise_for_status()
            js = r.json()
            lst = (((js or {}).get("result") or {}).get("list") or [])
            if not lst:
                raise RuntimeError(f"oi empty: {js}")
            # newest->oldest -> перевернём
            rows=[]
            for it in reversed(lst):
                # it: {"openInterest": "...", "timestamp": "..."}
                ts=int(it.get("timestamp") or it.get("ts") or 0)
                oi=float(it.get("openInterest") or 0.0)
                rows.append((ts,oi))
            return rows
        except Exception:
            continue
    raise RuntimeError("oi fetch failed")

# =======================
# Analytics
# =======================

def realized_vol(prices: List[Tuple[str,float]], window_min: int) -> Optional[float]:
    if len(prices) < 3: return None
    vals = [p for _,p in prices if p>0]
    if len(vals) < 3: return None
    rets = []
    for i in range(1,len(vals)):
        try: rets.append(math.log(vals[i]/vals[i-1]))
        except: pass
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
# Breakout Engine v0 (оставлен как есть)
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

# =======================
# SIGNALS_5M Engine (5m + OI)
# =======================

class Signals5mEngine:
    def __init__(self, cfg: Config, logger: logging.Logger, session: requests.Session, db: DB, send_alert_fn):
        self.cfg = cfg
        self.log = logger
        self.sess = session
        self.db = db
        self.send_alert = send_alert_fn
        self.last_signal_ts: Dict[str, float] = {}

    def _cooldown_ok(self, symbol: str) -> bool:
        cd = max(1, self.cfg.CooldownMinutes) * 60
        return (time.time() - self.last_signal_ts.get(symbol, 0)) >= cd

    def _mark_signalled(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

    def _kline_baseline_vol(self, klines: List[Tuple[int,float,float,float,float,float]]) -> Optional[float]:
        # берём всё, кроме последней свечи, как базу
        vols = [v for (_,_,_,_,_,v) in klines[:-1] if v is not None]
        if len(vols) < 3: return None
        return sum(vols)/len(vols)

    def _oi_increase_pct_last5m(self, oi_rows: List[Tuple[int,float]]) -> Optional[float]:
        if len(oi_rows) < 2: return None
        a = oi_rows[-2][1]; b = oi_rows[-1][1]
        if a <= 0: return None
        return (b/a - 1.0)*100.0

    def check_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        # ликвидность 24h
        snap = self.db.last(symbol)
        vol24h_usd = snap[2] if snap else None
        if (vol24h_usd is None) or (vol24h_usd < self.cfg.Min24hVolumeUSD):
            return None
        approx_1m = approx_notional_1m_from_24h(vol24h_usd)
        if (approx_1m is None) or (approx_1m < self.cfg.MinNotionalUSD):
            return None

        # kline 5m
        limit = max(12, self.cfg.Baseline5mHours*12 + 2)  # база + 2 свечи
        try:
            kl = fetch_bybit_kline_5m(self.sess, self.cfg, symbol, limit=limit)
        except Exception as e:
            self.log.debug(f"kline5m fail {symbol}: {e}")
            return None
        if len(kl) < 3:
            return None

        # Δ5m цены и объём-спайк
        _,_,_,_, prev_close, _ = kl[-2]
        _,_,_,_, last_close,  last_vol  = kl[-1]
        if prev_close <= 0 or last_close <= 0:
            return None

        d5m = (last_close/prev_close - 1.0)*100.0
        base_vol = self._kline_baseline_vol(kl)
        if base_vol is None or base_vol <= 0:
            return None
        vol_ratio = (last_vol / base_vol) if base_vol>0 else 0.0

        if abs(d5m) < self.cfg.PriceSpikePct5m:
            return None
        if vol_ratio < self.cfg.VolumeSpikeX5m:
            return None

        # OI 5m
        try:
            oi_rows = fetch_bybit_oi_5m(self.sess, self.cfg, symbol, limit=2)
            oi_pct = self._oi_increase_pct_last5m(oi_rows)
        except Exception as e:
            self.log.debug(f"oi5m fail {symbol}: {e}")
            oi_pct = None

        if (oi_pct is None) or (oi_pct < self.cfg.OIIncreasePct5m):
            return None

        # cooldown
        if not self._cooldown_ok(symbol):
            return None

        sig = {
            "mode": "signals_5m",
            "symbol": symbol,
            "price": last_close,
            "delta_5m_pct": d5m,
            "volume_ratio_5m": vol_ratio,
            "oi_5m_pct": oi_pct,
            "vol24h_usd": vol24h_usd,
            "notional1m_est_usd": approx_1m,
            "atr2h_pct": atr_like_pct(self.db.history(symbol, max(10, self.cfg.VolWindowMin))),  # контекст
            "ts": now(),
            "strength": round(min(5.0, (abs(d5m)/max(0.1,self.cfg.PriceSpikePct5m))*1.0 + (vol_ratio/self.cfg.VolumeSpikeX5m)*0.5 + (oi_pct/self.cfg.OIIncreasePct5m)*0.5, 2), 2),
        }
        self._mark_signalled(symbol)
        return sig

# =======================
# Telegram alerts
# =======================

def format_signal_text(sig: Dict[str, Any]) -> str:
    mode = sig.get("mode","signal").upper()
    if mode == "SIGNALS_5M":
        atr_txt = f"{sig['atr2h_pct']:.2f}%" if isinstance(sig.get("atr2h_pct"), (float,int)) else "NA"
        return (
            f"🚀 [{mode}] {sig['symbol']}\n"
            f"Price: {sig['price']:.8g}\n"
            f"Δ5m: {sig['delta_5m_pct']:+.2f}% | ATR2h≈{atr_txt}\n"
            f"OI 5m: {sig['oi_5m_pct']:+.2f}% | Vol(5m)/Avg: ×{sig['volume_ratio_5m']:.2f}\n"
            f"24h Notional≈${sig['vol24h_usd']:.0f} | 1m≈${sig['notional1m_est_usd']:.0f}\n"
            f"Strength: {sig['strength']:.2f}\n"
            f"Time: {sig['ts']}"
        )
    # fallback / старые сигналы
    return (
        f"🚀 [{mode}] {sig['symbol']}\n"
        f"Price: {sig.get('price',0):.8g}\n"
        f"Δ60s: {sig.get('price_change_60s_pct',0):+.2f}% | ATR2h≈{sig.get('atr2h_pct','NA')}\n"
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
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(10,len(syms))])[0])
        rows=[]
        for s in syms:
            snap=db.last(s)
            if snap:
                ts,price,vq,vb=snap
                act=(vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
                rows.append({"symbol":s,"activity":float(act or 0.0),"price":price,"ts":ts,
                             "note":"24h USD (global) if source=coingecko"})
            else:
                rows.append({"symbol":s,"activity":0.0,"price":None,"ts":None})
        rows.sort(key=lambda r: r["activity"], reverse=True)
        return {"kind":"activity","data":rows[:limit]}
    def _vol(self, qs):
        cfg=_GLOBALS["cfg"]; db=_GLOBALS["db"]
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(10,len(syms))])[0])
        win=int(qs.get("window_min",[cfg.VolWindowMin])[0]); out=[]
        for s in syms:
            hist=db.history(s,win); vol=realized_vol(hist,win)
            out.append({"symbol":s,"volatility_pct_day":vol,"last_price":(hist[-1][1] if hist else None)})
        out.sort(key=lambda r: (r["volatility_pct_day"] if r["volatility_pct_day"] is not None else -1), reverse=True)
        return {"kind":"volatility","window_min":win,"data":out[:limit]}
    def _trend(self, qs):
        cfg=_GLOBALS["cfg"]; db=_GLOBALS["db"]
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(10,len(syms))])[0])
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
    syms=get_universe(cfg)

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
# Telegram menu (optional)
# =======================

def build_keyboard() -> 'ReplyKeyboardMarkup':
    if not AI_TELEGRAM:
        return None  # type: ignore
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
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
        mode = sig.get("mode","signal").upper()
        if mode == "SIGNALS_5M":
            lines.append(f"{sig['ts']} | {sig['symbol']} | Δ5m={sig['delta_5m_pct']:+.2f}% | OI5m={sig['oi_5m_pct']:+.2f}% | str={sig['strength']:.2f}")
        else:
            lines.append(f"{sig['ts']} | {sig['symbol']} | Δ60s={sig.get('price_change_60s_pct',0):+.2f}% | str={sig.get('strength',0):.2f}")
    return "\n".join(lines)

async def telegram_polling_main(cfg: Config, logger: logging.Logger):
    if not AI_TELEGRAM:
        logger.warning("aiogram не установлен — Telegram меню отключено (alerts работают через sendMessage)")
        return
    if not cfg.TelegramBotToken:
        logger.warning("TELEGRAM_BOT_TOKEN отсутствует — Telegram меню отключено")
        return

    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message

    bot = Bot(cfg.TelegramBotToken)
    dp = Dispatcher()

    try:
        info = await bot.get_webhook_info()
        if getattr(info, "url", ""):
            logger.warning(f"Telegram webhook is SET: {info.url} — при активном вебхуке polling не получит апдейты")
        else:
            logger.info("Telegram webhook: <empty> (ok for polling)")
    except Exception as e:
        logger.warning(f"GetWebhookInfo failed: {e}")

    @dp.message(commands={"start"})
    async def start_cmd(msg: Message):
        if cfg.TelegramAllowedChatId and str(msg.chat.id) != str(cfg.TelegramAllowedChatId):
            return
        kb = build_keyboard()
        await msg.answer("Привет! Выбери режим:", reply_markup=kb)

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

    # движки
    engine_breakout = BreakoutEngineV0(cfg, db, logger, send_alert_fn=lambda s: send_signal_alert(s, logger))
    engine_5m = Signals5mEngine(cfg, logger, sess, db, send_alert_fn=lambda s: send_signal_alert(s, logger))

    def data_loop():
        try:
            while not http_stop.is_set() and not _SHUTDOWN:
                run_once(cfg, logger, sess, db)

                # включаем сигнал-движок в нужных режимах
                mode = cfg.Mode.lower()
                syms = get_universe(cfg)
                if mode in ("signals_5m", "breakout"):
                    for s in syms:
                        try:
                            if mode == "signals_5m":
                                sig = engine_5m.check_symbol(s)
                            else:
                                sig = engine_breakout.check_symbol(s)
                            if sig:
                                logger.info(f"[signal] {sig}")
                                with _GLOBALS["signals_lock"]:
                                    _GLOBALS["signals_buffer"].append(sig)
                                    if len(_GLOBALS["signals_buffer"]) > 300:
                                        _GLOBALS["signals_buffer"] = _GLOBALS["signals_buffer"][-300:]
                                (engine_5m.send_alert if mode=="signals_5m" else engine_breakout.send_alert)(sig)
                        except Exception as e:
                            logger.warning(f"signal error {s}: {e}")

                sleep_total=max(1, int(cfg.UniverseRefreshMin*60))
                for _ in range(sleep_total):
                    if http_stop.is_set() or _SHUTDOWN:
                        break
                    time.sleep(1)
        finally:
            logger.info("Data loop stopped")

    data_thr = threading.Thread(target=data_loop, daemon=True)
    data_thr.start()

    return http_stop, http_thr, data_thr, sess, db, engine_5m

# =======================
# Main
# =======================

def main():
    cfg=parse_args()
    logger=setup_logger(cfg)

    logger.info(f"CFG: TelegramPolling={cfg.TelegramPolling} | Token={'set' if cfg.TelegramBotToken else 'missing'} | AllowedChatId={cfg.TelegramAllowedChatId} | Mode={cfg.Mode}")
    install_signals(logger)
    logger.info(f"BUILD {BUILD_TAG} | hosts: bytick={cfg.ByBitRestBase}, bybit={cfg.ByBitRestFallback}, binance={cfg.PriceFallbackBinance}")

    http_stop, http_thr, data_thr, sess, db, engine_5m = start_workers(cfg, logger)

    if cfg.TelegramPolling and cfg.TelegramBotToken:
        if not AI_TELEGRAM:
            logger.warning("TELEGRAM_POLLING=true, но aiogram не установлен — меню отключено")
        else:
            try:
                import asyncio
                asyncio.run(telegram_polling_main(cfg, logger))
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")
    else:
        logger.info(f"Skip TG: polling={cfg.TelegramPolling} token={'set' if cfg.TelegramBotToken else 'missing'} AI_TELEGRAM={AI_TELEGRAM}")

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
