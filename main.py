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

# --- aiogram (опционально, для меню/кнопок) ---
AI_TELEGRAM = True
try:
    from aiogram import Bot, Dispatcher, F
    if TYPE_CHECKING:
        from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
    AI_TELEGRAM = True
    from aiogram.types import Message
except Exception:
    AI_TELEGRAM = False

BUILD_TAG = "screener-5m-signals-2025-09-30"

# =======================
# Config
# =======================

@dataclass
class Config:
    # Bybit/Binance хосты
    ByBitRestBase: str = "api.bytick.com"
    ByBitRestFallback: str = "api.bybit.com"
    PriceFallbackBinance: str = "api.binance.com"

    # Universe (для меню/ручек /activity /volatility /trend)
    UniverseMax: int = 10
    UniverseMode: str = "TOP"         # TOP | ALL
    UniverseRefreshMin: int = 1
    UniverseList: Optional[List[str]] = None

    # Network
    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"          # linear | inverse | option
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

    # ----- РЕЖИМЫ -----
    Mode: str = os.getenv("MODE", "signals_5m")  # "signals_5m" | "breakout" | activity|volatility|trend

    # ----- БАЗА ДЛЯ 5М СИГНАЛОВ -----
    BaselineHours: int = int(os.getenv("BASELINE_HOURS", "1"))  # база для среднего объёма 5м
    PriceSpikePct5m: float = float(os.getenv("PRICE_SPIKE_PCT_5M", "0.7"))
    VolumeSpikeX5m: float = float(os.getenv("VOLUME_SPIKE_X_5M", "3.0"))
    OIIncreasePct5m: float = float(os.getenv("OI_INCREASE_PCT_5M", "0.5"))

    # ликвидность
    Min24hVolumeUSD: float = float(os.getenv("MIN_24H_VOLUME_USD", "50000000"))
    MinNotionalUSD: float = float(os.getenv("MIN_NOTIONAL_USD", "100000"))
    CooldownMinutes: int = int(os.getenv("COOLDOWN_MINUTES", "15"))

    # источники
    KlineSource: str = os.getenv("KLINE_SOURCE", "bybit")
    OISource: str = os.getenv("OI_SOURCE", "bybit")

    # Вселенная для сигналов: "ALL" (все из COINGECKO_ID) или "SAME" (как в меню)
    SignalsUniverse: str = os.getenv("SIGNALS_UNIVERSE", "ALL").upper()

    # Telegram
    TelegramPolling: bool = (os.getenv("TELEGRAM_POLLING", "true").lower() == "true")
    TelegramBotToken: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    TelegramAlertChatId: Optional[str] = os.getenv("TELEGRAM_ALERT_CHAT_ID")
    TelegramAllowedChatId: Optional[str] = os.getenv("TELEGRAM_ALLOWED_CHAT_ID")  # ограничение приёма команд


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
    p = argparse.ArgumentParser(description="Screener 5m signals bot")

    # Universe
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL"])
    p.add_argument("--max", type=int, default=env("UNIVERSE_MAX",10,int))
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

    # HTTP (PORT autoload)
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
DEFAULT_ALL = DEFAULT_TOP + [
    "APTUSDT","ARBUSDT","OPUSDT","NEARUSDT","SUIUSDT",
    "LTCUSDT","MATICUSDT","ETCUSDT","ATOMUSDT","AAVEUSDT",
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
# Cache (optional)
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
# CSV
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
# CoinGecko symbol -> id
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
# Fetchers (snapshots 24h + 5m kline + 5m OI)
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

def get_snapshot(session, cfg, symbol, logger):
    # Bytick
    try:
        price, vq, vb = fetch_bybit(session, cfg, symbol)
        return "bytick", price, vq, vb
    except Exception as e:
        logger.warning(f"[bytick] {symbol} fail: {e}")

    # Bybit fallback
    try:
        price, vq, vb = fetch_bybit_fb(session, cfg, symbol)
        return "bybit", price, vq, vb
    except Exception as e:
        logger.warning(f"[bybit-fallback] {symbol} fail: {e}")

    # Binance
    try:
        price, vq, vb = fetch_binance(session, cfg, symbol)
        return "binance", price, vq, vb
    except Exception as e:
        logger.warning(f"[binance] {symbol} fail: {e}")

    # CoinGecko — опускаем здесь (снапшоты нам важны, но выше обычно хватает)
    return None

# ---- 5m KLINE (Bybit public) ----

def _parse_bybit_kline_list(lst: List[List[str]]) -> List[Dict[str, float]]:
    out=[]
    for it in lst:
        # ожидаем формат: [start, open, high, low, close, volume, turnover]
        try:
            start = int(it[0])
            o = float(it[1]); h=float(it[2]); l=float(it[3]); c=float(it[4])
            vol_base = float(it[5]) if it[5] is not None else None
            turnover = float(it[6]) if it[6] is not None else None
        except Exception:
            continue
        out.append({
            "t": start,
            "o": o, "h": h, "l": l, "c": c,
            "vol_base": vol_base,
            "turnover": turnover
        })
    out.sort(key=lambda x: x["t"])
    return out

def fetch_bybit_kline_5m(session, cfg, symbol, limit=200) -> List[Dict[str, float]]:
    sym = n_bybit(symbol)
    params = {"category": cfg.Category, "symbol": sym, "interval": "5", "limit": str(limit)}
    url = f"https://{cfg.ByBitRestBase}/v5/market/kline"
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    try:
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        if lst:
            return _parse_bybit_kline_list(lst)
    except Exception:
        pass
    # fallback к bybit.com
    url = f"https://{cfg.ByBitRestFallback}/v5/market/kline"
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    lst = (((data or {}).get("result") or {}).get("list") or [])
    return _parse_bybit_kline_list(lst)

# ---- 5m OI (Bybit public) ----

def fetch_bybit_oi_5m(session, cfg, symbol, limit=5) -> List[Tuple[int,float]]:
    """
    Возвращает список (ts_ms, openInterest) с шагом 5m, отсортированный по времени.
    """
    sym = n_bybit(symbol)
    params = {"category": cfg.Category, "symbol": sym, "interval": "5min", "limit": str(limit)}
    url = f"https://{cfg.ByBitRestBase}/v5/market/open-interest"
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    try:
        r.raise_for_status()
        data = r.json()
        lst = (((data or {}).get("result") or {}).get("list") or [])
        if lst:
            out=[]
            for it in lst:
                try:
                    ts = int(it.get("timestamp") or it.get("ts") or 0)
                    oi = float(it.get("openInterest") or it.get("open_interest") or 0.0)
                    out.append((ts, oi))
                except Exception:
                    pass
            out.sort(key=lambda x: x[0])
            return out
    except Exception:
        pass
    # fallback
    url = f"https://{cfg.ByBitRestFallback}/v5/market/open-interest"
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    lst = (((data or {}).get("result") or {}).get("list") or [])
    out=[]
    for it in lst:
        try:
            ts = int(it.get("timestamp") or it.get("ts") or 0)
            oi = float(it.get("openInterest") or it.get("open_interest") or 0.0)
            out.append((ts, oi))
        except Exception:
            pass
    out.sort(key=lambda x: x[0])
    return out

# =======================
# Analytics
# =======================

def realized_vol(prices: List[Tuple[str,float]], window_min: int) -> Optional[float]:
    if len(prices) < 3: return None
    vals = [p for _,p in prices if p>0]
    if len(vals) < 3: return None
    rets = []
    for i in range(1,len(vals)):
        try:
            rets.append(math.log(vals[i]/vals[i-1]))
        except Exception:
            pass
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
    if len(prices) < 3:
        return None
    vals = [p for _, p in prices if p > 0]
    if len(vals) < 3:
        return None
    acc = 0.0
    n = 0
    for i in range(1, len(vals)):
        c0, c1 = vals[i-1], vals[i]
        if c0 > 0:
            acc += abs(c1 - c0) / c0
            n += 1
    if n == 0:
        return None
    return (acc / n) * 100.0

def pct_change(a: float, b: float) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0

def approx_notional_1m_from_24h(vol24h_usd: Optional[float]) -> Optional[float]:
    if vol24h_usd is None:
        return None
    return vol24h_usd / 1440.0

# =======================
# Engines
# =======================

def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

class BreakoutEngineV0:
    """Оставляем как есть (на случай Mode=breakout); НЕ используется для 5м."""
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
        win_min = 2 * 60
        hist = self.db.history(symbol, win_min)
        if len(hist) < max(10, win_min // 2):
            return None

        atr = atr_like_pct(hist)
        if atr is None or atr > 1.2:
            return None

        last_price = hist[-1][1]
        prev_price = hist[-2][1] if len(hist) >= 2 else None
        dprice_pct = pct_change(last_price, prev_price)
        if dprice_pct is None or abs(dprice_pct) < 0.7:
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
            "price_change_5m_pct": dprice_pct,  # для унификации ключа
            "atr2h_pct": atr,
            "vol24h_usd": vol24h_usd,
            "notional1m_est_usd": approx_1m,
            "ts": now(),
            "strength": round(min(5.0, abs(dprice_pct) / max(0.1, 1.2) * 1.2), 2),
            "mode": "breakout_v0"
        }
        self._mark_signalled(symbol)
        return sig

class Signals5mEngine:
    """Основной движок 5м: цена + всплеск 5м объёма + рост OI (5м) + фильтры ликвидности."""
    def __init__(self, cfg: Config, db: DB, session: requests.Session, logger: logging.Logger, send_alert_fn):
        self.cfg = cfg
        self.db = db
        self.sess = session
        self.log = logger
        self.send_alert = send_alert_fn
        self.last_signal_ts: Dict[str, float] = {}

    def _cooldown_ok(self, symbol: str) -> bool:
        cd = max(1, self.cfg.CooldownMinutes) * 60
        t0 = self.last_signal_ts.get(symbol, 0)
        return (time.time() - t0) >= cd

    def _mark_signalled(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

    def _signal_universe(self) -> List[str]:
        if self.cfg.SignalsUniverse == "ALL":
            return list(COINGECKO_ID.keys())
        return get_universe(self.cfg)

    def _fetch_5m_kline(self, symbol: str) -> Optional[List[Dict[str, float]]]:
        try:
            return fetch_bybit_kline_5m(self.sess, self.cfg, symbol, limit=max(50, self.cfg.BaselineHours*12 + 5))
        except Exception as e:
            self.log.debug(f"[sigdebug] {symbol}: kline_5m fail: {e}")
            return None

    def _fetch_5m_oi(self, symbol: str) -> Optional[List[Tuple[int,float]]]:
        try:
            return fetch_bybit_oi_5m(self.sess, self.cfg, symbol, limit=5)
        except Exception as e:
            self.log.debug(f"[sigdebug] {symbol}: oi_5m fail: {e}")
            return None

    def check_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        # 1) KLINE 5m
        kl = self._fetch_5m_kline(symbol)
        if not kl or len(kl) < 5:
            self.log.debug(f"[sigdebug] {symbol}: skip no_kline")
            return None

        last = kl[-1]
        o = last["o"]; c = last["c"]
        if o <= 0:
            self.log.debug(f"[sigdebug] {symbol}: skip bad_open")
            return None
        dprice_pct = (c/o - 1.0) * 100.0
        if abs(dprice_pct) < self.cfg.PriceSpikePct5m:
            self.log.debug(f"[sigdebug] {symbol}: skip price_spike {dprice_pct:.3f}% < {self.cfg.PriceSpikePct5m}%")
            return None

        # 2) Volume spike (notional turnover предпочтительно)
        base_len = max(6, self.cfg.BaselineHours * 12)  # 12 пятиминуток в час
        base_slice = kl[-(base_len+1):-1] if len(kl) > base_len else kl[:-1]
        def _k_turnover(k):
            if k.get("turnover") and k["turnover"] > 0:
                return k["turnover"]
            if k.get("vol_base") and k["vol_base"]>0:
                mid = (k["h"]+k["l"]+k["c"]+k["o"])/4.0
                return k["vol_base"]*mid
            return 0.0
        last_notional = _k_turnover(last)
        avg_notional = (sum(_k_turnover(k) for k in base_slice)/max(1,len(base_slice))) if base_slice else 0.0
        if avg_notional <= 0:
            self.log.debug(f"[sigdebug] {symbol}: skip no_baseline_vol")
            return None
        spike_x = last_notional / max(1e-9, avg_notional)
        if spike_x < self.cfg.VolumeSpikeX5m:
            self.log.debug(f"[sigdebug] {symbol}: skip vol_spike {spike_x:.2f}x < {self.cfg.VolumeSpikeX5m}x")
            return None

        # 3) Open Interest 5m — нужен рост
        oi = self._fetch_5m_oi(symbol)
        if not oi or len(oi) < 2 or oi[-1][1] <= 0 or oi[-2][1] <= 0:
            self.log.debug(f"[sigdebug] {symbol}: skip no_oi")
            return None
        oi_pct = (oi[-1][1]/oi[-2][1] - 1.0) * 100.0
        if oi_pct < self.cfg.OIIncreasePct5m:
            self.log.debug(f"[sigdebug] {symbol}: skip oi_increase {oi_pct:.3f}% < {self.cfg.OIIncreasePct5m}%")
            return None

        # 4) Ликвидность по снапшоту 24h
        snap = _GLOBALS["db"].last(symbol)
        vol24h_usd = snap[2] if snap else None
        if (vol24h_usd is None) or (vol24h_usd < self.cfg.Min24hVolumeUSD):
            self.log.debug(f"[sigdebug] {symbol}: skip low_24h ${vol24h_usd}")
            return None
        approx_1m = approx_notional_1m_from_24h(vol24h_usd)
        if (approx_1m is None) or (approx_1m < self.cfg.MinNotionalUSD):
            self.log.debug(f"[sigdebug] {symbol}: skip low_1m ${approx_1m}")
            return None

        # 5) ATR контекст (2 часа из БД)
        atr = atr_like_pct(self.db.history(symbol, 120)) or 0.0

        # 6) Cooldown
        if not self._cooldown_ok(symbol):
            self.log.debug(f"[sigdebug] {symbol}: skip cooldown")
            return None

        sig = {
            "symbol": symbol,
            "price": c,
            "price_change_5m_pct": dprice_pct,
            "atr2h_pct": atr,
            "vol24h_usd": vol24h_usd,
            "notional1m_est_usd": approx_1m,
            "ts": now(),
            "strength": round(min(5.0, (abs(dprice_pct)/max(0.1,self.cfg.PriceSpikePct5m)) * min(3.0, spike_x/ self.cfg.VolumeSpikeX5m) * 0.6), 2),
            "mode": self.cfg.Mode.lower()
        }
        self._mark_signalled(symbol)
        return sig

# =======================
# Telegram alerts
# =======================

def format_signal_text(sig: Dict[str, Any]) -> str:
    mode = (sig.get("mode") or "signal").upper()
    return (
        f"🚀 [{mode}] {sig['symbol']}\n"
        f"Price: {sig['price']:.8g}\n"
        f"Δ5m: {sig['price_change_5m_pct']:+.2f}% | ATR2h≈{sig['atr2h_pct']:.2f}%\n"
        f"24h Notional≈${sig['vol24h_usd']:.0f} | 1m≈${sig['notional1m_est_usd']:.0f}\n"
        f"Strength: {sig['strength']:.2f}\n"
        f"Time: {sig['ts']}"
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
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(20,len(syms))])[0])
        rows=[]
        for s in syms:
            snap=db.last(s)
            if snap:
                ts,price,vq,vb=snap
                act=(vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
                rows.append({"symbol":s,"activity":float(act or 0.0),"price":price,"ts":ts,
                             "note":"24h USD (by exchange snapshot)"})
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
# Telegram bot (aiogram, опционально)
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
        lines.append(f"{sig['ts']} | {sig['symbol']} | Δ5m={sig['price_change_5m_pct']:+.2f}% | ATR2h≈{sig['atr2h_pct']:.2f}% | str={sig['strength']:.2f}")
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

    # Диагностика вебхука (в polling он должен быть пуст)
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

    # HTTP server
    http_stop = threading.Event()
    http_thr = threading.Thread(target=run_http, args=(cfg.HttpPort, http_stop, logger), daemon=True)
    http_thr.start()

    # Engines
    breakout_engine = BreakoutEngineV0(cfg, db, logger, send_alert_fn=lambda s: send_signal_alert(s, logger))
    signals5m_engine = Signals5mEngine(cfg, db, sess, logger, send_alert_fn=lambda s: send_signal_alert(s, logger))

    # Main data loop (снапшоты + сигнальный проход)
    def data_loop():
        try:
            while not http_stop.is_set() and not _SHUTDOWN:
                ts=now(); STATE["last_cycle_start"]=ts
                ensure_csv(cfg.CsvFile, cfg.PrintOnly)
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

                # Сигнальный проход
                if cfg.Mode.lower() in ("signals_5m","breakout"):
                    engine = signals5m_engine if cfg.Mode.lower()=="signals_5m" else breakout_engine
                    signal_syms = engine._signal_universe()
                    for s in signal_syms:
                        try:
                            sig = engine.check_symbol(s)
                            if sig:
                                logger.info(f"[signal] {sig}")
                                with _GLOBALS["signals_lock"]:
                                    _GLOBALS["signals_buffer"].append(sig)
                                    if len(_GLOBALS["signals_buffer"]) > 500:
                                        _GLOBALS["signals_buffer"] = _GLOBALS["signals_buffer"][-500:]
                                engine.send_alert(sig)
                        except Exception as e:
                            logger.warning(f"signal error {s}: {e}")

                # sleep
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

def install_signals(logger):
    def _h(signum, frame):
        global _SHUTDOWN
        _SHUTDOWN=True
        logger.info(f"Signal {signum} -> stop")
    signal.signal(signal.SIGINT,_h)
    signal.signal(signal.SIGTERM,_h)

def main():
    cfg=parse_args()
    logger=setup_logger(cfg)

    logger.info(f"CFG: TelegramPolling={cfg.TelegramPolling} | Token={'set' if cfg.TelegramBotToken else 'missing'} | AllowedChatId={cfg.TelegramAllowedChatId} | Mode={cfg.Mode}")

    install_signals(logger)

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
