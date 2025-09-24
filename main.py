#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, csv, json, math, signal, sqlite3, threading, argparse, logging, asyncio
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse as _urlparse, urlparse, parse_qs

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# --- Telegram (polling) ---
try:
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
    from aiogram.filters import CommandStart
except Exception:
    Bot = Dispatcher = None  # если aiogram не установлен, HTTP всё равно запустится

BUILD_TAG = "screener-breakout-coingecko-2025-09-24"

# =======================
# Config
# =======================

@dataclass
class Config:
    # Sources
    ByBitRestBase: str = "api.bytick.com"
    ByBitRestFallback: str = "api.bybit.com"
    PriceFallbackBinance: str = "api.binance.com"

    UniverseMax: int = 50
    UniverseMode: str = "TOP"         # TOP | ALL
    UniverseRefreshMin: int = 15
    UniverseList: Optional[List[str]] = None

    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"          # linear | inverse | option
    Concurrency: int = 12

    LogFile: str = "bot.log"
    CsvFile: str = "prices.csv"
    DbFile: str = "prices.sqlite3"
    LogLevel: str = "INFO"
    PrintOnly: bool = False

    HttpPort: int = 8080
    Once: bool = False
    Loop: bool = True

    VolWindowMin: int = 120
    TrendWindowMin: int = 120

    # Telegram
    TgToken: Optional[str] = None
    TgAllowedChat: Optional[str] = None

    # Breakout defaults (можно PATCH /config)
    Mode: str = "breakout"            # breakout | activity | volatility | trend
    BaselineHours: float = 2.0        # 1–4
    LowVolThresholdPct: float = 1.2   # ATR-like %
    Min24hVolumeUSD: float = 50_000_000.0
    Min2hVolumeUSD: float = 10_000_000.0

    Spike_VolRatioMin: float = 3.0
    Spike_PricePctMin: float = 0.7    # альтернатива через ATR ratio — см. ниже
    Spike_MinNotionalUSD: float = 100_000.0
    Spike_ConfirmWithOI: bool = False # включи True, когда появится доступный OI
    Spike_OIChange1mPctMin: float = 0.8
    Spike_UseCVD: bool = False        # включим позже, когда подвезём trades WS
    Spike_CvdRatioMin: float = 2.0
    Spike_ImbalanceMin: float = 0.65
    Spike_CooldownMin: int = 15
    Spike_ClassifyLiquidations: bool = True

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
    p = argparse.ArgumentParser(description="Screener bot: HTTP + Telegram + BreakoutEngine")

    # Universe
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL"])
    p.add_argument("--max", type=int, default=env("UNIVERSE_MAX",50,int))
    p.add_argument("--refresh", type=int, default=env("UNIVERSE_REFRESH_MIN",15,int))
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

    # Telegram
    p.add_argument("--tg-token", default=env("TELEGRAM_BOT_TOKEN", None))
    p.add_argument("--tg-allow", default=env("TELEGRAM_ALLOWED_CHAT_ID", None))

    a = p.parse_args()

    return Config(
        ByBitRestBase=clean_host(a.bybit_base),
        ByBitRestFallback=clean_host(a.bybit_fallback),
        PriceFallbackBinance=clean_host(a.binance),
        UniverseMax=a.max, UniverseMode=a.mode, UniverseRefreshMin=a.refresh,
        UniverseList=[s.strip() for s in a.list.split(",")] if a.list else None,
        RequestTimeout=a.timeout, MaxRetries=a.retries, BackoffFactor=a.backoff, Category=a.category,
        Concurrency=a.concurrency,
        LogFile=a.log, CsvFile=a.csv, DbFile=a.db, LogLevel=a.level, PrintOnly=a.print_only,
        HttpPort=a.http, Once=a.once, Loop=a.loop or (not a.once),
        VolWindowMin=a.vol_window, TrendWindowMin=a.trend_window,
        TgToken=a.tg_token, TgAllowedChat=a.tg_allow
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
    s.trust_env = True  # HTTPS_PROXY/HTTP_PROXY из ENV, если понадобится
    return s

def n_bybit(sym:str)->str: return sym.replace("-","").upper()
def n_binance(sym:str)->str: return sym.replace("-","").upper()

# =======================
# CoinGecko maps
# =======================

COINGECKO_ID = {
    "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
    "XRPUSDT": "ripple", "BNBUSDT": "binancecoin", "DOGEUSDT": "dogecoin",
    "ADAUSDT": "cardano", "TONUSDT": "toncoin", "TRXUSDT": "tron",
    "LINKUSDT": "chainlink", "APTUSDT": "aptos", "ARBUSDT": "arbitrum",
    "OPUSDT": "optimism", "NEARUSDT": "near", "SUIUSDT": "sui",
    "LTCUSDT": "litecoin", "MATICUSDT": "matic-network",
    "ETCUSDT": "ethereum-classic", "ATOMUSDT": "cosmos", "AAVEUSDT": "aave",
    "EOSUSDT": "eos", "XLMUSDT":"stellar", "FILUSDT":"filecoin",
    "INJUSDT":"injective-protocol", "WLDUSDT":"worldcoin-wld",
    "PEPEUSDT":"pepe", "SHIBUSDT":"shiba-inu", "FTMUSDT":"fantom",
    "KASUSDT":"kaspa", "RUNEUSDT":"thorchain", "SEIUSDT":"sei-network",
    "PYTHUSDT":"pyth-network", "TIAUSDT":"celestia", "ORDIUSDT":"ordinals",
    "JUPUSDT":"jupiter-exchange-solana",
}

# =======================
# Fetchers (Bybit/Binance used only if доступно)
# =======================

def fetch_binance_24h(session, cfg, symbol):
    sym = n_binance(symbol)
    url = f"https://{cfg.PriceFallbackBinance}/api/v3/ticker/24hr"
    r = session.get(url, params={"symbol": sym}, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    d = r.json()
    price = float(d["lastPrice"])
    vq = float(d.get("quoteVolume")) if d.get("quoteVolume") else None
    vb = float(d.get("volume")) if d.get("volume") else None
    return price, vq, vb

def fetch_coingecko_markets(session, cfg, symbol):
    coin_id = COINGECKO_ID.get(symbol.upper())
    if not coin_id:
        raise RuntimeError(f"coingecko id not mapped for {symbol}")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    r = session.get(url, params={"vs_currency":"usd","ids":coin_id,"precision":"full"},
                    timeout=cfg.RequestTimeout)
    r.raise_for_status()
    arr = r.json()
    if not isinstance(arr, list) or not arr:
        raise RuntimeError(f"empty markets for {symbol}")
    it = arr[0]
    price = float(it["current_price"])
    vol_usd_24h = float(it.get("total_volume") or 0.0)
    vol_base = (vol_usd_24h/price) if price>0 else None
    return price, vol_usd_24h, vol_base

def fetch_coingecko_1m_series(session, coin_id:str, minutes:int, timeout:int)->Tuple[List[Tuple[int,float]], List[Tuple[int,float]]]:
    """
    Возвращает (prices, volumes) за ~последние 1 день, минутные значения.
    prices: [(ts_ms, price)], volumes: [(ts_ms, vol_usd_for_interval)]
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    # days=1 + interval=minute — Coingecko вернёт минутные точки
    r = session.get(url, params={"vs_currency":"usd","days":1,"interval":"minute"}, timeout=timeout)
    r.raise_for_status()
    d = r.json()
    prices = d.get("prices") or []
    vols = d.get("total_volumes") or []
    return prices, vols

# =======================
# Analytics (ATR-like/trend/vol)
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

def atr_like_pct_from_series(prices: List[float]) -> float:
    """
    Приближение ATR-like % без H/L: средний |ΔP|/P (по минутам), в %.
    """
    if len(prices) < 3: return 0.0
    s=0.0; c=0
    for i in range(1,len(prices)):
        p0=prices[i-1]; p1=prices[i]
        if p0>0 and p1>0:
            s += abs(p1-p0)/p0
            c += 1
    return (s/c)*100.0 if c>0 else 0.0

# =======================
# DB (цены/активность для меню)
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
            c.execute("""CREATE TABLE IF NOT EXISTS signals(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL,
              payload TEXT NOT NULL)""")
            con.commit()
        finally:
            con.close()
    def insert_price(self, ts:str, sym:str, src:str, price:float, vq:Optional[float], vb:Optional[float]):
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
    def insert_signal(self, ts:str, sym:str, kind:str, payload:Dict[str,Any]):
        try:
            con=sqlite3.connect(self.path); c=con.cursor()
            c.execute("INSERT INTO signals(ts,symbol,kind,payload) VALUES(?,?,?,?)",
                      (ts, sym.upper(), kind, json.dumps(payload, ensure_ascii=False)))
            con.commit()
        except Exception as e:
            self.log.error(f"DB signal insert error {sym}: {e}")
        finally:
            try: con.close()
            except: pass
    def last_signals(self, limit:int=50)->List[Dict[str,Any]]:
        try:
            con=sqlite3.connect(self.path); c=con.cursor()
            c.execute("""SELECT ts,symbol,kind,payload FROM signals ORDER BY id DESC LIMIT ?""",(limit,))
            out=[]
            for ts,s,k,p in c.fetchall():
                try: payload=json.loads(p)
                except: payload={"raw":p}
                out.append({"ts":ts,"symbol":s,"kind":k,"payload":payload})
            return out
        except Exception as e:
            self.log.error(f"DB last_signals error: {e}"); return []
        finally:
            try: con.close()
            except: pass

# =======================
# Utils
# =======================

def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def ensure_csv_header(path:str, print_only:bool):
    if print_only: return
    if not os.path.isfile(path):
        with open(path,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp","symbol","source","price","vol_quote_24h","vol_base_24h"])

# =======================
# Breakout Engine
# =======================

class BreakoutEngine:
    """
    Детект «Выход из консолидации» на базе минутной серии CoinGecko.
    База: последние ~2ч (настраивается), Триггер: 1m (объём/цена).
    OI/CVD — опционально (по умолчанию отключены).
    """
    def __init__(self, cfg:Config, session:requests.Session, db:DB, logger:logging.Logger):
        self.cfg=cfg; self.sess=session; self.db=db; self.log=logger
        self.cooldowns: Dict[str, float] = {}  # symbol -> next_allowed_ts

    def _cooldown_ok(self, sym:str)->bool:
        t = self.cooldowns.get(sym, 0.0)
        return time.time() >= t

    def _arm_cooldown(self, sym:str):
        self.cooldowns[sym] = time.time() + max(1, self.cfg.Spike_CooldownMin)*60

    def _fetch_series(self, symbol:str)->Tuple[List[float], List[float]]:
        coin_id = COINGECKO_ID.get(symbol.upper())
        prices_raw, vols_raw = fetch_coingecko_1m_series(self.sess, coin_id, minutes=180, timeout=self.cfg.RequestTimeout)
        # Нормализуем: берём последние N минут
        # prices_raw: [[ts_ms, price], ...], vols_raw: [[ts_ms, vol_usd], ...]
        # Приведём длины к общему минимуму
        n = min(len(prices_raw), len(vols_raw))
        prices = [float(p[1]) for p in prices_raw[-n:]]
        vols   = [float(v[1]) for v in vols_raw[-n:]]
        return prices, vols

    def _baseline_stats(self, prices:List[float], vols:List[float], baseline_min:int)->Dict[str,Any]:
        if len(prices) < baseline_min+2 or len(vols) < baseline_min+2:
            return {"ok":False, "reason":"series too short"}
        base_p = prices[-(baseline_min+1):-1]  # последние baseline_min минут, без текущей
        base_v = vols  [-(baseline_min+1):-1]
        # ATR-like %
        atr_like = atr_like_pct_from_series(base_p)
        # средний 1m оборот в USD
        avg_turnover_1m = sum(base_v)/max(1,len(base_v))
        # кумулятив за 2h
        cum_turnover_2h = sum(base_v)
        return {
            "ok": True,
            "atr_like_pct": atr_like,
            "avg_turnover_1m": avg_turnover_1m,
            "cum_turnover_2h": cum_turnover_2h,
        }

    def _last_1m(self, prices:List[float], vols:List[float])->Tuple[float,float,float]:
        if len(prices)<2 or len(vols)<1: return 0.0,0.0,0.0
        p0=prices[-2]; p1=prices[-1]
        dp_pct = ((p1-p0)/p0*100.0) if p0>0 else 0.0
        vol_1m = vols[-1]
        notional_1m = vol_1m  # у Coingecko в USD
        return dp_pct, vol_1m, notional_1m

    def _liquidity_gate(self, vol24h_usd:float, cum2h_usd:float)->bool:
        return (vol24h_usd >= self.cfg.Min24hVolumeUSD) and (cum2h_usd >= self.cfg.Min2hVolumeUSD)

    def _detect_one(self, symbol:str)->Optional[Dict[str,Any]]:
        try:
            # 1) 24h маркеты (быстрые фильтры)
            price, vol24h_usd, _vb = fetch_coingecko_markets(self.sess, self.cfg, symbol)

            # 2) минутная серия для baseline и 1m-спайка
            prices, vols = self._fetch_series(symbol)
            baseline_min = int(self.cfg.BaselineHours*60)

            b = self._baseline_stats(prices, vols, baseline_min)
            if not b["ok"]:
                return None

            # База: low-vol и ликвидность
            if b["atr_like_pct"] > self.cfg.LowVolThresholdPct:
                return None
            if not self._liquidity_gate(vol24h_usd, b["cum_turnover_2h"]):
                return None

            # Триггер: всплеск
            dp_pct, vol_1m, notional_1m = self._last_1m(prices, vols)
            vol_ratio = (vol_1m / b["avg_turnover_1m"]) if b["avg_turnover_1m"]>0 else 0.0

            cond_price = abs(dp_pct) >= self.cfg.Spike_PricePctMin
            cond_vol   = vol_ratio >= self.cfg.Spike_VolRatioMin
            cond_not   = notional_1m >= self.cfg.Spike_MinNotionalUSD

            if not (cond_price and cond_vol and cond_not):
                return None

            # OI / CVD — заглушки (можно подключить позже; сейчас просто метки)
            oi_1m_pct = None
            cvd_ratio = None
            imbalance = None
            oi_confirmed = False

            if self.cfg.Spike_ConfirmWithOI and (oi_1m_pct is not None):
                oi_confirmed = (oi_1m_pct >= self.cfg.Spike_OIChange1mPctMin)
                if not oi_confirmed:
                    return None

            # Сигнал
            score = 0.0
            score += min(3.0, vol_ratio)         # вес объёма
            score += min(2.0, abs(dp_pct)/ self.cfg.Spike_PricePctMin)  # вес цены
            if oi_confirmed: score += 0.7

            payload = {
                "symbol": symbol,
                "price": price,
                "dp_pct_1m": round(dp_pct,3),
                "vol_ratio": round(vol_ratio,2),
                "notional_1m": int(notional_1m),
                "baseline": {
                    "hours": self.cfg.BaselineHours,
                    "atr_like_pct": round(b["atr_like_pct"],3),
                    "avg_turnover_1m": int(b["avg_turnover_1m"]),
                    "cum_turnover_2h": int(b["cum_turnover_2h"]),
                    "vol24h_usd": int(vol24h_usd),
                },
                "flags": {
                    "early": True,
                    "oi_confirmed": oi_confirmed,
                    "uses_coingecko": True
                },
                "score": round(score,2),
            }
            return payload
        except Exception as e:
            self.log.debug(f"breakout detect error {symbol}: {e}")
            return None

    def scan(self, symbols:List[str])->List[Dict[str,Any]]:
        out=[]
        for s in symbols:
            if not self._cooldown_ok(s):
                continue
            sig = self._detect_one(s)
            if sig:
                out.append(sig)
                self._arm_cooldown(s)
        return out

# =======================
# Telegram bot (polling)
# =======================

class TGBotRunner:
    def __init__(self, cfg:Config, db:DB, logger:logging.Logger):
        self.cfg=cfg; self.db=db; self.log=logger
        self._thread=None

    def start(self):
        if not (self.cfg.TgToken and Bot and Dispatcher):
            self.log.info("TG: token not set or aiogram not available — bot disabled")
            return
        self._thread=threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.log.info("TG: polling thread started")

    def _run_loop(self):
        asyncio.run(self._main())

    async def _main(self):
        bot=Bot(self.cfg.TgToken)
        dp=Dispatcher()

        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Активность"), KeyboardButton(text="Волатильность")],
                [KeyboardButton(text="Тренд"), KeyboardButton(text="Режим: Breakout")],
            ],
            resize_keyboard=True
        )

        def allowed(msg: Message) -> bool:
            if not self.cfg.TgAllowedChat: return True
            try:
                return str(msg.chat.id) == str(self.cfg.TgAllowedChat)
            except Exception:
                return False

        @dp.message(CommandStart())
        async def on_start(msg: Message):
            if not allowed(msg):
                await msg.answer("Доступ ограничён.")
                return
            await msg.answer("Привет! Выбирай:", reply_markup=kb)

        @dp.message(F.text.lower().in_(["активность","волатильность","тренд","режим: breakout"]))
        async def on_buttons(msg: Message):
            if not allowed(msg):
                await msg.answer("Доступ ограничён."); return
            txt = msg.text.lower()
            try:
                if txt == "активность":
                    text = tg_format_activity_top(self.db, self.cfg)
                elif txt == "волатильность":
                    text = tg_format_vol_top(self.db, self.cfg)
                elif txt == "тренд":
                    text = tg_format_trend_top(self.db, self.cfg)
                else:
                    text = f"Режим уже: {self.cfg.Mode}"
                await msg.answer(text or "Пока нет данных.")
            except Exception as e:
                self.log.error(f"TG handler error: {e}")
                await msg.answer("Ошибка внутри бота.")

        await dp.start_polling(bot, allowed_updates=["message"])

def tg_format_activity_top(db:DB, cfg:Config, limit:int=10)->str:
    syms=get_universe(cfg)
    rows=[]
    for s in syms:
        snap=db.last(s)
        if not snap: continue
        ts,price,vq,vb=snap
        act=(vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
        rows.append((s, float(act or 0.0), price))
    rows.sort(key=lambda x: x[1], reverse=True)
    rows=rows[:limit]
    lines=[f"🏁 Топ по активности (24h USD):"]
    for i,(s,a,p) in enumerate(rows,1):
        lines.append(f"{i:>2}. {s:<9} act≈${int(a):,}  px={p:g}".replace(",", " "))
    return "\n".join(lines)

def tg_format_vol_top(db:DB, cfg:Config, limit:int=10)->str:
    syms=get_universe(cfg); win=cfg.VolWindowMin
    rows=[]
    for s in syms:
        hist=db.history(s,win)
        vol=realized_vol(hist,win)
        if vol is None: continue
        rows.append((s, vol, hist[-1][1] if hist else None))
    rows.sort(key=lambda x: x[1], reverse=True)
    rows=rows[:limit]
    lines=[f"📈 Топ по реализ. волатильности (~%/день, окно {win}м):"]
    for i,(s,v,p) in enumerate(rows,1):
        vtxt = f"{v:.2f}%" if v is not None else "—"
        lines.append(f"{i:>2}. {s:<9} vol={vtxt}  px={p if p is not None else '—'}")
    return "\n".join(lines)

def tg_format_trend_top(db:DB, cfg:Config, limit:int=10)->str:
    syms=get_universe(cfg); win=cfg.TrendWindowMin
    rows=[]
    for s in syms:
        hist=db.history(s,win)
        tr=linear_trend_pct_day(hist,win)
        if tr is None: continue
        rows.append((s, tr, hist[-1][1] if hist else None))
    rows.sort(key=lambda x: x[1], reverse=True)
    rows=rows[:limit]
    lines=[f"📊 Топ по тренду (~%/день, окно {win}м):"]
    for i,(s,t,p) in enumerate(rows,1):
        ttxt = f"{t:.2f}%" if t is not None else "—"
        lines.append(f"{i:>2}. {s:<9} trend={ttxt}  px={p if p is not None else '—'}")
    return "\n".join(lines)

# =======================
# State + HTTP handler
# =======================

STATE={"ok":0,"fail":0,"last_cycle_start":"","last_cycle_end":""}
_GLOBALS={"cfg":None,"db":None,"sess":None,"bo":None,"tg":None}
_SHUTDOWN=False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p=urlparse(self.path); path=p.path; qs=parse_qs(p.query or "")
        if path=="/health":
            self._json(200, {"status":"ok","stats":STATE,"build":BUILD_TAG,"mode":_GLOBALS["cfg"].Mode})
        elif path=="/activity":
            self._json(200, self._activity(qs))
        elif path=="/volatility":
            self._json(200, self._vol(qs))
        elif path=="/trend":
            self._json(200, self._trend(qs))
        elif path=="/signals":
            self._json(200, {"data":_GLOBALS["db"].last_signals(50)})
        elif path=="/ip":
            self._json(200, self._ip())
        else:
            self._raw(404, "not found")
    def do_PATCH(self):
        p=urlparse(self.path); path=p.path
        if path=="/config":
            ln=int(self.headers.get("Content-Length","0"))
            body=self.rfile.read(ln).decode("utf-8") if ln>0 else "{}"
            try:
                cfg=_GLOBALS["cfg"]; data=json.loads(body or "{}")
                # обновляем только известные ключи
                for k,v in data.items():
                    if hasattr(cfg, k):
                        setattr(cfg, k, v)
                self._json(200, {"ok":True, "cfg": {k:getattr(cfg,k) for k in ["Mode","BaselineHours","LowVolThresholdPct",
                                                                              "Min24hVolumeUSD","Min2hVolumeUSD",
                                                                              "Spike_VolRatioMin","Spike_PricePctMin",
                                                                              "Spike_MinNotionalUSD","Spike_ConfirmWithOI",
                                                                              "Spike_OIChange1mPctMin","Spike_UseCVD",
                                                                              "Spike_CvdRatioMin","Spike_ImbalanceMin",
                                                                              "Spike_CooldownMin","Spike_ClassifyLiquidations"]}})
            except Exception as e:
                self._json(400, {"ok":False, "error":str(e)})
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
                             "note":"24h USD (global, CoinGecko) может отличаться от биржевого"})
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

def run_http(port:int, stop_evt:threading.Event, logger:logging.Logger):
    httpd=HTTPServer(("0.0.0.0",port), Handler)
    httpd.timeout=1.0
    logger.info(f"HTTP on :{port} (/health /activity /volatility /trend /signals /ip /config[PATCH])")
    while not stop_evt.is_set():
        httpd.handle_request()
    logger.info("HTTP stopped")

# =======================
# Collection loop (prices for menu + breakout scan)
# =======================

def collect_once(cfg:Config, logger:logging.Logger, sess:requests.Session, db:DB, bo:BreakoutEngine, tg:Optional[Bot]):
    ts=now_str(); ensure_csv_header(cfg.CsvFile, cfg.PrintOnly)
    syms=get_universe(cfg)

    # 1) Быстрый снэпшот для меню: (price, vol24h) через CoinGecko (приоритет)
    def worker(sym:str):
        try:
            price, vq, vb = fetch_coingecko_markets(sess, cfg, sym)
            return (sym, ("coingecko", price, vq, vb), None)
        except Exception as e:
            # fallback Binance (может дать 451)
            try:
                price, vq, vb = fetch_binance_24h(sess, cfg, sym)
                return (sym, ("binance", price, vq, vb), None)
            except Exception as e2:
                return (sym, None, f"{e} | {e2}")

    results:Dict[str,Tuple[str,float,Optional[float],Optional[float]]]={}
    errs:Dict[str,str]={}
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
                with open(cfg.CsvFile,"a",newline="",encoding="utf-8") as f:
                    csv.writer(f).writerow([ts,s,src,f"{price:.10g}", vq if vq is not None else "", vb if vb is not None else ""])
            db.insert_price(ts,s,src,price,vq,vb)
            ok+=1
        else:
            logger.warning(f"{s}: нет данных ({errs.get(s,'unknown error')})")
            fail+=1

    # 2) Breakout scan (только если режим breakout)
    if cfg.Mode.lower()=="breakout":
        sigs = bo.scan(syms)
        for payload in sigs:
            db.insert_signal(now_str(), payload["symbol"], "BREAKOUT", payload)
            # Телеграм-алёрт
            if tg is not None:
                try:
                    text = format_breakout_msg(payload)
                    chat_id = cfg.TgAllowedChat
                    if chat_id:
                        asyncio.run(send_tg(tg, chat_id, text))
                except Exception as e:
                    logger.error(f"TG send error: {e}")

    STATE["ok"]+=ok; STATE["fail"]+=fail; STATE["last_cycle_start"]=ts; STATE["last_cycle_end"]=now_str()

def format_breakout_msg(p:Dict[str,Any])->str:
    base=p["baseline"]
    lines=[
        f"🚀 [BREAKOUT] {p['symbol']} — score={p['score']}",
        f"Price {p['dp_pct_1m']}% / 1m, Vol {p['vol_ratio']}× avg(2h), Notional1m ≈ ${p['notional_1m']:,}".replace(","," "),
        f"Base {base['hours']}h: ATR~{base['atr_like_pct']}%, cum≈${base['cum_turnover_2h']:,}, avg1m≈${base['avg_turnover_1m']:,}, 24h≈${base['vol24h_usd']:,}".replace(","," "),
        f"Flags: early={p['flags']['early']} oi_confirmed={p['flags']['oi_confirmed']} (src=CoinGecko)"
    ]
    return "\n".join(lines)

async def send_tg(bot:Bot, chat_id:str, text:str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        # если polling идёт в другом loop — создадим временный
        async with Bot(token=bot.token) as t:
            await t.send_message(chat_id, text)

# =======================
# Runner
# =======================

def install_signals(logger):
    def _h(signum, frame):
        global _SHUTDOWN
        _SHUTDOWN=True
        logger.info(f"Signal {signum} -> stop")
    signal.signal(signal.SIGINT,_h)
    signal.signal(signal.SIGTERM,_h)

def run_loop(cfg:Config, logger:logging.Logger):
    logger.info(f"BUILD {BUILD_TAG} | mode={cfg.Mode}")
    sess=build_session(cfg)
    db=DB(cfg.DbFile, logger)
    bo=BreakoutEngine(cfg, sess, db, logger)

    _GLOBALS.update({"cfg":cfg,"db":db,"sess":sess,"bo":bo})

    # HTTP
    http_stop=threading.Event()
    http_thr=threading.Thread(target=run_http, args=(cfg.HttpPort, http_stop, logger), daemon=True)
    http_thr.start()

    # Telegram polling (если токен задан)
    tg_bot=None
    if cfg.TgToken and Bot and Dispatcher:
        # отдельный раннер, но для отправки алёртов держим Bot здесь
        tg_runner = TGBotRunner(cfg, db, logger)
        tg_runner.start()
        tg_bot = Bot(cfg.TgToken)

    try:
        while not _SHUTDOWN:
            collect_once(cfg, logger, sess, db, bo, tg_bot)
            if cfg.Once: break
            sleep_total=max(5, int(cfg.UniverseRefreshMin*60))
            for _ in range(sleep_total):
                if _SHUTDOWN: break
                time.sleep(1)
    finally:
        http_stop.set()
        for _ in range(50):
            if not http_thr.is_alive(): break
            time.sleep(0.1)
        logger.info("Stopped")

def main():
    cfg=parse_args()
    logger=setup_logger(cfg)
    install_signals(logger)
    if cfg.Loop:
        run_loop(cfg, logger)
    else:
        sess=build_session(cfg)
        db=DB(cfg.DbFile, logger)
        bo=BreakoutEngine(cfg, sess, db, logger)
        _GLOBALS.update({"cfg":cfg,"db":db,"sess":sess,"bo":bo})
        collect_once(cfg, logger, sess, db, bo, None)

if __name__=="__main__":
    main()
