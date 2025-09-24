#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, csv, json, math, signal, sqlite3, threading, argparse, logging, asyncio, contextlib
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse as _urlparse, urlparse, parse_qs

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

BUILD_TAG = "screener-main-coingecko-eu-2025-09-24b"

# =======================
# Config
# =======================

@dataclass
class Config:
    ByBitRestBase: str = "api.bytick.com"
    ByBitRestFallback: str = "api.bybit.com"
    PriceFallbackBinance: str = "api.binance.com"

    UniverseMax: int = 50
    UniverseMode: str = "TOP"         # TOP | ALL
    UniverseRefreshMin: int = 30
    UniverseList: Optional[List[str]] = None

    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"          # linear | inverse | option
    Concurrency: int = 4              # <=4 чтобы не ловить 429 у CG

    CacheTTL: int = 15

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

    # CoinGecko throttle: минимальный интервал между запросами (сек)
    CG_MinIntervalSec: float = 0.25   # ~4 rps безопасно

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
    if not v: return v
    v = v.strip()
    if "://" in v:
        parsed = _urlparse(v)
        host = (parsed.netloc or parsed.path or "").strip("/")
    else:
        host = v.strip("/")
    return host.split("/")[0].strip()

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Screener bot: /health /activity /volatility /trend /ip")

    # Universe
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL"])
    p.add_argument("--max", type=int, default=env("UNIVERSE_MAX",50,int))
    p.add_argument("--refresh", type=int, default=env("UNIVERSE_REFRESH_MIN",30,int))
    p.add_argument("--list", type=str, default=env("UNIVERSE_LIST",None))

    # Network
    p.add_argument("--timeout", type=int, default=env("REQUEST_TIMEOUT",10,int))
    p.add_argument("--retries", type=int, default=env("MAX_RETRIES",3,int))
    p.add_argument("--backoff", type=float, default=env("BACKOFF_FACTOR",0.6,float))
    p.add_argument("--category", default=env("BYBIT_CATEGORY","linear"), choices=["linear","inverse","option"])
    p.add_argument("--concurrency", type=int, default=env("CONCURRENCY",4,int))

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

    # CoinGecko throttle
    p.add_argument("--cg-interval", type=float, default=env("COINGECKO_MIN_INTERVAL","0.25",float))

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
        VolWindowMin=a.vol_window, TrendWindowMin=a.trend_window,
        CG_MinIntervalSec=a.cg_interval
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
# Fetchers + throttle
# =======================

_cg_lock = threading.Lock()
_cg_last_ts = 0.0

def _coingecko_throttle(min_interval_sec: float):
    global _cg_last_ts
    with _cg_lock:
        now = time.time()
        wait = _cg_last_ts + min_interval_sec - now
        if wait > 0:
            time.sleep(wait)
            now = time.time()
        _cg_last_ts = now

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
    vq = float(item["turnover24h"]) if item.get("turnover24h") else None
    vb = float(item["volume24h"]) if item.get("volume24h") else None
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
    _coingecko_throttle(cfg.CG_MinIntervalSec)
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

# =======================
# State + HTTP handler
# =======================

STATE={"ok":0,"fail":0,"last_cycle_start":"","last_cycle_end":""}
_GLOBALS={"cfg":None,"db":None}
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
                             "note":"24h USD (global) if source=coingecko"})
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

# =======================
# HTTP server
# =======================

def run_http(port:int, stop_evt:threading.Event, logger:logging.Logger):
    httpd=HTTPServer(("0.0.0.0",port), Handler)
    httpd.timeout=1.0
    logger.info(f"HTTP on :{port} (/health /activity /volatility /trend /ip)")
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
            print(f"{s}: {price} [{src}]")
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
# Telegram bot (aiogram v3) — в отдельном потоке, БЕЗ сигналов
# =======================

try:
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
    from aiogram.filters import CommandStart
    _TG_AVAILABLE = True
except Exception:
    _TG_AVAILABLE = False

def _tg_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Активность"), KeyboardButton(text="Волатильность")],
            [KeyboardButton(text="Тренд")],
        ],
        resize_keyboard=True
    )

async def _tg_runner(cfg: Config, logger: logging.Logger):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_whitelist = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    allow_any = (chat_whitelist == "")

    if not token:
        logger.info("Telegram: токен не задан, бот отключён.")
        return

    bot = Bot(token=token)
    dp = Dispatcher()

    def _allowed(chat_id: int) -> bool:
        if allow_any:
            return True
        allowed_ids = {x.strip() for x in chat_whitelist.split(",") if x.strip()}
        return str(chat_id) in allowed_ids

    @dp.message(CommandStart())
    async def _start(m: Message):
        if not _allowed(m.chat.id):
            return
        await m.answer("Привет! Я скринер. Кнопки ниже:", reply_markup=_tg_kb())

    @dp.message(F.text.in_({"Активность","Волатильность","Тренд"}))
    async def _menu(m: Message):
        if not _allowed(m.chat.id):
            return
        text = (m.text or "").strip()
        try:
            base = f"http://127.0.0.1:{_GLOBALS['cfg'].HttpPort}"
            if text == "Активность":
                r = requests.get(f"{base}/activity?limit=10", timeout=10).json()
                rows = r.get("data", [])[:10]
                lines = []
                for it in rows:
                    price = it.get("price")
                    act = it.get("activity") or 0
                    lines.append(f"◆ {it['symbol']}: ${price if price is not None else '-'} | act≈{round(float(act)):,}")
                msg = "ТОП по активности (24h):\n" + ("\n".join(lines) if lines else "нет данных")
            elif text == "Волатильность":
                r = requests.get(f"{base}/volatility?window_min={_GLOBALS['cfg'].VolWindowMin}&limit=10", timeout=10).json()
                rows = r.get("data", [])[:10]
                lines = []
                for it in rows:
                    vol = it.get("volatility_pct_day")
                    lines.append(f"◆ {it['symbol']}: {round(vol,2) if vol is not None else '-'}%/day")
                msg = f"Волатильность (окно { _GLOBALS['cfg'].VolWindowMin }м):\n" + ("\n".join(lines) if lines else "нет данных")
            else:
                r = requests.get(f"{base}/trend?window_min={_GLOBALS['cfg'].TrendWindowMin}&limit=10", timeout=10).json()
                rows = r.get("data", [])[:10]
                lines = []
                for it in rows:
                    tr = it.get("trend_pct_day")
                    lines.append(f"◆ {it['symbol']}: {round(tr,2) if tr is not None else '-'}%/day")
                msg = f"Тренд (окно { _GLOBALS['cfg'].TrendWindowMin }м):\n" + ("\n".join(lines) if lines else "нет данных")
            await m.answer(msg)
        except Exception as e:
            await m.answer(f"Ошибка: {e}")

    @dp.message(F.text == "ping")
    async def _ping(m: Message):
        if not _allowed(m.chat.id):
            return
        await m.answer("pong ✅")

    logger.info("Telegram: polling start")
    try:
        with contextlib.suppress(Exception):
            await bot.delete_webhook(drop_pending_updates=False)
        # ВАЖНО: handle_signals=False — чтобы не трогать set_wakeup_fd в потоке
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types(), handle_signals=False)
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()

def start_telegram_in_thread(cfg: Config, logger: logging.Logger):
    if not _TG_AVAILABLE:
        logger.info("Telegram: aiogram не установлен — бот отключён.")
        return None

    def _target():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_tg_runner(cfg, logger))
        except Exception as e:
            logging.getLogger("bot").error(f"Telegram thread error: {e}")

    thr = threading.Thread(target=_target, daemon=True)
    thr.start()
    return thr

# =======================
# Run loop
# =======================

def run_loop(cfg:Config, logger:logging.Logger):
    logger.info(f"BUILD {BUILD_TAG} | hosts: bytick={cfg.ByBitRestBase}, bybit={cfg.ByBitRestFallback}, binance={cfg.PriceFallbackBinance}")
    sess=build_session(cfg)
    db=DB(cfg.DbFile, logger)
    _GLOBALS["db"]=db

    http_stop=None
    http_thr=None

    # HTTP
    if isinstance(cfg.HttpPort,int) and cfg.HttpPort>0:
        http_stop=threading.Event()
        http_thr=threading.Thread(target=run_http, args=(cfg.HttpPort, http_stop, logger), daemon=True)
        http_thr.start()

    # Telegram
    try:
        start_telegram_in_thread(cfg, logger)
    except Exception as e:
        logger.error(f"Telegram start error: {e}")

    try:
        while not _SHUTDOWN:
            run_once(cfg, logger, sess, db)
            if cfg.Once: break
            sleep_total=max(1, int(cfg.UniverseRefreshMin*60))
            for _ in range(sleep_total):
                if _SHUTDOWN: break
                time.sleep(1)
    finally:
        if http_stop is not None:
            http_stop.set()
            for _ in range(50):
                if http_thr and not http_thr.is_alive(): break
                time.sleep(0.1)
        logger.info("Stopped")

# =======================
# Main
# =======================

def main():
    cfg=parse_args()
    _GLOBALS["cfg"]=cfg
    logger=setup_logger(cfg)
    install_signals(logger)

    if cfg.Loop:
        run_loop(cfg, logger)
    else:
        sess=build_session(cfg)
        db=DB(cfg.DbFile, logger)
        _GLOBALS["db"]=db
        run_once(cfg, logger, sess, db)

if __name__=="__main__":
    main()
