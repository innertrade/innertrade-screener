#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, csv, json, math, signal, sqlite3, threading, argparse, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# -------- Config --------
@dataclass
class Config:
    ByBitRestBase: str = "bytick.com"
    ByBitRestFallback: str = "bybit.com"
    PriceFallbackBinance: str = "binance.com"
    UniverseMax: int = 50
    UniverseMode: str = "TOP"       # TOP | ALL
    UniverseRefreshMin: int = 15
    UniverseList: Optional[List[str]] = None
    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Category: str = "linear"        # Bybit v5: linear|inverse|option
    Concurrency: int = 10
    RatePerSecond: float = 20.0
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

def env(name, default, cast=None):
    v = os.getenv(name)
    if v is None: return default
    if cast:
        try: return cast(v)
        except: return default
    return v

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Screener with /activity /volatility /trend")
    p.add_argument("--mode", default=env("UNIVERSE_MODE","TOP"), choices=["TOP","ALL"])
    p.add_argument("--max", type=int, default=env("UNIVERSE_MAX",50,int))
    p.add_argument("--refresh", type=int, default=env("UNIVERSE_REFRESH_MIN",15,int))
    p.add_argument("--list", type=str, default=env("UNIVERSE_LIST",None))
    p.add_argument("--timeout", type=int, default=env("REQUEST_TIMEOUT",10,int))
    p.add_argument("--retries", type=int, default=env("MAX_RETRIES",3,int))
    p.add_argument("--backoff", type=float, default=env("BACKOFF_FACTOR",0.6,float))
    p.add_argument("--category", default=env("BYBIT_CATEGORY","linear"), choices=["linear","inverse","option"])
    p.add_argument("--concurrency", type=int, default=env("CONCURRENCY",10,int))
    p.add_argument("--rps", type=float, default=env("RATE_PER_SECOND",20.0,float))
    p.add_argument("--log", default=env("LOG_FILE","bot.log"))
    p.add_argument("--csv", default=env("CSV_FILE","prices.csv"))
    p.add_argument("--db", default=env("DB_FILE","prices.sqlite3"))
    p.add_argument("--level", default=env("LOG_LEVEL","INFO"), choices=["DEBUG","INFO","WARNING","ERROR"])
    p.add_argument("--print-only", action="store_true", default=env("PRINT_ONLY","false").lower()=="true")
    p.add_argument("--http", type=int, default=env("HTTP_PORT",8080,int))
    p.add_argument("--bybit-base", default=env("BYBIT_REST_BASE","bytick.com"))
    p.add_argument("--bybit-fallback", default=env("BYBIT_REST_FALLBACK","bybit.com"))
    p.add_argument("--binance", default=env("PRICE_FALLBACK_BINANCE","binance.com"))
    p.add_argument("--once", action="store_true")
    p.add_argument("--loop", action="store_true")
    p.add_argument("--vol-window", type=int, default=env("VOL_WINDOW_MIN",120,int))
    p.add_argument("--trend-window", type=int, default=env("TREND_WINDOW_MIN",120,int))
    a = p.parse_args()
    return Config(
        ByBitRestBase=a.bybit_base, ByBitRestFallback=a.bybit_fallback, PriceFallbackBinance=a.binance,
        UniverseMax=a.max, UniverseMode=a.mode, UniverseRefreshMin=a.refresh,
        UniverseList=[s.strip() for s in a.list.split(",")] if a.list else None,
        RequestTimeout=a.timeout, MaxRetries=a.retries, BackoffFactor=a.backoff, Category=a.category,
        Concurrency=a.concurrency, RatePerSecond=a.rps, CacheTTL=15, LogFile=a.log, CsvFile=a.csv, DbFile=a.db,
        LogLevel=a.level, PrintOnly=a.print_only, HttpPort=a.http, Once=a.once, Loop=a.loop or (not a.once),
        VolWindowMin=a.vol_window, TrendWindowMin=a.trend_window
    )

# -------- Logger --------
def setup_logger(cfg: Config) -> logging.Logger:
    lg = logging.getLogger("bot")
    lg.setLevel(getattr(logging, cfg.LogLevel.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    h = logging.StreamHandler(sys.stdout); h.setFormatter(fmt)
    lg.handlers.clear(); lg.addHandler(h)
    if not cfg.PrintOnly:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(cfg.LogFile, maxBytes=3_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt); lg.addHandler(fh)
    return lg

# -------- Universe --------
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
    if cfg.UniverseList: return [s.upper() for s in cfg.UniverseList][:cfg.UniverseMax]
    base = DEFAULT_TOP if cfg.UniverseMode.upper()=="TOP" else DEFAULT_ALL
    return base[:cfg.UniverseMax]

# -------- HTTP / Retry session --------
def build_session(cfg: Config) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=cfg.MaxRetries, backoff_factor=cfg.BackoffFactor,
                  status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({"User-Agent":"ScreenerBot/mini"})
    return s

def n_bybit(sym:str)->str: return sym.replace("-","").upper()
def n_binance(sym:str)->str: return sym.replace("-","").upper()

# returns (price, quoteVol24h, baseVol24h)
def fetch_bybit(s:requests.Session, cfg:Config, sym:str)->Tuple[float,Optional[float],Optional[float]]:
    url=f"https://api.{cfg.ByBitRestBase}/v5/market/tickers"
    r=s.get(url, params={"category":cfg.Category,"symbol":n_bybit(sym)}, timeout=cfg.RequestTimeout)
    r.raise_for_status(); d=r.json()["result"]["list"][0]
    price=float(d["lastPrice"])
    vq=float(d["turnover24h"]) if d.get("turnover24h") is not None else None
    vb=float(d["volume24h"]) if d.get("volume24h") is not None else None
    return price,vq,vb
def fetch_bybit_fb(s:requests.Session, cfg:Config, sym:str)->Tuple[float,Optional[float],Optional[float]]:
    url=f"https://api.{cfg.ByBitRestFallback}/v5/market/tickers"
    r=s.get(url, params={"category":cfg.Category,"symbol":n_bybit(sym)}, timeout=cfg.RequestTimeout)
    r.raise_for_status(); d=r.json()["result"]["list"][0]
    price=float(d["lastPrice"])
    vq=float(d.get("turnover24h")) if d.get("turnover24h") is not None else None
    vb=float(d.get("volume24h")) if d.get("volume24h") is not None else None
    return price,vq,vb
def fetch_binance(s:requests.Session, cfg:Config, sym:str)->Tuple[float,Optional[float],Optional[float]]:
    url=f"https://api.{cfg.PriceFallbackBinance}/api/v3/ticker/24hr"
    r=s.get(url, params={"symbol":n_binance(sym)}, timeout=cfg.RequestTimeout)
    r.raise_for_status(); d=r.json()
    price=float(d["lastPrice"])
    vq=float(d.get("quoteVolume")) if d.get("quoteVolume") is not None else None
    vb=float(d.get("volume")) if d.get("volume") is not None else None
    return price,vq,vb

def snapshot(s, cfg, sym, logger):
    try: p,vq,vb=fetch_bybit(s,cfg,sym); return "bytick",p,vq,vb
    except Exception as e: logger.debug(f"bytick {sym} fail: {e}")
    try: p,vq,vb=fetch_bybit_fb(s,cfg,sym); return "bybit",p,vq,vb
    except Exception as e: logger.debug(f"bybitFB {sym} fail: {e}")
    try: p,vq,vb=fetch_binance(s,cfg,sym); return "binance",p,vq,vb
    except Exception as e: logger.debug(f"binance {sym} fail: {e}")
    return None

# -------- DB --------
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
            return (r[0], float(r[1]),
                    (float(r[2]) if r[2] is not None else None),
                    (float(r[3]) if r[3] is not None else None)) if r else None
        except Exception as e:
            self.log.error(f"DB last error {sym}: {e}"); return None
        finally:
            try: con.close()
            except: pass

# -------- CSV --------
def ensure_csv(path:str, print_only:bool):
    if print_only: return
    if not os.path.isfile(path):
        with open(path,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp","symbol","source","price","vol_quote_24h","vol_base_24h"])
def append_csv(path:str, row:List[Any], print_only:bool):
    if print_only: return
    with open(path,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow(row)

# -------- Vol & Trend --------
def realized_vol(prices:List[Tuple[str,float]], window_min:int)->Optional[float]:
    if len(prices)<3: return None
    vals=[p for _,p in prices if p>0]
    if len(vals)<3: return None
    rets=[]
    for i in range(1,len(vals)):
        try: rets.append(math.log(vals[i]/vals[i-1]))
        except: pass
    if len(rets)<2: return None
    m=sum(rets)/len(rets)
    var=sum((x-m)**2 for x in rets)/(len(rets)-1)
    std=math.sqrt(var)
    scale=math.sqrt(1440.0/max(1.0,float(window_min)))
    return std*scale*100.0
def linear_trend_pct_day(prices:List[Tuple[str,float]], window_min:int)->Optional[float]:
    if len(prices)<3: return None
    ys=[p for _,p in prices]; xs=list(range(len(ys)))
    n=len(xs); sx=sum(xs); sy=sum(ys)
    sxx=sum(x*x for x in xs); sxy=sum(xs[i]*ys[i] for i in range(n))
    denom=n*sxx - sx*sx
    if denom==0: return None
    slope=(n*sxy - sx*sy)/denom
    last=ys[-1]
    if last<=0: return None
    steps_per_day=1440.0/max(1.0,float(window_min))/n
    return (slope/last)*steps_per_day*100.0

# -------- State & HTTP --------
STATE={"ok":0,"fail":0,"last_cycle_start":"","last_cycle_end":""}
_GLOBALS={"cfg":None,"db":None}
_SHUTDOWN=False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p=urlparse(self.path); path=p.path; qs=parse_qs(p.query or "")
        if path=="/health":
            self._json(200, {"status":"ok","stats":STATE})
        elif path=="/activity":
            self._json(200, self._activity(qs))
        elif path=="/volatility":
            self._json(200, self._vol(qs))
        elif path=="/trend":
            self._json(200, self._trend(qs))
        else:
            self._raw(404, "not found")
    def _activity(self, qs):
        cfg:_cfg=_GLOBALS["cfg"]; db:_db=_GLOBALS["db"]
        syms=get_universe(cfg); limit=int(qs.get("limit",[min(20,len(syms))])[0])
        rows=[]
        for s in syms:
            snap=db.last(s)
            if snap:
                ts,price,vq,vb=snap
                act=(vq if vq is not None else (vb*price if (vb is not None and price is not None) else 0.0))
                rows.append({"symbol":s,"activity":float(act or 0.0),"price":price,"ts":ts})
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
    httpd=HTTPServer(("0.0.0.0",port), Handler); httpd.timeout=1.0
    logger.info(f"HTTP on :{port} (/health /activity /volatility /trend)")
    while not stop_evt.is_set(): httpd.handle_request()
    logger.info("HTTP stopped")

# -------- Loop --------
def now(): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def run_once(cfg:Config, logger:logging.Logger, sess:requests.Session, db:DB):
    ts=now(); STATE["last_cycle_start"]=ts
    ensure_csv(cfg.CsvFile, cfg.PrintOnly)
    syms=get_universe(cfg)
    results:Dict[str,Tuple[str,float,Optional[float],Optional[float]]]={}; errs={}
    def worker(sym:str):
        snap=snapshot(sess,cfg,sym,logger)
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
            except Exception as e: errs[s]=str(e)
    ok=0; fail=0
    for s in syms:
        if s in results:
            src,price,vq,vb=results[s]
            print(f"{s}: {price} [{src}]")
            logger.info(f"{s}: {price} [{src}] volQ24h={vq} volB24h={vb}")
            append_csv(cfg.CsvFile, [ts,s,src,f"{price:.10g}",vq if vq is not None else "", vb if vb is not None else ""], cfg.PrintOnly)
            db.insert(ts,s,src,price,vq,vb); ok+=1
        else:
            logger.warning(f"{s}: нет данных ({errs.get(s,'unknown error')})"); fail+=1
    STATE["ok"]+=ok; STATE["fail"]+=fail; STATE["last_cycle_end"]=now()

def install_signals(logger):
    def _h(signum, frame):
        global _SHUTDOWN; _SHUTDOWN=True; logger.info(f"Signal {signum} -> stop")
    signal.signal(signal.SIGINT,_h); signal.signal(signal.SIGTERM,_h)

def run_loop(cfg:Config, logger:logging.Logger):
    sess=build_session(cfg); db=DB(cfg.DbFile, logger)
    http_stop=threading.Event(); http_thr=threading.Thread(target=run_http, args=(cfg.HttpPort,http_stop,logger), daemon=True)
    http_thr.start()
    try:
        while not _SHUTDOWN:
            run_once(cfg, logger, sess, db)
            if cfg.Once: break
            for _ in range(max(1,int(cfg.UniverseRefreshMin*60))):
                if _SHUTDOWN: break
                time.sleep(1)
    finally:
        http_stop.set()
        for _ in range(50):
            if not http_thr.is_alive(): break
            time.sleep(0.1)
        logger.info("Stopped")

# -------- Main --------
def main():
    cfg=parse_args(); logger=setup_logger(cfg); install_signals(logger)
    if cfg.Loop: run_loop(cfg, logger)
    else:
        sess=build_session(cfg); db=DB(cfg.DbFile, logger); run_once(cfg, logger, sess, db)
if __name__=="__main__":
    main()
