import asyncio
import json
import logging
import os
import contextlib
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, Message
from aiogram.filters import Command

from psycopg_pool import AsyncConnectionPool

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("screener")

# -------------------- ENV --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com").strip()
BYBIT_REST_FALLBACK = os.getenv("BYBIT_REST_FALLBACK", "https://api.bytick.com").strip()

SYMBOLS = os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
).split(",")

# OI polling toggles (REST is optional now)
ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "1").strip() not in ("0", "false", "False", "")
OI_POLL_SECONDS = int(os.getenv("OI_POLL_SECONDS", "90"))
OI_INTERVAL = os.getenv("OI_INTERVAL", "5min").strip()

if not TELEGRAM_TOKEN or not WEBHOOK_BASE or not WEBHOOK_SECRET or not DATABASE_URL:
    raise RuntimeError("Missing required ENV vars")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
VERSION = "v1.6.0-oi-ws"

# -------------------- Globals --------------------
bot: Bot | None = None
dp: Dispatcher | None = None
app: web.Application | None = None

db_pool: AsyncConnectionPool | None = None
ws_session: aiohttp.ClientSession | None = None
oi_session: aiohttp.ClientSession | None = None

ws_task: asyncio.Task | None = None
watchdog_task: asyncio.Task | None = None
oi_task: asyncio.Task | None = None

ws_cache: dict[str, dict] = {}
ws_connected: bool = False
ws_last_msg_ts: str | None = None

trade_buckets: dict[tuple[str, datetime], dict] = {}
ob_buckets: dict[tuple[str, datetime], dict] = {}

# NEW: OI from WS buckets (per minute, last value in minute)
oi_ws_buckets: dict[tuple[str, datetime], float] = {}

# -------------------- SQL --------------------
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS ws_ticker (
  symbol           text PRIMARY KEY,
  last             double precision,
  price24h_pcnt    double precision,
  turnover24h      double precision,
  updated_at       timestamptz NOT NULL DEFAULT now()
);
"""

SQL_UPSERT = """
INSERT INTO ws_ticker (symbol, last, price24h_pcnt, turnover24h, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (symbol) DO UPDATE
SET last = COALESCE(EXCLUDED.last, ws_ticker.last),
    price24h_pcnt = COALESCE(EXCLUDED.price24h_pcnt, ws_ticker.price24h_pcnt),
    turnover24h = COALESCE(EXCLUDED.turnover24h, ws_ticker.turnover24h),
    updated_at = NOW();
"""

SQL_SELECT_SORTED = """
SELECT symbol,
       COALESCE(price24h_pcnt, 0) AS p24,
       COALESCE(turnover24h, 0)   AS tov,
       COALESCE(last, 0)          AS last
FROM ws_ticker
ORDER BY {order_by} {direction}
LIMIT %s;
"""

SQL_COUNT = "SELECT COUNT(*) FROM ws_ticker;"

SQL_INSERT_TRADES = """
INSERT INTO trades_1m (ts, symbol, trades_count, qty_sum)
VALUES (%s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET trades_count = trades_1m.trades_count + EXCLUDED.trades_count,
    qty_sum      = COALESCE(trades_1m.qty_sum,0) + COALESCE(EXCLUDED.qty_sum,0);
"""

SQL_GET_ONE = """
SELECT symbol, last, price24h_pcnt, turnover24h, updated_at
FROM ws_ticker
WHERE symbol = %s;
"""

SQL_TRADES_LATEST = """
SELECT ts, trades_count, qty_sum
FROM trades_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_INSERT_OB = """
INSERT INTO ob_1m (ts, symbol, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET best_bid   = EXCLUDED.best_bid,
    best_ask   = EXCLUDED.best_ask,
    bid_qty    = EXCLUDED.bid_qty,
    ask_qty    = EXCLUDED.ask_qty,
    spread_bps = EXCLUDED.spread_bps,
    depth_usd  = EXCLUDED.depth_usd;
"""

SQL_OB_LATEST = """
SELECT ts, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
FROM ob_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_INSERT_OI = """
INSERT INTO oi_1m (ts, symbol, oi_usd)
VALUES (%s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET oi_usd = EXCLUDED.oi_usd;
"""

SQL_OI_LATEST = """
SELECT ts, oi_usd
FROM oi_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_GET_LAST_PRICE = "SELECT last FROM ws_ticker WHERE symbol = %s;"

# -------------------- DB helpers --------------------
async def init_db():
    global db_pool
    db_pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=4, open=False)
    await db_pool.open()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_CREATE)
            await conn.commit()
    log.info("DB ready")

async def upsert_ticker(symbol: str, last: float | None, p24: float | None, turnover: float | None):
    if db_pool is None: return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT, (symbol, last, p24, turnover))
            await conn.commit()

async def insert_trades_batch(items: list[tuple[datetime, str, int, float]]):
    if not items or db_pool is None: return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for ts_min, sym, cnt, qty in items:
                await cur.execute(SQL_INSERT_TRADES, (ts_min, sym, cnt, qty))
        await conn.commit()

async def insert_ob_batch(items: list[tuple[datetime, str, float, float, float, float, float, float]]):
    if not items or db_pool is None: return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for (ts_min, sym, bid, ask, bqty, aqty, spread_bps, depth_usd) in items:
                await cur.execute(SQL_INSERT_OB, (ts_min, sym, bid, ask, bqty, aqty, spread_bps, depth_usd))
        await conn.commit()

async def insert_oi(ts_min: datetime, symbol: str, oi_usd: float | None):
    if oi_usd is None or db_pool is None: return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_INSERT_OI, (ts_min, symbol, oi_usd))
        await conn.commit()

async def select_sorted(order_by: str, limit: int = 10, desc: bool = True):
    if db_pool is None: return []
    if order_by == "turnover": ob = "tov"
    elif order_by in ("p24", "price24h_pcnt"): ob = "p24"
    else: ob = "last"
    direction = "DESC" if desc else "ASC"
    sql = SQL_SELECT_SORTED.format(order_by=ob, direction=direction)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            return await cur.fetchall()

async def count_rows() -> int:
    if db_pool is None: return 0
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_COUNT)
            n = (await cur.fetchone())[0]
            return int(n or 0)

async def get_one(symbol: str):
    if db_pool is None: return None
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_GET_ONE, (symbol.upper(),))
            return await cur.fetchone()

async def trades_latest(symbol: str, limit: int = 5):
    if db_pool is None: return []
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_TRADES_LATEST, (symbol.upper(), limit))
            return await cur.fetchall()

async def ob_latest(symbol: str, limit: int = 5):
    if db_pool is None: return []
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_OB_LATEST, (symbol.upper(), limit))
            return await cur.fetchall()

async def oi_latest(symbol: str, limit: int = 5):
    if db_pool is None: return []
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_OI_LATEST, (symbol.upper(), limit))
            return await cur.fetchall()

async def get_last_price(symbol: str) -> float | None:
    sym = symbol.upper()
    v = ws_cache.get(sym)
    if v and isinstance(v.get("last"), (int, float)) and v["last"] > 0:
        return float(v["last"])
    if db_pool is None: return None
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_GET_LAST_PRICE, (sym,))
            row = await cur.fetchone()
            if row and row[0]:
                try:
                    val = float(row[0]);  return val if val > 0 else None
                except Exception:
                    return None
    return None

# -------------------- Webhook reliability --------------------
async def ensure_webhook(bot: Bot, url: str, retries: int = 8) -> bool:
    delay = 1
    for i in range(retries):
        try:
            ok = await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
            if ok:
                log.info("Webhook set to %s", url);  return True
        except Exception as e:
            log.warning("set_webhook attempt %d failed: %s", i + 1, e)
        await asyncio.sleep(delay); delay = min(delay * 2, 30)
    log.error("Failed to set webhook after %d attempts", retries);  return False

async def webhook_watchdog(bot: Bot, url: str):
    while True:
        try:
            info = await bot.get_webhook_info()
            if not info.url:
                log.warning("Webhook empty; resetting...")
                try:
                    await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
                    log.info("Webhook restored by watchdog")
                except Exception as e:
                    log.warning("Watchdog failed to set webhook: %s", e)
        except Exception as e:
            log.warning("webhook_watchdog error: %s", e)
        await asyncio.sleep(300)

# -------------------- WS ingest --------------------
async def ws_consumer():
    global ws_session, ws_connected, ws_last_msg_ts
    backoff = 1
    while True:
        ws_connected = False; ws_last_msg_ts = None
        ws_session = aiohttp.ClientSession()
        log.info("Bybit WS connecting: %s", BYBIT_WS_URL)
        try:
            async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
                args = (
                    [f"tickers.{s}" for s in SYMBOLS if s]
                    + [f"publicTrade.{s}" for s in SYMBOLS if s]
                    + [f"orderbook.1.{s}" for s in SYMBOLS if s]
                )
                await ws.send_json({"op": "subscribe", "args": args})
                log.info("WS subscribed: %d topics", len(args))
                ws_connected = True; backoff = 1

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json(loads=json.loads); topic = data.get("topic") or ""

                        # ---- tickers ----
                        if topic.startswith("tickers."):
                            payload = data.get("data")
                            items = [payload] if isinstance(payload, dict) else (payload or [])
                            for it in items:
                                symbol = (it.get("symbol") or "").upper()
                                if not symbol: continue
                                raw_last = it.get("lastPrice")
                                raw_p24  = it.get("price24hPcnt")
                                raw_tov  = it.get("turnover24h")
                                raw_oi_v = it.get("openInterestValue")
                                raw_oi   = it.get("openInterest")

                                # last / p24 / turnover
                                try:   last = float(raw_last) if raw_last not in (None, "") else None
                                except: last = None
                                try:   p24 = (float(raw_p24) * 100.0) if raw_p24 not in (None, "") else None
                                except: p24 = None
                                try:   tov = float(raw_tov) if raw_tov not in (None, "") else None
                                except: tov = None

                                # ---- OI USD from WS ----
                                oi_usd = None
                                if raw_oi_v not in (None, ""):
                                    try: oi_usd = float(raw_oi_v)
                                    except: oi_usd = None
                                if oi_usd is None and raw_oi not in (None, ""):
                                    try:
                                        oi = float(raw_oi)
                                        _last = None
                                        if last is not None: _last = last
                                        else:
                                            _last = await get_last_price(symbol)
                                        if _last and oi > 0:
                                            oi_usd = oi * _last
                                    except: oi_usd = None
                                if oi_usd is not None:
                                    ts_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                                    oi_ws_buckets[(symbol, ts_min)] = float(oi_usd)

                                if last is None and p24 is None and tov is None and oi_usd is None:
                                    continue

                                prev = ws_cache.get(symbol, {})
                                ws_cache[symbol] = {
                                    "last": last if last is not None else prev.get("last", 0.0),
                                    "p24":  p24  if p24  is not None else prev.get("p24",  0.0),
                                    "tov":  tov  if tov  is not None else prev.get("tov",  0.0),
                                    "ts": datetime.now(timezone.utc).isoformat(),
                                }
                                await upsert_ticker(symbol, last, p24, tov)
                                ws_last_msg_ts = datetime.now(timezone.utc).isoformat()

                        # ---- publicTrade ----
                        elif topic.startswith("publicTrade."):
                            payload = data.get("data") or []
                            if isinstance(payload, dict): payload = [payload]
                            ts_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                            for tr in payload:
                                sym = (tr.get("s") or "").upper()
                                if not sym: continue
                                try: qty = float(tr.get("v") or 0)
                                except: qty = 0.0
                                key = (sym, ts_min)
                                b = trade_buckets.setdefault(key, {"count": 0, "qty": 0.0})
                                b["count"] += 1; b["qty"] += qty
                                ws_last_msg_ts = datetime.now(timezone.utc).isoformat()

                        # ---- orderbook.1 ----
                        elif topic.startswith("orderbook.1."):
                            payload = data.get("data") or {}; sym = (data.get("topic") or "").split(".")[-1].upper()
                            if not sym: continue
                            bids = payload.get("b") or []; asks = payload.get("a") or []
                            try:    best_bid = float(bids[0][0]); bid_qty = float(bids[0][1])
                            except: best_bid, bid_qty = None, None
                            try:    best_ask = float(asks[0][0]); ask_qty = float(asks[0][1])
                            except: best_ask, ask_qty = None, None
                            if best_bid is None or best_ask is None: continue
                            ts_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                            key = (sym, ts_min)
                            ob_buckets[key] = {"bid": float(best_bid), "ask": float(best_ask),
                                               "bid_qty": float(bid_qty or 0.0), "ask_qty": float(ask_qty or 0.0)}
                            ws_last_msg_ts = datetime.now(timezone.utc).isoformat()

                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                        log.warning("WS closed or error"); break
        except Exception as e:
            log.warning("WS loop error: %s", e)
        finally:
            with contextlib.suppress(Exception): await ws_session.close()
            ws_connected = False
            log.info("WS consumer finished; reconnecting in %ss...", backoff)
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

# -------------------- Flushers --------------------
async def trades_flusher_loop():
    while True:
        try:
            await asyncio.sleep(60)
            if not trade_buckets: continue
            items = []
            for (sym, ts_min), data in list(trade_buckets.items()):
                items.append((ts_min, sym, int(data.get("count", 0)), float(data.get("qty", 0.0))))
                trade_buckets.pop((sym, ts_min), None)
            await insert_trades_batch(items)
            log.info("Flushed %d trade buckets", len(items))
        except Exception as e:
            log.exception("trades_flusher_loop error: %s", e)

async def ob_flusher_loop():
    while True:
        try:
            await asyncio.sleep(60)
            if not ob_buckets: continue
            items = []
            for (sym, ts_min), data in list(ob_buckets.items()):
                bid = float(data.get("bid", 0.0)); ask = float(data.get("ask", 0.0))
                bq  = float(data.get("bid_qty", 0.0)); aq  = float(data.get("ask_qty", 0.0))
                mid = (bid + ask) / 2.0 if (bid and ask) else 0.0
                spread_bps = ((ask - bid) / mid * 10000.0) if mid > 0 else 0.0
                depth_usd  = (bq + aq) * mid
                items.append((ts_min, sym, bid, ask, bq, aq, spread_bps, depth_usd))
                ob_buckets.pop((sym, ts_min), None)
            await insert_ob_batch(items)
            log.info("Flushed %d orderbook buckets", len(items))
        except Exception as e:
            log.exception("ob_flusher_loop error: %s", e)

# NEW: OI from WS flusher loop
async def oi_ws_flusher_loop():
    while True:
        try:
            await asyncio.sleep(60)
            if not oi_ws_buckets: continue
            to_write = []
            for (sym, ts_min), oi_val in list(oi_ws_buckets.items()):
                to_write.append((ts_min, sym, float(oi_val)))
                oi_ws_buckets.pop((sym, ts_min), None)
            # write
            if to_write:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        for ts_min, sym, oi_val in to_write:
                            await cur.execute(SQL_INSERT_OI, (ts_min, sym, oi_val))
                    await conn.commit()
                log.info("Flushed %d OI(WS) buckets", len(to_write))
        except Exception as e:
            log.exception("oi_ws_flusher_loop error: %s", e)

# -------------------- OI polling with fallback (REST, optional) --------------------
OI_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; InnertradeScreener/1.0)",
    "Connection": "close",
}

async def fetch_oi_from(base_url: str, session: aiohttp.ClientSession, symbol: str):
    url = f"{base_url}/v5/market/open-interest"
    params = {"category": "linear", "symbol": symbol, "interval": OI_INTERVAL, "limit": "1"}
    async with session.get(url, params=params, headers=OI_HEADERS) as resp:
        if resp.status != 200:
            return resp.status, None
        j = await resp.json(loads=json.loads)
        return 200, j

async def fetch_oi_one(symbol: str) -> tuple[datetime | None, float | None]:
    global oi_session
    if oi_session is None:
        oi_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))

    # base
    try:
        status, j = await fetch_oi_from(BYBIT_REST_BASE, oi_session, symbol)
    except Exception as e:
        log.warning("OI %s base error: %s", symbol, e)
        status, j = 599, None

    # fallback if needed
    if status in (401, 403, 429, 500, 502, 503, 504):
        try:
            status_fb, j_fb = await fetch_oi_from(BYBIT_REST_FALLBACK, oi_session, symbol)
            if status_fb == 200:
                status, j = status_fb, j_fb
                log.info("OI %s served by fallback domain", symbol)
            else:
                status = status_fb
        except Exception as e:
            log.warning("OI %s fallback error: %s", symbol, e)

    if status != 200 or not j:
        log.warning("OI %s http %s", symbol, status)
        return None, None

    try:
        data = ((j or {}).get("result") or {}).get("list") or []
        if not data:
            return None, None
        item = data[0]
        ts_ms = item.get("timestamp") or item.get("ts") or item.get("t")
        ts = None
        if ts_ms:
            try: ts = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            except Exception: ts = None
        ts_min = (ts or datetime.now(timezone.utc)).replace(second=0, microsecond=0)

        oi_usd = None
        val = item.get("openInterestValue")
        if val not in (None, ""):
            try: oi_usd = float(val)
            except Exception: oi_usd = None
        if oi_usd is None:
            raw_oi = item.get("openInterest")
            if raw_oi not in (None, ""):
                try:
                    oi = float(raw_oi); last = await get_last_price(symbol) or 0.0
                    if oi > 0 and last > 0: oi_usd = oi * last
                except Exception: oi_usd = None
        return ts_min, oi_usd
    except Exception as e:
        log.warning("OI %s parse error: %s", symbol, e)
        return None, None

async def oi_poll_loop():
    if not ENABLE_OI_POLL:
        log.info("OI polling disabled by ENV");  return
    log.info("OI polling enabled: every %ss, interval=%s", OI_POLL_SECONDS, OI_INTERVAL)
    while True:
        try:
            start = datetime.now(timezone.utc)
            for sym in [s for s in SYMBOLS if s]:
                ts_min, oi_usd = await fetch_oi_one(sym)
                if ts_min and oi_usd is not None:
                    await insert_oi(ts_min, sym, float(oi_usd))
            took = (datetime.now(timezone.utc) - start).total_seconds()
            log.info("OI poll cycle done in %.2fs", took)
        except Exception as e:
            log.exception("oi_poll_loop error: %s", e)
        await asyncio.sleep(max(1, OI_POLL_SECONDS))

# -------------------- Bot --------------------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
        ],
        resize_keyboard=True
    )

async def cmd_start(message: Message):
    await message.answer(
        "🧭 Market mood\n"
        f"Добро пожаловать в Innertrade Screener {VERSION} (tickers + trades + orderbook + OI via WS/REST).\n",
        reply_markup=kb_main()
    )

async def cmd_status(message: Message):
    n = await count_rows()
    ws_ts = ws_last_msg_ts or "—"
    await message.answer(
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Source: Bybit (public WS + REST OI)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: {ws_connected}\n"
        f"WS last msg: {ws_ts}\n"
        f"DB rows (ws_ticker): {n}"
    )

async def cmd_diag(message: Message):
    n = await count_rows()
    sample = list(ws_cache.items())[:3]
    sample_txt = "\n".join(
        f"{k}: p24={v['p24']:.2f} last={v['last']} tov~{int(v['tov']):,}".replace(",", " ")
        for k, v in sample
    ) or "—"
    await message.answer("diag\n" f"rows={n}\n" f"cache_sample:\n{sample_txt}")

def fmt_activity(rows: list[tuple]) -> str:
    if not rows: return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | turnover24h ~ {int(tov):,}".replace(",", " "))
    return "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_volatility(rows: list[tuple]) -> str:
    if not rows: return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | last {last}")
    return "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_trend(rows: list[tuple]) -> str:
    if not rows: return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  ≈  24h% {p24:.2f}  | last {last}")
    return "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines[:10])

async def cmd_activity(message: Message):
    rows = await select_sorted("turnover", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
                      key=lambda z: z[2], reverse=True)[:10]
    await message.answer(fmt_activity(rows))

async def cmd_volatility(message: Message):
    rows = await select_sorted("p24", 50, desc=True)
    rows = sorted(rows, key=lambda r: abs(r[1]), reverse=True)[:10]
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
                      key=lambda z: abs(z[1]), reverse=True)[:10]
    await message.answer(fmt_volatility(rows))

async def cmd_trend(message: Message):
    rows = await select_sorted("p24", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
                      key=lambda z: z[1], reverse=True)[:10]
    await message.answer(fmt_trend(rows))

async def cmd_now(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /now SYMBOL (пример: /now BTCUSDT)"); return
    sym = parts[1].upper()
    row = await get_one(sym)
    if not row:
        await message.answer(f"{sym}: нет данных в БД."); return
    symbol, last, p24, tov, updated = row
    await message.answer(
        f"{symbol}\nlast: {last}\n24h%: {p24}\nturnover24h: {tov}\nupdated_at: {updated}"
    )

async def cmd_diag_trades(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_trades SYMBOL [N]\nНапр.: /diag_trades BTCUSDT 5"); return
    sym = parts[1].upper()
    try: limit = int(parts[2]) if len(parts) >= 3 else 5
    except: limit = 5
    rows = await trades_latest(sym, limit)
    if not rows:
        await message.answer(f"{sym}: нет строк в trades_1m (пока нет сделок/ждите минуту)."); return
    lines = [f"{sym} — последние {len(rows)} мин:"]
    for ts, cnt, qty in rows:
        lines.append(f"{ts:%H:%M}  trades={cnt}  qty={qty}")
    await message.answer("\n".join(lines))

async def cmd_diag_ob(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_ob SYMBOL [N]\nНапр.: /diag_ob BTCUSDT 5"); return
    sym = parts[1].upper()
    try: limit = int(parts[2]) if len(parts) >= 3 else 5
    except: limit = 5
    rows = await ob_latest(sym, limit)
    if not rows:
        await message.answer(f"{sym}: нет строк в ob_1m (ожидайте минуту)."); return
    lines = [f"{sym} — последние {len(rows)} мин (best level):"]
    for ts, bid, ask, bq, aq, sp_bps, depth in rows:
        lines.append(f"{ts:%H:%M}  bid={bid}({bq})  ask={ask}({aq})  spread={sp_bps:.2f}bps  depth≈${depth:,.0f}".replace(",", " "))
    await message.answer("\n".join(lines))

async def cmd_diag_oi(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_oi SYMBOL [N]\nНапр.: /diag_oi BTCUSDT 5"); return
    sym = parts[1].upper()
    try: limit = int(parts[2]) if len(parts) >= 3 else 5
    except: limit = 5
    rows = await oi_latest(sym, limit)
    if not rows:
        await message.answer(f"{sym}: нет строк в oi_1m (ожидайте цикл)."); return
    lines = [f"{sym} — последние {len(rows)} мин (OI, $):"]
    for ts, oi_usd in rows:
        lines.append(f"{ts:%H:%M}  oi_usd≈${oi_usd:,.0f}".replace(",", " "))
    await message.answer("\n".join(lines))

async def on_text(message: Message):
    t = (message.text or "").strip()
    if t == "📊 Активность":   await cmd_activity(message)
    elif t == "⚡ Волатильность": await cmd_volatility(message)
    elif t == "📈 Тренд":      await cmd_trend(message)
    elif t == "🫧 Bubbles":    await message.answer("WS Bubbles (24h %, size~turnover24h)")
    else:
        await message.answer(
            "Команды: /start /status /activity /volatility /trend /diag /now SYMBOL "
            "/diag_trades SYMBOL [N] /diag_ob SYMBOL [N] /diag_oi SYMBOL [N]"
        )

# -------------------- HTTP --------------------
async def handle_health(request: web.Request): return web.Response(text="ok")
async def handle_root(request: web.Request):   return web.Response(text="Innertrade screener is alive")
async def handle_status_http(request: web.Request):
    n = await count_rows(); ws_ts = ws_last_msg_ts or "—"
    body = ("Status\n"
            f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
            "Source: Bybit (public WS + REST OI)\n"
            f"Version: {VERSION}\n"
            f"Bybit WS: {BYBIT_WS_URL}\n"
            f"WS connected: {ws_connected}\n"
            f"WS last msg: {ws_ts}\n"
            f"DB rows: {n}\n")
    return web.Response(text=body)

async def handle_diag_http(request: web.Request):
    n = await count_rows(); sample = list(ws_cache.items())[:3]
    sample_txt = "\n".join(f"{k}: p24={v['p24']:.2f} last={v['last']} tov~{int(v['tov']):,}".replace(",", " ")
                           for k, v in sample) or "—"
    return web.Response(text=f"diag\nrows={n}\ncache_sample:\n{sample_txt}\n")

async def handle_webhook(request: web.Request):
    assert bot and dp
    try: data = await request.json()
    except Exception: return web.Response(status=400, text="bad json")
    from aiogram.types import Update
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return web.Response(text="ok")

def build_app() -> web.Application:
    global bot, dp, app
    tg_session = AiohttpSession()
    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"), session=tg_session)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_activity, Command("activity"))
    dp.message.register(cmd_volatility, Command("volatility"))
    dp.message.register(cmd_trend, Command("trend"))
    dp.message.register(cmd_diag, Command("diag"))
    dp.message.register(cmd_now, Command("now"))
    dp.message.register(cmd_diag_trades, Command("diag_trades"))
    dp.message.register(cmd_diag_ob, Command("diag_ob"))
    dp.message.register(cmd_diag_oi, Command("diag_oi"))
    dp.message.register(on_text, F.text)

    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/status", handle_status_http)
    app.router.add_get("/diag", handle_diag_http)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app

async def on_startup():
    global ws_task, watchdog_task, bot, oi_task
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
    asyncio.create_task(trades_flusher_loop())
    asyncio.create_task(ob_flusher_loop())
    asyncio.create_task(oi_ws_flusher_loop())  # NEW: OI from WS
    if ENABLE_OI_POLL:
        oi_task = asyncio.create_task(oi_poll_loop())

    assert bot
    url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
    await ensure_webhook(bot, url)
    watchdog_task = asyncio.create_task(webhook_watchdog(bot, url))

async def on_shutdown():
    global ws_task, ws_session, db_pool, bot, watchdog_task, oi_task, oi_session
    if bot:
        with contextlib.suppress(Exception): await bot.session.close()
    for t in (ws_task, watchdog_task, oi_task):
        if t:
            t.cancel()
            with contextlib.suppress(Exception): await t
    if ws_session:
        with contextlib.suppress(Exception): await ws_session.close()
    if oi_session:
        with contextlib.suppress(Exception): await oi_session.close()
    if db_pool:
        with contextlib.suppress(Exception): await db_pool.close()

def run():
    application = build_app()
    application.on_startup.append(lambda app: on_startup())
    application.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(application, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        pass
