# main.py — v1.8.7-shutdown-helper
# Fix: безопасные helper'ы для закрытия сессий/тасков вместо "if ...: with ..."
import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
import contextlib
from typing import Optional, Any, Dict

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, Message
from aiogram.filters import Command

from psycopg_pool import AsyncConnectionPool

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("screener")

# -------------------- ENV --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
SYMBOLS = os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
).split(",")

BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bytick.com").strip()
BYBIT_REST_FALLBACK = os.getenv("BYBIT_REST_FALLBACK", "https://api.bybit.com").strip()

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "1").strip().lower() in ("1", "true", "yes")
OI_POLL_SECS = int(os.getenv("OI_POLL_SECS", "90"))
OI_INTERVAL_MIN = int(os.getenv("OI_INTERVAL_MIN", "5"))

ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "1").strip().lower() in ("1", "true", "yes")
PRICE_POLL_SECS = int(os.getenv("PRICE_POLL_SECS", "1800"))
PRICE_LOOKBACK_H = int(os.getenv("PRICE_LOOKBACK_H", "192"))

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE or not WEBHOOK_BASE.startswith("http"):
    raise RuntimeError("WEBHOOK_BASE is required (https://...)")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
VERSION = "v1.8.7-shutdown-helper"

# -------------------- Globals --------------------
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None

db_pool: Optional[AsyncConnectionPool] = None

ws_session: Optional[aiohttp.ClientSession] = None
ws_task: Optional[asyncio.Task] = None

rest_session: Optional[aiohttp.ClientSession] = None
oi_task: Optional[asyncio.Task] = None
price_task: Optional[asyncio.Task] = None

ws_cache: dict[str, dict] = {}
ws_last_msg_iso: Optional[str] = None

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
SQL_CREATE_EXTRA = """
CREATE TABLE IF NOT EXISTS trades_1m (
  symbol  text NOT NULL,
  ts      timestamptz NOT NULL,
  count   integer NOT NULL,
  qty_sum double precision NOT NULL,
  PRIMARY KEY(symbol, ts)
);
CREATE TABLE IF NOT EXISTS ob_1m (
  symbol  text NOT NULL,
  ts      timestamptz NOT NULL,
  bid     double precision,
  ask     double precision,
  bq      double precision,
  aq      double precision,
  spread_bps double precision,
  depth_usd double precision,
  PRIMARY KEY(symbol, ts)
);
CREATE TABLE IF NOT EXISTS oi_1m (
  symbol  text NOT NULL,
  ts      timestamptz NOT NULL,
  oi_usd  double precision,
  PRIMARY KEY(symbol, ts)
);
CREATE TABLE IF NOT EXISTS price_1h (
  symbol  text NOT NULL,
  ts      timestamptz NOT NULL,
  open    double precision,
  high    double precision,
  low     double precision,
  close   double precision,
  PRIMARY KEY(symbol, ts)
);
"""

# psycopg (pyformat)
SQL_UPSERT = """
INSERT INTO ws_ticker (symbol, last, price24h_pcnt, turnover24h, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (symbol) DO UPDATE
SET last = EXCLUDED.last,
    price24h_pcnt = EXCLUDED.price24h_pcnt,
    turnover24h = EXCLUDED.turnover24h,
    updated_at = NOW();
"""
SQL_SELECT_SORTED_TMPL = """
SELECT symbol,
       COALESCE(price24h_pcnt, 0) AS p24,
       COALESCE(turnover24h, 0)   AS tov,
       COALESCE(last, 0)          AS last
FROM ws_ticker
ORDER BY {order_by} {direction}
LIMIT %s;
"""
SQL_COUNT = "SELECT COUNT(*) FROM ws_ticker;"

SQL_INSERT_TRADES_1M = """
INSERT INTO trades_1m (symbol, ts, count, qty_sum)
VALUES (%s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET count = EXCLUDED.count, qty_sum = EXCLUDED.qty_sum;
"""
SQL_INSERT_OB_1M = """
INSERT INTO ob_1m (symbol, ts, bid, ask, bq, aq, spread_bps, depth_usd)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET bid = EXCLUDED.bid, ask = EXCLUDED.ask,
    bq = EXCLUDED.bq, aq = EXCLUDED.aq,
    spread_bps = EXCLUDED.spread_bps,
    depth_usd = EXCLUDED.depth_usd;
"""
SQL_INSERT_OI_1M = """
INSERT INTO oi_1m (symbol, ts, oi_usd)
VALUES (%s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET oi_usd = EXCLUDED.oi_usd;
"""
SQL_INSERT_PRICE_1H = """
INSERT INTO price_1h (symbol, ts, open, high, low, close)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, ts) DO UPDATE
SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close;
"""

# -------------------- Helpers (safe close/cancel) --------------------
async def aclose_safely(obj: Any, method: str = "close"):
    """Безопасно закрыть aiohttp session или похожий объект."""
    if obj is None:
        return
    try:
        m = getattr(obj, method, None)
        if m:
            res = m()
            if asyncio.iscoroutine(res):
                await res
    except Exception:
        pass

async def cancel_task_safely(task: Optional[asyncio.Task]):
    if task:
        task.cancel()
        with contextlib.suppress(Exception):
            await task

# -------------------- DB helpers --------------------
async def init_db():
    global db_pool
    db_pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=4)
    await db_pool.open()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_CREATE)
            await cur.execute(SQL_CREATE_EXTRA)
            await conn.commit()
    log.info("DB ready")

async def upsert_ticker(symbol: str, last: float | None, p24: float | None, turnover: float | None):
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT, (symbol, last, p24, turnover))
            await conn.commit()

async def select_sorted(order_by: str, limit: int = 10, desc: bool = True):
    if order_by == "turnover":
        ob = "tov"
    elif order_by in ("p24", "price24h_pcnt"):
        ob = "p24"
    else:
        ob = "last"
    direction = "DESC" if desc else "ASC"
    sql = SQL_SELECT_SORTED_TMPL.format(order_by=ob, direction=direction)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            return await cur.fetchall()

async def count_rows() -> int:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_COUNT)
            n = (await cur.fetchone())[0]
            return int(n or 0)

# -------------------- REST helpers --------------------
async def ensure_rest_session():
    global rest_session
    if rest_session is None or rest_session.closed:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bybit.com/",
            "Connection": "keep-alive",
        }
        rest_session = aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=10))

async def rest_get_json(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    await ensure_rest_session()
    assert rest_session
    for base, tag in ((BYBIT_REST_BASE, "base"), (BYBIT_REST_FALLBACK, "fallback")):
        url = base.rstrip("/") + path
        try:
            async with rest_session.get(url, params=params) as resp:
                status = resp.status
                txt = await resp.text()
                if 200 <= status < 300:
                    try:
                        return json.loads(txt)
                    except Exception:
                        log.warning("REST %s json parse error [%s] %s", tag, status, url)
                        return None
                else:
                    log.warning("REST %s %s -> HTTP %s", tag, url, status)
        except Exception as e:
            log.warning("REST %s %s failed: %s", tag, url, repr(e))
    return None

# -------------------- WS ingest --------------------
async def ws_consumer():
    global ws_session, ws_last_msg_iso
    ws_session = aiohttp.ClientSession()
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    try:
        async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
            args = [f"tickers.{s}" for s in SYMBOLS]
            await ws.send_json({"op": "subscribe", "args": args})
            log.info(f"WS subscribed: {len(args)} topics")
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json(loads=json.loads)
                    ws_last_msg_iso = datetime.now(timezone.utc).isoformat()
                    if (data.get("topic") or "").startswith("tickers."):
                        payload = data.get("data")
                        items = [payload] if isinstance(payload, dict) else (payload or [])
                        for it in items:
                            symbol = (it.get("symbol") or "").upper()
                            try:
                                last = float(it.get("lastPrice", "0") or 0)
                            except Exception:
                                last = 0.0
                            try:
                                p24 = float(it.get("price24hPcnt", "0") or 0) * 100.0
                            except Exception:
                                p24 = 0.0
                            try:
                                turnover = float(it.get("turnover24h", "0") or 0)
                            except Exception:
                                turnover = 0.0
                            ws_cache[symbol] = {"last": last, "p24": p24, "tov": turnover, "ts": ws_last_msg_iso}
                            await upsert_ticker(symbol, last, p24, turnover)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error("WS error: %s", msg.data)
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                    log.warning("WS closed by server")
                    break
    except Exception as e:
        log.exception("WS consumer failed: %s", e)
    finally:
        await aclose_safely(ws_session)

# -------------------- Pollers --------------------
async def poll_oi_loop():
    if not ENABLE_OI_POLL:
        return
    log.info("OI polling enabled: every %ss, interval=%smin", OI_POLL_SECS, OI_INTERVAL_MIN)
    while True:
        t0 = asyncio.get_event_loop().time()
        try:
            for s in SYMBOLS:
                data = await rest_get_json("/v5/market/open-interest",
                                           {"category": "linear", "symbol": s, "interval": str(OI_INTERVAL_MIN)})
                if not data:
                    continue
                try:
                    if data.get("retCode") == 0:
                        lst = (data.get("result") or {}).get("list") or []
                        if lst:
                            it = lst[-1]
                            oi = float(it.get("openInterest", "0") or 0)
                            ts = datetime.fromtimestamp(int(it.get("timestamp", 0)) / 1000, tz=timezone.utc)
                            async with db_pool.connection() as conn:
                                async with conn.cursor() as cur:
                                    await cur.execute(SQL_INSERT_OI_1M, (s, ts, oi))
                                    await conn.commit()
                    else:
                        log.warning("OI REST retCode!=0 for %s: %s", s, data.get("retMsg"))
                except Exception as e:
                    log.warning("OI parse/save %s failed: %s", s, repr(e))
        except Exception as e:
            log.warning("OI poll cycle error: %s", repr(e))
        finally:
            dt = asyncio.get_event_loop().time() - t0
            log.info("OI poll cycle done in %.2fs", dt)
            await asyncio.sleep(OI_POLL_SECS)

async def poll_price_loop():
    if not ENABLE_PRICE_POLL:
        return
    log.info("Price polling enabled: every %ss (1h candles ~%sh back)", PRICE_POLL_SECS, PRICE_LOOKBACK_H)
    while True:
        try:
            for s in SYMBOLS:
                data = await rest_get_json("/v5/market/kline",
                                           {"category": "linear", "symbol": s, "interval": "60",
                                            "limit": str(PRICE_LOOKBACK_H)})
                if not data or data.get("retCode") != 0:
                    log.warning("Kline %s fetch failed (ret=%s)", s, data and data.get("retCode"))
                    continue
                kl = (data.get("result") or {}).get("list") or []
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        for row in kl:
                            ts = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc)
                            o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                            await cur.execute(SQL_INSERT_PRICE_1H, (s, ts, o, h, l, c))
                        await conn.commit()
        except Exception as e:
            log.warning("Price poll cycle error: %s", repr(e))
        finally:
            log.info("Price poll cycle done")
            await asyncio.sleep(PRICE_POLL_SECS)

# -------------------- Bot --------------------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="🔍 Активность+"), KeyboardButton(text="📄 Паспорт")],
        ],
        resize_keyboard=True,
    )

def fmt_activity(rows):
    if not rows:
        return "Нет данных."
    lines = []
    for i, (sym, p24, tov, last) in enumerate(rows, 1):
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | turnover24h ~ {int(tov):,}".replace(",", " "))
    return "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_volatility(rows):
    if not rows:
        return "Нет данных."
    lines = []
    for i, (sym, p24, tov, last) in enumerate(rows, 1):
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | last {last}")
    return "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_trend(rows):
    if not rows:
        return "Нет данных."
    lines = []
    for i, (sym, p24, tov, last) in enumerate(rows, 1):
        lines.append(f"{i}) {sym}  ≈  24h% {p24:.2f}  | last {last}")
    return "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines[:10])

async def cmd_start(message: Message):
    await message.answer(
        "🧭 Market mood\n"
        f"Добро пожаловать в Innertrade Screener {VERSION} (WS tickers/trades/orderbook + OI via WS/REST + optional prices).",
        reply_markup=kb_main(),
    )

async def cmd_status(message: Message):
    n = await count_rows()
    await message.answer(
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Source: Bybit (public WS + REST OI/Prices)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"REST base: {BYBIT_REST_BASE}\n"
        f"REST fallback: {BYBIT_REST_FALLBACK}\n"
        f"WS connected: True\n"
        f"WS last msg: {ws_last_msg_iso or '—'}\n"
        f"DB rows (ws_ticker): {n}\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_INTERVAL_MIN}min, every {OI_POLL_SECS}s)\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} (every {PRICE_POLL_SECS}s, ~{PRICE_LOOKBACK_H}h back)\n"
    )

async def cmd_activity(message: Message):
    rows = await select_sorted("turnover", 10, True)
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()], key=lambda z: z[2], reverse=True)[:10]
    await message.answer(fmt_activity(rows))

async def cmd_volatility(message: Message):
    rows = await select_sorted("p24", 50, True)
    rows = sorted(rows, key=lambda r: abs(r[1]), reverse=True)[:10]
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()], key=lambda z: abs(z[1]), reverse=True)[:10]
    await message.answer(fmt_volatility(rows))

async def cmd_trend(message: Message):
    rows = await select_sorted("p24", 10, True)
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()], key=lambda z: z[1], reverse=True)[:10]
    await message.answer(fmt_trend(rows))

async def cmd_activity2(message: Message):
    rows = await select_sorted("turnover", 10, True)
    if not rows and ws_cache:
        rows = sorted([(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()], key=lambda z: z[2], reverse=True)[:10]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    since = now - timedelta(hours=24)
    lines = ["🔍 Активность+ (композит за ~24ч)", ""]
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for i, (sym, p24, tov, last) in enumerate(rows, 1):
                await cur.execute(
                    "SELECT COALESCE(SUM(count),0), COALESCE(SUM(qty_sum),0) FROM trades_1m WHERE symbol=%s AND ts>%s",
                    (sym, since),
                )
                cnt, _qty = await cur.fetchone()
                await cur.execute(
                    "SELECT bid,ask,bq,aq,spread_bps,depth_usd FROM ob_1m WHERE symbol=%s ORDER BY ts DESC LIMIT %s",
                    (sym, 1),
                )
                ob = await cur.fetchone()
                if ob:
                    bid, ask, bq, aq, spread_bps, depth_usd = ob
                else:
                    bid = ask = bq = aq = spread_bps = depth_usd = None
                await cur.execute(
                    "SELECT oi_usd FROM oi_1m WHERE symbol=%s AND ts>%s ORDER BY ts ASC LIMIT %s", (sym, since, 1)
                )
                row0 = await cur.fetchone()
                await cur.execute("SELECT oi_usd FROM oi_1m WHERE symbol=%s ORDER BY ts DESC LIMIT %s", (sym, 1))
                row1 = await cur.fetchone()
                oi_delta_pct = None
                if row0 and row1 and row0[0] and row1[0] and row0[0] > 0:
                    oi_delta_pct = (row1[0] - row0[0]) / row0[0] * 100.0
                score = 0.0
                if tov and tov > 0:
                    score += min(1.0, (tov / 1e9))
                if cnt and cnt > 0:
                    score += min(1.0, (cnt / 4e5))
                if depth_usd and depth_usd > 0:
                    score += min(0.5, depth_usd / 1e6 * 0.5)
                if spread_bps is not None:
                    score += 0.2 if spread_bps <= 0.02 else (0.1 if spread_bps <= 0.05 else 0.0)
                if oi_delta_pct is not None:
                    score += max(-0.5, min(0.5, oi_delta_pct / 20.0))
                line = f"{i}) {sym}  score {score:+.2f}  | turnover ~ {int(tov):,} | trades ~ {int(cnt):,}".replace(",", " ")
                if depth_usd is not None and spread_bps is not None:
                    line += f" | depth≈${int(depth_usd):,}".replace(",", " ")
                    line += f" | spread≈{(spread_bps or 0):.1f}bps"
                if oi_delta_pct is not None:
                    line += f" | OIΔ {oi_delta_pct:+.1f}%"
                lines.append(line)
    await message.answer("\n".join(lines[:12]))

async def cmd_passport(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /passport SYMBOL\nНапр.: /passport BTCUSDT")
        return
    sym = parts[1].upper()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT last, price24h_pcnt, turnover24h, updated_at FROM ws_ticker WHERE symbol=%s", (sym,))
            w = await cur.fetchone()
            await cur.execute("SELECT ts, close FROM price_1h WHERE symbol=%s ORDER BY ts DESC LIMIT %s", (sym, 168))
            rows = await cur.fetchall()
    last, p24, tov, upd = (w or (None, None, None, None))
    d24 = d7 = None
    if rows:
        c_now = rows[0][1]
        if len(rows) >= 25 and rows[24][1]:
            d24 = (c_now - rows[24][1]) / rows[24][1] * 100.0
        if len(rows) >= 24 * 7 + 1 and rows[24 * 7][1]:
            d7 = (c_now - rows[24 * 7][1]) / rows[24 * 7][1] * 100.0
    lines = [f"{sym}"]
    if last is not None:
        lines.append(f"last: {last}")
    if p24 is not None:
        lines.append(f"24h% (WS): {p24:.4f}")
    if tov is not None:
        lines.append(f"turnover24h: {tov:.2f}")
    if d24 is not None:
        lines.append(f"Δ24h (1h kline): {d24:+.2f}%")
    if d7 is not None:
        lines.append(f"Δ7d (1h kline): {d7:+.2f}%")
    if upd is not None:
        lines.append(f"updated_at: {upd}")
    await message.answer("\n".join(lines))

async def cmd_now(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT last, price24h_pcnt, turnover24h, updated_at FROM ws_ticker WHERE symbol=%s", (sym,))
            w = await cur.fetchone()
    if not w:
        await message.answer(f"{sym}: нет данных в ws_ticker.")
        return
    last, p24, tov, upd = w
    await message.answer(f"{sym}\nlast: {last}\n24h%: {p24}\nturnover24h: {tov}\nupdated_at: {upd}")

async def cmd_diag_trades(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_trades SYMBOL [N]\nНапр.: /diag_trades BTCUSDT 10")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 10
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT ts, count, qty_sum FROM trades_1m WHERE symbol=%s ORDER BY ts DESC LIMIT %s", (sym, n))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"trades_1m {sym}: нет строк")
        return
    lines = [f"trades_1m {sym} (latest {n})"]
    for ts, c, q in rows:
        lines.append(f"{ts.isoformat()}  count={c}  qty_sum={q}")
    await message.answer("\n".join(lines))

async def cmd_diag_ob(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_ob SYMBOL [N]\nНапр.: /diag_ob BTCUSDT 5")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 5
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT ts,bid,ask,bq,aq,spread_bps,depth_usd FROM ob_1m WHERE symbol=%s ORDER BY ts DESC LIMIT %s",
                (sym, n),
            )
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"ob_1m {sym}: нет строк")
        return
    lines = [f"ob_1m {sym} (latest {n})"]
    for ts, bid, ask, bq, aq, sp, dep in rows:
        lines.append(
            f"{ts.isoformat()}  bid={bid} ask={ask}  bq={bq} aq={aq}  spread={sp:.2f}bps  depth≈{int(dep):,}".replace(
                ",", " "
            )
        )
    await message.answer("\n".join(lines))

async def cmd_diag_oi(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_oi SYMBOL [N]\nНапр.: /diag_oi BTCUSDT 10")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 10
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT ts, oi_usd FROM oi_1m WHERE symbol=%s ORDER BY ts DESC LIMIT %s", (sym, n))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"oi_1m {sym}: нет строк")
        return
    lines = [f"oi_1m {sym} (latest {n})"]
    for ts, oi in rows:
        lines.append(f"{ts.isoformat()}  oi≈${int(oi):,}".replace(",", " "))
    await message.answer("\n".join(lines))

async def on_text(message: Message):
    t = (message.text or "").strip()
    if t == "📊 Активность":
        await cmd_activity(message)
    elif t == "⚡ Волатильность":
        await cmd_volatility(message)
    elif t == "📈 Тренд":
        await cmd_trend(message)
    elif t == "🫧 Bubbles":
        await message.answer("WS Bubbles (24h %, size~turnover24h)")
    elif t == "🔍 Активность+":
        await cmd_activity2(message)
    elif t == "📄 Паспорт":
        await message.answer("Usage: /passport SYMBOL\nНапр.: /passport BTCUSDT")
    else:
        await message.answer(
            "Принял: {t}\n(команды: /start /status /activity /activity2 /volatility /trend /diag_trades /diag_ob /diag_oi /passport /now)".format(
                t=t
            )
        )

# -------------------- Webhook server --------------------
async def handle_health(request: web.Request):
    return web.Response(text="ok")

async def handle_root(request: web.Request):
    return web.Response(text="Innertrade screener is alive")

async def handle_webhook(request: web.Request):
    assert bot and dp
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="bad json")
    from aiogram.types import Update
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return web.Response(text="ok")

def build_app() -> web.Application:
    global bot, dp
    tg_session = AiohttpSession()
    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"), session=tg_session)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_activity, Command("activity"))
    dp.message.register(cmd_activity2, Command("activity2"))
    dp.message.register(cmd_volatility, Command("volatility"))
    dp.message.register(cmd_trend, Command("trend"))
    dp.message.register(cmd_diag_trades, Command("diag_trades"))
    dp.message.register(cmd_diag_ob, Command("diag_ob"))
    dp.message.register(cmd_diag_oi, Command("diag_oi"))
    dp.message.register(cmd_passport, Command("passport"))
    dp.message.register(cmd_now, Command("now"))
    dp.message.register(on_text, F.text)

    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app

async def on_startup():
    global ws_task, oi_task, price_task
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
    if ENABLE_OI_POLL:
        oi_task = asyncio.create_task(poll_oi_loop())
    if ENABLE_PRICE_POLL:
        price_task = asyncio.create_task(poll_price_loop())
    assert bot
    url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
    try:
        await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
        log.info("Webhook set to %s", url)
    except Exception as e:
        log.warning("set_webhook attempt 1 failed: %s", repr(e))
        await asyncio.sleep(1.0)
        await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
        log.info("Webhook set to %s", url)

async def on_shutdown():
    global bot, ws_task, oi_task, price_task, ws_session, rest_session, db_pool
    # Телеграм
    if bot:
        with contextlib.suppress(Exception):
            await bot.delete_webhook(drop_pending_updates=False)
        await aclose_safely(getattr(bot, "session", None), "close")
    # Таски
    await cancel_task_safely(ws_task)
    await cancel_task_safely(oi_task)
    await cancel_task_safely(price_task)
    # Сессии
    await aclose_safely(ws_session)
    await aclose_safely(rest_session)
    # БД
    if db_pool:
        with contextlib.suppress(Exception):
            await db_pool.close()

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
