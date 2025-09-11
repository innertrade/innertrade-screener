import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import (
    KeyboardButton,
    ReplyKeyboardMarkup,
    Message,
)
from aiogram.filters import Command

import psycopg
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
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT").split(",")

if not TELEGRAM_TOKEN or not WEBHOOK_BASE or not WEBHOOK_SECRET or not DATABASE_URL:
    raise RuntimeError("Missing required ENV vars")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"

# -------------------- Globals --------------------
bot: Bot | None = None
dp: Dispatcher | None = None
app: web.Application | None = None

db_pool: AsyncConnectionPool | None = None
ws_session: aiohttp.ClientSession | None = None
ws_task: asyncio.Task | None = None

ws_cache: dict[str, dict] = {}

# trades bucket (in-memory)
trade_buckets: dict[tuple[str, datetime], dict] = {}

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
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (symbol) DO UPDATE
SET last = EXCLUDED.last,
    price24h_pcnt = EXCLUDED.price24h_pcnt,
    turnover24h = EXCLUDED.turnover24h,
    updated_at = NOW();
"""

SQL_INSERT_TRADES = """
INSERT INTO trades_1m (ts, symbol, trades_count, qty_sum)
VALUES ($1, $2, $3, $4)
ON CONFLICT (symbol, ts) DO UPDATE
SET trades_count = trades_1m.trades_count + EXCLUDED.trades_count,
    qty_sum = COALESCE(trades_1m.qty_sum,0) + COALESCE(EXCLUDED.qty_sum,0);
"""

# -------------------- DB helpers --------------------
async def init_db():
    global db_pool
    db_pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=4)
    await db_pool.open()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_CREATE)
            await conn.commit()
    log.info("DB ready")

async def upsert_ticker(symbol: str, last: float | None, p24: float | None, turnover: float | None):
    if db_pool is None:
        return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT, (symbol, last, p24, turnover))
            await conn.commit()

async def flush_trades():
    """Флашим все buckets в trades_1m"""
    if not trade_buckets:
        return
    now = datetime.now(timezone.utc)
    to_flush = list(trade_buckets.items())
    trade_buckets.clear()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for (sym, ts_min), data in to_flush:
                await cur.execute(SQL_INSERT_TRADES, (ts_min, sym, data["count"], data["qty"]))
        await conn.commit()
    log.info(f"Flushed {len(to_flush)} trade buckets at {now}")

# -------------------- Bybit WS ingest --------------------
async def ws_consumer():
    global ws_session
    ws_session = aiohttp.ClientSession()
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    try:
        async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
            args = [f"tickers.{s}" for s in SYMBOLS] + [f"publicTrade.{s}" for s in SYMBOLS]
            await ws.send_json({"op": "subscribe", "args": args})
            log.info(f"WS subscribed: {len(args)} topics")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json(loads=json.loads)
                    topic = data.get("topic") or ""
                    if topic.startswith("tickers."):
                        payload = data.get("data")
                        items = [payload] if isinstance(payload, dict) else (payload or [])
                        for it in items:
                            symbol = (it.get("symbol") or "").upper()
                            last = float(it.get("lastPrice") or 0)
                            p24 = float(it.get("price24hPcnt") or 0) * 100
                            turnover = float(it.get("turnover24h") or 0)
                            ws_cache[symbol] = {"last": last, "p24": p24, "tov": turnover}
                            await upsert_ticker(symbol, last, p24, turnover)

                    elif topic.startswith("publicTrade."):
                        payload = data.get("data") or []
                        if isinstance(payload, dict):
                            payload = [payload]
                        for trade in payload:
                            symbol = (trade.get("s") or "").upper()
                            qty = float(trade.get("v") or 0)
                            ts_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                            key = (symbol, ts_min)
                            bucket = trade_buckets.setdefault(key, {"count": 0, "qty": 0.0})
                            bucket["count"] += 1
                            bucket["qty"] += qty

                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                    log.warning("WS closed or error")
                    break
    except Exception as e:
        log.exception("WS consumer failed: %s", e)
    finally:
        if ws_session:
            await ws_session.close()
        log.info("WS consumer finished")

async def trades_flusher_loop():
    while True:
        await asyncio.sleep(60)
        try:
            await flush_trades()
        except Exception:
            log.exception("flush_trades failed")

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
        "Добро пожаловать в Innertrade Screener v1.3.0-activity (trades ingest).",
        reply_markup=kb_main()
    )

# ... (остальные команды /status, /diag, /activity, /volatility, /trend, /now остаются как были) ...

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
    global bot, dp, app
    tg_session = AiohttpSession()
    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"), session=tg_session)
    dp = Dispatcher()
    dp.message.register(cmd_start, Command("start"))
    # остальные команды регистрируем как раньше...
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app

async def on_startup():
    global ws_task
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
    asyncio.create_task(trades_flusher_loop())
    assert bot
    url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
    await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
    log.info("Webhook set to %s", url)

async def on_shutdown():
    global ws_task, ws_session, db_pool, bot
    if bot:
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass
        await bot.session.close()
    if ws_task:
        ws_task.cancel()
        with contextlib.suppress(Exception):
            await ws_task
    if ws_session:
        await ws_session.close()
    if db_pool:
        await db_pool.close()

def run():
    application = build_app()
    application.on_startup.append(lambda app: on_startup())
    application.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(application, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    import contextlib
    try:
        run()
    except KeyboardInterrupt:
        pass
