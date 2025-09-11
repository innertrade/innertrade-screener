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
SYMBOLS = os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
).split(",")

if not TELEGRAM_TOKEN or not WEBHOOK_BASE or not WEBHOOK_SECRET or not DATABASE_URL:
    raise RuntimeError("Missing required ENV vars")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
VERSION = "v1.3.2-trades-diag"

# -------------------- Globals --------------------
bot: Bot | None = None
dp: Dispatcher | None = None
app: web.Application | None = None

db_pool: AsyncConnectionPool | None = None
ws_session: aiohttp.ClientSession | None = None
ws_task: asyncio.Task | None = None
watchdog_task: asyncio.Task | None = None

# WS state + cache
ws_cache: dict[str, dict] = {}
ws_connected: bool = False
ws_last_msg_ts: str | None = None

# trades bucket (in-memory, per 1m)
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
    if db_pool is None:
        return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT, (symbol, last, p24, turnover))
            await conn.commit()

async def insert_trades_batch(items: list[tuple[datetime, str, int, float]]):
    if not items or db_pool is None:
        return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for ts_min, sym, cnt, qty in items:
                await cur.execute(SQL_INSERT_TRADES, (ts_min, sym, cnt, qty))
        await conn.commit()

async def select_sorted(order_by: str, limit: int = 10, desc: bool = True):
    if db_pool is None:
        return []
    if order_by == "turnover":
        ob = "tov"
    elif order_by in ("p24", "price24h_pcnt"):
        ob = "p24"
    else:
        ob = "last"
    direction = "DESC" if desc else "ASC"

    sql = SQL_SELECT_SORTED.format(order_by=ob, direction=direction)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            return await cur.fetchall()

async def count_rows() -> int:
    if db_pool is None:
        return 0
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_COUNT)
            n = (await cur.fetchone())[0]
            return int(n or 0)

async def get_one(symbol: str):
    if db_pool is None:
        return None
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_GET_ONE, (symbol.upper(),))
            return await cur.fetchone()

async def trades_latest(symbol: str, limit: int = 5):
    if db_pool is None:
        return []
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_TRADES_LATEST, (symbol.upper(), limit))
            return await cur.fetchall()

# -------------------- Webhook reliability --------------------
async def ensure_webhook(bot: Bot, url: str, retries: int = 8) -> bool:
    delay = 1
    for i in range(retries):
        try:
            ok = await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
            if ok:
                log.info("Webhook set to %s", url)
                return True
        except Exception as e:
            log.warning("set_webhook attempt %d failed: %s", i + 1, e)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30)
    log.error("Failed to set webhook after %d attempts", retries)
    return False

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

# -------------------- Bybit WS ingest (tickers + publicTrade) w/ reconnect --------------------
async def ws_consumer():
    global ws_session, ws_connected, ws_last_msg_ts
    backoff = 1
    while True:
        ws_connected = False
        ws_last_msg_ts = None
        ws_session = aiohttp.ClientSession()
        log.info("Bybit WS connecting: %s", BYBIT_WS_URL)
        try:
            async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
                args = [f"tickers.{s}" for s in SYMBOLS if s] + [f"publicTrade.{s}" for s in SYMBOLS if s]
                await ws.send_json({"op": "subscribe", "args": args})
                log.info("WS subscribed: %d topics", len(args))
                ws_connected = True
                backoff = 1

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json(loads=json.loads)
                        topic = data.get("topic") or ""

                        if topic.startswith("tickers."):
                            payload = data.get("data")
                            items = [payload] if isinstance(payload, dict) else (payload or [])
                            for it in items:
                                symbol = (it.get("symbol") or "").upper()
                                if not symbol:
                                    continue
                                raw_last = it.get("lastPrice")
                                raw_p24  = it.get("price24hPcnt")
                                raw_tov  = it.get("turnover24h")
                                try:
                                    last = float(raw_last) if raw_last not in (None, "") else None
                                except Exception:
                                    last = None
                                try:
                                    p24 = (float(raw_p24) * 100.0) if raw_p24 not in (None, "") else None
                                except Exception:
                                    p24 = None
                                try:
                                    tov = float(raw_tov) if raw_tov not in (None, "") else None
                                except Exception:
                                    tov = None
                                if last is None and p24 is None and tov is None:
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

                        elif topic.startswith("publicTrade."):
                            payload = data.get("data") or []
                            if isinstance(payload, dict):
                                payload = [payload]
                            ts_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                            for tr in payload:
                                sym = (tr.get("s") or "").upper()
                                if not sym:
                                    continue
                                try:
                                    qty = float(tr.get("v") or 0)
                                except Exception:
                                    qty = 0.0
                                key = (sym, ts_min)
                                b = trade_buckets.setdefault(key, {"count": 0, "qty": 0.0})
                                b["count"] += 1
                                b["qty"] += qty
                                ws_last_msg_ts = datetime.now(timezone.utc).isoformat()

                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                        log.warning("WS closed or error")
                        break
        except Exception as e:
            log.warning("WS loop error: %s", e)
        finally:
            with contextlib.suppress(Exception):
                await ws_session.close()
            ws_connected = False
            log.info("WS consumer finished; reconnecting in %ss...", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

# trades flusher loop (each minute)
async def trades_flusher_loop():
    while True:
        try:
            await asyncio.sleep(60)
            if not trade_buckets:
                continue
            items = []
            for (sym, ts_min), data in list(trade_buckets.items()):
                items.append((ts_min, sym, int(data.get("count", 0)), float(data.get("qty", 0.0))))
                trade_buckets.pop((sym, ts_min), None)
            await insert_trades_batch(items)
            log.info("Flushed %d trade buckets", len(items))
        except Exception as e:
            log.exception("trades_flusher_loop error: %s", e)

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
        f"Добро пожаловать в Innertrade Screener {VERSION} (Bybit WS + DB + trades).\n",
        reply_markup=kb_main()
    )

async def cmd_status(message: Message):
    n = await count_rows()
    ws_ts = ws_last_msg_ts or "—"
    await message.answer(
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: {ws_connected}\n"
        f"WS last msg: {ws_ts}\n"
        f"DB rows: {n}"
    )

async def cmd_diag(message: Message):
    n = await count_rows()
    sample = list(ws_cache.items())[:3]
    sample_txt = "\n".join(
        f"{k}: p24={v['p24']:.2f} last={v['last']} tov~{int(v['tov']):,}".replace(",", " ")
        for k, v in sample
    ) or "—"
    await message.answer(
        "diag\n"
        f"rows={n}\n"
        f"cache_sample:\n{sample_txt}"
    )

def fmt_activity(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | turnover24h ~ {int(tov):,}".replace(",", " "))
    return "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_volatility(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | last {last}")
    return "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_trend(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  ≈  24h% {p24:.2f}  | last {last}")
    return "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines[:10])

async def cmd_activity(message: Message):
    rows = await select_sorted("turnover", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: z[2],
            reverse=True
        )[:10]
    await message.answer(fmt_activity(rows))

async def cmd_volatility(message: Message):
    rows = await select_sorted("p24", 50, desc=True)
    rows = sorted(rows, key=lambda r: abs(r[1]), reverse=True)[:10]
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: abs(z[1]),
            reverse=True
        )[:10]
    await message.answer(fmt_volatility(rows))

async def cmd_trend(message: Message):
    rows = await select_sorted("p24", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: z[1],
            reverse=True
        )[:10]
    await message.answer(fmt_trend(rows))

async def cmd_now(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /now SYMBOL (пример: /now BTCUSDT)")
        return
    sym = parts[1].upper()
    row = await get_one(sym)
    if not row:
        await message.answer(f"{sym}: нет данных в БД.")
        return
    symbol, last, p24, tov, updated = row
    await message.answer(
        f"{symbol}\n"
        f"last: {last}\n"
        f"24h%: {p24}\n"
        f"turnover24h: {tov}\n"
        f"updated_at: {updated}"
    )

async def cmd_diag_trades(message: Message):
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_trades SYMBOL [N]\nНапр.: /diag_trades BTCUSDT 5")
        return
    sym = parts[1].upper()
    try:
        limit = int(parts[2]) if len(parts) >= 3 else 5
    except Exception:
        limit = 5
    rows = await trades_latest(sym, limit)
    if not rows:
        await message.answer(f"{sym}: нет строк в trades_1m (пока нет сделок/ждите минуту).")
        return
    lines = [f"{sym} — последние {len(rows)} мин:"]
    for ts, cnt, qty in rows:
        lines.append(f"{ts:%H:%M}  trades={cnt}  qty={qty}")
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
    else:
        await message.answer(
            f"Принял: {t}\n(команды: /start /status /activity /volatility /trend /diag /now SYMBOL /diag_trades SYMBOL [N])"
        )

# -------------------- HTTP endpoints --------------------
async def handle_health(request: web.Request):
    return web.Response(text="ok")

async def handle_root(request: web.Request):
    return web.Response(text="Innertrade screener is alive")

async def handle_status_http(request: web.Request):
    n = await count_rows()
    ws_ts = ws_last_msg_ts or "—"
    body = (
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: {ws_connected}\n"
        f"WS last msg: {ws_ts}\n"
        f"DB rows: {n}\n"
    )
    return web.Response(text=body)

async def handle_diag_http(request: web.Request):
    n = await count_rows()
    sample = list(ws_cache.items())[:3]
    sample_txt = "\n".join(
        f"{k}: p24={v['p24']:.2f} last={v['last']} tov~{int(v['tov']):,}".replace(",", " ")
        for k, v in sample
    ) or "—"
    body = f"diag\nrows={n}\ncache_sample:\n{sample_txt}\n"
    return web.Response(text=body)

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
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_activity, Command("activity"))
    dp.message.register(cmd_volatility, Command("volatility"))
    dp.message.register(cmd_trend, Command("trend"))
    dp.message.register(cmd_diag, Command("diag"))
    dp.message.register(cmd_now, Command("now"))
    dp.message.register(cmd_diag_trades, Command("diag_trades"))
    dp.message.register(on_text, F.text)

    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/status", handle_status_http)
    app.router.add_get("/diag", handle_diag_http)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    return app

async def on_startup():
    global ws_task, watchdog_task, bot
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
    asyncio.create_task(trades_flusher_loop())

    assert bot
    url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
    await ensure_webhook(bot, url)
    watchdog_task = asyncio.create_task(webhook_watchdog(bot, url))

async def on_shutdown():
    global ws_task, ws_session, db_pool, bot, watchdog_task
    # не удаляем вебхук — пусть остаётся
    if bot:
        with contextlib.suppress(Exception):
            await bot.session.close()

    for t in (ws_task, watchdog_task):
        if t:
            t.cancel()
            with contextlib.suppress(Exception):
                await t

    if ws_session:
        with contextlib.suppress(Exception):
            await ws_session.close()

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
