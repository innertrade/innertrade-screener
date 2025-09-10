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
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()  # https://your-app.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))
TZ = os.getenv("TZ", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
SYMBOLS = os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
).split(",")

# required env checks
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE or not WEBHOOK_BASE.startswith("http"):
    raise RuntimeError("WEBHOOK_BASE is required (https://...)")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required (any non-empty string)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required (postgres)")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"

# -------------------- Globals --------------------
bot: Bot | None = None
dp: Dispatcher | None = None
app: web.Application | None = None

db_pool: AsyncConnectionPool | None = None
ws_session: aiohttp.ClientSession | None = None
ws_task: asyncio.Task | None = None

# кэш тикеров от WS (как фолбэк, пока БД не прогрелась)
ws_cache: dict[str, dict] = {}

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

# psycopg3: используем %s-плейсхолдеры и COALESCE, чтобы не затирать отсутствующие поля
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

# -------------------- DB helpers --------------------
async def init_db():
    global db_pool
    # убираем warning: не открываем пул в конструкторе
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
            rows = await cur.fetchall()
            return rows

async def count_rows() -> int:
    if db_pool is None:
        return 0
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_COUNT)
            n = (await cur.fetchone())[0]
            return int(n or 0)

# -------------------- Bybit WS ingest --------------------
async def ws_consumer():
    global ws_session
    ws_session = aiohttp.ClientSession()
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    try:
        async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
            # подписка на tickers.SYMBOL
            args = [f"tickers.{s}" for s in SYMBOLS if s]
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
                            if not symbol:
                                continue

                            # берём сырые значения; если поля нет — None
                            raw_last = it.get("lastPrice")
                            raw_p24  = it.get("price24hPcnt")    # доля (0.0123 → 1.23%)
                            raw_tov  = it.get("turnover24h")

                            # конвертируем только если присутствуют
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

                            # пропускаем пустой апдейт (ничего не обновляется)
                            if last is None and p24 is None and tov is None:
                                continue

                            # КЭШ: обновляем только пришедшие поля
                            prev = ws_cache.get(symbol, {})
                            ws_cache[symbol] = {
                                "last": last if last is not None else prev.get("last", 0.0),
                                "p24":  p24  if p24  is not None else prev.get("p24",  0.0),
                                "tov":  tov  if tov  is not None else prev.get("tov",  0.0),
                                "ts": datetime.now(timezone.utc).isoformat(),
                            }

                            # БД: None → «оставить прежнее значение» благодаря COALESCE в UPSERT
                            await upsert_ticker(symbol, last, p24, tov)

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error("WS error: %s", msg.data)
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                    log.warning("WS closed by server")
                    break
    except Exception as e:
        log.exception("WS consumer failed: %s", e)
    finally:
        if ws_session:
            await ws_session.close()
        log.info("WS consumer finished")

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
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "Добро пожаловать в Innertrade Screener v1.1.0-db-ws (Bybit WS + DB).",
        reply_markup=kb_main()
    )

async def cmd_status(message: Message):
    n = await count_rows()
    await message.answer(
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        "Version: v1.1.0-db-ws\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
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

# Тексты-кнопки
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
            f"Принял: {t}\n(команды: /start /status /activity /volatility /trend /diag)"
        )

# -------------------- HTTP endpoints --------------------
async def handle_health(request: web.Request):
    return web.Response(text="ok")

async def handle_root(request: web.Request):
    return web.Response(text="Innertrade screener is alive")

# удобные HTTP-версии /status и /diag
async def handle_status_http(request: web.Request):
    n = await count_rows()
    body = (
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        "Version: v1.1.0-db-ws\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
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
    dp.message.register(on_text, F.text)

    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/status", handle_status_http)
    app.router.add_get("/diag", handle_diag_http)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    return app

async def on_startup():
    global ws_task
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
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
    try:
        run()
    except KeyboardInterrupt:
        pass
