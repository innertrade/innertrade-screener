import os
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Optional, Tuple, List
import contextlib

import pytz
import aiohttp
from aiohttp import web, ClientTimeout

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import CommandStart, Command

from psycopg_pool import AsyncConnectionPool

# ------------- Конфиг/лог -------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("innertrade")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE  = os.getenv("WEBHOOK_BASE", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
DATABASE_URL  = os.getenv("DATABASE_URL", "").strip()
BYBIT_WS_URL  = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
TZ            = os.getenv("TZ", "Europe/Moscow")

if not TELEGRAM_TOKEN:  raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE:    raise RuntimeError("WEBHOOK_BASE is required")
if not WEBHOOK_SECRET:  raise RuntimeError("WEBHOOK_SECRET is required (any non-empty string)")
if not DATABASE_URL:    raise RuntimeError("DATABASE_URL is required")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
WEBHOOK_URL  = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "10000"))

# ------------- Bot/DP/DB -------------
tg_timeout = ClientTimeout(total=15)
tg_session = AiohttpSession(timeout=tg_timeout)
bot = Bot(token=TELEGRAM_TOKEN, session=tg_session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

db_pool: AsyncConnectionPool = AsyncConnectionPool(DATABASE_URL, open=False, min_size=1, max_size=5)

# ------------- WS state -------------
ws_task: Optional[asyncio.Task] = None
ws_connected: bool = False
symbols_seed = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT"]

# ------------- SQL -------------
DDL_CREATE = """
CREATE TABLE IF NOT EXISTS ws_ticks (
    symbol TEXT PRIMARY KEY,
    last DOUBLE PRECISION,
    chg24 DOUBLE PRECISION,
    turnover24 DOUBLE PRECISION,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

SELECT_SORTED_TMPL = """
SELECT symbol,
       COALESCE(last,0)      AS last,
       COALESCE(chg24,0)     AS chg24,
       COALESCE(turnover24,0) AS turnover24
FROM ws_ticks
ORDER BY {order_by} DESC NULLS LAST
LIMIT %s;
"""

# ------------- DB helpers -------------
async def db_init() -> None:
    await db_pool.open()
    log.info("DB pool opened")
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(DDL_CREATE)
            await conn.commit()

async def db_upsert_tick(symbol: str, last: Optional[float], chg24: Optional[float], turnover24: Optional[float]) -> None:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO ws_ticks(symbol,last,chg24,turnover24,ts) VALUES (%s,%s,%s,%s,NOW()) "
                "ON CONFLICT (symbol) DO UPDATE SET last=EXCLUDED.last, chg24=EXCLUDED.chg24, "
                "turnover24=EXCLUDED.turnover24, ts=NOW();",
                (symbol, last, chg24, turnover24)
            )
            await conn.commit()

async def select_sorted(order_by: str, limit: int = 10) -> List[Tuple[str, float, float, float]]:
    order_by = order_by if order_by in {"turnover24","chg24","last"} else "turnover24"
    sql = SELECT_SORTED_TMPL.format(order_by=order_by)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
            return rows or []

# ------------- WS worker -------------
def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        if isinstance(x, (int,float)): return float(x)
        return float(str(x))
    except Exception:
        return None

async def ws_worker(app: web.Application) -> None:
    global ws_connected
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    topics = [f"tickers.{s}" for s in symbols_seed]
    sub = {"op":"subscribe", "args": topics}

    timeout = aiohttp.ClientTimeout(total=0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.ws_connect(BYBIT_WS_URL, heartbeat=20) as ws:
                ws_connected = True
                await ws.send_json(sub)
                log.info(f"WS subscribed: {len(topics)} topics")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = msg.json()
                        except Exception:
                            continue
                        if str(data.get("topic","")).startswith("tickers."):
                            d = data.get("data")
                            items = d if isinstance(d, list) else ([d] if d else [])
                            for it in items:
                                symbol = it.get("symbol")
                                last = safe_float(it.get("lastPrice"))
                                chg  = safe_float(it.get("price24hPcnt"))  # доля
                                turn = safe_float(it.get("turnover24h"))
                                if symbol:
                                    await db_upsert_tick(symbol, last, chg, turn)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            log.info("WS worker cancelled")
        except Exception as e:
            log.exception(f"WS worker error: {e}")
        finally:
            ws_connected = False

# ------------- UI -------------
def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Активность", callback_data="activity")],
        [InlineKeyboardButton(text="⚡ Волатильность", callback_data="volatility")],
        [InlineKeyboardButton(text="📈 Тренд", callback_data="trend")],
        [InlineKeyboardButton(text="🫧 Bubbles", callback_data="bubbles")],
        [InlineKeyboardButton(text="📰 Новости", callback_data="news")],
        [InlineKeyboardButton(text="🧮 Калькулятор", callback_data="calc")],
        [InlineKeyboardButton(text="⭐ Watchlist", callback_data="watchlist")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
    ])

def fmt_pc(x: Optional[float]) -> str:
    if x is None:
        return "—"
    # Bybit price24hPcnt = доля (0.0123 -> 1.23%)
    val = x * 100
    return f"{val:.2f}"

def fmt_turnover(x: Optional[float]) -> str:
    if not x or x == 0:
        return "—"
    for suf, base in (("T",1e12),("B",1e9),("M",1e6)):
        if x >= base:
            return f"{x/base:.0f}{suf}"
    return f"{x:.0f}"

def fmt_last(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:g}"

async def reply_event(event: Message|CallbackQuery, text: str):
    if isinstance(event, Message):
        await event.answer(text, reply_markup=kb_main())
    else:
        await event.message.answer(text, reply_markup=kb_main())

# ------------- Handlers -------------
@dp.message(CommandStart())
async def h_start(m: Message):
    await m.answer(
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "Добро пожаловать в Innertrade Screener v1.0.1-db-ws (Bybit WS + DB).",
        reply_markup=kb_main()
    )

@dp.message(Command("status"))
async def h_status(m: Message):
    now = datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M:%S")
    await m.answer(
        "Status\n"
        f"Time: {now} ({TZ})\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        "Version: v1.0.1-db-ws\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: {ws_connected}\n",
        reply_markup=kb_main()
    )

@dp.callback_query(F.data == "activity")
@dp.message(Command("activity"))
async def h_activity(e: Message|CallbackQuery):
    rows = await select_sorted("turnover24", 10)
    if not rows:
        return await reply_event(e, "🔥 Активность\nНет данных (БД пуста).")
    lines = []
    for i,(sym,last,chg,turn) in enumerate(rows,1):
        lines.append(f"{i}) <b>{sym}</b>  24h% {fmt_pc(chg)}  | turnover24h ~ {fmt_turnover(turn)}")
    await reply_event(e, "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines))

@dp.callback_query(F.data == "volatility")
@dp.message(Command("volatility"))
async def h_volatility(e: Message|CallbackQuery):
    rows = await select_sorted("chg24", 10)
    if not rows:
        return await reply_event(e, "⚡ Волатильность\nНет данных.")
    lines = []
    for i,(sym,last,chg,turn) in enumerate(rows,1):
        lines.append(f"{i}) <b>{sym}</b>  24h% {fmt_pc(chg)}  | last {fmt_last(last)}")
    await reply_event(e, "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines))

@dp.callback_query(F.data == "trend")
@dp.message(Command("trend"))
async def h_trend(e: Message|CallbackQuery):
    rows = await select_sorted("chg24", 10)
    if not rows:
        return await reply_event(e, "📈 Тренд\nНет данных.")
    lines = []
    for i,(sym,last,chg,turn) in enumerate(rows,1):
        lines.append(f"{i}) <b>{sym}</b>  ≈  24h% {fmt_pc(chg)}  | last {fmt_last(last)}")
    await reply_event(e, "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines))

@dp.message(Command("diag"))
async def h_diag(m: Message):
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM ws_ticks;")
                cnt = (await cur.fetchone() or [0])[0]
    except Exception as e:
        log.exception("diag db error: %s", e)
        cnt = -1
    await m.answer(f"diag\nws_connected={ws_connected}\nrows_in_db={cnt}\n", reply_markup=kb_main())

# Catch-all: покажет, что бот вообще получает и обрабатывает текст
@dp.message()
async def h_catch_all(m: Message):
    txt = (m.text or "").strip()
    await m.answer(f"Принял: <code>{txt}</code>\n(команды: /start /status /activity /volatility /trend /diag)", reply_markup=kb_main())

# ------------- Aiohttp (webhook + health) -------------
async def handle_root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")

async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "service": "innertrade-screener",
        "version": "v1.0.1-db-ws",
        "webhook": True,
        "ws_connected": ws_connected,
    })

async def handle_webhook(request: web.Request) -> web.Response:
    try:
        body = await request.text()
        update = Update.model_validate_json(body)
        # логируем кратко, что прилетело
        kind = "unknown"
        desc = ""
        if update.message:
            kind = "message"
            desc = (update.message.text or "").replace("\n"," ")[:200]
        elif update.callback_query:
            kind = "callback_query"
            desc = (update.callback_query.data or "")[:200]
        log.info(f"Update kind={kind} desc={desc}")
        try:
            await dp.feed_update(bot, update)
        except Exception as e:
            log.exception("handler error: %s", e)
        return web.Response(text="ok")
    except Exception as e:
        log.exception("bad update: %s", e)
        # всё равно 200, чтобы не копить pending; ошибку мы уже залогировали
        return web.Response(text="ok")

async def on_startup(app: web.Application):
    await db_init()
    await bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )
    log.info(f"Webhook set to {WEBHOOK_URL}")
    global ws_task
    ws_task = asyncio.create_task(ws_worker(app))

async def on_cleanup(app: web.Application):
    global ws_task
    if ws_task:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task
    await db_pool.close()
    await bot.session.close()

def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Только GET — HEAD зарегистрируется автоматически
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app

def main():
    web.run_app(build_app(), host=HOST, port=PORT)

if __name__ == "__main__":
    main()
