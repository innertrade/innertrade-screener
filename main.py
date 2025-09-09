import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz
from aiohttp import web, ClientTimeout
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.enums import ParseMode

import aiohttp
from psycopg_pool import AsyncConnectionPool

# -------------------------
# Конфиг и логирование
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
TZ = os.getenv("TZ", "Europe/Moscow")

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE:
    raise RuntimeError("WEBHOOK_BASE is required")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required (any non-empty string)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
WEBHOOK_URL = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "10000"))

# -------------------------
# Глобальные объекты
# -------------------------
# Telegram session с аккуратным таймаутом (без trust_env — его нет в 3.13.1)
tg_timeout = ClientTimeout(total=15)
tg_session = AiohttpSession(timeout=tg_timeout)
bot = Bot(token=TELEGRAM_TOKEN, session=tg_session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Пул БД
db_pool: AsyncConnectionPool = AsyncConnectionPool(DATABASE_URL, open=False, min_size=1, max_size=5)

# Состояние WS
ws_task: Optional[asyncio.Task] = None
ws_connected: bool = False
symbols_seed = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT"]

# -------------------------
# БД схемы
# -------------------------
DDL_CREATE = """
CREATE TABLE IF NOT EXISTS ws_ticks (
    symbol TEXT PRIMARY KEY,
    last DOUBLE PRECISION,
    chg24 NUMERIC,
    turnover24 NUMERIC,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_TICK = """
INSERT INTO ws_ticks(symbol, last, chg24, turnover24, ts)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (symbol) DO UPDATE SET
    last = EXCLUDED.last,
    chg24 = EXCLUDED.chg24,
    turnover24 = EXCLUDED.turnover24,
    ts = NOW();
"""

SELECT_SORTED = """
SELECT symbol, COALESCE(last,0) AS last, COALESCE(chg24,0) AS chg24, COALESCE(turnover24,0) AS turnover24
FROM ws_ticks
ORDER BY {order_by} DESC NULLS LAST
LIMIT %s;
"""

# psycopg3 placeholder style — %s; порядок параметров через execute


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
                "INSERT INTO ws_ticks(symbol, last, chg24, turnover24, ts) VALUES (%s,%s,%s,%s,NOW()) "
                "ON CONFLICT (symbol) DO UPDATE SET last=EXCLUDED.last, chg24=EXCLUDED.chg24, "
                "turnover24=EXCLUDED.turnover24, ts=NOW();",
                (symbol, last, chg24, turnover24),
            )
            await conn.commit()


async def select_sorted(order_by: str, limit: int = 10) -> List[Tuple[str, float, float, float]]:
    allowed = {"turnover24", "chg24", "last"}
    if order_by not in allowed:
        order_by = "turnover24"
    sql = SELECT_SORTED.format(order_by=order_by)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
            # rows: List[Tuple(symbol, last, chg24, turnover24)]
            return rows or []


# -------------------------
# Bybit WS worker
# -------------------------
async def ws_worker(app: web.Application) -> None:
    global ws_connected
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    topics = [f"tickers.{sym}" for sym in symbols_seed]
    payload = {"op": "subscribe", "args": topics}

    session_timeout = aiohttp.ClientTimeout(total=0)  # не ограничиваем
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        try:
            async with session.ws_connect(BYBIT_WS_URL, heartbeat=20) as ws:
                ws_connected = True
                await ws.send_json(payload)
                log.info(f"WS subscribed: {len(topics)} topics")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = msg.json()
                        except Exception:
                            continue
                        # ожидаем формат Bybit v5 tickers
                        if data.get("topic", "").startswith("tickers."):
                            # data["data"] может быть массивом или объектом
                            d = data.get("data")
                            if isinstance(d, list):
                                items = d
                            else:
                                items = [d] if d else []
                            for it in items:
                                symbol = it.get("symbol")
                                last = safe_float(it.get("lastPrice"))
                                chg24 = safe_float(it.get("price24hPcnt"))  # доля (пример: 0.0123 => 1.23%)
                                # turnover24 может быть строкой
                                turnover24 = safe_float(it.get("turnover24h"))
                                if symbol:
                                    await db_upsert_tick(symbol, last, chg24, turnover24)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            log.info("WS worker cancelled")
        except Exception as e:
            log.exception(f"WS worker error: {e}")
        finally:
            ws_connected = False


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x))
    except Exception:
        return None


# -------------------------
# UI / клавиатура
# -------------------------
def main_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="📊 Активность", callback_data="activity")],
        [InlineKeyboardButton(text="⚡ Волатильность", callback_data="volatility")],
        [InlineKeyboardButton(text="📈 Тренд", callback_data="trend")],
        [InlineKeyboardButton(text="🫧 Bubbles", callback_data="bubbles")],
        [InlineKeyboardButton(text="📰 Новости", callback_data="news")],
        [InlineKeyboardButton(text="🧮 Калькулятор", callback_data="calc")],
        [InlineKeyboardButton(text="⭐ Watchlist", callback_data="watchlist")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# -------------------------
# Handlers
# -------------------------
@dp.message(F.text.in_({"/start", "/menu"}))
async def on_start(message: Message):
    await message.answer(
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "Добро пожаловать в Innertrade Screener v1.0.1-db-ws (Bybit WS + DB).",
        reply_markup=main_keyboard(),
    )


@dp.message(F.text == "/status")
async def on_status(message: Message):
    now = datetime.now(pytz.timezone(TZ))
    status = (
        f"Status\n"
        f"Time: {now.strftime('%Y-%m-%d %H:%M:%S')} ({TZ})\n"
        f"Mode: active | Quiet: False\n"
        f"Source: Bybit (public WS)\n"
        f"Version: v1.0.1-db-ws\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: {ws_connected}\n"
    )
    await message.answer(status, reply_markup=main_keyboard())


@dp.callback_query(F.data == "activity")
@dp.message(F.text.in_({"Активность", "/activity"}))
async def on_activity(event: Message | Any):
    rows = await select_sorted("turnover24", limit=10)
    if not rows:
        text = "🔥 Активность\nНет данных (БД пуста)."
    else:
        lines = []
        for i, (sym, last, chg24, turnover) in enumerate(rows, 1):
            chg_str = fmt_pc(chg24)
            turn_str = fmt_turnover(turnover)
            last_str = "—" if last is None else f"{last}"
            lines.append(f"{i}) <b>{sym}</b>  24h% {chg_str}  | turnover24h ~ {turn_str}")
        text = "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines)
    await reply_event(event, text, reply_markup=main_keyboard())


@dp.callback_query(F.data == "volatility")
@dp.message(F.text.in_({"Волатильность", "/volatility"}))
async def on_volatility(event: Message | Any):
    rows = await select_sorted("chg24", limit=10)
    if not rows:
        text = "⚡ Волатильность\nНет данных."
    else:
        lines = []
        for i, (sym, last, chg24, turnover) in enumerate(rows, 1):
            lines.append(f"{i}) <b>{sym}</b>  24h% {fmt_pc(chg24)}  | last {fmt_last(last)}")
        text = "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines)
    await reply_event(event, text, reply_markup=main_keyboard())


@dp.callback_query(F.data == "trend")
@dp.message(F.text.in_({"Тренд", "/trend"}))
async def on_trend(event: Message | Any):
    rows = await select_sorted("chg24", limit=10)
    if not rows:
        text = "📈 Тренд\nНет данных."
    else:
        lines = []
        for i, (sym, last, chg24, _) in enumerate(rows, 1):
            lines.append(f"{i}) <b>{sym}</b>  ≈  24h% {fmt_pc(chg24)}  | last {fmt_last(last)}")
        text = "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines)
    await reply_event(event, text, reply_markup=main_keyboard())


@dp.message(F.text == "/diag")
async def on_diag(message: Message):
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM ws_ticks;")
                cnt = (await cur.fetchone() or [0])[0]
    except Exception as e:
        cnt = -1
        log.exception("diag db error: %s", e)

    await message.answer(
        f"diag\nws_connected={ws_connected}\nrows_in_db={cnt}\n", reply_markup=main_keyboard()
    )


def fmt_pc(x: Optional[float]) -> str:
    if x is None:
        return "—"
    # Bybit price24hPcnt приходит долей (0.0123 -> 1.23%)
    return f"{x*100:.2f}" if abs(x) < 10 else f"{x:.2f}"


def fmt_turnover(x: Optional[float]) -> str:
    if x is None or x == 0:
        return "—"
    # грубая человеческая форма
    units = [("T", 1e12), ("B", 1e9), ("M", 1e6)]
    for suf, base in units:
        if x >= base:
            return f"{x/base:.0f}{suf}"
    return f"{x:.0f}"


def fmt_last(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:g}"


async def reply_event(event: Any, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    # Удобный ответ и на message, и на callback
    if isinstance(event, Message):
        await event.answer(text, reply_markup=reply_markup)
    else:
        # callback_query
        await event.message.answer(text, reply_markup=reply_markup)


# -------------------------
# Aiohttp app / webhook
# -------------------------
async def handle_health(request: web.Request) -> web.Response:
    # Только GET! HEAD будет зарегистрирован автоматом aiohttp
    data = {
        "ok": True,
        "service": "innertrade-screener",
        "version": "v1.0.1-db-ws",
        "webhook": True,
        "ws_connected": ws_connected,
    }
    return web.json_response(data)


async def handle_root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")


async def handle_webhook(request: web.Request) -> web.Response:
    try:
        body = await request.text()
        update = Update.model_validate_json(body)
    except Exception:
        return web.Response(status=400, text="bad update")

    await dp.feed_update(bot, update)
    return web.Response(text="ok")


async def on_startup(app: web.Application):
    # DB
    await db_init()
    # Webhook
    await bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )
    log.info(f"Webhook set to {WEBHOOK_URL}")
    # WS worker
    global ws_task
    ws_task = asyncio.create_task(ws_worker(app))


async def on_cleanup(app: web.Application):
    # Останавливаем WS
    global ws_task
    if ws_task:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task
    # Закрываем БД и телеграм-сессию
    await db_pool.close()
    await bot.session.close()


def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # РОУТЫ: только GET — без явного HEAD
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    return app


# -------------------------
# ENTRY
# -------------------------
import contextlib

def main():
    app = build_app()
    # Запускаем HTTP сервер; без верхнего asyncio.run(), aiohttp сам рулит лупом.
    web.run_app(app, host=HOST, port=PORT)

if __name__ == "__main__":
    main()
