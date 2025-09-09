# main.py
# Innertrade Screener — v1.0.1-db-ws
# Совместимо с aiogram 3.13.x и psycopg 3.2.x (async)
# Требуемые ENV:
# TELEGRAM_TOKEN, WEBHOOK_BASE, WEBHOOK_SECRET, PORT(=10000),
# DATABASE_URL (postgres://... или postgresql://...),
# BYBIT_WS_URL (по умолчанию wss://stream.bybit.com/v5/public/linear),
# TZ (например, Europe/Moscow)

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import pytz
import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, Update, ReplyKeyboardMarkup, KeyboardButton,
    CallbackQuery
)

from psycopg_pool import AsyncConnectionPool

# ----------------------- ЛОГИ -----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# ----------------------- ENV -----------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip().rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip().strip("/")
PORT = int(os.getenv("PORT", "10000"))
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow").strip()

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE:
    raise RuntimeError("WEBHOOK_BASE is required (e.g. https://your-service.onrender.com)")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required (any non-empty string)")

# ----------------------- GLOBALS -----------------------
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
app: web.Application | None = None

# Пул соединений к БД
db_pool: AsyncConnectionPool | None = None

# Состояние WS
ws_session: aiohttp.ClientSession | None = None
ws_task: asyncio.Task | None = None
ws_connected = asyncio.Event()

# Символы для подписки (топ ликвид по умолчанию)
DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT",
]

# Карта столбцов для безопасной сортировки
COL_MAP = {
    "turnover": "turnover24h",
    "change": "change24h",
    "last": "last",
}

# ----------------------- УТИЛИТЫ -----------------------
def now_str_tz() -> str:
    try:
        tz = pytz.timezone(TZ)
    except Exception:
        tz = pytz.timezone("Europe/Moscow")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S (%Z)")

def hnum(n: float | int | None) -> str:
    if n is None:
        return "—"
    try:
        n = float(n)
    except Exception:
        return "—"
    # компактный формат для turnover
    for unit in ["", "K", "M", "B", "T"]:
        if abs(n) < 1000.0:
            if unit == "":
                return f"{n:,.0f}".replace(",", " ")
            else:
                return f"{n:,.0f} {unit}".replace(",", " ")
        n /= 1000.0
    return f"{n:.0f} P"

def kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True
    )

# ----------------------- БД -----------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ticker_cache (
    symbol       text PRIMARY KEY,
    last         double precision,
    change24h    double precision,
    turnover24h  double precision,
    updated_at   timestamptz default now()
);
"""

UPSERT_SQL = """
INSERT INTO ticker_cache(symbol, last, change24h, turnover24h, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (symbol) DO UPDATE SET
  last = EXCLUDED.last,
  change24h = EXCLUDED.change24h,
  turnover24h = EXCLUDED.turnover24h,
  updated_at = now();
"""

async def db_init_pool():
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required for DB mode")
    # Не открываем пул в конструкторе — чтобы не ловить Deprecation warning
    db_pool = AsyncConnectionPool(DATABASE_URL, open=False)
    await db_pool.open()
    async with db_pool.connection() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute(CREATE_SQL)
    log.info("DB pool opened")

async def db_close_pool():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        log.info("DB pool closed")

async def upsert_ticker(symbol: str, last: float | None, change24h: float | None, turnover24h: float | None):
    if db_pool is None:
        return
    async with db_pool.connection() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute(
                UPSERT_SQL,
                (symbol, last, change24h, turnover24h)
            )

async def select_sorted(metric: str, limit: int = 10):
    """
    Безопасная сортировка по whitelisted столбцам.
    Возвращает список кортежей (symbol, last, change24h, turnover24h)
    """
    if db_pool is None:
        return []
    col = COL_MAP.get(metric, "turnover24h")
    sql = f"""
        SELECT symbol, last, change24h, turnover24h
        FROM ticker_cache
        ORDER BY {col} DESC NULLS LAST
        LIMIT %s
    """
    async with db_pool.connection() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
            return rows or []

# ----------------------- WS -----------------------
async def ws_worker():
    """Подключение к Bybit public WS и апсерт тикеров в БД."""
    global ws_session
    topics = [f"tickers.{sym}" for sym in DEFAULT_SYMBOLS]

    params = {
        "op": "subscribe",
        "args": topics,
    }

    while True:
        try:
            if ws_session is None:
                ws_session = aiohttp.ClientSession()
            log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
            async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=20) as ws:
                # подписка
                await ws.send_json(params)
                log.info(f"WS subscribed: {len(topics)} topics")
                ws_connected.set()

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = msg.json(loads=json.loads)
                        except Exception:
                            # иногда Bybit присылает текст; пробуем распарсить вручную
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                        # ожидаем формат {"topic":"tickers.BTCUSDT", "data":[{...}] } или {"data":{...,"symbol":"BTCUSDT"}}
                        topic = data.get("topic") or ""
                        payload = data.get("data")
                        if not payload:
                            continue

                        # Bybit может вернуть объект или массив объектов
                        items = payload if isinstance(payload, list) else [payload]
                        for itm in items:
                            symbol = itm.get("symbol")
                            if not symbol:
                                # иногда topic вида tickers.BTCUSDT — вытащим оттуда
                                if topic.startswith("tickers."):
                                    symbol = topic.split(".", 1)[1]
                            try:
                                last = float(itm.get("lastPrice", itm.get("last_price", "nan")))
                            except Exception:
                                last = None
                            try:
                                ch = float(itm.get("price24hPcnt", itm.get("price24hPcnt_e4", "nan")))
                                # Bybit часто даёт долю (0.0123 => 1.23%)
                                change24h = ch * 100.0 if abs(ch) < 50 else ch
                            except Exception:
                                change24h = None
                            try:
                                # оборот за 24 часа (turnover24h), может приходить строкой
                                tov = float(itm.get("turnover24h", itm.get("turnover_24h", "nan")))
                            except Exception:
                                tov = None

                            if symbol:
                                await upsert_ticker(symbol, last, change24h, tov)

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

        except asyncio.CancelledError:
            log.info("WS worker cancelled")
            break
        except Exception as e:
            ws_connected.clear()
            log.exception("WS worker error: %s", e)
            await asyncio.sleep(3.0)
        finally:
            ws_connected.clear()
            if ws_session:
                await ws_session.close()
                ws_session = None
            await asyncio.sleep(2.0)

# ----------------------- ХЭНДЛЕРЫ -----------------------
WELCOME = (
    "🧭 Market mood\n"
    "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
    "Добро пожаловать в Innertrade Screener v1.0.1-db-ws (Bybit WS + DB)."
)

@dp.message(CommandStart())
async def on_start(message: Message):
    await message.answer(WELCOME, reply_markup=kb())

@dp.message(Command("status"))
async def on_status(message: Message):
    await message.answer(
        "Status\n"
        f"Time: {now_str_tz()}\n"
        f"Mode: active | Quiet: False\n"
        f"Source: Bybit (public WS)\n"
        f"Version: v1.0.1-db-ws\n"
        f"Bybit WS: {BYBIT_WS_URL}"
    )

@dp.message(F.text == "📊 Активность")
@dp.message(Command("activity"))
async def on_activity(message: Message):
    rows = await select_sorted("turnover", limit=10)
    if not rows:
        await message.answer("🔥 Активность\nНет данных (ожидаю WS/БД).")
        return

    lines = ["🔥 Активность (топ по turnover24h)"]
    for i, (sym, last, ch, tov) in enumerate(rows, start=1):
        ch_str = "—" if ch is None else f"{ch:.2f}"
        last_str = "—" if last is None else f"{last:.4f}" if last < 100 else f"{last:,.2f}".replace(",", " ")
        lines.append(f"{i}) {sym}  24h% {ch_str}  | turnover24h ~ {hnum(tov)} | last {last_str}")
    await message.answer("\n".join(lines))

@dp.message(F.text == "⚡ Волатильность")
@dp.message(Command("vol"))
async def on_vol(message: Message):
    rows = await select_sorted("change", limit=10)
    if not rows:
        await message.answer("⚡ Волатильность\nНет данных.")
        return
    lines = ["⚡ Волатильность (топ по 24h%)"]
    for i, (sym, last, ch, tov) in enumerate(rows, start=1):
        ch_str = "—" if ch is None else f"{ch:.2f}"
        last_str = "—" if last is None else f"{last:.4f}" if last < 100 else f"{last:,.2f}".replace(",", " ")
        lines.append(f"{i}) {sym}  24h% {ch_str}  | last {last_str}")
    await message.answer("\n".join(lines))

@dp.message(F.text == "📈 Тренд")
@dp.message(Command("trend"))
async def on_trend(message: Message):
    rows = await select_sorted("change", limit=10)
    if not rows:
        await message.answer("📈 Тренд\nНет данных.")
        return
    lines = ["📈 Тренд (упрощённо по 24h%)"]
    for i, (sym, last, ch, tov) in enumerate(rows, start=1):
        ch_str = "—" if ch is None else f"{ch:.2f}"
        last_str = "—" if last is None else f"{last:.4f}" if last < 100 else f"{last:,.2f}".replace(",", " ")
        lines.append(f"{i}) {sym}  ≈  24h% {ch_str}  | last {last_str}")
    await message.answer("\n".join(lines))

@dp.message(F.text == "🫧 Bubbles")
@dp.message(Command("bubbles"))
async def on_bubbles(message: Message):
    rows = await select_sorted("turnover", limit=10)
    if not rows:
        await message.answer("🫧 Bubbles\nНет данных.")
        return
    # пока текстовый вывод
    lines = ["WS Bubbles (24h %, size~turnover24h)"]
    for i, (sym, last, ch, tov) in enumerate(rows, start=1):
        ch_str = "—" if ch is None else f"{ch:.2f}"
        lines.append(f"{i}) {sym}  24h% {ch_str}  | size ~ {hnum(tov)}")
    await message.answer("\n".join(lines))

@dp.message(F.text == "📰 Новости")
@dp.message(Command("news"))
async def on_news(message: Message):
    await message.answer(
        "🧭 Market mood\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "📰 Макро (последний час)\n• demo headline"
    )

@dp.message(F.text == "🧮 Калькулятор")
@dp.message(Command("calc"))
async def on_calc(message: Message):
    await message.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")

@dp.message(F.text == "⭐ Watchlist")
@dp.message(Command("watchlist"))
async def on_watchlist(message: Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

@dp.message(F.text == "⚙️ Настройки")
@dp.message(Command("settings"))
async def on_settings(message: Message):
    await message.answer(
        "⚙️ Настройки\n"
        "Биржа: Bybit (USDT perp, WS)\n"
        "Режим: active | Quiet: False\n"
        "Watchlist: —\n\n"
        "Команды:\n"
        "• /add SYMBOL  — добавить (например, /add SOLUSDT)\n"
        "• /rm SYMBOL   — удалить\n"
        "• /watchlist   — показать лист\n"
        "• /passive     — автосводки/сигналы ON\n"
        "• /active      — автосводки/сигналы OFF\n"
        "• /menu        — восстановить клавиатуру"
    )

@dp.message(Command("menu"))
async def on_menu(message: Message):
    await message.answer("Меню восстановлено.", reply_markup=kb())

# ----------------------- ВЕБХУК СЕРВЕР -----------------------
async def handle_health(request: web.Request):
    return web.json_response({
        "ok": True,
        "service": "innertrade-screener",
        "version": "v1.0.1-db-ws",
        "webhook": True,
        "ws_connected": ws_connected.is_set(),
        "time": now_str_tz(),
    })

async def handle_root(request: web.Request):
    return web.Response(text="ok", status=200)

async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return web.Response(status=200)

def build_app() -> web.Application:
    global app
    app = web.Application()
    # маршруты
    app.add_routes([
        web.head("/", handle_root),
        web.get("/", handle_root),
        web.head("/health", handle_health),
        web.get("/health", handle_health),
        web.post(f"/webhook/{WEBHOOK_SECRET}", handle_webhook),
    ])
    return app

async def set_webhook():
    url = f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}"
    await bot.set_webhook(url, allowed_updates=["message", "callback_query"], max_connections=40)
    log.info(f"Webhook set to {url}")

# ----------------------- ЖИЗНЕННЫЙ ЦИКЛ -----------------------
async def startup():
    await set_webhook()
    await db_init_pool()
    # запускаем WS
    global ws_task
    ws_task = asyncio.create_task(ws_worker())

async def shutdown():
    # удаляем вебхук, чтобы TG не слал в никуда
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    # останавливаем WS
    global ws_task, ws_session
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except Exception:
            pass
    if ws_session:
        await ws_session.close()
        ws_session = None
    await db_close_pool()
    await bot.session.close()

def serve():
    application = build_app()
    loop = asyncio.get_event_loop()

    async def _on_start(_app):
        await startup()

    async def _on_cleanup(_app):
        await shutdown()

    application.on_startup.append(_on_start)
    application.on_cleanup.append(_on_cleanup)

    # Грейсфул для Render
    try:
        web.run_app(application, host="0.0.0.0", port=PORT, print=None)
    except KeyboardInterrupt:
        pass

# ----------------------- MAIN -----------------------
if __name__ == "__main__":
    serve()
