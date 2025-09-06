# main.py
import os
import asyncio
import logging
from typing import Optional

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# --------- Конфиг / переменные окружения ---------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")  # публичный URL сервиса на Render
TZ = os.getenv("TZ", "Europe/Moscow")
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

APP_PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")  # можно оставить по умолчанию
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # не обязателен
VERSION = os.getenv("BUILD_VERSION", "v0.9-webhook")

# --------- Логирование ---------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("innertrade")

# --------- Бот / Диспетчер ---------
if not TELEGRAM_TOKEN:
    raise RuntimeError("Env TELEGRAM_TOKEN is required")

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()

# --------- (опционально) БД через psycopg ---------
_psql_pool = None
try:
    import psycopg
    from psycopg_pool import AsyncConnectionPool  # type: ignore

    async def init_db_pool() -> Optional[AsyncConnectionPool]:
        global _psql_pool
        if not DATABASE_URL:
            log.info("DATABASE_URL not set: DB features disabled")
            return None
        _psql_pool = AsyncConnectionPool(conninfo=DATABASE_URL, open=False, max_size=4)
        await _psql_pool.open()
        # создадим простую табличку для будущих свечей (id -> pk, symbol, ts, ohlc)
        async with _psql_pool.connection() as ac:
            await ac.execute(
                """
                create table if not exists candles_5m (
                    id bigserial primary key,
                    symbol text not null,
                    ts timestamptz not null,
                    open numeric,
                    high numeric,
                    low numeric,
                    close numeric,
                    unique(symbol, ts)
                );
                """
            )
        log.info("DB pool ready")
        return _psql_pool

except Exception as e:
    log.warning("psycopg not available or init failed: %s", e)

# --------- Клавиатура ---------
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Активность"),
                KeyboardButton(text="⚡ Волатильность"),
            ],
            [
                KeyboardButton(text="📈 Тренд"),
                KeyboardButton(text="🫧 Bubbles"),
            ],
            [
                KeyboardButton(text="📰 Новости"),
                KeyboardButton(text="🧮 Калькулятор"),
            ],
            [
                KeyboardButton(text="⭐ Watchlist"),
                KeyboardButton(text="⚙️ Настройки"),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

# --------- Хендлеры ---------
@dp.message(CommandStart())
async def on_start(m: Message):
    await m.answer(
        "🧭 <b>Market mood</b>\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit WS).\n"
        "Команда /menu — вернёт клавиатуру.",
        reply_markup=main_menu(),
    )

@dp.message(Command("menu"))
async def on_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=main_menu())

@dp.message(Command("status"))
async def on_status(m: Message):
    source = "Bybit (public WS)"
    db_state = "ON" if _psql_pool else "OFF"
    await m.answer(
        "<b>Status</b>\n"
        f"Mode: active | Quiet: False\n"
        f"Source: {source}\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"DB: {db_state}\n",
        reply_markup=main_menu(),
    )

@dp.message(Command("diag"))
async def on_diag(m: Message, command: CommandObject):
    # Мини-диагностика без падений
    parts = [
        "diag",
        f"ws_url: {BYBIT_WS_URL}",
        f"db: {'ON' if _psql_pool else 'OFF'}",
    ]
    # если передали аргументы — покажем
    if command.args:
        parts.append(f"args: {command.args}")
    await m.answer("\n".join(parts))

# Текстовые кнопки (без команд)
@dp.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    await m.answer("🔥 Активность\nПодбираю данные (WS)…")

@dp.message(F.text == "⚡ Волатильность")
async def on_vola(m: Message):
    await m.answer("⚡ Волатильность\nПодбираю данные (WS)…")

@dp.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    await m.answer("📈 Тренд (упрощённо, WS)\nПодбираю данные…")

@dp.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("🫧 Bubbles\nСобираю WS-тикеры…")

@dp.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer("📰 Макро (последний час)\n• demo headline")

@dp.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (Excel добавим позже).")

@dp.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

@dp.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
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
        "• /menu        — восстановить клавиатуру",
    )

# --------- AIOHTTP приложение (вебхук + health) ---------
async def health_handler(_: web.Request) -> web.Response:
    return web.Response(text="ok")

async def on_startup(app: web.Application):
    # DB pool (если доступно)
    if "psycopg" in globals():
        try:
            await init_db_pool()
        except Exception as e:
            log.warning("DB init skipped: %s", e)

    # ставим вебхук
    if not BASE_URL:
        log.warning("BASE_URL is empty: webhook will not be set (bot won't receive updates)")
        return
    webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET or None,
        drop_pending_updates=True,
        allowed_updates=["message"],
    )
    log.info("Webhook set to %s", webhook_url)

async def on_shutdown(app: web.Application):
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass
    # закрываем пул БД
    global _psql_pool
    if _psql_pool:
        await _psql_pool.close()
        _psql_pool = None

def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health_handler)

    # Регистрируем обработчик вебхука
    wh = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET or None)
    wh.register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

# --------- Точка входа ---------
if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=APP_PORT)
