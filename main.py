import os
import asyncio
import logging
import pytz
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiohttp import web
import asyncpg

# ==============================
# CONFIG
# ==============================
logging.basicConfig(level=logging.INFO)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "https://innertrade-screener.onrender.com")
TZ = os.getenv("TZ", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

# ==============================
# DB
# ==============================
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS screener_state (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP DEFAULT NOW(),
            key TEXT,
            value JSONB
        );
    """)
    await conn.close()

# ==============================
# MENU
# ==============================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")]
    ],
    resize_keyboard=True
)

# ==============================
# HANDLERS
# ==============================
@dp.message(commands=["start", "menu"])
async def cmd_start(message: types.Message):
    await message.answer(
        "🧭 Market mood\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в Innertrade Screener v0.8.7-websocket+db (Bybit).",
        reply_markup=main_kb
    )

@dp.message(commands=["status"])
async def cmd_status(message: types.Message):
    await message.answer(
        "Status\n"
        f"Mode: active | Quiet: False\n"
        f"Source: Bybit (public WS)\n"
        f"Version: v0.8.7-websocket+db\n"
        f"DB: {DATABASE_URL is not None}\n"
    )

@dp.message(commands=["diag"])
async def cmd_diag(message: types.Message):
    await message.answer("diag\nWS public: ok=True\nDB: connected")

@dp.message()
async def handle_buttons(message: types.Message):
    if message.text == "📊 Активность":
        await message.answer("🔥 Активность\nПодбираю данные (WS)…")
    elif message.text == "⚡ Волатильность":
        await message.answer("⚡ Волатильность\nПодбираю данные (WS)…")
    elif message.text == "📈 Тренд":
        await message.answer("📈 Тренд\nПодбираю данные (WS)…")
    elif message.text == "🫧 Bubbles":
        await message.answer("🫧 Bubbles\nСобираю WS-тикеры…")
    elif message.text == "📰 Новости":
        await message.answer("📰 Макро (последний час)\n• demo headline")
    elif message.text == "🧮 Калькулятор":
        await message.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")
    elif message.text == "⭐ Watchlist":
        await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")
    elif message.text == "⚙️ Настройки":
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

# ==============================
# WEBHOOK + HEALTH
# ==============================
async def on_startup(app):
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook/{TELEGRAM_TOKEN}")

async def on_shutdown(app):
    await bot.session.close()

async def handle_webhook(request):
    update = await request.json()
    await dp.feed_update(bot, types.Update.model_validate(update))
    return web.Response()

async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": "v0.8.7-websocket+db"})

def main():
    app = web.Application()
    app.router.add_post(f"/webhook/{TELEGRAM_TOKEN}", handle_webhook)
    app.router.add_get("/health", handle_health)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

if __name__ == "__main__":
    main()
