import os
import logging
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiohttp import ClientSession
from dotenv import load_dotenv

# --- загрузка окружения ---
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_HOST = os.getenv("BYBIT_HOST", "https://api.bybit.com")

# --- логирование ---
logging.basicConfig(level=logging.INFO)

# --- инициализация бота ---
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

HTTP_HEADERS = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

# --- меню ---
def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True
    )

# /start
@dp.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer(
        "🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в Innertrade Screener v0.9-beta (Bybit).",
        reply_markup=main_menu()
    )

# /status
@dp.message(F.text == "/status")
async def cmd_status(message: Message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = (
        f"Status\n"
        f"Time: {now}\n"
        f"Mode: active | Quiet: False\n"
        f"Source: Bybit (linear USDT)\n"
        f"Watchlist: —\n"
        f"Webhook: ON\n"
        f"Version: v0.9-beta"
    )
    await message.answer(text)

# /diag
@dp.message(F.text == "/diag")
async def cmd_diag(message: Message):
    async with ClientSession() as session:
        # тест сервера Bybit
        url_time = f"{BYBIT_HOST}/v5/market/time"
        async with session.get(url_time, headers=HTTP_HEADERS) as resp_time:
            data_time = await resp_time.json()

        # тикер BTCUSDT
        url_ticker = f"{BYBIT_HOST}/v5/market/tickers?category=linear&symbol=BTCUSDT"
        async with session.get(url_ticker, headers=HTTP_HEADERS) as resp_ticker:
            data_ticker = await resp_ticker.json()

    server_time = data_time.get("result", {}).get("timeSecond", "—")
    ticker = (
        data_ticker.get("result", {})
        .get("list", [{}])[0]
        if "result" in data_ticker else {}
    )

    last_price = ticker.get("lastPrice", "—")
    volume_24h = ticker.get("turnover24h", "—")

    text = (
        f"Diag BTCUSDT:\n"
        f"Server time: {server_time}\n"
        f"Last price: {last_price}\n"
        f"24h Vol: {volume_24h}"
    )
    await message.answer(text)

# --- заглушки для кнопок ---
@dp.message(F.text == "📊 Активность")
async def btn_activity(message: Message):
    await message.answer("🔥 Активность\nНет данных (тихо/таймаут/лимиты).")

@dp.message(F.text == "⚡ Волатильность")
async def btn_vol(message: Message):
    await message.answer("⚡ Волатильность\nНет данных.")

@dp.message(F.text == "📈 Тренд")
async def btn_trend(message: Message):
    await message.answer("📈 Тренд\nНет данных.")

@dp.message(F.text == "🫧 Bubbles")
async def btn_bubbles(message: Message):
    await message.answer("🫧 Bubbles\nНет данных.")

@dp.message(F.text == "📰 Новости")
async def btn_news(message: Message):
    await message.answer("📰 Макро (последний час)\n• CPI (US) 3.1% vs 3.2% прогноз — риск-он")

@dp.message(F.text == "🧮 Калькулятор")
async def btn_calc(message: Message):
    await message.answer("Шаблон риск-менеджмента")

@dp.message(F.text == "⭐ Watchlist")
async def btn_watchlist(message: Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

@dp.message(F.text == "⚙️ Настройки")
async def btn_settings(message: Message):
    text = (
        "⚙️ Настройки\n"
        "Биржа: Bybit (USDT perp)\n"
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
    await message.answer(text)

# /menu
@dp.message(F.text == "/menu")
async def cmd_menu(message: Message):
    await message.answer("Меню восстановлено.", reply_markup=main_menu())

# health endpoint (для Render)
from aiohttp import web
async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": "v0.9-beta"})

app = web.Application()
app.router.add_get("/health", handle_health)

if __name__ == "__main__":
    import asyncio
    from aiogram.webhook.aiohttp_server import setup_application

    async def main():
        dp.include_router(dp)  # регистрируем роутеры
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 10000)
        await site.start()
        print("Health endpoint running on /health")

        await dp.start_polling(bot)

    asyncio.run(main())
