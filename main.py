import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiohttp import ClientSession

# Логирование
logging.basicConfig(level=logging.INFO)

# Переменные окружения
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_KEY = os.getenv("BYBIT_KEY")
BYBIT_SECRET = os.getenv("BYBIT_SECRET")
BYBIT_HOST = os.getenv("BYBIT_HOST", "https://api.bybit.com")

# Инициализация бота и диспетчера
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

# HTTP headers
HTTP_HEADERS = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

# Главное меню
main_kb = ReplyKeyboardMarkup(
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
        "🧭 <b>Market mood</b>\nBTC.D: — | Funding avg: — | F&G: —\n\n"
        "Добро пожаловать в <b>Innertrade Screener</b> v0.9-beta (Bybit).",
        reply_markup=main_kb
    )

# 📊 Активность
@dp.message(F.text == "📊 Активность")
async def activity(message: Message):
    async with ClientSession() as session:
        url = f"{BYBIT_HOST}/v5/market/tickers?category=linear&symbol=BTCUSDT"
        async with session.get(url, headers=HTTP_HEADERS) as resp:
            data = await resp.json()
    if "result" in data and "list" in data["result"]:
        ticker = data["result"]["list"][0]
        text = (
            f"📊 <b>Активность</b>\n"
            f"Symbol: {ticker['symbol']}\n"
            f"Last Price: {ticker['lastPrice']}\n"
            f"24h Volume: {ticker['turnover24h']}"
        )
    else:
        text = "📊 Активность\nНет данных (API пустой)."
    await message.answer(text)

# ⚡ Волатильность
@dp.message(F.text == "⚡ Волатильность")
async def volatility(message: Message):
    await message.answer("⚡ Волатильность\n(Заглушка — данные будут позже).")

# 📈 Тренд
@dp.message(F.text == "📈 Тренд")
async def trend(message: Message):
    await message.answer("📈 Тренд\n(Заглушка — данные будут позже).")

# 🫧 Bubbles
@dp.message(F.text == "🫧 Bubbles")
async def bubbles(message: Message):
    await message.answer("🫧 Bubbles\n(Заглушка — инфографика позже).")

# 📰 Новости
@dp.message(F.text == "📰 Новости")
async def news(message: Message):
    await message.answer("📰 Макро (последний час)\n• CPI (US) 3.1% vs 3.2% прогноз — риск-он")

# 🧮 Калькулятор
@dp.message(F.text == "🧮 Калькулятор")
async def calc(message: Message):
    await message.answer("Шаблон риск-менеджмента")

# ⭐ Watchlist
@dp.message(F.text == "⭐ Watchlist")
async def watchlist(message: Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

# ⚙️ Настройки
@dp.message(F.text == "⚙️ Настройки")
async def settings(message: Message):
    await message.answer(
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

# Health-check endpoint
from aiohttp import web
async def health(request):
    return web.Response(text='ok')

async def main():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
