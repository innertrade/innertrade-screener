import os
import aiohttp
import asyncio
import logging
import pytz
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import hmac
import hashlib
import time

# ---------------------------
# Настройки
# ---------------------------
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_HOST = os.getenv("BYBIT_HOST", "https://api.bybit.com")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
TZ = os.getenv("TZ", "Europe/Moscow")

bot = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

HTTP_HEADERS = {"User-Agent": "InnertradeScreener/0.9 (+render.com)"}


# ---------------------------
# Вспомогательные
# ---------------------------
def _sign(params: dict) -> dict:
    """Подпись для приватных запросов Bybit"""
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        return params
    ts = str(int(time.time() * 1000))
    params["api_key"] = BYBIT_API_KEY
    params["timestamp"] = ts
    sorted_params = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    sign = hmac.new(BYBIT_API_SECRET.encode(), sorted_params.encode(), hashlib.sha256).hexdigest()
    params["sign"] = sign
    return params


async def fetch_json(session, url, params=None, private=False):
    try:
        if private and BYBIT_API_KEY and BYBIT_API_SECRET:
            params = _sign(params or {})
        async with session.get(url, params=params, headers=HTTP_HEADERS, timeout=10) as r:
            return await r.json()
    except Exception as e:
        logging.error(f"fetch_json error {url}: {e}")
        return None


# ---------------------------
# Хэндлеры
# ---------------------------
@dp.message(commands=["start"])
async def cmd_start(message: types.Message):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📊 Активность"), KeyboardButton("⚡ Волатильность"))
    kb.add(KeyboardButton("📈 Тренд"), KeyboardButton("🫧 Bubbles"))
    kb.add(KeyboardButton("📰 Новости"), KeyboardButton("🧮 Калькулятор"))
    kb.add(KeyboardButton("⭐ Watchlist"), KeyboardButton("⚙️ Настройки"))

    await message.answer(
        f"🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в Innertrade Screener v0.9-with-keys (Bybit).",
        reply_markup=kb,
    )


@dp.message(commands=["diag"])
async def cmd_diag(message: types.Message):
    async with aiohttp.ClientSession() as session:
        # свечи BTCUSDT
        kline = await fetch_json(
            session,
            f"{BYBIT_HOST}/v5/market/kline",
            params={"symbol": "BTCUSDT", "interval": "5", "limit": "2"},
            private=True,
        )
        # тикер BTCUSDT
        ticker = await fetch_json(
            session,
            f"{BYBIT_HOST}/v5/market/tickers",
            params={"category": "linear", "symbol": "BTCUSDT"},
            private=True,
        )

    await message.answer(
        f"Diag BTCUSDT:\n"
        f"kline: {kline if kline else 'None'}\n\n"
        f"ticker: {ticker if ticker else 'None'}"
    )


@dp.message(commands=["status"])
async def cmd_status(message: types.Message):
    now = datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M:%S")
    await message.answer(
        f"Status\nTime: {now}\nMode: active | Quiet: False\nSource: Bybit\n"
        f"Webhook: ON\nVersion: v0.9-with-keys\nHost: {BYBIT_HOST}\n"
        f"API: {'ON' if BYBIT_API_KEY else 'OFF'}"
    )


# ---------------------------
# Запуск
# ---------------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
