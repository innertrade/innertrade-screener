import os
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.client.default import DefaultBotProperties

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME", "localhost")

# === Bot/Dispatcher ===
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

# === Routers ===
router = Router()

@router.message(F.text == "/start")
async def cmd_start(message: Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True
    )
    await message.answer(
        "🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в Innertrade Screener v0.8.2-webhook (Bybit).",
        reply_markup=kb
    )

@router.message(F.text == "/status")
async def cmd_status(message: Message):
    await message.answer("✅ Bot online (webhook).")

dp.include_router(router)

# === Webhook ===
WEBHOOK_PATH = f"/webhook/{TELEGRAM_TOKEN}"
WEBHOOK_URL = f"https://{HOSTNAME}{WEBHOOK_PATH}"

app = web.Application()

async def health(request):
    return web.Response(text="ok")

app.router.add_get("/health", health)

async def on_startup(bot: Bot):
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

async def main():
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    await on_startup(bot)
    print(f"🚀 Webhook set: {WEBHOOK_URL}")

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
