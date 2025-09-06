import os
import asyncio
import logging
import html
from aiohttp import web, ClientSession, ClientTimeout

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# --------- ENV ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
BYBIT_HOST = os.getenv("BYBIT_HOST", "https://api.bybit.com").rstrip("/")
VERSION = "v0.8.4-webhook"

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https URL, e.g. https://<service>.onrender.com")

# --------- LOG ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --------- BOT / DP / ROUTER ----------
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()

# --------- KEYBOARD ----------
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите раздел…",
    )

# --------- HANDLERS ----------
@router.message(F.text == "/start")
async def cmd_start(m: Message):
    await m.answer(
        "🧭 <b>Market mood</b>\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit).",
        reply_markup=bottom_menu()
    )

@router.message(F.text == "/menu")
async def cmd_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=bottom_menu())

@router.message(F.text == "/status")
async def cmd_status(m: Message):
    await m.answer(
        "<b>Status</b>\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public endpoints)\n"
        f"Version: {VERSION}\n"
        f"Bybit host: {BYBIT_HOST}"
    )

@router.message(F.text == "/diag")
async def cmd_diag(m: Message):
    # Быстрый пинг публичных эндпоинтов Bybit
    timeout = ClientTimeout(total=10)
    try:
        async with ClientSession(timeout=timeout) as s:
            url_ticker = f"{BYBIT_HOST}/v5/market/tickers?category=linear&symbol=BTCUSDT"
            url_kline  = f"{BYBIT_HOST}/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=5"
            headers = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

            # тикер
            st_t, cut_t = None, ""
            try:
                async with s.get(url_ticker, headers=headers) as r:
                    st_t = r.status
                    txt = await r.text()
                    cut_t = txt[:220]
            except Exception as e:
                cut_t = f"ERR {type(e).__name__}: {e}"

            # клайны
            st_k, cut_k = None, ""
            try:
                async with s.get(url_kline, headers=headers) as r:
                    st_k = r.status
                    txt = await r.text()
                    cut_k = txt[:220]
            except Exception as e:
                cut_k = f"ERR {type(e).__name__}: {e}"

        # Экранируем на всякий случай, чтобы Телеграм не пытался парсить <…>
        safe_t = html.escape(cut_t).replace("\n", " ")
        safe_k = html.escape(cut_k).replace("\n", " ")

        msg = (
            "<b>diag</b>\n"
            f"host: {BYBIT_HOST}\n"
            f"ticker: status={st_t} body[:200]=<code>{safe_t}</code>\n"
            f"kline : status={st_k} body[:200]=<code>{safe_k}</code>"
        )
        await m.answer(msg)
    except Exception as e:
        # На случай, если даже это поломается, отключим HTML в этом единственном сообщении
        fallback = f"diag failed: {type(e).__name__}: {e}"
        await m.answer(fallback, parse_mode=None)

# Кнопки — быстрые ответы-заглушки
@router.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    await m.answer("🔥 Активность\nПодбираю данные… (временная заглушка)", reply_markup=bottom_menu())

@router.message(F.text == "⚡ Волатильность")
async def on_vol(m: Message):
    await m.answer("⚡ Волатильность\nПодбираю данные… (временная заглушка)", reply_markup=bottom_menu())

@router.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    await m.answer("📈 Тренд\nПодбираю данные… (временная заглушка)", reply_markup=bottom_menu())

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("🫧 Bubbles\nСтрою инфографику… (временная заглушка)", reply_markup=bottom_menu())

@router.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer("📰 Макро (последний час)\n• demo headline", reply_markup=bottom_menu())

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (заглушка).", reply_markup=bottom_menu())

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=bottom_menu())

@router.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
        "⚙️ Настройки\nБиржа: Bybit (USDT perp)\nРежим: active | Quiet: False\nWatchlist: —\n\n"
        "Команды:\n"
        "• /add SYMBOL  — добавить (например, /add SOLUSDT)\n"
        "• /rm SYMBOL   — удалить\n"
        "• /watchlist   — показать лист\n"
        "• /passive     — автосводки/сигналы ON\n"
        "• /active      — автосводки/сигналы OFF\n"
        "• /menu        — восстановить клавиатуру",
        reply_markup=bottom_menu()
    )

dp.include_router(router)

# --------- AIOHTTP (health + webhook) ----------
async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": VERSION, "host": BYBIT_HOST})

WEBHOOK_PATH = f"/webhook/{TELEGRAM_TOKEN}"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"

app = web.Application()
app.router.add_get("/health", handle_health)

async def on_startup():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await bot.set_webhook(WEBHOOK_URL)
    logging.info(f"Webhook set to: {WEBHOOK_URL}")

async def main():
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    await on_startup()
    logging.info("HTTP server started")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
