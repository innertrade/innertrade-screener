# main.py
import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import pytz
from aiohttp import web, ClientSession, WSMsgType

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# =======================
# Конфигурация из ENV
# =======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").strip()  # например: https://innertrade-screener-bot.onrender.com
TZ = os.getenv("TZ", "Europe/Moscow")
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")

# Версия и эмодзи
BOT_VERSION = "v0.9.1-webhook-ws"
MOOD_LINE = "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)"

if not TELEGRAM_TOKEN:
    raise RuntimeError("ENV TELEGRAM_TOKEN is required")
if not BASE_URL or not BASE_URL.startswith("http"):
    raise RuntimeError("ENV BASE_URL is required and must start with http(s)")

# =======================
# Логирование
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# =======================
# Глобальные состояния
# =======================
router = Router()

# Кеш данных из публичного WS Bybit
ws_state: Dict[str, Any] = {
    "ok": False,
    "err": None,
    "tickers": {},     # symbol -> {lastPrice, price24hPcnt, turnover24h, ...}
    "kline_5m": {},    # symbol -> list of ohlc rows (по желанию потом наполним)
    "symbols": [],     # список символов, которые мы показали в последнем срезе
}

# набор по умолчанию (топ USDT linear; можно расширять)
DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT",
]

HTTP_HEADERS = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}


def now_tz() -> str:
    try:
        tz = pytz.timezone(TZ)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S (%Z)")


# =======================
# Клавиатура
# =======================
def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


# =======================
# Рендеры сообщений
# =======================
def render_activity() -> str:
    # сортируем по turnover24h (обороту) по убыванию
    items = list(ws_state["tickers"].items())
    items.sort(key=lambda kv: float(kv[1].get("turnover24h", 0) or 0), reverse=True)
    top = items[:10] if items else []
    if not top:
        return f"{MOOD_LINE}\n\n🔥 Активность\nНет данных (WS пусто)."

    lines = [MOOD_LINE, "", "🔥 Активность (Bybit WS)"]
    for i, (sym, data) in enumerate(top, 1):
        pct = data.get("price24hPcnt")
        last = data.get("lastPrice")
        t24 = data.get("turnover24h")
        try:
            pct_disp = f"{float(pct)*100:.2f}" if pct is not None else "0.00"
        except Exception:
            pct_disp = "0.00"
        try:
            t24_disp = f"{float(t24):,.0f}".replace(",", " ")
        except Exception:
            t24_disp = "0"
        lines.append(f"{i}) {sym}  24h% {pct_disp}  | turnover24h ~ {t24_disp}")
    return "\n".join(lines)


def render_volatility() -> str:
    # упрощённо: сорт по |24h%|
    items = list(ws_state["tickers"].items())
    def abs_pct(v):  # безопасно берём абсолют 24h%
        p = v[1].get("price24hPcnt")
        try:
            return abs(float(p) * 100.0)
        except Exception:
            return 0.0
    items.sort(key=abs_pct, reverse=True)
    top = items[:10] if items else []
    if not top:
        return f"{MOOD_LINE}\n\n⚡ Волатильность\nНет данных."

    lines = [MOOD_LINE, "", "⚡ Волатильность (24h %, Bybit WS)"]
    for i, (sym, data) in enumerate(top, 1):
        try:
            pct = float(data.get("price24hPcnt", 0)) * 100.0
            last = float(data.get("lastPrice", 0))
            lines.append(f"{i}) {sym}  24h% {pct:+.2f}  | last {last}")
        except Exception:
            lines.append(f"{i}) {sym}  —")
    return "\n".join(lines)


def render_trend() -> str:
    # упрощённо: просто вывести top по 24h%
    items = list(ws_state["tickers"].items())
    items.sort(key=lambda kv: float(kv[1].get("price24hPcnt", 0) or 0), reverse=True)
    top = items[:10] if items else []
    if not top:
        return f"{MOOD_LINE}\n\n📈 Тренд\nНет данных."

    lines = [MOOD_LINE, "", "📈 Тренд (упрощённо по 24h%, Bybit WS)"]
    for i, (sym, data) in enumerate(top, 1):
        try:
            pct = float(data.get("price24hPcnt", 0)) * 100.0
            last = float(data.get("lastPrice", 0))
            approx = "≈"
            lines.append(f"{i}) {sym}  {approx}  24h% {pct:+.2f}  | last {last}")
        except Exception:
            lines.append(f"{i}) {sym}  —")
    return "\n".join(lines)


def render_bubbles() -> str:
    # пока только заглушка текста (графику можно будет рисовать позже)
    return "WS Bubbles (24h %, size~turnover24h)"


def render_status() -> str:
    return (
        "Status\n"
        f"Time: {now_tz()}\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        f"Version: {BOT_VERSION}\n"
        f"Bybit WS: {BYBIT_WS}\n"
    )


def render_diag() -> str:
    ok = ws_state["ok"]
    err = ws_state["err"]
    count = len(ws_state["tickers"])
    return f"diag\nws_ok={ok} | ws_err={err}\nsymbols_cached={count}"


# =======================
# Хэндлеры команд
# =======================
@router.message(Command("start"))
@router.message(Command("menu"))
async def cmd_start(message: Message):
    await message.answer(
        f"{MOOD_LINE}\n\nДобро пожаловать в Innertrade Screener {BOT_VERSION} (Bybit WS).",
        reply_markup=main_keyboard(),
    )

@router.message(Command("status"))
async def cmd_status(message: Message):
    await message.answer(render_status(), reply_markup=main_keyboard())

@router.message(Command("diag"))
async def cmd_diag(message: Message):
    await message.answer(render_diag(), reply_markup=main_keyboard())


# =======================
# Хэндлеры кнопок (тексты)
# =======================
@router.message(F.text == "📊 Активность")
async def on_activity(message: Message):
    await message.answer(render_activity(), reply_markup=main_keyboard())

@router.message(F.text == "⚡ Волатильность")
async def on_volatility(message: Message):
    await message.answer(render_volatility(), reply_markup=main_keyboard())

@router.message(F.text == "📈 Тренд")
async def on_trend(message: Message):
    await message.answer(render_trend(), reply_markup=main_keyboard())

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(message: Message):
    await message.answer(MOOD_LINE)
    await message.answer(render_bubbles(), reply_markup=main_keyboard())

@router.message(F.text == "📰 Новости")
async def on_news(message: Message):
    # Пока демо, потом подключим реальные источники или webhook
    await message.answer(
        f"{MOOD_LINE}\n\n📰 Макро (последний час)\n• demo headline",
        reply_markup=main_keyboard(),
    )

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(message: Message):
    await message.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).", reply_markup=main_keyboard())

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(message: Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=main_keyboard())

@router.message(F.text == "⚙️ Настройки")
async def on_settings(message: Message):
    text = (
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
        "• /menu        — восстановить клавиатуру\n"
    )
    await message.answer(text, reply_markup=main_keyboard())


# =======================
# Публичный WS Bybit
# =======================
async def bybit_ws_consumer():
    """
    Подключается к BYBIT_WS и подписывается на tickers для DEFAULT_SYMBOLS.
    Заполняет ws_state["tickers"] данными с поля tickers.
    """
    topics = [f"tickers.{sym}" for sym in DEFAULT_SYMBOLS]
    sub_msg = {"op": "subscribe", "args": topics}

    while True:
        try:
            async with ClientSession(headers=HTTP_HEADERS) as sess:
                log.info(f"Bybit WS connecting: {BYBIT_WS}")
                async with sess.ws_connect(BYBIT_WS, heartbeat=15) as ws:
                    # подписка
                    await ws.send_str(json.dumps(sub_msg))
                    log.info(f"WS subscribed: {len(topics)} topics")
                    ws_state["ok"] = True
                    ws_state["err"] = None

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # формат tickers: {"topic":"tickers.BTCUSDT","data":{"symbol":"BTCUSDT", ...}}
                            topic = data.get("topic")
                            if topic and topic.startswith("tickers."):
                                payload = data.get("data") or {}
                                sym = payload.get("symbol")
                                if sym:
                                    # извлекаем безопасно интересующие поля
                                    last = payload.get("lastPrice")
                                    p24 = payload.get("price24hPcnt")
                                    t24 = payload.get("turnover24h")
                                    ws_state["tickers"][sym] = {
                                        "lastPrice": last,
                                        "price24hPcnt": p24,
                                        "turnover24h": t24,
                                    }
                                    if sym not in ws_state["symbols"]:
                                        ws_state["symbols"].append(sym)
                        elif msg.type == WSMsgType.ERROR:
                            raise RuntimeError(f"WS error: {ws.exception()}")
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE):
                            raise RuntimeError("WS closed by server")
        except Exception as e:
            ws_state["ok"] = False
            ws_state["err"] = str(e)
            log.exception(f"WS loop error: {e}")
            await asyncio.sleep(3.0)  # backoff и повторить


# =======================
# Aiohttp app + Webhook
# =======================
def build_app(bot: Bot, dp: Dispatcher) -> web.Application:
    app = web.Application()

    # Health endpoints (чтобы Render и мы могли проверять живость)
    async def health_ok(request: web.Request):
        return web.Response(text="ok", content_type="text/plain")

    app.router.add_get("/", health_ok)
    app.router.add_get("/health", health_ok)

    # Регистрируем webhook-хэндлер
    token_prefix = TELEGRAM_TOKEN.split(":", 1)[0]
    WEBHOOK_PATH = f"/webhook/{token_prefix}"

    wh = SimpleRequestHandler(dispatcher=dp, bot=bot)
    wh.register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)
    return app


async def on_startup(bot: Bot, dp: Dispatcher):
    # Устанавливаем вебхук
    token_prefix = TELEGRAM_TOKEN.split(":", 1)[0]
    webhook_url = f"{BASE_URL}/webhook/{token_prefix}"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)
    log.info(f"Webhook set to {webhook_url}")

    # Запускаем задачу для WS
    asyncio.create_task(bybit_ws_consumer())


async def on_shutdown(bot: Bot, dp: Dispatcher):
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass


async def main():
    # Инициализация бота/диспетчера
    bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)

    # Хуки старта/остановки
    dp.startup.register(lambda: on_startup(bot, dp))
    dp.shutdown.register(lambda: on_shutdown(bot, dp))

    # Aiohttp приложение
    app = build_app(bot, dp)

    # Стартуем HTTP-сервер (Render слушает PORT)
    port = int(os.getenv("PORT", "10000"))
    log.info(f"App starting on 0.0.0.0:{port}")
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("App stopped")
