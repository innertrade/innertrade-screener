import asyncio
import json
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone
from typing import Dict, Any, List

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


# ---------------------------
# CONFIG
# ---------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("innertrade")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").rstrip("/")  # e.g. https://your-app.onrender.com
TZ = os.getenv("TZ", "MSK")
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear").strip()

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE:
    raise RuntimeError("WEBHOOK_BASE is required")

# last 10 majors (перечень можно менять)
SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT",
]

# ---------------------------
# RUNTIME STATE
# ---------------------------
router = Router()
dp = Dispatcher()
dp.include_router(router)

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)

# in-memory кэш с WS
ws_state: Dict[str, Dict[str, Any]] = {
    # "BTCUSDT": {"last": 0.0, "pcnt24h": 0.0, "turnover24h": 0.0, "ts": 0}
}
ws_ok: bool = False
ws_err: str | None = None
ws_task: asyncio.Task | None = None


# ---------------------------
# KEYBOARDS / HELPERS
# ---------------------------
def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def fmt_mood_header() -> str:
    # заглушка рыночного настроения (как и прежде, статично)
    return "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n"


def top_by(key: str, reverse: bool = True) -> List[tuple[str, Dict[str, Any]]]:
    items = list(ws_state.items())
    items.sort(key=lambda kv: (kv[1].get(key) or 0.0), reverse=reverse)
    return items[:10]


# ---------------------------
# HANDLERS
# ---------------------------
@router.message(CommandStart())
async def on_start(message: Message):
    text = (
        f"{fmt_mood_header()}\n"
        f"Добро пожаловать в Innertrade Screener v0.9.7-webhook-ws (Bybit WS)."
    )
    await message.answer(text, reply_markup=main_kb())


@router.message(Command("status"))
async def on_status(message: Message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S (UTC)")
    text = (
        "Status\n"
        f"Time: {now}\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        "Version: v0.9.7-webhook-ws\n"
        f"Bybit WS: {BYBIT_WS}\n"
    )
    await message.answer(text)


@router.message(Command("diag"))
async def on_diag(message: Message):
    global ws_ok, ws_err
    sym_count = len(ws_state)
    text = (
        "diag\n"
        f"ws_ok={ws_ok} | ws_err={ws_err}\n"
        f"symbols_cached={sym_count}\n"
    )
    await message.answer(text)


@router.message(F.text == "📊 Активность")
async def on_activity(message: Message):
    # Используем turnover24h (оборот) как прокси «активности»
    header = fmt_mood_header()
    if not ws_state:
        await message.answer(header + "\n🔥 Активность\nНет данных (WS пусто).")
        return

    lines = ["🔥 Активность (Bybit WS)"]
    for i, (sym, r) in enumerate(top_by("turnover24h", True), start=1):
        t = r.get("turnover24h")
        pcnt = r.get("pcnt24h")
        t_txt = f"{int(t):,}".replace(",", " ") if t else "—"
        pcnt_txt = f"{pcnt:.2f}" if pcnt is not None else "—"
        lines.append(f"{i}) {sym}  24h% {pcnt_txt}  | turnover24h ~ {t_txt}")
    await message.answer(header + "\n" + "\n".join(lines))


@router.message(F.text == "⚡ Волатильность")
async def on_vol(message: Message):
    header = fmt_mood_header()
    if not ws_state:
        await message.answer(header + "\n⚡ Волатильность\nНет данных.")
        return

    # как «волатильность» — берём modulus 24h% по убыванию
    items = list(ws_state.items())
    items.sort(key=lambda kv: abs(kv[1].get("pcnt24h") or 0.0), reverse=True)

    lines = ["⚡ Волатильность (24h %, Bybit WS)"]
    for i, (sym, r) in enumerate(items[:10], start=1):
        pcnt = r.get("pcnt24h")
        last = r.get("last")
        pcnt_txt = f"{pcnt:.2f}" if pcnt is not None else "—"
        last_txt = f"{last}" if last is not None else "—"
        lines.append(f"{i}) {sym}  24h% {pcnt_txt}  | last {last_txt}")
    await message.answer(header + "\n" + "\n".join(lines))


@router.message(F.text == "📈 Тренд")
async def on_trend(message: Message):
    header = fmt_mood_header()
    if not ws_state:
        await message.answer(header + "\n📈 Тренд\nНет данных.")
        return

    # «упрощённо по 24h%»
    lines = ["📈 Тренд (упрощённо по 24h%, Bybit WS)"]
    for i, (sym, r) in enumerate(top_by("pcnt24h", True), start=1):
        pcnt = r.get("pcnt24h")
        last = r.get("last")
        pcnt_txt = f"{pcnt:.2f}" if pcnt is not None else "—"
        last_txt = f"{last}" if last is not None else "—"
        lines.append(f"{i}) {sym}  ≈  24h% {pcnt_txt}  | last {last_txt}")
    await message.answer(header + "\n" + "\n".join(lines))


@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(message: Message):
    header = fmt_mood_header()
    await message.answer(header + "\nWS Bubbles (24h %, size~turnover24h)")


@router.message(F.text == "📰 Новости")
async def on_news(message: Message):
    header = fmt_mood_header()
    await message.answer(header + "\n📰 Макро (последний час)\n• demo headline")


@router.message(F.text == "🧮 Калькулятор")
async def on_calc(message: Message):
    await message.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")


@router.message(F.text == "⭐ Watchlist")
async def on_watch(message: Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")


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
    await message.answer(text)


@router.message(Command("menu"))
async def on_menu(message: Message):
    await message.answer("Клавиатура восстановлена.", reply_markup=main_kb())


# ---------------------------
# BYBIT WS BACKGROUND
# ---------------------------
async def ws_worker():
    """
    Подключаемся к Bybit Public WS и подписываемся на tickers по SYMBOLS.
    Обновляем ws_state по мере прихода сообщений.
    """
    global ws_ok, ws_err
    while True:
        ws_ok = False
        ws_err = None
        try:
            timeout = aiohttp.ClientTimeout(total=None, connect=20)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                log.info(f"Bybit WS connecting: {BYBIT_WS}")
                async with sess.ws_connect(BYBIT_WS, heartbeat=25) as ws:
                    # подписка на тикеры
                    topics = [f"tickers.{s}" for s in SYMBOLS]
                    sub = {"op": "subscribe", "args": topics}
                    await ws.send_json(sub)
                    log.info(f"WS subscribed: {len(topics)} topics")
                    ws_ok = True

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json(loads=json.loads)
                            topic = data.get("topic")
                            if not topic:
                                continue
                            if topic.startswith("tickers."):
                                # схема Bybit v5 public/tickers
                                # payload в data["data"] — словарь
                                d = data.get("data") or {}
                                sym = d.get("symbol")
                                last = d.get("lastPrice")
                                pcnt = d.get("price24hPcnt")
                                turn = d.get("turnover24h")
                                # преобразования
                                try:
                                    last_f = float(last) if last is not None else None
                                except Exception:
                                    last_f = None
                                try:
                                    pcnt_f = float(pcnt) * 100.0 if pcnt is not None else None
                                except Exception:
                                    pcnt_f = None
                                try:
                                    turn_f = float(turn) if turn is not None else None
                                except Exception:
                                    turn_f = None

                                if sym:
                                    ws_state[sym] = {
                                        "last": last_f,
                                        "pcnt24h": pcnt_f,
                                        "turnover24h": turn_f,
                                        "ts": data.get("ts") or 0,
                                    }
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            raise RuntimeError(f"WS closed/error: {msg.type}")
        except asyncio.CancelledError:
            log.info("WS worker cancelled")
            raise
        except Exception as e:
            ws_err = str(e)
            log.error(f"WS error: {e}")
            await asyncio.sleep(5)  # backoff и повтор
        finally:
            ws_ok = False


# ---------------------------
# AIOHTTP APP / WEBHOOK
# ---------------------------
def build_app() -> web.Application:
    app = web.Application()

    # health
    async def handle_health(_: web.Request):
        return web.json_response({"ok": True, "service": "innertrade-screener", "version": "v0.9.7-webhook-ws", "ws_ok": ws_ok, "ws_err": ws_err})

    app.router.add_get("/", lambda _: web.json_response({"ok": True, "root": True}))
    app.router.add_get("/health", handle_health)

    # webhook path — делаем как /webhook/<last8 токена> чтобы не светить весь токен в логах
    token_suffix = TELEGRAM_TOKEN[-8:]
    webhook_path = f"/webhook/{token_suffix}"

    # aiogram webhook handler
    req_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    req_handler.register(app, path=webhook_path)
    setup_application(app, dp, bot=bot)

    # старты/остановки
    async def on_startup(_):
        # ставим вебхук
        url = f"{WEBHOOK_BASE}{webhook_path}"
        await bot.set_webhook(url)
        log.info(f"Webhook set to {url}")

        # запускаем WS
        global ws_task
        ws_task = asyncio.create_task(ws_worker())

    async def on_cleanup(_):
        # снимаем вебхук (не обязательно, но аккуратно)
        with suppress(Exception):
            await bot.delete_webhook(drop_pending_updates=False)

        # останавливаем WS
        global ws_task
        if ws_task and not ws_task.done():
            ws_task.cancel()
            with suppress(asyncio.CancelledError):
                await ws_task

        # закрываем сессию бота
        await bot.session.close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def serve():
    app = build_app()
    port = int(os.getenv("PORT", "10000"))
    web.run_app(app, host="0.0.0.0", port=port)


# ---------------------------
# ENTRY
# ---------------------------
if __name__ == "__main__":
    serve()
