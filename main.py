# main.py
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import pytz
from aiohttp import web, ClientSession, ClientConnectorError, WSServerHandshakeError

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

# --- ЛОГИ ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("innertrade.main")

# --- ОКРУЖЕНИЕ ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear").strip()
PORT = int(os.getenv("PORT", "10000"))

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

# если WEBHOOK_PATH не задан — делаем «/webhook/<последние 16 символов токена>»
if not WEBHOOK_PATH:
    tail = TELEGRAM_TOKEN[-16:].replace(":", "_")
    WEBHOOK_PATH = f"/webhook/{tail}"
elif not WEBHOOK_PATH.startswith("/"):
    # нормализуем, чтобы aiohttp не ругался «path should be started with /»
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

APP_VERSION = "v0.9.0-webhook-ws"

# --- ВРЕМЯ ---
try:
    TZ = pytz.timezone(TZ_NAME)
except Exception:
    TZ = pytz.timezone("Europe/Moscow")

def now_local() -> str:
    dt = datetime.now(timezone.utc).astimezone(TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S (%Z)")

# --- ХРАНИЛКА ДАННЫХ (in-memory) ---
class DataStore:
    """
    Простейший кэш данных от публичного Bybit WS:
      - symbol -> {last: float, price24hPcnt: float, turnover24h: float}
    """
    def __init__(self) -> None:
        self._symbols: Dict[str, Dict[str, Any]] = {}
        self._last_ws_ok: bool = False
        self._last_ws_err: Optional[str] = None

    def update_ticker(self, data: Dict[str, Any]) -> None:
        symbol = str(data.get("symbol", "")).upper()
        if not symbol:
            return
        last_price = float(data.get("lastPrice", 0) or 0)
        # Bybit в tickers присылает price24hPcnt в долях (например -0.0123 == -1.23%)
        pct_24h = float(data.get("price24hPcnt", 0) or 0) * 100.0
        turnover24h = float(data.get("turnover24h", 0) or 0)

        self._symbols[symbol] = {
            "last": last_price,
            "pct24h": pct_24h,
            "turnover24h": turnover24h,
        }

    def top_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        # Сортируем по обороту за 24h (по убыванию)
        rows = [
            {"symbol": s, **v} for s, v in self._symbols.items()
            if v.get("turnover24h", 0) > 0
        ]
        rows.sort(key=lambda r: r["turnover24h"], reverse=True)
        return rows[:limit]

    def top_volatility(self, limit: int = 10) -> List[Dict[str, Any]]:
        # Здесь используем |24h%| как прокси-волатильности (упрощение)
        rows = [
            {"symbol": s, **v} for s, v in self._symbols.items()
        ]
        rows.sort(key=lambda r: abs(r.get("pct24h", 0)), reverse=True)
        return rows[:limit]

    def top_trend(self, limit: int = 10) -> List[Dict[str, Any]]:
        # Положительный тренд по 24h%, убывающе
        rows = [
            {"symbol": s, **v} for s, v in self._symbols.items()
        ]
        rows.sort(key=lambda r: r.get("pct24h", 0), reverse=True)
        return rows[:limit]

    def diag(self) -> Dict[str, Any]:
        return {
            "count": len(self._symbols),
            "ws_ok": self._last_ws_ok,
            "ws_err": self._last_ws_err,
        }

    def set_ws_ok(self, ok: bool, err: Optional[str] = None) -> None:
        self._last_ws_ok = ok
        self._last_ws_err = err

STORE = DataStore()

# --- TELEGRAM ---
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- КЛАВИАТУРА ---
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="🔄 /menu")],
    ],
    resize_keyboard=True,
)

# --- ОТВЕТЫ/РЕНДЕРЫ ---
def render_header() -> str:
    return "🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n"

def render_activity(rows: List[Dict[str, Any]], source: str) -> str:
    if not rows:
        return f"{render_header()}\n🔥 <b>Активность</b>\nНет данных (тихо/таймаут/лимиты).\n"
    lines = [render_header(), f"🔥 <b>Активность</b> ({source})"]
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}) {r['symbol']}  24h% {r['pct24h']:+.2f}  | turnover24h ~ {int(r['turnover24h']):,}".replace(",", " "))
    return "\n".join(lines)

def render_volatility(rows: List[Dict[str, Any]], source: str) -> str:
    if not rows:
        return f"{render_header()}\n⚡ <b>Волатильность</b>\nНет данных.\n"
    lines = [render_header(), f"⚡ <b>Волатильность</b> (24h %, {source})"]
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}) {r['symbol']}  24h% {r['pct24h']:+.2f}  | last {r['last']}")
    return "\n".join(lines)

def render_trend(rows: List[Dict[str, Any]], source: str) -> str:
    if not rows:
        return f"{render_header()}\n📈 <b>Тренд</b>\nНет данных.\n"
    lines = [render_header(), f"📈 <b>Тренд</b> (упрощённо по 24h%, {source})"]
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}) {r['symbol']}  ≈  24h% {r['pct24h']:+.2f}  | last {r['last']}")
    return "\n".join(lines)

# --- ХЕНДЛЕРЫ ---
@router.message(CommandStart())
async def on_start(m: Message):
    text = (
        f"{render_header()}\n"
        f"Добро пожаловать в Innertrade Screener {APP_VERSION} (Bybit WS).\n"
    )
    await m.answer(text, reply_markup=MAIN_KB)

@router.message(Command("menu"))
async def on_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=MAIN_KB)

@router.message(Command("status"))
async def on_status(m: Message):
    src = "Bybit (public WS)"
    text = (
        "Status\n"
        f"Time: {now_local()}\n"
        "Mode: active | Quiet: False\n"
        f"Source: {src}\n"
        f"Version: {APP_VERSION}\n"
        f"Bybit WS: {BYBIT_WS}\n"
    )
    await m.answer(text, reply_markup=MAIN_KB)

@router.message(Command("diag"))
async def on_diag(m: Message):
    d = STORE.diag()
    text = (
        "diag\n"
        f"ws_ok={d['ws_ok']} | ws_err={d['ws_err']}\n"
        f"symbols_cached={d['count']}\n"
    )
    await m.answer(text, reply_markup=MAIN_KB)

@router.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    rows = STORE.top_activity(10)
    source = "Bybit WS"
    if not rows:
        await m.answer(f"{render_header()}\n🔥 <b>Активность</b>\nПодбираю данные (WS)…", reply_markup=MAIN_KB)
    else:
        await m.answer(render_activity(rows, source), reply_markup=MAIN_KB)

@router.message(F.text == "⚡ Волатильность")
async def on_volatility(m: Message):
    rows = STORE.top_volatility(10)
    source = "Bybit WS"
    if not rows:
        await m.answer(f"{render_header()}\n⚡ <b>Волатильность</b>\nПодбираю данные (WS)…", reply_markup=MAIN_KB)
    else:
        await m.answer(render_volatility(rows, source), reply_markup=MAIN_KB)

@router.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    rows = STORE.top_trend(10)
    source = "Bybit WS"
    if not rows:
        await m.answer(f"{render_header()}\n📈 <b>Тренд</b>\nПодбираю данные…", reply_markup=MAIN_KB)
    else:
        await m.answer(render_trend(rows, source), reply_markup=MAIN_KB)

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    rows = STORE.top_activity(10)
    if not rows:
        await m.answer(f"{render_header()}\n🫧 <b>Bubbles</b>\nСобираю WS-тикеры…", reply_markup=MAIN_KB)
    else:
        await m.answer("WS Bubbles (24h %, size~turnover24h)", reply_markup=MAIN_KB)

@router.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer(
        f"{render_header()}\n📰 <b>Макро (последний час)</b>\n• demo headline",
        reply_markup=MAIN_KB,
    )

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).", reply_markup=MAIN_KB)

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=MAIN_KB)

@router.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
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
    await m.answer(text, reply_markup=MAIN_KB)

# --- WS КОНСЬЮМЕР BYBIT ---
SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","LINKUSDT","TRXUSDT","TONUSDT",
]

async def ws_consumer():
    """
    Подключаемся к Bybit public WS и подписываемся на тикеры.
    endpoint задаётся через BYBIT_WS (по умолчанию linear perp публичный).
    """
    args = [f"tickers.{s}" for s in SYMBOLS]
    subscribe = {"op": "subscribe", "args": args}

    backoff = 1
    while True:
        try:
            async with ClientSession() as sess:
                log.info(f"WS connect: {BYBIT_WS}")
                async with sess.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                    await ws.send_str(json.dumps(subscribe))
                    STORE.set_ws_ok(True, None)
                    backoff = 1
                    log.info("WS subscribed to tickers")

                    async for msg in ws:
                        if msg.type == web.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                            except Exception:
                                # иногда приходят pings/pongs как текст
                                continue
                            topic = data.get("topic", "")
                            if topic.startswith("tickers."):
                                # Bybit v5 tickers: data -> list[ dict ]
                                payload = data.get("data")
                                if isinstance(payload, dict):
                                    STORE.update_ticker(payload)
                                elif isinstance(payload, list):
                                    for row in payload:
                                        if isinstance(row, dict):
                                            STORE.update_ticker(row)
                        elif msg.type == web.WSMsgType.ERROR:
                            err = ws.exception()
                            raise err if err else RuntimeError("WS unknown error")
        except (ClientConnectorError, WSServerHandshakeError) as e:
            log.warning(f"WS connect/handshake failed: {e}")
            STORE.set_ws_ok(False, str(e))
        except asyncio.CancelledError:
            log.info("WS consumer cancelled")
            raise
        except Exception as e:
            log.exception(f"WS loop error: {e}")
            STORE.set_ws_ok(False, str(e))

        # backoff
        await asyncio.sleep(min(backoff, 30))
        backoff = min(backoff * 2, 30)

# --- AIOHTTP APP + ВЕБХУК ---
async def on_health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": APP_VERSION})

async def on_startup(app: web.Application):
    # Устанавливаем вебхук
    webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(webhook_url)
    log.info(f"Webhook set: {webhook_url}")

    # Стартуем WS-консьюмера
    app["ws_task"] = app.loop.create_task(ws_consumer())

async def on_cleanup(app: web.Application):
    # Снимаем вебхук (не обязательно на Render, но корректно)
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass
    # Останавливаем WS-таск
    task = app.get("ws_task")
    if task and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", on_health)

    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

    wh = SimpleRequestHandler(dispatcher=dp, bot=bot)
    wh.register(app, path=WEBHOOK_PATH)  # path уже начинается с "/"

    setup_application(app, dp, bot=bot)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

# --- ENTRYPOINT ---
if __name__ == "__main__":
    import contextlib
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
