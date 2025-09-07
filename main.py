# main.py
# Innertrade Screener — v0.9.6-webhook-ws
# aiogram v3.13 / aiohttp / Bybit v5 public WS
# Режим: webhook + публичные WS-тикеры. БД опциональна (psycopg-пул создаётся, если есть DATABASE_URL).

import os
import ssl
import json
import math
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional

import pytz
from datetime import datetime, timezone

from aiohttp import web, ClientSession, WSMsgType, ClientConnectorError

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, KeyboardButton, ReplyKeyboardMarkup,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application as setup_aiogram_app

# --- ЛОГИ ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# --- ОКРУЖЕНИЕ ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")

WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").rstrip("/")  # напр. https://innertrade-screener-bot.onrender.com
if not WEBHOOK_BASE:
    raise RuntimeError("WEBHOOK_BASE is required")

WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")  # должен начинаться с "/"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = f"/{WEBHOOK_PATH}"

PORT = int(os.getenv("PORT", "10000"))
TZ = os.getenv("TZ", "Europe/Moscow")

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
VERSION = os.getenv("APP_VERSION", "v0.9.6-webhook-ws")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()  # опционально (Neon/Postgres)
USE_DB = bool(DATABASE_URL)

# --- СИМВОЛЫ ПО УМОЛЧАНИЮ ---
DEFAULT_SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "LINKUSDT", "TRXUSDT", "TONUSDT",
]

# --- ВРЕМЕННАЯ ЗОНА ---
try:
    TZINFO = pytz.timezone(TZ)
except Exception:
    TZINFO = timezone.utc

def now_tz() -> str:
    return datetime.now(TZINFO).strftime("%Y-%m-%d %H:%M:%S (%Z)")

# --- ПУЛ БД (опционально) ---
_psycopg_pool = None

async def init_db_pool():
    """Инициализация пула соединений (если задан DATABASE_URL)."""
    global _psycopg_pool
    if not USE_DB:
        return
    try:
        # ленивое подключение
        from psycopg_pool import AsyncConnectionPool
        _psycopg_pool = AsyncConnectionPool(
            conninfo=DATABASE_URL,
            open=False,  # откроем явно
            max_size=int(os.getenv("DB_POOL_MAX", "5")),
            kwargs={"sslmode": "require"},  # для Neon
        )
        await _psycopg_pool.open()
        log.info("DB pool opened")
        # Простейшая инициализация таблиц (пример; можно расширять по потребностям)
        async with _psycopg_pool.connection() as aconn:
            async with aconn.cursor() as cur:
                await cur.execute("""
                create table if not exists ws_snapshots (
                    id bigserial primary key,
                    ts timestamptz not null default now(),
                    symbol text not null,
                    last_price double precision,
                    pct_24h double precision,
                    turnover_24h double precision
                );
                """)
    except Exception as e:
        log.exception("DB init failed: %s", e)

async def close_db_pool():
    global _psycopg_pool
    if _psycopg_pool:
        await _psycopg_pool.close()
        log.info("DB pool closed")

async def db_insert_snapshot(rows: List[Dict[str, Any]]):
    """Сохранить пачку тикеров (не критично; не мешает работе бота, если БД нет)."""
    if not _psycopg_pool or not rows:
        return
    try:
        async with _psycopg_pool.connection() as aconn:
            async with aconn.cursor() as cur:
                await cur.executemany(
                    """
                    insert into ws_snapshots(symbol, last_price, pct_24h, turnover_24h)
                    values (%(symbol)s, %(last_price)s, %(pct_24h)s, %(turnover_24h)s);
                    """,
                    rows,
                )
    except Exception as e:
        log.warning("DB insert failed (non-blocking): %s", e)

# --- КЕШ WS ---
class WsCache:
    def __init__(self):
        self.connected: bool = False
        self.err: Optional[str] = None
        self._symbols: List[str] = DEFAULT_SYMBOLS.copy()
        self._tickers: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    async def set_symbols(self, symbols: List[str]):
        async with self._lock:
            self._symbols = symbols[:]

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._runner(), name="bybit-ws-runner")

    async def stop(self):
        if self._task and not self._task.done():
            self._stop.set()
            await self._task

    async def _runner(self):
        """Петля переподключения WS с экспоненциальной паузой при сбоях."""
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                self.err = str(e)
                log.warning("WS loop error: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _connect_once(self):
        self.err = None
        session_timeout = aiohttp_client_timeout()
        async with ClientSession(timeout=session_timeout) as sess:
            log.info("Bybit WS connecting: %s", BYBIT_WS_URL)
            try:
                async with sess.ws_connect(BYBIT_WS_URL, ssl=ssl.create_default_context()) as ws:
                    self.connected = True
                    # Подписка на тикеры (Bybit v5): args = ["tickers.SYMBOL", ...]
                    args = [f"tickers.{s}" for s in self._symbols]
                    sub = {"op": "subscribe", "args": args}
                    await ws.send_json(sub)
                    log.info("WS subscribed (v5): %d tickers", len(args))

                    # Основной приём
                    while not self._stop.is_set():
                        msg = await ws.receive(timeout=30.0)
                        if msg.type == WSMsgType.TEXT:
                            await self._handle_text(msg.data)
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE):
                            raise RuntimeError("WS closed by server")
                        elif msg.type == WSMsgType.ERROR:
                            raise RuntimeError(f"WS error: {ws.exception()}")
                        else:
                            # пинги/pongs/бинары игнорим
                            pass
            except ClientConnectorError as e:
                raise RuntimeError(f"WS connect failed: {e}")
            finally:
                self.connected = False

    async def _handle_text(self, data: str):
        try:
            obj = json.loads(data)
        except Exception:
            return

        # Bybit v5 ping/pong
        if obj.get("op") == "ping":
            # мы по клиентской стороне не шлём 'ping', ок
            return
        if obj.get("op") == "pong":
            return

        topic = obj.get("topic", "")
        if topic.startswith("tickers."):
            rows = obj.get("data") or []
            if isinstance(rows, dict):
                rows = [rows]
            formatted_rows: List[Dict[str, Any]] = []
            async with self._lock:
                for it in rows:
                    symbol = it.get("symbol")
                    if not symbol:
                        continue
                    last = safe_float(it.get("lastPrice"))
                    # price24hPcnt приходит в долях (например "0.0123" == +1.23%)
                    pct = safe_float(it.get("price24hPcnt"))
                    if pct is not None:
                        pct *= 100.0
                    turn = safe_float(it.get("turnover24h"))
                    self._tickers[symbol] = {
                        "symbol": symbol,
                        "last": last,
                        "pct24h": pct,
                        "turnover24h": turn,
                        "ts": time.time(),
                    }
                    formatted_rows.append({
                        "symbol": symbol,
                        "last_price": last,
                        "pct_24h": pct,
                        "turnover_24h": turn,
                    })

            # сохраняем сэмпл в БД (не блокируем)
            if formatted_rows:
                asyncio.create_task(db_insert_snapshot(formatted_rows))

    async def top_by_turnover(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self._lock:
            vals = list(self._tickers.values())
        vals.sort(key=lambda r: (r.get("turnover24h") or 0.0), reverse=True)
        return vals[:limit]

    async def top_by_abs_change(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self._lock:
            vals = list(self._tickers.values())
        vals.sort(key=lambda r: abs(r.get("pct24h") or 0.0), reverse=True)
        return vals[:limit]

    async def top_trend(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self._lock:
            vals = list(self._tickers.values())
        vals.sort(key=lambda r: (r.get("pct24h") or -1e9), reverse=True)
        return vals[:limit]

    def size(self) -> int:
        return len(self._tickers)

# --- вспомогательные ---
def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def aiohttp_client_timeout():
    # Не импортируем Timeout снаружи, чтобы не тянуть aiohttp.* тут
    from aiohttp import ClientTimeout
    return ClientTimeout(total=30)

def fmt_num(n: Optional[float]) -> str:
    if n is None:
        return "—"
    if n == 0:
        return "0"
    if abs(n) >= 1000000000:
        return f"{n/1e9:.0f}B"
    if abs(n) >= 1000000:
        return f"{n/1e6:.0f}M"
    if abs(n) >= 1000:
        return f"{n/1e3:.0f}K"
    return f"{n:.2f}"

def fmt_pct(p: Optional[float]) -> str:
    if p is None:
        return "—"
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.2f}"

# --- ТЕЛЕГРАМ ---
router = Router()
ws_cache = WsCache()

def build_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@router.message(CommandStart())
async def on_start(m: Message):
    kb = build_keyboard()
    text = (
        "🧭 Market mood\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в Innertrade Screener {VERSION} (Bybit WS)."
    )
    await m.answer(text, reply_markup=kb)

@router.message(Command("menu"))
async def on_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=build_keyboard())

@router.message(Command("status"))
async def on_status(m: Message):
    text = (
        "Status\n"
        f"Time: {now_tz()}\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS_URL}"
    )
    await m.answer(text)

@router.message(Command("diag"))
async def on_diag(m: Message):
    text = (
        "diag\n"
        f"ws_ok={ws_cache.connected} | ws_err={ws_cache.err}\n"
        f"symbols_cached={ws_cache.size()}"
    )
    await m.answer(text)

# Кнопки

@router.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    rows = await ws_cache.top_by_turnover(10)
    lines = [
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n",
        "🔥 Активность (Bybit WS)",
    ]
    if not rows:
        lines.append("Нет данных (WS пусто).")
    else:
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i}) {r['symbol']}  24h% {fmt_pct(r.get('pct24h'))}  | turnover24h ~ {fmt_num(r.get('turnover24h'))}"
            )
    await m.answer("\n".join(lines))

@router.message(F.text == "⚡ Волатильность")
async def on_volatility(m: Message):
    rows = await ws_cache.top_by_abs_change(10)
    lines = [
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n",
        "⚡ Волатильность (24h %, Bybit WS)",
    ]
    if not rows:
        lines.append("Нет данных.")
    else:
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i}) {r['symbol']}  24h% {fmt_pct(r.get('pct24h'))}  | last {r.get('last') or 0.0}"
            )
    await m.answer("\n".join(lines))

@router.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    rows = await ws_cache.top_trend(10)
    lines = [
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n",
        "📈 Тренд (упрощённо по 24h%, Bybit WS)",
    ]
    if not rows:
        lines.append("Нет данных.")
    else:
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i}) {r['symbol']}  ≈  24h% {fmt_pct(r.get('pct24h'))}  | last {r.get('last') or 0.0}"
            )
    await m.answer("\n".join(lines))

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("WS Bubbles (24h %, size~turnover24h)")

@router.message(F.text == "📰 Новости")
async def on_news(m: Message):
    text = (
        "🧭 Market mood\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "📰 Макро (последний час)\n"
        "• demo headline"
    )
    await m.answer(text)

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

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
        "• /menu        — восстановить клавиатуру"
    )
    await m.answer(text)

# --- AIOHTTP + AIROGRAM WEBHOOK ---

def build_app() -> web.Application:
    app = web.Application()

    # health / root
    async def handle_root(_):
        return web.Response(status=404, text="Not Found")
    async def handle_health(_):
        # быстрая проверка вебхука и WS
        return web.json_response({
            "ok": True,
            "service": "innertrade-screener",
            "version": VERSION,
            "webhook": True,
            "ws_ok": ws_cache.connected,
            "ws_err": ws_cache.err,
        })

    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)

    # Bot & Dispatcher
    bot = Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dp = Dispatcher()
    dp.include_router(router)

    # Aiogram Webhook handler
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )

    # Регистрируем путь вебхука
    webhook_handler.register(app, path=WEBHOOK_PATH)

    # Интегрируем lifecycle хуки (graceful shutdown)
    setup_aiogram_app(app, dp, bot=bot)

    # Сохраняем объекты в app для on_startup / on_cleanup
    app["bot"] = bot
    app["dp"] = dp

    async def on_startup(_app: web.Application):
        # Устанавливаем webhook в Telegram
        wh_url = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"
        await bot.set_webhook(wh_url)
        log.info("Webhook set to %s", wh_url)

        # DB (если есть)
        await init_db_pool()

        # WS
        await ws_cache.start()

        log.info("App started on 0.0.0.0:%s", PORT)

    async def on_cleanup(_app: web.Application):
        # Снимаем вебхук? Обычно не обязательно; оставим как есть.
        # await bot.delete_webhook(drop_pending_updates=False)
        await ws_cache.stop()
        await close_db_pool()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app

def serve():
    app = build_app()
    # В Render нужно слушать 0.0.0.0:PORT
    web.run_app(app, host="0.0.0.0", port=PORT)

# --- ENTRYPOINT ---
if __name__ == "__main__":
    serve()
