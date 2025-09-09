import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from aiohttp import web, ClientSession, ClientWebSocketResponse, WSMsgType
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from psycopg_pool import AsyncConnectionPool

# ---------------------------
# Config / ENV
# ---------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()  # e.g. https://innertrade-screener-bot.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "G7-ADVmM").strip()
WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear").strip()
# 10 ликвидных символов по умолчанию (можно расширить через ENV)
BYBIT_SYMBOLS = os.getenv(
    "BYBIT_SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
).replace(" ", "").split(",")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")
TZ = pytz.timezone(TIMEZONE)

BOT_VERSION = "v1.0.0-db-ws"
BOT_NAME = "Innertrade Screener"

# TTL «свежести» записей
ROW_TTL = int(os.getenv("ROW_TTL_MIN", "5"))  # минуты

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("innertrade")

# ---------------------------
# Globals (инициализируются в on_startup)
# ---------------------------
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
pool: Optional[AsyncConnectionPool] = None
ws_task: Optional[asyncio.Task] = None
ws_state: Dict[str, Any] = {
    "connected": False,
    "last_msg_ts": None,  # UTC datetime
    "last_error": None,
    "subscribed": False,
    "topics": [],
}

# ---------------------------
# DB helpers
# ---------------------------
UPSERT_SQL = """
INSERT INTO ws_tickers (symbol, last_price, pct_24h, turnover_24h, updated_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (symbol) DO UPDATE
SET last_price   = EXCLUDED.last_price,
    pct_24h      = EXCLUDED.pct_24h,
    turnover_24h = EXCLUDED.turnover_24h,
    updated_at   = NOW();
"""

SELECT_RECENT_SQL = f"""
SELECT symbol, last_price, pct_24h, turnover_24h, updated_at
FROM ws_tickers
WHERE updated_at > NOW() - INTERVAL '{ROW_TTL} minutes'
"""

SELECT_STATS_SQL = """
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE pct_24h IS NOT NULL) AS with_pct,
  COUNT(*) FILTER (WHERE last_price IS NOT NULL) AS with_last,
  MIN(EXTRACT(EPOCH FROM (NOW() - updated_at)))::int AS min_age_sec,
  AVG(EXTRACT(EPOCH FROM (NOW() - updated_at)))::int AS avg_age_sec,
  MAX(EXTRACT(EPOCH FROM (NOW() - updated_at)))::int AS max_age_sec
FROM ws_tickers;
"""

# ---------------------------
# WebSocket collector
# ---------------------------
async def ws_collector():
    """
    Поддерживает WS соединение с Bybit, подписывается на tickers.24h и kline.5 (для тех же символов),
    парсит апдейты и UPSERT в таблицу ws_tickers. Автовосстановление при обрывах.
    """
    backoff = 1
    topics_ticker = [f"tickers.24h.{sym}" for sym in BYBIT_SYMBOLS]
    topics_kline = [f"kline.5.{sym}" for sym in BYBIT_SYMBOLS]
    topics = topics_ticker + topics_kline

    while True:
        try:
            async with ClientSession() as session:
                log.info("Bybit WS connecting: %s", BYBIT_WS)
                ws: ClientWebSocketResponse
                async with session.ws_connect(BYBIT_WS, heartbeat=20, timeout=30) as ws:
                    ws_state.update(
                        {
                            "connected": True,
                            "last_error": None,
                            "subscribed": False,
                            "topics": topics,
                        }
                    )
                    # подписка
                    await ws.send_json({"op": "subscribe", "args": topics})
                    ws_state["subscribed"] = True
                    log.info("WS subscribed: %d topics", len(topics))

                    backoff = 1  # сбросили backoff после успешного коннекта

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            ws_state["last_msg_ts"] = datetime.now(timezone.utc)
                            data = json.loads(msg.data)

                            # Пинги Bybit (op ping/pong)
                            if "op" in data and data["op"] == "ping":
                                await ws.send_json({"op": "pong"})
                                continue

                            # Поток тикеров: topic = tickers.24h.SYMBOL
                            topic = data.get("topic")
                            if topic and topic.startswith("tickers.24h."):
                                try:
                                    await handle_ticker_24h(data)
                                except Exception as e:
                                    log.exception("handle_ticker_24h error: %s", e)

                            # Поток свечей (на будущее, если захочешь доп.расчёты):
                            # topic = kline.5.SYMBOL; здесь пока не используем
                            continue

                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSING, WSMsgType.ERROR):
                            raise ConnectionError(f"WS closed: {msg.type}")

        except asyncio.CancelledError:
            log.info("WS worker cancelled")
            ws_state["connected"] = False
            raise
        except Exception as e:
            ws_state.update({"connected": False, "last_error": str(e)})
            log.warning("WS error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)  # эксп. backoff до 30с


async def handle_ticker_24h(payload: Dict[str, Any]):
    """
    Пример сообщения:
    {
      "topic":"tickers.24h.BTCUSDT",
      "ts": 1719999999999,
      "type":"snapshot|delta",
      "data": { "symbol":"BTCUSDT", "lastPrice":"111111.0", "price24hPcnt":"0.0123", "turnover24h":"123456789" ... }
    }
    """
    if not pool:
        return
    data = payload.get("data") or {}
    if isinstance(data, list):
        # иногда приходит массив; нормализуем
        for d in data:
            await upsert_from_ticker_dict(d)
    else:
        await upsert_from_ticker_dict(data)


async def upsert_from_ticker_dict(d: Dict[str, Any]):
    symbol = d.get("symbol") or d.get("s")
    if not symbol:
        return

    # lastPrice/turnover24h/price24hPcnt приходят строками
    def to_float(x: Any) -> Optional[float]:
        if x is None or x == "" or x == "-":
            return None
        try:
            return float(x)
        except Exception:
            return None

    last_price = to_float(d.get("lastPrice"))
    turnover_24h = to_float(d.get("turnover24h"))
    pct_24h = to_float(d.get("price24hPcnt"))

    # UPSERT
    async with pool.connection() as conn:
        await conn.execute(UPSERT_SQL, symbol, last_price, pct_24h, turnover_24h)


# ---------------------------
# Format helpers
# ---------------------------
def fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "—"
    if x >= 1_000_000_000:
        return f"{x/1_000_000_000:.0f}B"
    if x >= 1_000_000:
        return f"{x/1_000_000:.0f}M"
    if x >= 1_000:
        return f"{x/1_000:.0f}K"
    return f"{x:.0f}"

def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x*100:.2f}"

def now_local_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------
# Bot UI
# ---------------------------
def menu_kbd() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def status_text(source_hint: str) -> str:
    return (
        "Status\n"
        f"Time: {now_local_str()} (MSK)\n"
        f"Mode: active | Quiet: False\n"
        f"Source: {source_hint}\n"
        f"Version: {BOT_VERSION}\n"
        f"Bybit WS: {BYBIT_WS}\n"
    )

async def fetch_recent(limit: int = 10) -> List[Tuple[str, Optional[float], Optional[float], Optional[float], datetime]]:
    if not pool:
        return []
    sql = SELECT_RECENT_SQL + " ORDER BY updated_at DESC LIMIT $1"
    async with pool.connection() as conn:
        rows = await conn.fetchall(sql, limit)
    return rows

async def select_sorted(by: str, limit: int = 10) -> List[Tuple[str, Optional[float], Optional[float], Optional[float], datetime]]:
    """
    by: 'turnover' | 'pct_abs' | 'pct_up' | 'pct_down'
    """
    if not pool:
        return []
    base = SELECT_RECENT_SQL
    if by == "turnover":
        order = "ORDER BY turnover_24h DESC NULLS LAST"
    elif by == "pct_abs":
        order = "ORDER BY ABS(pct_24h) DESC NULLS LAST"
    elif by == "pct_up":
        order = "ORDER BY pct_24h DESC NULLS LAST"
    else:  # pct_down
        order = "ORDER BY pct_24h ASC NULLS LAST"
    sql = f"{base} {order} LIMIT $1"
    async with pool.connection() as conn:
        rows = await conn.fetchall(sql, limit)
    return rows

def render_activity(rows) -> str:
    lines = ["🧭 Market mood", "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)", ""]
    lines.append("🔥 Активность (Bybit WS + DB)")
    if not rows:
        lines.append("Нет данных (ожидаю снапшоты).")
        return "\n".join(lines)
    for i, (sym, last, pct, turn, ts) in enumerate(rows, start=1):
        lines.append(
            f"{i}) {sym:<7}  24h% {fmt_pct(pct)}  | turnover24h ~ {fmt_money(turn)}"
        )
    return "\n".join(lines)

def render_vol(rows) -> str:
    lines = ["🧭 Market mood", "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)", ""]
    lines.append("⚡ Волатильность (24h %, Bybit WS + DB)")
    if not rows:
        lines.append("Нет данных.")
        return "\n".join(lines)
    for i, (sym, last, pct, turn, ts) in enumerate(rows, start=1):
        last_str = "—" if last is None else f"{last:g}"
        lines.append(f"{i}) {sym:<7}  24h% {fmt_pct(pct)}  | last {last_str}")
    return "\n".join(lines)

def render_trend(rows) -> str:
    lines = ["🧭 Market mood", "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)", ""]
    lines.append("📈 Тренд (упрощённо по 24h%, Bybit WS + DB)")
    if not rows:
        lines.append("Нет данных.")
        return "\n".join(lines)
    for i, (sym, last, pct, turn, ts) in enumerate(rows, start=1):
        last_str = "—" if last is None else f"{last:g}"
        lines.append(f"{i}) {sym:<7}  ≈  24h% {fmt_pct(pct)}  | last {last_str}")
    return "\n".join(lines)

def settings_text() -> str:
    return (
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

# ---------------------------
# Handlers
# ---------------------------
async def on_start(message: types.Message):
    kb = menu_kbd()
    text = (
        "🧭 Market mood\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в {BOT_NAME} {BOT_VERSION} (Bybit WS + DB)."
    )
    await message.answer(text, reply_markup=kb)

async def on_status(message: types.Message):
    source = "Bybit (DB+WS)"
    await message.answer(status_text(source))

async def on_activity(message: types.Message):
    rows = await select_sorted("turnover", limit=10)
    await message.answer(render_activity(rows))

async def on_vol(message: types.Message):
    rows = await select_sorted("pct_abs", limit=10)
    await message.answer(render_vol(rows))

async def on_trend(message: types.Message):
    rows = await select_sorted("pct_up", limit=10)
    await message.answer(render_trend(rows))

async def on_bubbles(message: types.Message):
    await message.answer("WS Bubbles (24h %, size~turnover24h)")

async def on_news(message: types.Message):
    text = (
        "🧭 Market mood\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        "📰 Макро (последний час)\n"
        "• demo headline"
    )
    await message.answer(text)

async def on_calc(message: types.Message):
    await message.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")

async def on_watchlist(message: types.Message):
    await message.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")

async def on_settings(message: types.Message):
    await message.answer(settings_text())

async def on_menu(message: types.Message):
    await message.answer("ok", reply_markup=menu_kbd())

async def on_diag(message: types.Message):
    if not pool:
        await message.answer("DB pool not ready")
        return
    # Stats
    async with pool.connection() as conn:
        stats = await conn.fetchone(SELECT_STATS_SQL)

    ws_connected = ws_state.get("connected")
    ws_err = ws_state.get("last_error")
    last_msg_ts: Optional[datetime] = ws_state.get("last_msg_ts")
    age_s = None
    if last_msg_ts:
        age_s = int((datetime.now(timezone.utc) - last_msg_ts).total_seconds())

    txt = [
        "diag",
        f"db_total={stats['total']} with_pct={stats['with_pct']} with_last={stats['with_last']}",
        f"age_sec min/avg/max = {stats['min_age_sec']}/{stats['avg_age_sec']}/{stats['max_age_sec']}",
        f"ws_connected={ws_connected} last_msg_age={age_s} ws_err={ws_err}",
        f"symbols_cached={len(BYBIT_SYMBOLS)}",
    ]
    await message.answer("\n".join(txt))

async def on_health(request: web.Request) -> web.Response:
    # Telegram webhook check helper
    return web.json_response(
        {
            "ok": True,
            "service": "innertrade-screener",
            "version": BOT_VERSION,
            "webhook": True,
            "ws_connected": ws_state.get("connected"),
            "last_msg_ts": ws_state.get("last_msg_ts").isoformat() if ws_state.get("last_msg_ts") else None,
        }
    )

# ---------------------------
# Aiogram / Webhook app
# ---------------------------
def build_app() -> web.Application:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    if not WEBHOOK_BASE:
        raise RuntimeError("WEBHOOK_BASE is required")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")

    application = web.Application()

    # bot / dispatcher
    global bot, dp
    bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    # routes
    application.router.add_get("/", lambda r: web.json_response({"ok": True, "name": BOT_NAME, "version": BOT_VERSION}))
    application.router.add_get("/health", on_health)

    # webhook handler
    async def handle_webhook(request: web.Request) -> web.Response:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot, update)
        return web.json_response({"ok": True})

    application.router.add_post(WEBHOOK_PATH, handle_webhook)

    # startup/shutdown
    application.on_startup.append(on_app_startup)
    application.on_shutdown.append(on_app_shutdown)

    return application


async def on_app_startup(app: web.Application):
    # set webhook
    assert bot is not None
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=WEBHOOK_BASE + WEBHOOK_PATH)
    log.info("Webhook set to %s", WEBHOOK_BASE + WEBHOOK_PATH)

    # db pool
    global pool
    pool = AsyncConnectionPool(DATABASE_URL, open=False, num_workers=4, max_size=10)
    await pool.open()
    log.info("DB pool opened")

    # register handlers
    assert dp is not None
    dp.message.register(on_start, Command("start"))
    dp.message.register(on_status, Command("status"))
    dp.message.register(on_diag, Command("diag"))
    dp.message.register(on_menu, Command("menu"))
    dp.message.register(on_health_cmd, Command("health"))

    # кнопки
    dp.message.register(on_activity, F.text == "📊 Активность")
    dp.message.register(on_vol, F.text == "⚡ Волатильность")
    dp.message.register(on_trend, F.text == "📈 Тренд")
    dp.message.register(on_bubbles, F.text == "🫧 Bubbles")
    dp.message.register(on_news, F.text == "📰 Новости")
    dp.message.register(on_calc, F.text == "🧮 Калькулятор")
    dp.message.register(on_watchlist, F.text == "⭐ Watchlist")
    dp.message.register(on_settings, F.text == "⚙️ Настройки")

    # ws collector
    global ws_task
    ws_task = asyncio.create_task(ws_collector())


async def on_app_shutdown(app: web.Application):
    # stop ws task
    global ws_task
    if ws_task and not ws_task.done():
        ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await ws_task

    # close db pool
    global pool
    if pool:
        await pool.close()
        log.info("DB pool closed")

    # delete webhook
    if bot:
        with suppress(Exception):
            await bot.delete_webhook(drop_pending_updates=True)


async def on_health_cmd(message: types.Message):
    await message.answer(f"Current webhook: {WEBHOOK_BASE + WEBHOOK_PATH}")

# ---------------------------
# Entrypoint
# ---------------------------
def serve():
    app = build_app()
    port = int(os.getenv("PORT", "10000"))
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    serve()
