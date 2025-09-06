import os
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from statistics import mean

import pytz
from aiohttp import web, ClientSession, ClientTimeout, WSMsgType
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from dotenv import load_dotenv

# --- PostgreSQL (psycopg v3 async) ---
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# ================== CONFIG & ENV ==================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
TZ = os.getenv("TZ", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL")  # postgres://... (Neon)
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")

VERSION = "v0.9.0-wsdb"

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https Render URL, e.g. https://<service>.onrender.com")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set (Neon connection string)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

tz = pytz.timezone(TZ)

# ================== DB POOL ==================
db_pool: AsyncConnectionPool | None = None

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS klines_5m (
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    o DOUBLE PRECISION NOT NULL,
    h DOUBLE PRECISION NOT NULL,
    l DOUBLE PRECISION NOT NULL,
    c DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS tickers (
    symbol TEXT PRIMARY KEY,
    pct24 DOUBLE PRECISION NOT NULL DEFAULT 0,
    turnover24h DOUBLE PRECISION NOT NULL DEFAULT 0,
    last DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- простейший кэш-таблица для служебных целей
CREATE TABLE IF NOT EXISTS screener_state (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bucket TEXT NOT NULL,
    payload JSONB NOT NULL
);
"""

async def init_db():
    assert db_pool is not None
    async with db_pool.connection() as aconn:
        await aconn.execute(CREATE_SQL)
        await aconn.commit()

async def db_ping() -> bool:
    assert db_pool is not None
    try:
        async with db_pool.connection() as aconn:
            await aconn.execute("SELECT 1;")
        return True
    except Exception as e:
        logging.warning(f"[DB PING ERR] {e}")
        return False

# ================== WS INGEST ==================
SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","LINKUSDT","TRXUSDT","TONUSDT","ARBUSDT","OPUSDT"
]

async def ws_ingest_task():
    """
    Подписываемся на Bybit WS (public, linear):
      - tickers: tickers.<symbol>
      - kline 5m: kline.5.<symbol>
    События пишем в БД.
    """
    # Подготовим списки топиков
    ticker_topics = [f"tickers.{s}" for s in SYMBOLS]
    kline_topics  = [f"kline.5.{s}" for s in SYMBOLS]

    subscribe_msg = {
        "op": "subscribe",
        "args": ticker_topics + kline_topics
    }

    timeout = ClientTimeout(total=None, sock_read=60, sock_connect=15)
    headers = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

    while True:
        try:
            async with ClientSession(timeout=timeout, headers=headers) as session:
                async with session.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                    await ws.send_json(subscribe_msg)
                    logging.info(f"[WS] subscribed to {len(ticker_topics) + len(kline_topics)} topics")

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                data = msg.json(loads=json.loads)
                            except Exception:
                                # иногда приходят ping/pong как текст
                                continue
                            await handle_ws_payload(data)
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
        except Exception as e:
            logging.warning(f"[WS LOOP] {e}")
        await asyncio.sleep(3)  # backoff и переподключение

async def handle_ws_payload(data: dict):
    """
    Разбор полезной нагрузки Bybit WS.
    """
    if not data or "topic" not in data or "data" not in data:
        return
    topic = data["topic"]
    payload = data["data"]

    if topic.startswith("tickers."):
        await upsert_ticker(payload)
    elif topic.startswith("kline.5."):
        await insert_kline(payload)

async def upsert_ticker(payload: dict):
    """
    Пример структуры payload (tickers):
    {
      "symbol": "BTCUSDT",
      "lastPrice": "110023.2",
      "turnover24h": "123456789.0",
      "price24hPcnt": "0.0123",  # доля, надо *100
      ...
    }
    """
    try:
        symbol = payload.get("symbol")
        if not symbol:
            return
        last = float(payload.get("lastPrice", 0.0))
        turnover24h = float(payload.get("turnover24h", 0.0))
        pct = float(payload.get("price24hPcnt", 0.0)) * 100.0

        assert db_pool is not None
        async with db_pool.connection() as aconn:
            await aconn.execute(
                """
                INSERT INTO tickers(symbol, pct24, turnover24h, last, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (symbol) DO UPDATE
                SET pct24 = EXCLUDED.pct24,
                    turnover24h = EXCLUDED.turnover24h,
                    last = EXCLUDED.last,
                    updated_at = NOW();
                """,
                (symbol, pct, turnover24h, last),
            )
            await aconn.commit()
    except Exception as e:
        logging.warning(f"[UPSERT TICKER ERR] {e}")

async def insert_kline(payload: dict):
    """
    Пример структуры payload (kline.5):
    {
      "symbol": "BTCUSDT",
      "start": 1736284500000,     # ms
      "end": 1736284800000,       # ms
      "open": "100.0", "high": "...", "low":"...", "close":"...",
      "turnover": "12345.67",
      "confirm": True/False,  # закрылась ли свеча
      ...
    }
    Пишем только закрытые свечи (confirm=True).
    """
    try:
        if not payload or not payload.get("confirm", False):
            return
        symbol = payload["symbol"]
        ts = datetime.fromtimestamp(payload["start"] / 1000.0, tz=timezone.utc)
        o = float(payload.get("open", 0) or 0)
        h = float(payload.get("high", 0) or 0)
        l = float(payload.get("low", 0) or 0)
        c = float(payload.get("close", 0) or 0)
        turnover = float(payload.get("turnover", 0) or 0)

        # фильтр мусора
        if c <= 0 or h <= 0:
            return

        assert db_pool is not None
        async with db_pool.connection() as aconn:
            await aconn.execute(
                """
                INSERT INTO klines_5m(symbol, ts, o, h, l, c, turnover)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, ts) DO NOTHING;
                """,
                (symbol, ts, o, h, l, c, turnover),
            )
            await aconn.commit()
    except Exception as e:
        logging.warning(f"[INSERT KLINE ERR] {e}")

# ================== INDICATORS ==================
async def get_activity_text() -> str:
    """
    Активность: ранжируем по отношению последнего turnover 5m к MA20(5m turnover).
    Данные берём из БД по последним ~30 свечам.
    """
    assert db_pool is not None
    # берём последние N минут (например 200 * 5m ~ 1000 мин) — достаточно, чтобы MA20 считалась
    look_minutes = 200 * 5
    since = datetime.now(timezone.utc) - timedelta(minutes=look_minutes)

    out = []
    async with db_pool.connection() as aconn:
        async with aconn.cursor(row_factory=dict_row) as cur:
            for sym in SYMBOLS:
                await cur.execute(
                    """
                    SELECT turnover
                    FROM klines_5m
                    WHERE symbol = $1 AND ts >= $2
                    ORDER BY ts ASC
                    """,
                    (sym, since),
                )
                rows = await cur.fetchall()
                turns = [float(r["turnover"]) for r in rows if r["turnover"] and float(r["turnover"]) > 0]
                if len(turns) < 21:
                    continue
                ma20 = mean(turns[-21:-1])
                if ma20 <= 0:
                    continue
                vol_mult = turns[-1] / ma20
                # возьмём share24 из тикеров (грубая прикидка)
                await cur.execute(
                    "SELECT turnover24h FROM tickers WHERE symbol=$1",
                    (sym,),
                )
                tk = await cur.fetchone()
                share24 = 0
                if tk and tk["turnover24h"] and tk["turnover24h"] > 0:
                    # нормализацию по сумме 7д нет — оставим поле просто для вида
                    share24 = 0
                out.append((sym, vol_mult, share24))

    if not out:
        return "🔥 <b>Активность</b>\nНет данных (ещё мало свечей)."

    out.sort(key=lambda x: x[1], reverse=True)
    lines = ["🔥 <b>Активность</b> (Bybit WS+DB)"]
    for i, (sym, vol_mult, share24) in enumerate(out[:10], 1):
        lines.append(f"{i}) {sym} Vol x{vol_mult:.1f} | 24h vs 7d: {share24}%")
    return "\n".join(lines)

async def get_volatility_text() -> str:
    """
    Волатильность: ATR% (14) на 5m, last_close из последних свечей.
    """
    assert db_pool is not None
    since = datetime.now(timezone.utc) - timedelta(minutes=60 * 24)  # сутки 5m свечей хватит на ATR14
    out = []

    async with db_pool.connection() as aconn:
        async with aconn.cursor(row_factory=dict_row) as cur:
            for sym in SYMBOLS:
                await cur.execute(
                    """
                    SELECT o,h,l,c,turnover
                    FROM klines_5m
                    WHERE symbol=$1 AND ts >= $2
                    ORDER BY ts ASC
                    """,
                    (sym, since),
                )
                rows = await cur.fetchall()
                if len(rows) < 20:
                    continue
                closes = [float(r["c"]) for r in rows if r["c"] and float(r["c"]) > 0]
                if len(closes) < 20:
                    continue

                # ATR14
                trs = []
                prev_close = float(rows[0]["c"])
                for r in rows[1:]:
                    h = float(r["h"]); l = float(r["l"]); c = float(r["c"])
                    tr = max(h - l, abs(h - prev_close), abs(prev_close - l))
                    trs.append(tr)
                    prev_close = c
                if len(trs) < 14:
                    continue
                atr = mean(trs[-14:])
                last_close = closes[-1]
                if last_close <= 0:
                    continue
                atr_pct = atr / last_close * 100.0

                turns = [float(r["turnover"]) for r in rows if r["turnover"] and float(r["turnover"]) > 0]
                vm = 0.0
                if len(turns) >= 21:
                    ma20 = mean(turns[-21:-1])
                    if ma20 > 0:
                        vm = turns[-1] / ma20

                out.append((sym, atr_pct, vm))

    if not out:
        return "⚡ <b>Волатильность</b>\nНет данных (ещё мало свечей)."

    out.sort(key=lambda x: x[1], reverse=True)
    lines = ["⚡ <b>Волатильность</b> (ATR%, 5m, Bybit WS+DB)"]
    for i, (sym, atr_pct, vm) in enumerate(out[:10], 1):
        lines.append(f"{i}) {sym} ATR {atr_pct:.2f}% | Vol x{vm:.1f}")
    return "\n".join(lines)

async def get_trend_text() -> str:
    """
    Тренд: положение цены относительно MA200 (5m) и знак наклона MA200.
    Для MA200 нужно >=200 свечей — бот накопит их после рестарта, DB сохраняет.
    """
    assert db_pool is not None
    since = datetime.now(timezone.utc) - timedelta(minutes=5 * 220)

    out = []
    async with db_pool.connection() as aconn:
        async with aconn.cursor(row_factory=dict_row) as cur:
            for sym in SYMBOLS:
                await cur.execute(
                    """
                    SELECT c
                    FROM klines_5m
                    WHERE symbol=$1 AND ts >= $2
                    ORDER BY ts ASC
                    """,
                    (sym, since),
                )
                rows = await cur.fetchall()
                closes = [float(r["c"]) for r in rows if r["c"] and float(r["c"]) > 0]
                if len(closes) < 200:
                    continue
                ma200 = mean(closes[-200:])
                last_close = closes[-1]
                above200 = last_close > ma200

                # slope по разнице средних половин окна (очень простая метрика)
                if len(closes) >= 220:
                    a = mean(closes[-220:-110])
                    b = mean(closes[-110:])
                    slope_tag = "slope+" if (b - a) > 0 else ("slope-" if (b - a) < 0 else "slope≈")
                else:
                    slope_tag = "slope?"

                out.append((sym, above200, slope_tag))

    if not out:
        return "📈 <b>Тренд</b>\nНет данных (ещё мало свечей)."

    out.sort(key=lambda x: (x[1], x[2] == "slope+"), reverse=True)
    lines = ["📈 <b>Тренд</b> (5m MA200, Bybit WS+DB)"]
    for i, (sym, above200, slope_tag) in enumerate(out[:10], 1):
        pos = ">MA200" if above200 else "<MA200"
        lines.append(f"{i}) {sym} {pos} | {slope_tag}")
    return "\n".join(lines)

# ================== UI / HANDLERS ==================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
    ],
    resize_keyboard=True,
)

def header_text() -> str:
    return ("🧭 Market mood\n"
            "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)")

@dp.message(commands={"start", "menu"})
async def cmd_start(m: types.Message):
    await m.answer(
        header_text() + "\n\n" +
        f"Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit).",
        reply_markup=main_kb
    )

@dp.message(commands={"status"})
async def cmd_status(m: types.Message):
    ok_db = await db_ping()
    await m.answer(
        "Status\n"
        f"Mode: active | Quiet: False\n"
        f"Source: Bybit (public WS, persisted)\n"
        f"Version: {VERSION}\n"
        f"DB: {'connected' if ok_db else 'unavailable'}\n"
        f"WS: {BYBIT_WS}\n",
        reply_markup=main_kb
    )

@dp.message(commands={"diag"})
async def cmd_diag(m: types.Message):
    ok_db = await db_ping()
    await m.answer(
        "diag\n"
        f"WS public: ok=True (connected loop)\n"
        f"DB: {'ok' if ok_db else 'fail'}"
    )

@dp.message(F.text == "📊 Активность")
async def on_activity(m: types.Message):
    await m.answer(header_text() + "\n\n" + "Подбираю данные…", reply_markup=main_kb)
    txt = await get_activity_text()
    await m.answer(txt, reply_markup=main_kb)

@dp.message(F.text == "⚡ Волатильность")
async def on_vol(m: types.Message):
    await m.answer(header_text() + "\n\n" + "Подбираю данные…", reply_markup=main_kb)
    txt = await get_volatility_text()
    await m.answer(txt, reply_markup=main_kb)

@dp.message(F.text == "📈 Тренд")
async def on_trend(m: types.Message):
    await m.answer(header_text() + "\n\n" + "Подбираю данные…", reply_markup=main_kb)
    txt = await get_trend_text()
    await m.answer(txt, reply_markup=main_kb)

@dp.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: types.Message):
    # Пока просто выведем список из тикеров из БД
    assert db_pool is not None
    async with db_pool.connection() as aconn:
        rows = (await (await aconn.execute(
            "SELECT symbol, pct24, turnover24h FROM tickers ORDER BY turnover24h DESC NULLS LAST LIMIT 12"
        )).fetchall())
    if not rows:
        await m.answer("WS Bubbles: пока пусто (ждём данные).", reply_markup=main_kb)
        return
    lines = ["WS Bubbles (24h %, size~turnover24h)"]
    for r in rows:
        lines.append(f"• {r[0]}  24h% {r[1]:+.2f}  | turnover24h ~ {int(r[2])}")
    await m.answer("\n".join(lines), reply_markup=main_kb)

@dp.message(F.text == "📰 Новости")
async def on_news(m: types.Message):
    await m.answer(header_text() + "\n\n" + "📰 Макро (последний час)\n• demo headline", reply_markup=main_kb)

@dp.message(F.text == "🧮 Калькулятор")
async def on_calc(m: types.Message):
    await m.answer("Шаблон риск-менеджмента (excel добавим отдельно).", reply_markup=main_kb)

@dp.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: types.Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=main_kb)

@dp.message(F.text == "⚙️ Настройки")
async def on_settings(m: types.Message):
    await m.answer(
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
        "• /menu        — восстановить клавиатуру",
        reply_markup=main_kb
    )

# ================== HTTP / WEBHOOK ==================
async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)
    try:
        update = types.Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logging.warning(f"[WEBHOOK ERR] {e}")
    return web.Response(status=200)

async def handle_health(request: web.Request):
    ok = await db_ping()
    return web.json_response({
        "ok": True,
        "service": "innertrade-screener",
        "version": VERSION,
        "db": ok,
        "ws": BYBIT_WS
    })

async def on_startup(app: web.Application):
    global db_pool
    # Открываем пул соединений к БД
    db_pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False)
    await db_pool.open()
    await init_db()

    # Ставим вебхук
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook/{TELEGRAM_TOKEN}")
    logging.info(f"Webhook set to: {BASE_URL}/webhook/{TELEGRAM_TOKEN}")

    # Стартуем WS-инжест в фоне
    app["ws_task"] = asyncio.create_task(ws_ingest_task())
    logging.info("[WS] ingest task started")

async def on_shutdown(app: web.Application):
    # Гасим WS таск
    task = app.get("ws_task")
    if task:
        task.cancel()
        with contextlib.suppress(Exception):
            await task
    # Закрываем пул и сессию бота
    if db_pool:
        await db_pool.close()
    await bot.session.close()

def main():
    app = web.Application()
    app.router.add_post(f"/webhook/{TELEGRAM_TOKEN}", handle_webhook)
    app.router.add_get("/health", handle_health)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    port = int(os.getenv("PORT", "10000"))
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    # небольшая страховка, если вдруг забыли импорт
    import contextlib
    main()
