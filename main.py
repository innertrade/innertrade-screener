import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from io import BytesIO
from statistics import mean

import aiohttp
import asyncpg
import numpy as np
import pytz

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    BufferedInputFile, Update
)
from aiohttp import web
from dotenv import load_dotenv

# ============== ENV & CONFIG ==============
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")
TZ = os.getenv("TZ", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")

VERSION = "v1.0-persist-ws"

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN env is required")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https URL, e.g. https://<service>.onrender.com")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL (Postgres/Neon) is required")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ============== SYMBOLS / INTERVALS ==============
SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","DOGEUSDT","ADAUSDT",
    "LINKUSDT","TRXUSDT","TONUSDT","ARBUSDT","OPUSDT"
]
INTERVAL = "5"  # 5m строка для Bybit v5: topic kline.5.SYMBOL

# ============== DB LAYER ==============
_pool: asyncpg.Pool | None = None

DDL_SQL = """
CREATE TABLE IF NOT EXISTS candles (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  ts BIGINT NOT NULL,              -- старт свечи (ms / or s -> будем хранить ms)
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low  DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL,
  turnover DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (symbol, interval, ts)
);

CREATE INDEX IF NOT EXISTS idx_candles_sym_int_ts ON candles(symbol, interval, ts DESC);

CREATE TABLE IF NOT EXISTS tickers (
  symbol TEXT PRIMARY KEY,
  price24hPcnt DOUBLE PRECISION,   -- в долях (на WS так приходит); при выводе *100
  turnover24h  DOUBLE PRECISION,
  last_price   DOUBLE PRECISION,
  updated_at   TIMESTAMPTZ DEFAULT now()
);
"""

async def db_init():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    async with _pool.acquire() as con:
        for stmt in DDL_SQL.strip().split(";\n\n"):
            if stmt.strip():
                await con.execute(stmt)

async def upsert_candle(symbol: str, interval: str, ts_ms: int,
                        o: float, h: float, l: float, c: float, v: float, t: float):
    assert _pool is not None
    sql = """
    INSERT INTO candles(symbol, interval, ts, open, high, low, close, volume, turnover)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT(symbol, interval, ts) DO UPDATE
    SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
        volume=EXCLUDED.volume, turnover=EXCLUDED.turnover;
    """
    async with _pool.acquire() as c:
        await c.execute(sql, symbol, interval, ts_ms, o, h, l, c, v, t)

async def upsert_ticker(symbol: str, price24hPcnt: float | None,
                        turnover24h: float | None, last_price: float | None):
    assert _pool is not None
    sql = """
    INSERT INTO tickers(symbol, price24hPcnt, turnover24h, last_price, updated_at)
    VALUES($1,$2,$3,$4, now())
    ON CONFLICT(symbol) DO UPDATE SET
      price24hPcnt = EXCLUDED.price24hPcnt,
      turnover24h  = EXCLUDED.turnover24h,
      last_price   = EXCLUDED.last_price,
      updated_at   = now();
    """
    async with _pool.acquire() as c:
        await c.execute(sql, symbol, price24hPcnt, turnover24h, last_price)

async def fetch_last_candles(symbol: str, interval: str, limit: int = 400):
    assert _pool is not None
    sql = """
    SELECT ts, open, high, low, close, volume, turnover
    FROM candles
    WHERE symbol=$1 AND interval=$2
    ORDER BY ts DESC
    LIMIT $3;
    """
    async with _pool.acquire() as c:
        rows = await c.fetch(sql, symbol, interval, limit)
    # вернём в прямом хрон. порядке
    return list(reversed(rows))

async def top_by_ticker(metric: str, limit: int = 10):
    """
    metric: 'turnover24h' | 'price24hPcnt_abs' | 'price24hPcnt'
    """
    assert _pool is not None
    if metric == "turnover24h":
        sql = "SELECT symbol, price24hPcnt, turnover24h, last_price FROM tickers ORDER BY turnover24h DESC NULLS LAST LIMIT $1;"
    elif metric == "price24hPcnt_abs":
        sql = "SELECT symbol, price24hPcnt, turnover24h, last_price FROM tickers ORDER BY ABS(price24hPcnt) DESC NULLS LAST LIMIT $1;"
    else:
        sql = "SELECT symbol, price24hPcnt, turnover24h, last_price FROM tickers ORDER BY price24hPcnt DESC NULLS LAST LIMIT $1;"

    async with _pool.acquire() as c:
        rows = await c.fetch(sql, limit)
    return rows

# ============== INDICATORS ==============
def moving_average(vals: list[float], length: int) -> float | None:
    if len(vals) < length:
        return None
    return float(mean(vals[-length:]))

def compute_atr(ohlc: list[tuple[float,float,float,float]], period: int = 14) -> float | None:
    if len(ohlc) < period + 1:
        return None
    trs = []
    prev_close = ohlc[0][3]
    for i in range(1, len(ohlc)):
        _, h, l, c = ohlc[i]
        tr = max(h - l, abs(h - prev_close), abs(prev_close - l))
        trs.append(tr)
        prev_close = c
    if len(trs) < period:
        return None
    return float(mean(trs[-period:]))

# ============== RENDER HELPERS ==============
def header_text() -> str:
    # заглушка шапки; реальные макро-метрики подставите позже
    return "🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)"

def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True, is_persistent=True,
        input_field_placeholder="Выберите раздел…",
    )

# ============== BUSINESS LOGIC (uses DB) ==============
async def render_activity() -> str:
    # Активность = сортировка по обороту 24h из таблицы tickers
    rows = await top_by_ticker("turnover24h", limit=10)
    if not rows:
        return "🔥 <b>Активность</b>\nНет данных (ожидаю WS тикеры)…"
    lines = ["🔥 <b>Активность</b> (Bybit WS)"]
    for i, r in enumerate(rows, 1):
        sym = r["symbol"]
        pct = (r["price24hPcnt"] or 0.0) * 100.0
        tov = r["turnover24h"] or 0.0
        lines.append(f"{i}) {sym}  24h% {pct:+.2f}  | turnover24h ~ {int(tov):,}".replace(",", " "))
    return "\n".join(lines)

async def render_volatility() -> str:
    # Волатильность = сортировка по |%24h| из tickers (пока нет очень длинной истории без REST)
    rows = await top_by_ticker("price24hPcnt_abs", limit=10)
    if not rows:
        return "⚡ <b>Волатильность</b>\nНет данных (ожидаю WS тикеры)…"
    lines = ["⚡ <b>Волатильность</b> (24h %, Bybit WS)"]
    for i, r in enumerate(rows, 1):
        sym = r["symbol"]
        pct = (r["price24hPcnt"] or 0.0) * 100.0
        last = r["last_price"] or 0.0
        lines.append(f"{i}) {sym}  24h% {pct:+.2f}  | last {last}")
    return "\n".join(lines)

async def render_trend() -> str:
    # Тренд = MA200 по 5m, если хватает свечей; иначе упрощённая версия по 24h%
    # Сначала проверим, у кого хватает истории
    ranked = []
    for sym in SYMBOLS:
        rows = await fetch_last_candles(sym, INTERVAL, limit=260)  # до 260 свечей 5м
        if len(rows) >= 200:
            closes = [float(r["close"]) for r in rows]
            ma200 = moving_average(closes, 200)
            last = closes[-1]
            above = last > (ma200 or last)
            ranked.append((sym, "MA200", above, last))
    if ranked:
        ranked.sort(key=lambda x: (x[2], x[3]), reverse=True)  # above True выше, затем цена
        lines = ["📈 <b>Тренд</b> (5m MA200, WS history)"]
        for i, (sym, _, above, last) in enumerate(ranked[:10], 1):
            tag = ">MA200" if above else "<MA200"
            lines.append(f"{i}) {sym}  {tag}  | last {last}")
        return "\n".join(lines)

    # Фоллбек — упрощённый по 24h%
    rows = await top_by_ticker("price24hPcnt", limit=10)
    if not rows:
        return "📈 <b>Тренд</b>\nНет данных (ожидаю WS)…"
    lines = ["📈 <b>Тренд</b> (упрощённо по 24h%, WS)"]
    for i, r in enumerate(rows, 1):
        sym = r["symbol"]
        pct = (r["price24hPcnt"] or 0.0) * 100.0
        last = r["last_price"] or 0.0
        approx = "↑" if pct > 0 else ("↓" if pct < 0 else "≈")
        lines.append(f"{i}) {sym}  {approx}  24h% {pct:+.2f}  | last {last}")
    return "\n".join(lines)

# ============== WS CONSUMER ==============
async def ws_consumer():
    """
    Подключаемся к Bybit Public WS и подписываемся:
      - tickers.SYMBOL
      - kline.5.SYMBOL
    Сохраняем в БД тикеры и свечи. При подписке Bybit шлёт снапшоты (включая ~последние 200 свечей).
    """
    sub_topics = []
    for s in SYMBOLS:
        sub_topics.append(f"tickers.{s}")
        sub_topics.append(f"kline.{INTERVAL}.{s}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                    logging.info(f"WS connected: {BYBIT_WS}")
                    # subscribe
                    await ws.send_json({"op": "subscribe", "args": sub_topics})

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json(loads=json.loads)
                            except Exception:
                                # Иногда Bybit шлёт пинг или plain text
                                text = msg.data
                                if text == "pong":
                                    continue
                                with contextlib.suppress(Exception):
                                    dj = json.loads(text)
                                    data = dj
                                if not isinstance(text, str):
                                    continue
                                # не JSON — пропускаем
                                continue

                            # обработка
                            if isinstance(data, dict):
                                topic = data.get("topic") or ""
                                if topic.startswith("tickers."):
                                    await handle_ws_ticker(data)
                                elif topic.startswith("kline."):
                                    await handle_ws_kline(data)
                                else:
                                    # pong/confirm/subscribed/ etc.
                                    pass

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.warning(f"WS ERROR: {msg}")
                            break
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                          aiohttp.WSMsgType.CLOSING):
                            break
            except Exception as e:
                logging.warning(f"WS connect fail: {e}")
                await asyncio.sleep(3.0)

async def handle_ws_ticker(payload: dict):
    """
    payload пример (Bybit v5):
    {
      "topic":"tickers.BTCUSDT",
      "type":"snapshot"|"delta",
      "data":[{"symbol":"BTCUSDT","lastPrice":"110023.2","turnover24h":"123...", "price24hPcnt":"-0.0115", ...}],
      "ts": 1710000000000
    }
    """
    arr = payload.get("data") or []
    if not arr:
        return
    for it in arr:
        try:
            symbol = it.get("symbol")
            last = float(it.get("lastPrice", 0) or 0)
            # На некоторых WS потоках поле называется "turnover24h", 24h проценты — "price24hPcnt"
            tov = float(it.get("turnover24h", 0) or 0)
            pct = float(it.get("price24hPcnt", 0) or 0)  # доли, не проценты
            await upsert_ticker(symbol, pct, tov, last)
        except Exception as e:
            logging.warning(f"[WS ticker parse] {e}")

async def handle_ws_kline(payload: dict):
    """
    payload пример (Bybit v5):
    {
      "topic":"kline.5.BTCUSDT",
      "type":"snapshot"|"delta",
      "data":[
        {"start":"1710000000000","end":"1710000300000","interval":"5",
         "open":"...","high":"...","low":"...","close":"...","volume":"...","turnover":"...", "confirm": true/false}
      ],
      "ts": ...
    }
    Сохраняем по ключу (symbol, interval, start_ts).
    """
    topic = payload.get("topic", "")
    parts = topic.split(".")
    if len(parts) != 3:
        return
    # parts: ["kline", "5", "BTCUSDT"]
    interval = parts[1]
    symbol = parts[2]
    arr = payload.get("data") or []
    for it in arr:
        try:
            ts_ms = int(it.get("start"))
            o = float(it.get("open", 0))
            h = float(it.get("high", 0))
            l = float(it.get("low", 0))
            c = float(it.get("close", 0))
            v = float(it.get("volume", 0))
            t = float(it.get("turnover", 0))
            await upsert_candle(symbol, interval, ts_ms, o, h, l, c, v, t)
        except Exception as e:
            logging.warning(f"[WS kline parse] {e}")

# ============== AIOHTTP SERVER (WEBHOOK) ==============
async def handle_health(_request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": VERSION, "source": "Bybit WS"})

async def handle_webhook(request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)
    try:
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logging.warning(f"[WEBHOOK ERR] {e}")
    return web.Response(text="OK")

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_post(f"/webhook/{TELEGRAM_TOKEN}", handle_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"HTTP server started on 0.0.0.0:{port}")

# ============== HANDLERS ==============
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(header_text() + f"\n\nДобро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit WS).",
                   reply_markup=bottom_menu())

@dp.message(Command("status"))
async def cmd_status(m: Message):
    await m.answer(
        "<b>Status</b>\n"
        f"Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}"
    )

@dp.message(Command("diag"))
async def cmd_diag(m: Message):
    # Быстрая проверка наличия данных в БД
    rows = await fetch_last_candles("BTCUSDT", INTERVAL, limit=5)
    await m.answer(
        "diag\n"
        f"WS consumer: running (see logs)\n"
        f"BTCUSDT candles(5m) in DB: {len(rows)}\n"
        "tickers filled: top turnover shown in /activity"
    )

@dp.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    await m.answer(header_text())
    txt = await render_activity()
    await m.answer(txt, reply_markup=bottom_menu())

@dp.message(F.text == "⚡ Волатильность")
async def on_vol(m: Message):
    await m.answer(header_text())
    txt = await render_volatility()
    await m.answer(txt, reply_markup=bottom_menu())

@dp.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    await m.answer(header_text())
    txt = await render_trend()
    await m.answer(txt, reply_markup=bottom_menu())

@dp.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    # Пока строим по 24h% + turnover (без matplotlib картинки, чтобы не задерживать)
    rows = await top_by_ticker("turnover24h", limit=12)
    if not rows:
        await m.answer("Не удалось собрать данные для Bubbles (ожидаю WS).")
        return
    lines = ["WS Bubbles (24h %, size~turnover24h)"]
    for r in rows:
        sym = r["symbol"]
        pct = (r["price24hPcnt"] or 0.0) * 100.0
        tov = r["turnover24h"] or 0.0
        lines.append(f"• {sym}  {pct:+.2f}%  turnover~{int(tov):,}".replace(",", " "))
    await m.answer("\n".join(lines), reply_markup=bottom_menu())

@dp.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer(header_text() + "\n\n📰 <b>Макро (последний час)</b>\n• demo headline")

@dp.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    # Заглушка: можно прикрутить генерацию Excel (openpyxl) — формулы из прежнего варианта
    await m.answer("Шаблон риск-менеджмента (Excel) добавим по кнопке чуть позже.")

@dp.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. (/add в этой версии ещё не подключён)")

@dp.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
        "⚙️ Настройки\n"
        "Биржа: Bybit (USDT perp, WS)\n"
        "Режим: active | Quiet: False\n"
        "Watchlist: —\n\n"
        "Команды:\n"
        "• /status — состояние\n"
        "• /diag   — диагностика\n"
        "• /menu   — восстановить клавиатуру"
    )

@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=bottom_menu())

# ============== ENTRYPOINT ==============
async def main():
    await db_init()
    await start_http_server()

    # Webhook
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    webhook_url = f"{BASE_URL}/webhook/{TELEGRAM_TOKEN}"
    allowed = dp.resolve_used_update_types()
    await bot.set_webhook(webhook_url, allowed_updates=allowed)
    logging.info(f"Webhook set to: {webhook_url}")

    # WS consumer
    asyncio.create_task(ws_consumer())

    # keep running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
