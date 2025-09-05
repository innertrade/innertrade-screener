import os
import asyncio
import logging
from io import BytesIO
from statistics import mean
from time import time
from datetime import datetime
import contextlib

import pytz
import aiohttp
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, Update, BufferedInputFile
)

from aiohttp import web
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side

# ---------------- ENV & CONFIG ----------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
TZ = os.getenv("TZ", "Europe/Stockholm")
VERSION = "v0.7-webhook"

if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https Render URL, e.g. https://<service>.onrender.com")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ---------------- CONSTANTS ----------------
BYBIT_API = "https://api.bybit.com"
REQUEST_TIMEOUT = 25
HTTP_HEADERS = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

SYMBOLS_BYBIT = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","LINKUSDT","TRXUSDT","TONUSDT","ARBUSDT","OPUSDT"
]

# ---------------- RUNTIME STATE ----------------
USERS: dict[int, dict] = {}
DEFAULT_USER = {
    "exchange": "bybit",
    "mode": "active",
    "quiet": False,
    "watchlist": [],
    "alerts": {"vol_mult": 1.5, "pct24_abs": 3.0, "cooldown_min": 20},
    "last_alert": {}
}

def ensure_user(uid: int) -> dict:
    if uid not in USERS:
        st = {k: (v.copy() if isinstance(v, dict) else (v[:] if isinstance(v, list) else v))
              for k, v in DEFAULT_USER.items()}
        st["watchlist"] = []
        st["last_alert"] = {}
        USERS[uid] = st
    return USERS[uid]

# ---------------- SIMPLE CACHE ----------------
CACHE: dict[str, tuple[float, str]] = {}

def cache_get(key: str, ttl: int):
    item = CACHE.get(key)
    if not item:
        return None
    ts, val = item
    return val if (time() - ts) < ttl else None

def cache_set(key: str, val: str):
    CACHE[key] = (time(), val)

# ---------------- MARKET HEADER ----------------
async def render_header_text() -> str:
    return """🧭 <b>Market mood</b>
BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)"""

# ---------------- HTTP HELPERS ----------------
async def http_get_json(session: aiohttp.ClientSession, url: str, params: dict | None = None):
    for attempt in range(4):
        try:
            async with session.get(url, params=params, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT) as r:
                text = await r.text()
                if r.status != 200:
                    logging.warning(f"[HTTP {r.status}] {url} {params} -> {text[:160]}")
                    await asyncio.sleep(0.8 + 0.3*attempt)
                    continue
                try:
                    return await r.json()
                except Exception as e:
                    logging.warning(f"[JSON ERR] {url} -> {e}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            logging.warning(f"[REQ ERR] {url} {params} -> {e}")
            await asyncio.sleep(0.9 + 0.2*attempt)
    return None

# ---------------- BYBIT PROVIDERS ----------------
async def bybit_klines(session: aiohttp.ClientSession, symbol: str, interval_minutes: int, limit: int):
    interval = str(interval_minutes) if interval_minutes in (1,3,5,15,30,60,120,240,360,720) else "5"
    url = f"{BYBIT_API}/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
    return await http_get_json(session, url, params)

async def bybit_ticker(session: aiohttp.ClientSession, symbol: str):
    url = f"{BYBIT_API}/v5/market/tickers"
    params = {"category": "linear", "symbol": symbol}
    return await http_get_json(session, url, params)

def parse_bybit_row(row):
    try:
        o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
        v = float(row[5]); turnover = float(row[6])
        return o, h, l, c, v, turnover
    except Exception:
        return 0,0,0,0,0,0

# ---------------- INDICATORS ----------------
def moving_average(values, length):
    if not values or len(values) < length:
        return None
    return mean(values[-length:])

def compute_atr(ohlc_rows, period: int = 14):
    if len(ohlc_rows) < period + 1:
        return None
    trs = []
    prev_close = ohlc_rows[0][3]
    for i in range(1, len(ohlc_rows)):
        _, h, l, c, *_ = ohlc_rows[i]
        tr = max(h - l, abs(h - prev_close), abs(prev_close - l))
        trs.append(tr)
        prev_close = c
    if len(trs) < period:
        return None
    return mean(trs[-period:])

def slope(values, lookback: int = 10):
    if len(values) < 2 * lookback:
        return 0.0
    return mean(values[-lookback:]) - mean(values[-2*lookback:-lookback])

# ---------------- RENDERERS ----------------
async def render_activity_text() -> str:
    return """🔥 <b>Активность</b>
Нет данных (тихо/таймаут/лимиты)."""

async def render_volatility_text() -> str:
    return """⚡ <b>Волатильность</b>
Нет данных."""

async def render_trend_text() -> str:
    return """📈 <b>Тренд</b>
Нет данных."""

# ---------------- COMMANDS ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    header = await render_header_text()
    await m.answer(header + f"""

Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit).""")

@dp.message(Command("status"))
async def cmd_status(m: Message):
    now = datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M:%S")
    await m.answer(f"""<b>Status</b>
Time: {now} ({TZ})
Mode: active | Quiet: False
Source: Bybit (linear USDT)
Watchlist: —
Webhook: ON
Version: {VERSION}""")

# ---------------- SERVER ----------------
async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": VERSION})

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
    return web.Response(status=200)

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_post(f"/webhook/{TOKEN}", handle_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"HTTP server started on 0.0.0.0:{port}")

# ---------------- ENTRY ----------------
async def main():
    await start_http_server()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    webhook_url = f"{BASE_URL}/webhook/{TOKEN}"
    allowed = dp.resolve_used_update_types()
    await bot.set_webhook(webhook_url, allowed_updates=allowed)
    logging.info(f"Webhook set to: {webhook_url}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
