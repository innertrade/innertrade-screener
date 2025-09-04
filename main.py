import asyncio
import os
import sys
import contextlib
from datetime import datetime
from time import time
from statistics import mean

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web, ClientSession
from dotenv import load_dotenv

# ------------------ ENV ------------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
TIMEZONE = os.getenv("TZ", "Europe/Moscow")
if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https Render URL (e.g., https://your-service.onrender.com)")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ------------------ STATE ------------------
USERS: dict[int, dict] = {}
DEFAULTS = {
    "preset": "intraday",
    "mode": "active",     # "passive" → автолента каждые N минут
    "quiet": False,       # тихие часы
}

def ensure_user(user_id: int) -> dict:
    if user_id not in USERS:
        USERS[user_id] = DEFAULTS.copy()
    return USERS[user_id]

# ------------------ SIMPLE CACHE ------------------
CACHE: dict[str, tuple[float, str]] = {}  # key -> (ts, text)
def cache_get(key: str, ttl=60):
    item = CACHE.get(key)
    if not item:
        return None
    ts, val = item
    return val if (time() - ts) < ttl else None
def cache_set(key: str, val: str):
    CACHE[key] = (time(), val)

# ------------------ CONSTANTS ------------------
SYMBOLS_BYBIT = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT",
    "TONUSDT","ARBUSDT","OPUSDT","TRXUSDT","LINKUSDT","BNBUSDT","ADAUSDT",
]
BYBIT_API = "https://api.bybit.com"  # v5
SEM_LIMIT = 6
REQUEST_TIMEOUT = 15
HTTP_HEADERS = {"User-Agent": "InnertradeScreener/1.0 (+render.com)"}

# ------------------ MARKET HEADER (stub) ------------------
async def get_market_header() -> dict:
    # TODO: подключить реальные источники F&G, BTC.D, avg funding
    return {"fg": 34, "fg_delta": -3, "btcd": 54.1, "btcd_delta": 0.3, "funding": 0.012}

async def render_header_text() -> str:
    h = await get_market_header()
    return (
        "🧭 <b>Market mood</b>\n"
        f"BTC.D: {h['btcd']:.1f}% ({h['btcd_delta']:+.1f}) | "
        f"Funding avg: {h['funding']:+.3f}% | "
        f"F&G: {h['fg']} ({h['fg_delta']:+d})"
    )

# ------------------ HTTP ------------------
async def http_get_json(session: ClientSession, url: str, params: dict | None = None):
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status != 200:
                    print(f"[HTTP {resp.status}] {url} {params} -> {text[:200]}", file=sys.stderr)
                    await asyncio.sleep(0.4)
                    continue
                try:
                    return await resp.json()
                except Exception as e:
                    print(f"[JSON ERR] {url} -> {e} :: {text[:200]}", file=sys.stderr)
                    await asyncio.sleep(0.4)
        except Exception as e:
            print(f"[REQ ERR] {url} {params} -> {e}", file=sys.stderr)
            await asyncio.sleep(0.6)
    return None

# ------------------ BYBIT PROVIDERS ------------------
async def bybit_klines(session: ClientSession, symbol: str, interval_minutes: int, limit: int):
    # Bybit intervals: "1","3","5","15","30","60","120","240","360","720","D","W","M"
    interval = str(interval_minutes) if interval_minutes in (1,3,5,15,30,60,120,240,360,720) else "5"
    url = f"{BYBIT_API}/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
    return await http_get_json(session, url, params)

def parse_bybit_row(row):
    # [start, open, high, low, close, volume, turnover]
    o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
    v = float(row[5]); turnover = float(row[6])  # USDT
    return o, h, l, c, v, None, turnover

async def fetch_bybit_5m_1h(session: ClientSession, sym: str):
    sem = asyncio.Semaphore(SEM_LIMIT)
    async with sem:
        try:
            k5, k1h = await asyncio.wait_for(asyncio.gather(
                bybit_klines(session, sym, 5, 200),
                bybit_klines(session, sym, 60, 168)
            ), timeout=REQUEST_TIMEOUT)
            return sym, k5, k1h
        except Exception:
            return sym, None, None

# ------------------ INDICATORS ------------------
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
    if len(values) < lookback + 1:
        return 0.0
    return mean(values[-lookback:]) - mean(values[-2*lookback:-lookback])

# ------------------ BUILDERS ------------------
async def build_activity_bybit(session: ClientSession) -> list[dict]:
    tasks = [fetch_bybit_5m_1h(session, sym) for sym in SYMBOLS_BYBIT]
    raw = await asyncio.gather(*tasks)
    out = []
    for sym, k5, k1h in raw:
        if not k5 or not k1h or "result" not in k5 or "result" not in k1h:
            continue
        l5 = k5["result"].get("list") or []
        l1h = k1h["result"].get("list") or []
        if len(l5) < 30 or len(l1h) < 25:
            continue
        rows5 = [parse_bybit_row(r) for r in l5]
        turnover5 = [r[6] for r in rows5]  # денежный объём (USDT)
        turn_ma = moving_average(turnover5[:-1], 20)
        if not turn_ma or turn_ma == 0:
            continue
        turn_mult = turnover5[-1] / turn_ma

        rows1h = [parse_bybit_row(r) for r in l1h]
        turn1h = [r[6] for r in rows1h]
        vol_24h = sum(turn1h[-24:])
        vol_7d  = sum(turn1h[-168:])
        share24 = int(round((vol_24h / vol_7d) * 100)) if vol_7d > 0 else 0

        heatscore = turn_mult
        out.append({
            "symbol": sym, "venue": "Bybit",
            "vol_mult": turn_mult, "share24": share24, "heatscore": heatscore
        })
    out.sort(key=lambda x: x["heatscore"], reverse=True)
    return out[:10]

async def build_volatility_bybit(session: ClientSession) -> list[dict]:
    tasks = [asyncio.create_task(bybit_klines(session, sym, 5, 400)) for sym in SYMBOLS_BYBIT]
    raws = await asyncio.gather(*tasks)

    out = []
    for sym, k5 in zip(SYMBOLS_BYBIT, raws):
        if not k5 or "result" not in k5:
            continue
        l5 = k5["result"].get("list") or []
        if len(l5) < 100:
            continue
        rows5 = [parse_bybit_row(r) for r in l5]
        closes = [r[3] for r in rows5]
        turnover = [r[6] for r in rows5]
        atr_val = compute_atr(rows5, 14)
        last_close = closes[-1]
        if not atr_val or last_close <= 0:
            continue
        atr_pct = (atr_val / last_close) * 100.0
        turn_ma = moving_average(turnover[:-1], 20)
        vol_mult = (turnover[-1] / turn_ma) if turn_ma else 0
        out.append({"symbol": sym, "venue": "Bybit", "atr_pct": atr_pct, "vol_mult": vol_mult})
    out.sort(key=lambda x: x["atr_pct"], reverse=True)
    return out[:10]

async def build_trend_bybit(session: ClientSession) -> list[dict]:
    tasks = [asyncio.create_task(bybit_klines(session, sym, 5, 400)) for sym in SYMBOLS_BYBIT]
    raws = await asyncio.gather(*tasks)

    res = []
    for sym, k5 in zip(SYMBOLS_BYBIT, raws):
        if not k5 or "result" not in k5:
            continue
        l5 = k5["result"].get("list") or []
        if len(l5) < 380:
            continue
        rows5 = [parse_bybit_row(r) for r in l5]
        closes = [r[3] for r in rows5]
        ma200 = moving_average(closes, 200)
        ma360 = moving_average(closes, 360)
        if ma200 is None or ma360 is None:
            continue
        last_close = closes[-1]
        above200 = last_close > ma200
        above360 = last_close > ma360
        slope200 = slope(closes[-220:], 10)
        atr_now = compute_atr(rows5, 14)
        atr_prev = compute_atr(rows5[-60:-15], 14) if len(rows5) > 75 else None
        vol_change = "↑" if (atr_prev and atr_now and atr_now > atr_prev) else ("↓" if atr_prev else "≈")
        res.append({
            "symbol": sym, "venue": "Bybit",
            "above200": above200, "above360": above360,
            "slope200": slope200, "vol_change": vol_change
        })
    res.sort(key=lambda x: (x["above200"], x["above360"], x["slope200"]), reverse=True)
    return res[:10]

# ------------------ RENDER ------------------
async def render_activity() -> str:
    key = "activity:bybit"
    cached = cache_get(key, ttl=60)
    if cached:
        return cached
    async with ClientSession() as s:
        items = await build_activity_bybit(s)
    if not items:
        text = "\n🔥 <b>Активность</b>\nНет данных (лимиты API/таймаут или рынок тихий)."
        cache_set(key, text)
        return text
    lines = ["\n🔥 <b>Активность</b> (Bybit)"]
    for i, r in enumerate(items, start=1):
        lines.append(
            f"{i}) {r['symbol']} ({r['venue']}) Vol x{r['vol_mult']:.1f} | 24h vs 7d: {r['share24']}%"
        )
    text = "\n".join(lines)
    cache_set(key, text)
    return text

async def render_volatility() -> str:
    key = "vol:bybit"
    cached = cache_get(key, ttl=60)
    if cached:
        return cached
    async with ClientSession() as s:
        items = await build_volatility_bybit(s)
    if not items:
        text = "\n⚡ <b>Волатильность</b>\nНет данных."
        cache_set(key, text)
        return text
    lines = ["\n⚡ <b>Волатильность</b>  (ATR%, 5m, Bybit)"]
    for i, r in enumerate(items, start=1):
        lines.append(
            f"{i}) {r['symbol']} ({r['venue']}) ATR {r['atr_pct']:.2f}% | Vol x{r['vol_mult']:.1f}"
        )
    text = "\n".join(lines)
    cache_set(key, text)
    return text

async def render_trend() -> str:
    key = "trend:bybit"
    cached = cache_get(key, ttl=60)
    if cached:
        return cached
    async with ClientSession() as s:
        items = await build_trend_bybit(s)
    if not items:
        text = "\n📈 <b>Тренд</b>\nНет данных."
        cache_set(key, text)
        return text
    lines = ["\n📈 <b>Тренд</b>  (5m, MA200/MA360, Bybit)"]
    for i, r in enumerate(items, start=1):
        pos = [">MA200" if r["above200"] else "<MA200", ">MA360" if r["above360"] else "<MA360"]
        slope_tag = "slope+" if r["slope200"] > 0 else ("slope-" if r["slope200"] < 0 else "slope≈")
        lines.append(
            f"{i}) {r['symbol']} ({r['venue']}) {' & '.join(pos)} | {slope_tag} | вола {r['vol_change']}"
        )
    text = "\n".join(lines)
    cache_set(key, text)
    return text

# ------------------ NEWS (stub) ------------------
async def get_news_digest() -> list[str]:
    now = datetime.now(pytz.timezone(TIMEZONE)).strftime("%H:%M")
    return [f"{now} CPI (US) 3.1% vs 3.2% прогноз — риск-он", "SEC одобрила спотовый ETF ..."]

# ------------------ KEYBOARDS ------------------
def main_menu_kb(settings: dict) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📊 Активность",   callback_data="menu:activity")
    b.button(text="⚡ Волатильность", callback_data="menu:volatility")
    b.button(text="📈 Тренд",        callback_data="menu:trend")
    b.button(text="📰 Новости",      callback_data="menu:news")
    b.button(text="⚙️ Настройки",    callback_data="menu:settings")
    b.adjust(2, 2, 1)
    return b.as_markup()

def settings_kb(settings: dict) -> InlineKeyboardMarkup:
    mode  = settings.get("mode", "active")
    quiet = settings.get("quiet", False)
    b = InlineKeyboardBuilder()
    b.button(text=("🔔 Пассивный (лента)" if mode == "passive" else "🔕 Активный (по запросу)"), callback_data="set:mode")
    b.button(text=("🌙 Тихие часы: ON" if quiet else "🌙 Тихие часы: OFF"), callback_data="set:quiet")
    b.button(text="⬅️ Назад", callback_data="menu:back")
    b.adjust(1, 1, 1)
    return b.as_markup()

# ------------------ COMMANDS ------------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    u = ensure_user(m.from_user.id)
    header = await render_header_text()
    await m.answer(
        header + "\n\n"
        "Добро пожаловать в <b>Innertrade Screener</b> — интрадэй-скринер (Bybit).\n"
        "Выберите раздел или откройте настройки.",
        reply_markup=main_menu_kb(u)
    )

@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    u = ensure_user(m.from_user.id)
    await m.answer("Главное меню:", reply_markup=main_menu_kb(u))

@dp.message(Command("hot"))
async def cmd_hot(m: Message):
    header = await render_header_text()
    body = await render_activity()
    await m.answer(header + "\n" + body)

@dp.message(Command("news"))
async def cmd_news(m: Message):
    header = await render_header_text()
    items = await get_news_digest()
    news = "\n".join([f"• {x}" for x in items])
    await m.answer(header + "\n\n📰 <b>Макро (последний час)</b>\n" + news)

@dp.message(Command("quiet"))
async def cmd_quiet(m: Message):
    u = ensure_user(m.from_user.id)
    u["quiet"] = not u.get("quiet", False)
    await m.answer(f"Тихие часы: {'ON' if u['quiet'] else 'OFF'}")

@dp.message(Command("status"))
async def cmd_status(m: Message):
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    u = ensure_user(m.from_user.id)
    await m.answer(
        "<b>Status</b>\n"
        f"Time: {now} ({TIMEZONE})\n"
        f"Mode: {u['mode']} | Quiet: {u['quiet']}\n"
        "Source: Bybit (linear USDT)\n"
        "Webhook: ON\n"
    )

@dp.message(Command("preset"))
async def cmd_preset(m: Message):
    u = ensure_user(m.from_user.id)
    kb = InlineKeyboardBuilder()
    for name in ["scalp", "intraday", "swing"]:
        flag = "✅ " if u.get("preset") == name else ""
        kb.button(text=f"{flag}{name.title()}", callback_data=f"preset:{name}")
    kb.adjust(3)
    await m.answer("Выберите пресет:", reply_markup=kb.as_markup())

@dp.message(Command("watchlist"))
async def cmd_watchlist(m: Message):
    await m.answer("Watchlist — в следующей итерации. Команды будут: /add SYMBOL, /remove SYMBOL.")

@dp.message(Command("filters"))
async def cmd_filters(m: Message):
    await m.answer("Тонкая настройка фильтров появится позже. Пока используем пресеты.")

# ------------------ CALLBACKS ------------------
@dp.callback_query(F.data.startswith("menu:"))
async def on_menu(cb: CallbackQuery):
    key = cb.data.split(":", 1)[1]
    try:
        if key == "activity":
            header = await render_header_text()
            body = await render_activity()
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        elif key == "volatility":
            header = await render_header_text()
            body = await render_volatility()
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        elif key == "trend":
            header = await render_header_text()
            body = await render_trend()
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        elif key == "news":
            header = await render_header_text()
            items = await get_news_digest()
            news = "\n".join([f"• {x}" for x in items])
            await cb.message.edit_text(header + "\n\n📰 <b>Макро (последний час)</b>\n" + news, reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        elif key == "settings":
            await cb.message.edit_text("Настройки:", reply_markup=settings_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        elif key == "back":
            await cb.message.edit_text("Главное меню:", reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
            await cb.answer()
        else:
            await cb.answer("Неизвестный раздел", show_alert=True)
    except Exception:
        try:
            if key in ("activity","volatility","trend","news"):
                header = await render_header_text()
                if key == "activity":
                    body = await render_activity()
                elif key == "volatility":
                    body = await render_volatility()
                elif key == "trend":
                    body = await render_trend()
                else:
                    items = await get_news_digest()
                    body = "\n\n📰 <b>Макро (последний час)</b>\n" + "\n".join([f"• {x}" for x in items])
                await cb.message.answer(header + "\n" + body, reply_markup=main_menu_kb(ensure_user(cb.from_user.id)))
                await cb.answer()
        except Exception:
            with contextlib.suppress(Exception):
                await cb.answer()

@dp.callback_query(F.data.startswith("set:"))
async def on_set(cb: CallbackQuery):
    u = ensure_user(cb.from_user.id)
    key = cb.data.split(":", 1)[1]
    if key == "mode":
        u["mode"] = "passive" if u.get("mode") == "active" else "active"
        await cb.message.edit_reply_markup(reply_markup=settings_kb(u))
        await cb.answer(f"Режим: {u['mode']}")
    elif key == "quiet":
        u["quiet"] = not u.get("quiet", False)
        await cb.message.edit_reply_markup(reply_markup=settings_kb(u))
        await cb.answer(f"Тихие часы: {'ON' if u['quiet'] else 'OFF'}")
    else:
        await cb.answer("Неизвестный параметр", show_alert=True)

@dp.callback_query(F.data.startswith("preset:"))
async def on_preset(cb: CallbackQuery):
    u = ensure_user(cb.from_user.id)
    name = cb.data.split(":", 1)[1]
    u["preset"] = name
    await cb.answer(f"Пресет: {name}")

# ------------------ PASSIVE STREAM ------------------
async def passive_stream_worker():
    tz = pytz.timezone(TIMEZONE)
    while True:
        for user_id, st in USERS.items():
            if st.get("mode") != "passive":
                continue
            if st.get("quiet"):
                now = datetime.now(tz).time()
                if 0 <= now.hour <= 7:  # пример тихих часов
                    continue
            header = await render_header_text()
            body = await render_activity()
            try:
                await bot.send_message(user_id, header + "\n" + body)
            except Exception:
                pass
        await asyncio.sleep(900)  # каждые 15 минут

# ------------------ AIOHTTP APP (webhook + health) ------------------
async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "time": datetime.utcnow().isoformat()})

async def handle_webhook(request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)
    try:
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception as e:
        print(f"[WEBHOOK ERR] {e}", file=sys.stderr)
    return web.Response(status=200)

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    # безопасный путь с токеном, чтобы чужие не дёргали
    app.router.add_post(f"/webhook/{TOKEN}", handle_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# ------------------ ENTRYPOINT ------------------
async def main():
    # Поднимаем HTTP, затем настраиваем webhook
    await start_http_server()
    # Сносим прошлый и ставим новый webhook
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    webhook_url = f"{BASE_URL}/webhook/{TOKEN}"
    allowed = dp.resolve_used_update_types()
    await bot.set_webhook(webhook_url, allowed_updates=allowed)

    # Пассивная лента
    asyncio.create_task(passive_stream_worker())

    # Спим; апдейты приходят через webhook -> handle_webhook
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())