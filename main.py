import asyncio
import os
import contextlib
from datetime import datetime
from statistics import mean, stdev

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web, ClientSession
from dotenv import load_dotenv

# ------------------ ENV ------------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
TIMEZONE = os.getenv("TZ", "Europe/Moscow")
if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ------------------ STATE ------------------
USERS: dict[int, dict] = {}
DEFAULTS = {
    "preset": "intraday",
    "mode": "active",       # "passive" → автолента каждые N минут
    "quiet": False,         # тихие часы
    "exchange": "binance",  # ВКЛЮЧИ Binance по умолчанию (есть реальные данные)
}

def ensure_user(user_id: int) -> dict:
    if user_id not in USERS:
        USERS[user_id] = DEFAULTS.copy()
    return USERS[user_id]

# ------------------ CONSTANTS ------------------
BINANCE_FAPI = "https://fapi.binance.com"
SYMBOLS_BINANCE = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT",
    "ADAUSDT","TONUSDT","ARBUSDT","OPUSDT","TRXUSDT","LINKUSDT",
]

# ------------------ DATA PROVIDERS ------------------
# Шапка рынка (пока заглушка, подключим позже)
async def get_market_header() -> dict:
    return {
        "fg": 34,          # Fear & Greed
        "fg_delta": -3,    # изменение за 24ч
        "btcd": 54.1,      # BTC dominance %
        "btcd_delta": 0.3, # изменение за 24ч
        "funding": 0.012,  # средний funding %
    }

# ----- BINANCE HELPERS -----
async def http_get_json(session: ClientSession, url: str, params: dict | None = None):
    for _ in range(3):
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            await asyncio.sleep(0.5)
    return None

async def binance_klines(session: ClientSession, symbol: str, interval: str, limit: int = 500):
    """
    Возвращает список свечей: [open_time, open, high, low, close, volume,
                                close_time, quote_vol, trades, taker_buy_base, taker_buy_quote, ignore]
    """
    url = f"{BINANCE_FAPI}/fapi/v1/klines"
    return await http_get_json(session, url, {"symbol": symbol, "interval": interval, "limit": limit})

def parse_kline_row(row):
    # Преобразуем, что нужно
    o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
    v = float(row[5]); n_trades = int(row[8])
    return o, h, l, c, v, n_trades

# ------------------ INDICATORS ------------------
def compute_atr(ohlc_rows, period: int = 14):
    """ATR по списку кандлов (o,h,l,c,...)"""
    if len(ohlc_rows) < period + 1:
        return None
    trs = []
    prev_close = ohlc_rows[0][3]  # close первой свечи
    for i in range(1, len(ohlc_rows)):
        _, h, l, c, _, _ = ohlc_rows[i]
        tr = max(h - l, abs(h - prev_close), abs(prev_close - l))
        trs.append(tr)
        prev_close = c
    if len(trs) < period:
        return None
    # SMA ATR
    return mean(trs[-period:])

def moving_average(values, length):
    if len(values) < length:
        return None
    return mean(values[-length:])

def slope(values, lookback: int = 10):
    """Простая оценка наклона: текущее среднее - среднее lookback свечей назад."""
    if len(values) < lookback + 1:
        return 0.0
    return mean(values[-lookback:]) - mean(values[-2*lookback:-lookback])

# ------------------ RENDER UTILS ------------------
async def render_header_text() -> str:
    hdr = await get_market_header()
    return (
        "🧭 <b>Market mood</b>\n"
        f"BTC.D: {hdr['btcd']:.1f}% ({hdr['btcd_delta']:+.1f}) | "
        f"Funding avg: {hdr['funding']:+.3f}% | "
        f"F&G: {hdr['fg']} ({hdr['fg_delta']:+d})"
    )

# ------------------ SECTIONS: ACTIVITY / VOLATILITY / TREND ------------------
async def build_activity_binance(session: ClientSession) -> list[dict]:
    """
    Активность (Binance):
    - Vol spike vs MA(20) на 5m
    - Trades/min proxy = kline['number of trades'] spike vs MA(20)
    - 24h vs 7d доля по объёму (чтобы понять "ещё в игре")
    """
    results = []
    for sym in SYMBOLS_BINANCE:
        # 5m для объёмов/сделок
        k5 = await binance_klines(session, sym, "5m", limit=200)
        if not k5 or len(k5) < 30:
            continue
        rows5 = [parse_kline_row(r) for r in k5]
        vols = [r[4] for r in rows5]  # volume
        trades = [r[5] for r in rows5]  # number of trades
        vol_ma = moving_average(vols[:-1], 20)
        tr_ma  = moving_average(trades[:-1], 20)
        if not vol_ma or not tr_ma:
            continue
        vol_now = vols[-1]
        tr_now  = trades[-1]
        vol_mult = vol_now / vol_ma if vol_ma else 0
        tr_mult  = tr_now / tr_ma if tr_ma else 0
        trades_flag = "↑" if tr_mult >= 1.5 else ("→" if tr_mult >= 0.9 else "↓")

        # 1h для 24h/7d
        k1h = await binance_klines(session, sym, "1h", limit=168)  # 7д
        if not k1h or len(k1h) < 25:
            continue
        rows1h = [parse_kline_row(r) for r in k1h]
        vols1h = [r[4] for r in rows1h]
        vol_24h = sum(vols1h[-24:])
        vol_7d  = sum(vols1h[-168:])
        share24 = int(round((vol_24h / vol_7d) * 100)) if vol_7d > 0 else 0

        heatscore = 0.6 * vol_mult + 0.4 * tr_mult
        results.append({
            "symbol": sym,
            "venue": "Binance",
            "vol_mult": vol_mult,
            "tr_mult": tr_mult,
            "tr_flag": trades_flag,
            "share24": share24,
            "heatscore": heatscore
        })

        # лёгкий лимит, чтобы не упираться в rate-limit
        await asyncio.sleep(0.05)

    results.sort(key=lambda x: x["heatscore"], reverse=True)
    return results[:10]

async def build_volatility_binance(session: ClientSession) -> list[dict]:
    """
    Волатильность (Binance):
    - ATR(14) на 5m → в % к цене
    - Vol spike vs MA(20) на 5m (как подтверждение)
    """
    out = []
    for sym in SYMBOLS_BINANCE:
        k5 = await binance_klines(session, sym, "5m", limit=400)
        if not k5 or len(k5) < 100:
            continue
        rows5 = [parse_kline_row(r) for r in k5]
        closes = [r[3] for r in rows5]  # close = index 3
        vols = [r[4] for r in rows5]

        atr_val = compute_atr(rows5, period=14)
        last_close = closes[-1]
        if not atr_val or last_close <= 0:
            continue
        atr_pct = (atr_val / last_close) * 100.0

        vol_ma = moving_average(vols[:-1], 20)
        vol_mult = (vols[-1] / vol_ma) if vol_ma else 0

        out.append({
            "symbol": sym,
            "venue": "Binance",
            "atr_pct": atr_pct,
            "vol_mult": vol_mult
        })
        await asyncio.sleep(0.05)

    out.sort(key=lambda x: x["atr_pct"], reverse=True)
    return out[:10]

async def build_trend_binance(session: ClientSession) -> list[dict]:
    """
    Тренд (Binance):
    - Позиция цены относительно MA200 и MA360 (на 5m)
    - Наклон (slope) MA200
    - Изменение волатильности: ATR_now vs ATR_avg_prev
    """
    res = []
    for sym in SYMBOLS_BINANCE:
        k5 = await binance_klines(session, sym, "5m", limit=400)
        if not k5 or len(k5) < 380:
            continue
        rows5 = [parse_kline_row(r) for r in k5]
        closes = [r[3] for r in rows5]
        highs  = [r[1] for r in rows5]  # NOTE: parse_kline_row returns (o,h,l,c,v,n)
        lows   = [r[2] for r in rows5]

        ma200 = moving_average(closes, 200)
        ma360 = moving_average(closes, 360)
        if ma200 is None or ma360 is None:
            continue
        last_close = closes[-1]
        above200 = last_close > ma200
        above360 = last_close > ma360
        slope200 = slope(closes[-220:], 10)  # локальный наклон по последним ~200 барам

        # Волатильность: ATR динамика
        # Соберём TR на 14 и сравним с прошлым средним
        ohlc = rows5
        atr_now = compute_atr(ohlc, 14)
        # среднее ATR по предыдущему блоку (например, свечи -40..-15)
        if len(ohlc) > 60 and atr_now:
            prev_block = ohlc[-60:-30]
            atr_prev = compute_atr(prev_block + ohlc[-30:-15], 14)  # грубое среднее
        else:
            atr_prev = None
        vol_change = "↑" if (atr_prev and atr_now > atr_prev) else ("↓" if atr_prev else "≈")

        res.append({
            "symbol": sym,
            "venue": "Binance",
            "above200": above200,
            "above360": above360,
            "slope200": slope200,
            "vol_change": vol_change
        })
        await asyncio.sleep(0.05)

    # Приоритизируем: сначала над МАми и с положительным наклоном
    res.sort(key=lambda x: (x["above200"], x["above360"], x["slope200"]), reverse=True)
    return res[:10]

# ------------------ RENDER SECTIONS ------------------
async def render_activity(exchange: str) -> str:
    if exchange == "binance":
        async with ClientSession() as s:
            items = await build_activity_binance(s)
        if not items:
            return "\n🔥 <b>Активность</b>\nНет данных."
        lines = ["\n🔥 <b>Активность</b>"]
        for i, r in enumerate(items, start=1):
            lines.append(
                f"{i}) {r['symbol']} ({r['venue']}) "
                f"Vol x{r['vol_mult']:.1f} | Trades {r['tr_flag']} | "
                f"24h vs 7d: {r['share24']}%"
            )
        return "\n".join(lines)
    else:
        return "\n🔥 <b>Активность</b>\nBybit подключим в следующем апдейте."

async def render_volatility(exchange: str) -> str:
    if exchange == "binance":
        async with ClientSession() as s:
            items = await build_volatility_binance(s)
        if not items:
            return "\n⚡ <b>Волатильность</b>\nНет данных."
        lines = ["\n⚡ <b>Волатильность</b>  (ATR%, 5m)"]
        for i, r in enumerate(items, start=1):
            lines.append(
                f"{i}) {r['symbol']} ({r['venue']}) ATR {r['atr_pct']:.2f}% | Vol x{r['vol_mult']:.1f}"
            )
        return "\n".join(lines)
    else:
        return "\n⚡ <b>Волатильность</b>\nBybit подключим в следующем апдейте."

async def render_trend(exchange: str) -> str:
    if exchange == "binance":
        async with ClientSession() as s:
            items = await build_trend_binance(s)
        if not items:
            return "\n📈 <b>Тренд</b>\nНет данных."
        lines = ["\n📈 <b>Тренд</b>  (5m, MA200/MA360)"]
        for i, r in enumerate(items, start=1):
            pos = []
            if r["above200"]: pos.append(">MA200")
            else: pos.append("<MA200")
            if r["above360"]: pos.append(">MA360")
            else: pos.append("<MA360")
            slope_tag = "slope+" if r["slope200"] > 0 else ("slope-" if r["slope200"] < 0 else "slope≈")
            lines.append(
                f"{i}) {r['symbol']} ({r['venue']}) {' & '.join(pos)} | {slope_tag} | вола {r['vol_change']}"
            )
        return "\n".join(lines)
    else:
        return "\n📈 <b>Тренд</b>\nBybit подключим в следующем апдейте."

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
    ex    = settings.get("exchange", "binance")

    b = InlineKeyboardBuilder()
    b.button(text=("🔔 Пассивный (лента)" if mode == "passive" else "🔕 Активный (по запросу)"), callback_data="set:mode")
    b.button(text=("🌙 Тихие часы: ON" if quiet else "🌙 Тихие часы: OFF"), callback_data="set:quiet")
    b.button(text=("Биржа: Bybit" if ex == "bybit" else "Биржа: Binance"), callback_data="set:exchange")
    b.button(text="⬅️ Назад", callback_data="menu:back")
    b.adjust(1, 1, 1, 1)
    return b.as_markup()

# ------------------ COMMANDS ------------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    u = ensure_user(m.from_user.id)
    header = await render_header_text()
    await m.answer(
        header + "\n\n"
        "Добро пожаловать в <b>Innertrade Screener</b> — интрадэй-скринер.\n"
        "Выберите раздел или откройте настройки.",
        reply_markup=main_menu_kb(u)
    )

@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    u = ensure_user(m.from_user.id)
    await m.answer("Главное меню:", reply_markup=main_menu_kb(u))

@dp.message(Command("hot"))
async def cmd_hot(m: Message):
    u = ensure_user(m.from_user.id)
    header = await render_header_text()
    body = await render_activity(u["exchange"])
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
        f"Mode: {u['mode']} | Quiet: {u['quiet']} | Exchange: {u['exchange']}\n"
        "Sources: Binance OK; Bybit (в разработке)\n"
        "Latency: varies by API\n"
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
    await m.answer("Watchlist появится в следующей итерации. Команды: /add SYMBOL, /remove SYMBOL.")

@dp.message(Command("filters"))
async def cmd_filters(m: Message):
    await m.answer("Тонкая настройка фильтров появится после первого запуска. Пока используем пресеты.")

# ------------------ CALLBACKS ------------------
@dp.callback_query(F.data.startswith("menu:"))
async def on_menu(cb: CallbackQuery):
    u = ensure_user(cb.from_user.id)
    key = cb.data.split(":", 1)[1]

    try:
        if key == "activity":
            header = await render_header_text()
            body = await render_activity(u["exchange"])
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(u))
            await cb.answer()

        elif key == "volatility":
            header = await render_header_text()
            body = await render_volatility(u["exchange"])
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(u))
            await cb.answer()

        elif key == "trend":
            header = await render_header_text()
            body = await render_trend(u["exchange"])
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(u))
            await cb.answer()

        elif key == "news":
            header = await render_header_text()
            items = await get_news_digest()
            news = "\n".join([f"• {x}" for x in items])
            await cb.message.edit_text(header + "\n\n📰 <b>Макро (последний час)</b>\n" + news, reply_markup=main_menu_kb(u))
            await cb.answer()

        elif key == "settings":
            await cb.message.edit_text("Настройки:", reply_markup=settings_kb(u))
            await cb.answer()

        elif key == "back":
            await cb.message.edit_text("Главное меню:", reply_markup=main_menu_kb(u))
            await cb.answer()

        else:
            await cb.answer("Неизвестный раздел", show_alert=True)

    except Exception:
        # fallback, если edit_text не сработал (message is not modified и т.п.)
        try:
            if key in ("activity", "volatility", "trend"):
                header = await render_header_text()
                if key == "activity":
                    body = await render_activity(u["exchange"])
                elif key == "volatility":
                    body = await render_volatility(u["exchange"])
                else:
                    body = await render_trend(u["exchange"])
                await cb.message.answer(header + "\n" + body, reply_markup=main_menu_kb(u))
                await cb.answer()
            elif key == "news":
                header = await render_header_text()
                items = await get_news_digest()
                news = "\n".join([f"• {x}" for x in items])
                await cb.message.answer(header + "\n\n📰 <b>Макро (последний час)</b>\n" + news, reply_markup=main_menu_kb(u))
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

    elif key == "exchange":
        u["exchange"] = "binance" if u.get("exchange") == "bybit" else "bybit"
        await cb.message.edit_reply_markup(reply_markup=settings_kb(u))
        await cb.answer(f"Биржа: {u['exchange'].title()}")

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
            body = await render_activity(st.get("exchange", "binance"))
            try:
                await bot.send_message(user_id, header + "\n" + body)
            except Exception:
                pass
        await asyncio.sleep(900)  # каждые 15 минут

# ------------------ HEALTH ------------------
async def health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "time": datetime.utcnow().isoformat()})

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# ------------------ ENTRYPOINT ------------------
async def main():
    asyncio.create_task(start_http_server())
    asyncio.create_task(passive_stream_worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
