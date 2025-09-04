import asyncio
import os
import contextlib
from datetime import datetime

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web
from dotenv import load_dotenv

# ------------------ ENV ------------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
TIMEZONE = os.getenv("TZ", "Europe/Moscow")
if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

# aiogram 3.13+: parse_mode через DefaultBotProperties
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ------------------ STATE (MVP) ------------------
USERS: dict[int, dict] = {}
DEFAULTS = {
    "preset": "intraday",
    "mode": "active",      # "passive" → автолента
    "quiet": False,        # тихие часы
    "exchange": "bybit",   # или "binance"
}

def ensure_user(user_id: int) -> dict:
    if user_id not in USERS:
        USERS[user_id] = DEFAULTS.copy()
    return USERS[user_id]

# ------------------ DATA PROVIDERS (stubs) ------------------
async def get_market_header() -> dict:
    """
    TODO: Подключить реальные источники:
      - Fear & Greed Index
      - BTC dominance
      - Средний funding по топ-парам
    """
    return {
        "fg": 34,          # Fear & Greed
        "fg_delta": -3,    # изменение за 24ч
        "btcd": 54.1,      # BTC dominance %
        "btcd_delta": 0.3, # изменение за 24ч
        "funding": 0.012,  # средний funding %
    }

async def get_hot_activity(exchange: str) -> list[dict]:
    """
    TODO: Реальные метрики: всплеск объёма vs MA, ΔOI, proxy trades/min, 24h vs 7d
    """
    return [
        {"symbol": "SOLUSDT", "venue": exchange, "doi": 12.4, "vol_mult": 2.3, "trades": "↑", "depth": "высок.", "share24": 52},
        {"symbol": "DOGEUSDT", "venue": exchange, "doi": 8.1,  "vol_mult": 1.9, "trades": "↑", "depth": "средн.", "share24": 41},
    ]

async def get_news_digest() -> list[str]:
    """
    TODO: Подключить макро/крипто-ленты (CPI/FOMC/листинги)
    """
    now = datetime.now(pytz.timezone(TIMEZONE)).strftime("%H:%M")
    return [
        f"{now} CPI (US) 3.1% vs 3.2% прогноз — риск-он",
        "SEC одобрила спотовый ETF ...",
    ]

# ------------------ RENDER ------------------
async def render_header_text() -> str:
    hdr = await get_market_header()
    return (
        "🧭 <b>Market mood</b>\n"
        f"BTC.D: {hdr['btcd']:.1f}% ({hdr['btcd_delta']:+.1f}) | "
        f"Funding avg: {hdr['funding']:+.3f}% | "
        f"F&G: {hdr['fg']} ({hdr['fg_delta']:+d})"
    )

async def render_activity_block(exchange: str) -> str:
    rows = await get_hot_activity(exchange)
    lines = ["\n🔥 <b>Активность</b>"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}) {r['symbol']} ({r['venue'].capitalize()}) "
            f"ΔOI {r['doi']:+.1f}% | Vol x{r['vol_mult']:.1f} | "
            f"Trades/min {r['trades']} | Depth: {r['depth']}\n"
            f"   24h vs 7d: {r['share24']}% недельного объёма за сутки → still in play"
        )
    return "\n".join(lines)

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
    ex    = settings.get("exchange", "bybit")

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
    body = await render_activity_block(u["exchange"])
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
        "Sources: Bybit OK, Binance OK\n"
        "Latency: market header ~0.2s (stub)\n"
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
            body = await render_activity_block(u["exchange"])
            await cb.message.edit_text(header + "\n" + body, reply_markup=main_menu_kb(u))
            await cb.answer()  # гасим «крутилку»

        elif key == "news":
            header = await render_header_text()
            items = await get_news_digest()
            news = "\n".join([f"• {x}" for x in items])
            await cb.message.edit_text(header + "\n\n📰 <b>Макро (последний час)</b>\n" + news, reply_markup=main_menu_kb(u))
            await cb.answer()

        elif key == "settings":
            await cb.message.edit_text("Настройки:", reply_markup=settings_kb(u))
            await cb.answer()

        elif key == "trend":
            await cb.answer("Раздел в разработке", show_alert=True)

        elif key == "volatility":
            await cb.answer("Раздел в разработке", show_alert=True)

        elif key == "back":
            await cb.message.edit_text("Главное меню:", reply_markup=main_menu_kb(u))
            await cb.answer()

        else:
            await cb.answer("Неизвестный раздел", show_alert=True)

    except Exception:
        # fallback: если edit_text не сработал (например, message is not modified)
        try:
            if key == "activity":
                header = await render_header_text()
                body = await render_activity_block(u["exchange"])
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
            body = await render_activity_block(st.get("exchange", "bybit"))
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
