import asyncio
import os
from datetime import datetime

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
TIMEZONE = os.getenv("TZ", "Europe/Stockholm")

if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

bot = Bot(TOKEN, parse_mode="HTML")
dp = Dispatcher()

# --- In-memory settings (MVP) ---
USERS = {}
DEFAULTS = {"preset": "intraday", "mode": "active", "quiet": False, "exchange": "bybit"}

# --- Dummy data ---
async def get_market_header():
    return {"fg": 34, "fg_delta": -3, "btcd": 54.1, "btcd_delta": 0.3, "funding": 0.012}

async def get_hot_activity(exchange: str):
    return [
        {"symbol": "SOLUSDT", "venue": exchange, "doi": 12.4, "vol_mult": 2.3, "trades": "↑", "depth": "высок.", "share24": 52},
        {"symbol": "DOGEUSDT", "venue": exchange, "doi": 8.1, "vol_mult": 1.9, "trades": "↑", "depth": "средн.", "share24": 41},
    ]

async def get_news_digest():
    now = datetime.now(pytz.timezone(TIMEZONE)).strftime("%H:%M")
    return [f"{now} CPI (US) 3.1% vs 3.2% прогноз — риск-он", "SEC одобрила спотовый ETF ..."]

# --- Helpers ---
def ensure_user(user_id: int):
    if user_id not in USERS:
        USERS[user_id] = DEFAULTS.copy()
    return USERS[user_id]

async def render_header() -> str:
    hdr = await get_market_header()
    return (
        f"🧭 <b>Market mood</b>\n"
        f"BTC.D: {hdr['btcd']:.1f}% ({hdr['btcd_delta']:+.1f}) | "
        f"Funding avg: {hdr['funding']:+.3f}% | "
        f"F&G: {hdr['fg']} ({hdr['fg_delta']:+d})"
    )

async def render_activity(exchange: str) -> str:
    rows = await get_hot_activity(exchange)
    lines = ["\n🔥 <b>Активность</b>"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}) {r['symbol']} ({r['venue'].capitalize()}) "
            f"ΔOI {r['doi']:+.1f}% | Vol x{r['vol_mult']:.1f} | Trades/min {r['trades']} | Depth: {r['depth']}\n"
            f"   24h vs 7d: {r['share24']}% недельного объёма за сутки → still in play"
        )
    return "\n".join(lines)

# --- Keyboards ---
def main_menu_kb(settings: dict) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📊 Активность", callback_data="menu:activity")
    b.button(text="⚡ Волатильность", callback_data="menu:volatility")
    b.button(text="📈 Тренд", callback_data="menu:trend")
    b.button(text="📰 Новости", callback_data="menu:news")
    b.button(text="⚙️ Настройки", callback_data="menu:settings")
    b.adjust(2, 2, 1)
    return b.as_markup()

# --- Handlers ---
@dp.message(Command("start"))
async def cmd_start(m: Message):
    u = ensure_user(m.from_user.id)
    header = await render_header()
    await m.answer(
        header + "\n\nДобро пожаловать в <b>Innertrade Screener</b> — интрадэй-скринер.\nВыберите раздел или откройте настройки.",
        reply_markup=main_menu_kb(u)
    )

@dp.message(Command("hot"))
async def cmd_hot(m: Message):
    u = ensure_user(m.from_user.id)
    header = await render_header()
    body = await render_activity(u["exchange"])
    await m.answer(header + "\n" + body)

@dp.message(Command("news"))
async def cmd_news(m: Message):
    header = await render_header()
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

# --- Health endpoint ---
async def health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "time": datetime.utcnow().isoformat()})

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

async def main():
    asyncio.create_task(start_http_server())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
