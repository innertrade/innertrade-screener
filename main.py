import os
import asyncio
import logging
import json
import html
from io import BytesIO
from typing import Dict, Any, List

from aiohttp import web, ClientSession, ClientTimeout, WSMsgType

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ========= ENV / CONFIG =========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

VERSION = "v0.8.7-websocket"
WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https URL, e.g. https://<service>.onrender.com")

SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","LINKUSDT","TRXUSDT","TONUSDT",
    "ARBUSDT","OPUSDT"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ========= BOT / ROUTER =========
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()

def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"),      KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"),    KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"),   KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите раздел…",
    )

# ========= WS HELPERS =========
def _ingest_ticker_payload(out: Dict[str, Dict[str, Any]], payload: Dict[str, Any]):
    """Сливаем тикер в out, выбирая 'лучшие' ненулевые значения."""
    sym = payload.get("symbol")
    if not sym:
        return
    try:
        last = float(payload.get("lastPrice", 0.0))
    except Exception:
        last = 0.0
    try:
        pct = float(payload.get("price24hPcnt", 0.0)) * 100.0
    except Exception:
        pct = 0.0
    try:
        turn = float(payload.get("turnover24h", 0.0))
    except Exception:
        turn = 0.0

    cur = out.get(sym, {"last": 0.0, "pct24": 0.0, "turn24": 0.0})
    # выбираем «лучшее»: ненулевое > нулевого; по turn24 — большее информативнее
    best_last = last if (last != 0.0 or cur["last"] == 0.0) else cur["last"]
    best_pct  = pct  if (pct  != 0.0 or cur["pct24"] == 0.0) else cur["pct24"]
    best_turn = turn if (turn > cur["turn24"]) else cur["turn24"]
    out[sym] = {"last": best_last, "pct24": best_pct, "turn24": best_turn}

async def ws_collect_tickers(symbols: List[str], collect_secs: float = 8.0) -> Dict[str, Dict[str, Any]]:
    """
    Подключаемся к публичному WS и собираем тикеры (24ч) для заданных символов.
    Возвращаем срез: {symbol: {"last": float, "turn24": float, "pct24": float}}
    """
    out: Dict[str, Dict[str, Any]] = {}
    timeout = ClientTimeout(total=collect_secs + 5.0)
    sub_args = [f"tickers.{s}" for s in symbols]

    async with ClientSession(timeout=timeout) as s:
        async with s.ws_connect(WS_PUBLIC_LINEAR) as ws:
            await ws.send_str(json.dumps({"op": "subscribe", "args": sub_args}))
            end_t = asyncio.get_event_loop().time() + collect_secs
            while asyncio.get_event_loop().time() < end_t:
                try:
                    msg = await ws.receive(timeout=collect_secs)
                except Exception:
                    break
                if msg.type != WSMsgType.TEXT:
                    if msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR, WSMsgType.CLOSE):
                        break
                    continue
                try:
                    data = msg.json(loads=json.loads)
                except Exception:
                    continue
                if not isinstance(data, dict):
                    continue
                topic = data.get("topic", "")
                if not topic.startswith("tickers."):
                    continue
                payload = data.get("data")
                # Bybit может прислать либо dict, либо list (snapshot)
                if isinstance(payload, dict):
                    _ingest_ticker_payload(out, payload)
                elif isinstance(payload, list):
                    for it in payload:
                        if isinstance(it, dict):
                            _ingest_ticker_payload(out, it)
    return out

def render_bubbles_png(items: List[Dict[str, Any]]) -> bytes:
    buf = BytesIO()
    if not items:
        fig = plt.figure(figsize=(8,4), dpi=160)
        ax = fig.add_subplot(111); ax.axis("off")
        ax.text(0.5,0.5,"Нет данных для пузырьков (WS)", ha="center", va="center", fontsize=16)
        fig.savefig(buf, format="png"); plt.close(fig)
        return buf.getvalue()

    turns = np.array([max(1.0, it["turn24"]) for it in items], dtype=float)
    sizes = np.sqrt(turns)
    k = (8000.0 / sizes.max()) if sizes.max() > 0 else 1.0
    s = sizes * k

    n = len(items)
    cols = int(np.ceil(np.sqrt(n)))
    rows = int(np.ceil(n / cols))
    xs, ys = [], []
    for i in range(n):
        r = i // cols
        c = i % cols
        xs.append(c)
        ys.append(rows - 1 - r)
    xs = np.array(xs); ys = np.array(ys)

    colors = ["#16a34a" if it["pct24"] > 0.5 else ("#dc2626" if it["pct24"] < -0.5 else "#6b7280") for it in items]
    labels = [f"{it['symbol']}\n{it['pct24']:+.1f}%" for it in items]

    fig = plt.figure(figsize=(12,7), dpi=160)
    ax = fig.add_subplot(111)
    ax.set_facecolor("#0b1020"); fig.patch.set_facecolor("#0b1020")
    ax.scatter(xs, ys, s=s, c=colors, alpha=0.85)
    for x, y, lab in zip(xs, ys, labels):
        ax.text(x, y, lab, ha="center", va="center", color="white", fontsize=9, weight="bold")
    ax.set_xticks([]); ax.set_yticks([])
    ax.set_xlim(-0.8, cols-0.2); ax.set_ylim(-0.2, rows-0.2)
    ax.set_title("Daily Bubbles (Bybit WS, 24h% | size ~ turnover24h)", color="white", fontsize=14)
    fig.tight_layout()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

# ========= HANDLERS =========
@router.message(F.text == "/start")
async def cmd_start(m: Message):
    await m.answer(
        "🧭 <b>Market mood</b>\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit WS).",
        reply_markup=bottom_menu()
    )

@router.message(F.text == "/menu")
async def cmd_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=bottom_menu())

@router.message(F.text == "/status")
async def cmd_status(m: Message):
    await m.answer(
        "<b>Status</b>\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}"
    )

@router.message(F.text == "/diag")
async def cmd_diag(m: Message):
    ok = {"ticker": False}
    timeout = ClientTimeout(total=10)
    try:
        async with ClientSession(timeout=timeout) as s:
            async with s.ws_connect(WS_PUBLIC_LINEAR) as ws:
                await ws.send_str(json.dumps({"op": "subscribe", "args": ["tickers.BTCUSDT"]}))
                for _ in range(20):
                    msg = await ws.receive(timeout=8.0)
                    if msg.type == WSMsgType.TEXT:
                        data = msg.json(loads=json.loads)
                        if isinstance(data, dict) and data.get("topic", "").startswith("tickers."):
                            ok["ticker"] = True
                            break
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        await m.answer(f"diag\nWS public: ok=False err={html.escape(err)}")
        return

    await m.answer(f"diag\nWS public: ok={ok['ticker']}")

@router.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    await m.answer("🔥 Активность\nПодбираю данные (WS)…", reply_markup=bottom_menu())
    data = await ws_collect_tickers(SYMBOLS, collect_secs=8.0)
    items = [{"symbol": s, **v} for s, v in data.items() if v.get("turn24", 0.0) > 0.0]
    if not items:
        await m.answer("🔥 Активность\nНет данных (WS).", reply_markup=bottom_menu())
        return
    items.sort(key=lambda x: x["turn24"], reverse=True)
    lines = ["🔥 <b>Активность</b> (Bybit WS)"]
    for i, it in enumerate(items[:10], 1):
        lines.append(f"{i}) {it['symbol']}  24h% {it['pct24']:+.2f}  | turnover24h ~ {it['turn24']:.0f}")
    await m.answer("\n".join(lines), reply_markup=bottom_menu())

@router.message(F.text == "⚡ Волатильность")
async def on_vol(m: Message):
    await m.answer("⚡ Волатильность\nПодбираю данные (WS)…", reply_markup=bottom_menu())
    data = await ws_collect_tickers(SYMBOLS, collect_secs=8.0)
    items = [{"symbol": s, **v} for s, v in data.items() if v.get("last", 0.0) > 0.0]
    if not items:
        await m.answer("⚡ Волатильность\nНет данных (WS).", reply_markup=bottom_menu())
        return
    items.sort(key=lambda x: abs(x["pct24"]), reverse=True)
    lines = ["⚡ <b>Волатильность</b> (24h %, Bybit WS)"]
    for i, it in enumerate(items[:10], 1):
        lines.append(f"{i}) {it['symbol']}  24h% {it['pct24']:+.2f}  | last {it['last']}")
    await m.answer("\n".join(lines), reply_markup=bottom_menu())

@router.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    await m.answer("📈 Тренд (упрощённо по 24h%, WS)\nПодбираю данные…", reply_markup=bottom_menu())
    data = await ws_collect_tickers(SYMBOLS, collect_secs=8.0)
    items = [{"symbol": s, **v} for s, v in data.items() if v.get("last", 0.0) > 0.0]
    if not items:
        await m.answer("📈 Тренд\nНет данных (WS).", reply_markup=bottom_menu())
        return
    items.sort(key=lambda x: x["pct24"], reverse=True)
    lines = ["📈 <b>Тренд</b> (упрощённо, Bybit WS)"]
    for i, it in enumerate(items[:10], 1):
        tag = "↑" if it["pct24"] > 0 else ("↓" if it["pct24"] < 0 else "≈")
        lines.append(f"{i}) {it['symbol']}  {tag}  24h% {it['pct24']:+.2f}  | last {it['last']}")
    await m.answer("\n".join(lines), reply_markup=bottom_menu())

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("🫧 Bubbles\nСобираю WS-тикеры…", reply_markup=bottom_menu())
    data = await ws_collect_tickers(SYMBOLS, collect_secs=10.0)
    items = [{"symbol": s, **v} for s, v in data.items() if v.get("turn24", 0.0) > 0.0]
    if not items:
        await m.answer("🫧 Bubbles\nНет данных (WS).", reply_markup=bottom_menu())
        return
    items.sort(key=lambda x: x["turn24"], reverse=True)
    png = render_bubbles_png(items[:16])
    await m.answer_photo(
        BufferedInputFile(png, filename="bubbles_ws.png"),
        caption="WS Bubbles (24h %, size~turnover24h)",
        reply_markup=bottom_menu()
    )

@router.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer("📰 Макро (последний час)\n• demo headline", reply_markup=bottom_menu())

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).", reply_markup=bottom_menu())

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=bottom_menu())

@router.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
        "⚙️ Настройки\nБиржа: Bybit (USDT perp, WS)\nРежим: active | Quiet: False\nWatchlist: —\n\n"
        "Команды:\n"
        "• /add SYMBOL  — добавить (например, /add SOLUSDT)\n"
        "• /rm SYMBOL   — удалить\n"
        "• /watchlist   — показать лист\n"
        "• /passive     — автосводки/сигналы ON\n"
        "• /active      — автосводки/сигналы OFF\n"
        "• /menu        — восстановить клавиатуру",
        reply_markup=bottom_menu()
    )

dp.include_router(router)

# ========= AIOHTTP (health + webhook) =========
async def handle_health(request):
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": VERSION, "source": "bybit-ws"})

WEBHOOK_PATH = f"/webhook/{TELEGRAM_TOKEN}"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"

app = web.Application()
app.router.add_get("/health", handle_health)

async def on_startup():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await bot.set_webhook(WEBHOOK_URL)
    logging.info(f"Webhook set to: {WEBHOOK_URL}")

async def main():
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    await on_startup()
    logging.info("HTTP server started")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
