import os
import asyncio
import logging
import html
import json
import time
from aiohttp import web, ClientSession, ClientTimeout, WSMsgType

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# ========= ENV / CONFIG =========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
# Разрешим переопределять основной REST-хост
BYBIT_HOST = os.getenv("BYBIT_HOST", "https://api.bybit.com").rstrip("/")
VERSION = "v0.8.5-webhook"

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")
if not BASE_URL.startswith("https://"):
    raise RuntimeError("BASE_URL must be your public https URL, e.g. https://<service>.onrender.com")

# Альтернативы REST (исторический зеркальный домен)
REST_HOSTS = [BYBIT_HOST, "https://api.bytick.com"]

# Публичный WS
WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"

HTTP_HEADERS = {
    "User-Agent": "InnertradeScreener/1.0 (+render.com)",
    "Accept": "application/json",
    "Accept-Encoding": "identity",
    "Connection": "close",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ========= BOT =========
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

# ========= SIMPLE REST HELPERS =========
async def rest_get_text(session: ClientSession, url: str) -> tuple[int|None, str]:
    try:
        async with session.get(url, headers=HTTP_HEADERS) as r:
            status = r.status
            text = await r.text()
            return status, text
    except Exception as e:
        return None, f"ERR {type(e).__name__}: {e}"

async def bybit_public_get(path_qs: str, timeout_total: float = 10.0) -> tuple[str, int|None, str]:
    """
    Пробуем по очереди REST_HOSTS. Возвращаем (host, status, text).
    """
    timeout = ClientTimeout(total=timeout_total)
    async with ClientSession(timeout=timeout) as s:
        for host in REST_HOSTS:
            url = f"{host}{path_qs}"
            st, txt = await rest_get_text(s, url)
            # если хоть что-то отличное от 403, возвращаем сразу
            if st != 403:
                return host, st, txt
            # 403 — попробуем следующий хост
        # если все хосты вернули 403 — отдадим последний
        return REST_HOSTS[-1], 403, "403 Forbidden (WAF/Proxy)"

# ========= WS FALLBACK =========
async def ws_fetch_ticker_and_kline(symbol: str = "BTCUSDT", kline_interval: str = "5") -> dict:
    """
    Подключаемся к публичному WS и запрашиваем:
      - tickers (24h) для symbol
      - kline.<interval>.<symbol>
    Ждём немного сообщений, возвращаем краткий слепок.
    """
    out = {"ws_ok": False, "ticker_recv": False, "kline_recv": False}
    try:
        timeout = ClientTimeout(total=12)
        async with ClientSession(timeout=timeout) as s:
            async with s.ws_connect(WS_PUBLIC_LINEAR) as ws:
                # подписки
                sub = {
                    "op": "subscribe",
                    "args": [f"tickers.{symbol}", f"kline.{kline_interval}.{symbol}"]
                }
                await ws.send_str(json.dumps(sub))

                t_end = time.time() + 6.0
                while time.time() < t_end:
                    msg = await ws.receive(timeout=6.0)
                    if msg.type == WSMsgType.TEXT:
                        data = msg.json(loads=json.loads)
                        topic = data.get("topic") or data.get("data", {}).get("topic")
                        # тикер
                        if isinstance(data, dict) and data.get("topic", "").startswith("tickers"):
                            out["ticker_recv"] = True
                        # клайны
                        if isinstance(data, dict) and data.get("topic", "").startswith("kline."):
                            out["kline_recv"] = True
                        if out["ticker_recv"] and out["kline_recv"]:
                            break
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
                out["ws_ok"] = (out["ticker_recv"] or out["kline_recv"])
    except Exception as e:
        out["ws_err"] = f"{type(e).__name__}: {e}"
    return out

# ========= HANDLERS =========
@router.message(F.text == "/start")
async def cmd_start(m: Message):
    await m.answer(
        "🧭 <b>Market mood</b>\n"
        "BTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)\n\n"
        f"Добро пожаловать в <b>Innertrade Screener</b> {VERSION} (Bybit).",
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
        "Source: Bybit (public endpoints)\n"
        f"Version: {VERSION}\n"
        f"Bybit host (pref): {BYBIT_HOST}"
    )

@router.message(F.text == "/diag")
async def cmd_diag(m: Message):
    # 1) Пробуем REST tickers / kline по очереди хостов
    host_t, st_t, body_t = await bybit_public_get("/v5/market/tickers?category=linear&symbol=BTCUSDT")
    host_k, st_k, body_k = await bybit_public_get("/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=5")

    rest_summary = (
        f"REST tickers: host={host_t} status={st_t}\n"
        f"REST kline  : host={host_k} status={st_k}"
    )

    # 2) Если оба 403 — пробуем WS
    ws_summary = ""
    if st_t == 403 and st_k == 403:
        ws = await ws_fetch_ticker_and_kline("BTCUSDT", "5")
        ws_summary = f"\nWS public: ok={ws.get('ws_ok')} ticker={ws.get('ticker_recv')} kline={ws.get('kline_recv')}"
        if "ws_err" in ws:
            ws_summary += f" err={ws['ws_err']}"

    # 3) Показать безопасные «срезы» тел ответов (не HTML!)
    cut_t = html.escape((body_t or "")[:220]).replace("\n", " ")
    cut_k = html.escape((body_k or "")[:220]).replace("\n", " ")

    msg = (
        "<b>diag</b>\n" +
        rest_summary +
        ws_summary +
        f"\n\nTicker body[:200]=<code>{cut_t}</code>\nKline  body[:200]=<code>{cut_k}</code>"
    )
    await m.answer(msg)

# Заглушки разделов — пока без реальных данных (REST сейчас режется)
@router.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    await m.answer("🔥 Активность\nПробую источники… (REST временно недоступен с текущих IP)", reply_markup=bottom_menu())

@router.message(F.text == "⚡ Волатильность")
async def on_vol(m: Message):
    await m.answer("⚡ Волатильность\nПробую источники… (REST временно недоступен с текущих IP)", reply_markup=bottom_menu())

@router.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    await m.answer("📈 Тренд\nПробую источники… (REST временно недоступен с текущих IP)", reply_markup=bottom_menu())

@router.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("🫧 Bubbles\nПробую источники… (REST временно недоступен с текущих IP)", reply_markup=bottom_menu())

@router.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer("📰 Макро (последний час)\n• demo headline", reply_markup=bottom_menu())

@router.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (заглушка).", reply_markup=bottom_menu())

@router.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)", reply_markup=bottom_menu())

@router.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
        "⚙️ Настройки\nБиржа: Bybit (USDT perp)\nРежим: active | Quiet: False\nWatchlist: —\n\n"
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
    return web.json_response({"ok": True, "service": "innertrade-screener", "version": VERSION, "host": BYBIT_HOST})

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
