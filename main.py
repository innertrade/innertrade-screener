import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, Update

import psycopg
from psycopg.rows import tuple_row
from psycopg_pool import AsyncConnectionPool

# ----------------------------
# Конфиг / ENV
# ----------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "G7-ADVmM").strip()
SERVICE_URL = os.getenv("SERVICE_URL", "https://innertrade-screener-bot.onrender.com").rstrip("/")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com").rstrip("/")
BYBIT_REST_FALLBACK = os.getenv("BYBIT_REST_FALLBACK", "https://api.bybit.com").rstrip("/")

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "1").lower() in ("1", "true", "yes", "y")
ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "1").lower() in ("1", "true", "yes", "y")

OI_POLL_SECONDS = int(os.getenv("OI_POLL_SECONDS", "90"))
OI_INTERVAL_MIN = int(os.getenv("OI_INTERVAL_MIN", "5"))  # интервал агрегации REST OI

PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "1800"))  # 30 мин
PRICE_BACK_HOURS = int(os.getenv("PRICE_BACK_HOURS", "192"))       # 8 дней часовыми свечами

SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT") \
    .replace(" ", "").split(",")

TOP_N = int(os.getenv("TOP_N", "10"))

# ----------------------------
# Логирование
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ----------------------------
# Глобальные объекты
# ----------------------------
bot: Bot | None = None
dp: Dispatcher | None = None
pool: AsyncConnectionPool | None = None

# ----------------------------
# SQL (все плейсхолдеры — %s!)
# ----------------------------
SQL_UPSERT_TICKER = """
INSERT INTO ws_ticker (symbol, last, price24h_pcnt, turnover24h, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (symbol) DO UPDATE SET
  last = EXCLUDED.last,
  price24h_pcnt = EXCLUDED.price24h_pcnt,
  turnover24h = EXCLUDED.turnover24h,
  updated_at = NOW();
"""

SQL_GET_TICKER = """
SELECT symbol, last, COALESCE(price24h_pcnt,0), COALESCE(turnover24h,0), updated_at
FROM ws_ticker
WHERE symbol = %s;
"""

SQL_TRADES_24H_SUM = """
SELECT
  COALESCE(SUM(trades_count),0) AS trades,
  COALESCE(SUM(qty_sum),0)      AS qty
FROM trades_1m
WHERE symbol = %s AND ts >= NOW() - interval '24 hours';
"""

SQL_OB_24H_METRICS = """
SELECT
  COALESCE(AVG(depth_usd),0)   AS depth_avg,
  COALESCE(AVG(spread_bps),0)  AS spread_avg
FROM ob_1m
WHERE symbol = %s AND ts >= NOW() - interval '24 hours';
"""

SQL_OI_DELTA_24H = """
WITH o AS (
  SELECT ts, oi_usd
  FROM oi_1m
  WHERE symbol = %s AND ts >= NOW() - interval '24 hours'
  ORDER BY ts
)
SELECT
  CASE
    WHEN COUNT(*) >= 2 THEN
      (MAX(oi_usd) - MIN(oi_usd)) / NULLIF(MIN(oi_usd), 0) * 100.0
    ELSE 0
  END AS oi_delta_pct
FROM o;
"""

SQL_PRICES_7D = """
SELECT ts, close
FROM prices_1h
WHERE symbol = %s AND ts >= NOW() - interval '7 days'
ORDER BY ts;
"""

# ----------------------------
# Утилиты
# ----------------------------
def fmt_usd(v: float) -> str:
    try:
        return f"{v:,.0f}".replace(",", " ")
    except Exception:
        return str(v)

def fmt_bps(x: float) -> str:
    try:
        return f"{x:.1f}bps"
    except Exception:
        return "0.0bps"

def score_activity(turnover: float, trades: int, depth: float, spread_bps: float, oi_delta_pct: float) -> float:
    """
    Простейший нормализованный скор:
    - оборот (лог)
    - кол-во сделок (лог)
    - глубина (лог)
    - спред (наоборот)
    - OI delta (в %)
    веса подогнаны грубо, чтобы совпадало с интуитивной сортировкой.
    """
    import math
    t = math.log10(max(turnover, 1.0))
    c = math.log10(max(trades, 1.0))
    d = math.log10(max(depth, 1.0))
    s = -min(spread_bps, 10.0) / 10.0  # чем меньше спред, тем лучше
    oi = oi_delta_pct / 20.0           # масштабируем
    return round(t*0.5 + c*0.4 + d*0.2 + s*0.2 + oi*0.2, 2)

# ----------------------------
# WS Ticker consumer (минимум)
# ----------------------------
async def upsert_ticker(symbol: str, last: float, p24: float, turnover: float):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT_TICKER, (symbol, last, p24, turnover))

async def ws_consumer():
    """
    Подписка только на тикеры (чтобы поддерживать /now и /status).
    Остальные метрики берём из БД (их наполняют твои аггрегаторы/ws/cron — как сейчас).
    """
    topics = [f"tickers.{sym}" for sym in SYMBOLS]
    sub_msg = {
        "op": "subscribe",
        "args": topics
    }
    url = BYBIT_WS_URL
    log.info(f"Bybit WS connecting: {url}")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    log.info(f"WS subscribed: {len(topics)} topics")
                    await ws.send_json(sub_msg)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            # ожидаем формат Bybit v5 tickers
                            if data.get("topic", "").startswith("tickers."):
                                d = data.get("data") or {}
                                symbol = d.get("symbol")
                                last = float(d.get("lastPrice", 0) or 0)
                                p24 = float(d.get("price24hPcnt", 0) or 0) * 100.0
                                turnover = float(d.get("turnover24h", 0) or 0)
                                if symbol:
                                    await upsert_ticker(symbol, last, p24, turnover)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"WS error: {e}")
            await asyncio.sleep(5.0)

# ----------------------------
# Периодические REST опросы (по желанию; Bybit часто даёт 403 без ключей)
# ----------------------------
async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict) -> tuple[int, dict|None]:
    try:
        async with session.get(url, params=params, timeout=15) as r:
            code = r.status
            if code == 200:
                return code, await r.json()
            return code, None
    except Exception:
        return -1, None

async def poll_oi():
    log.info(f"OI polling enabled: every {OI_POLL_SECONDS}s, interval={OI_INTERVAL_MIN}min")
    base_url = f"{BYBIT_REST_BASE}/v5/market/open-interest"
    fb_url = f"{BYBIT_REST_FALLBACK}/v5/market/open-interest"
    params_tpl = {"category": "linear", "intervalTime": f"{OI_INTERVAL_MIN}min"}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                for sym in SYMBOLS:
                    p = params_tpl | {"symbol": sym}
                    code, _ = await fetch_json(s, base_url, p)
                    if code != 200:
                        log.warning(f"REST base {base_url} -> HTTP {code}")
                        code2, _ = await fetch_json(s, fb_url, p)
                        if code2 != 200:
                            log.warning(f"REST fallback {fb_url} -> HTTP {code2}")
                            log.warning(f"OI {sym} http {code2 if code2!=200 else code}")
                log.info("OI poll cycle done in ~OK")
        except Exception:
            pass
        await asyncio.sleep(OI_POLL_SECONDS)

async def poll_prices():
    log.info(f"Price polling enabled: every {PRICE_POLL_SECONDS}s (1h candles ~{PRICE_BACK_HOURS}h back)")
    base_url = f"{BYBIT_REST_BASE}/v5/market/kline"
    fb_url = f"{BYBIT_REST_FALLBACK}/v5/market/kline"
    params_tpl = {"category": "linear", "interval": "60"}
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                for sym in SYMBOLS:
                    p = params_tpl | {"symbol": sym, "limit": str(min(PRICE_BACK_HOURS, 200))}
                    code, _ = await fetch_json(s, base_url, p)
                    if code != 200:
                        log.warning(f"REST base {base_url} -> HTTP {code}")
                        code2, _ = await fetch_json(s, fb_url, p)
                        if code2 != 200:
                            log.warning(f"REST fallback {fb_url} -> HTTP {code2}")
                            log.warning(f"Kline {sym} fetch failed (both)")
                log.info("Price poll cycle done")
        except Exception:
            pass
        await asyncio.sleep(PRICE_POLL_SECONDS)

# ----------------------------
# Хэндлеры бота
# ----------------------------
async def cmd_start(message: Message):
    txt = (
        "🧭 Market mood\n"
        f"Добро пожаловать в Innertrade Screener v1.8.4-ops.\n\n"
        "Доступные команды:\n"
        "/status — состояние сервиса\n"
        "/now [SYMBOL] — текущие данные по тикеру (по умолчанию BTCUSDT)\n"
        "/activity2 — Активность+ (композит ~24ч)\n"
        "/diag_trades SYMBOL [N] — последние N минут по сделкам\n"
        "/diag_ob SYMBOL [N] — последние N минут по стакану\n"
        "/diag_oi SYMBOL [N] — последние N минут по OI\n"
    )
    await message.answer(txt)

async def cmd_status(message: Message):
    ws_line = f"Bybit WS: {BYBIT_WS_URL}"
    oi_line = f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_INTERVAL_MIN}min, every {OI_POLL_SECONDS}s)"
    pr_line = f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} (every {PRICE_POLL_SECONDS}s, ~{PRICE_BACK_HOURS}h back)"
    # БД строки в ws_ticker
    rows = 0
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute("SELECT COUNT(*) FROM ws_ticker;")
            r = await cur.fetchone()
            rows = r[0] if r else 0
    txt = (
        "Status\n"
        f"Time: {datetime.now(tz=timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')} (MSK)\n"
        "Source: Bybit (public WS + REST OI/Prices)\n"
        "Version: v1.8.4-ops\n"
        f"{ws_line}\n"
        f"{oi_line}\n"
        f"{pr_line}\n"
        f"DB rows (ws_ticker): {rows}\n"
    )
    await message.answer(txt)

async def cmd_now(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) >= 2 else "BTCUSDT"
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(SQL_GET_TICKER, (sym,))
            row = await cur.fetchone()
    if not row:
        await message.answer(f"{sym}: нет данных.")
        return
    symbol, last, p24, turnover, updated_at = row
    txt = (
        f"{symbol}\n"
        f"last: {last}\n"
        f"24h%: {round(p24, 4)}\n"
        f"turnover24h: {turnover}\n"
        f"updated_at: {updated_at}\n"
    )
    await message.answer(txt)

async def cmd_diag_trades(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_trades SYMBOL [N]\nНапр.: /diag_trades BTCUSDT 10")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 10
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute("""
                SELECT ts, trades_count, qty_sum
                FROM trades_1m
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT %s;
            """, (sym, n))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"trades_1m {sym}: нет данных.")
        return
    rows = list(reversed(rows))
    out = [f"trades_1m {sym} (latest {len(rows)})"]
    for ts, c, q in rows:
        out.append(f"{ts.isoformat()}  count={c}  qty_sum={q}")
    await message.answer("\n".join(out))

async def cmd_diag_ob(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_ob SYMBOL [N]\nНапр.: /diag_ob BTCUSDT 5")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 5
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute("""
                SELECT ts, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
                FROM ob_1m
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT %s;
            """, (sym, n))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"ob_1m {sym}: нет данных.")
        return
    rows = list(reversed(rows))
    out = [f"ob_1m {sym} (latest {len(rows)})"]
    for ts, bb, ba, bq, aq, sp, depth in rows:
        out.append(f"{ts.isoformat()}  bid={bb} ask={ba}  bq={bq} aq={aq}  spread={fmt_bps(sp)}  depth≈{fmt_usd(depth)}")
    await message.answer("\n".join(out))

async def cmd_diag_oi(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /diag_oi SYMBOL [N]\nНапр.: /diag_oi BTCUSDT 10")
        return
    sym = parts[1].upper()
    n = int(parts[2]) if len(parts) > 2 else 10
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute("""
                SELECT ts, oi_usd
                FROM oi_1m
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT %s;
            """, (sym, n))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"{sym}: нет строк в oi_1m (ожидайте цикл опроса).")
        return
    rows = list(reversed(rows))
    out = [f"oi_1m {sym} (latest {len(rows)})"]
    for ts, oi in rows:
        out.append(f"{ts.isoformat()}  oi≈${fmt_usd(oi)}")
    await message.answer("\n".join(out))

async def cmd_activity2(message: Message):
    # Композит за 24ч по всем SYMBOLS
    entries = []
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            for sym in SYMBOLS:
                # ticker
                await cur.execute("SELECT COALESCE(turnover24h,0) FROM ws_ticker WHERE symbol=%s;", (sym,))
                r = await cur.fetchone()
                turnover = float(r[0]) if r else 0.0
                # trades
                await cur.execute(SQL_TRADES_24H_SUM, (sym,))
                r = await cur.fetchone()
                trades = int(r[0]) if r else 0
                # orderbook
                await cur.execute(SQL_OB_24H_METRICS, (sym,))
                r = await cur.fetchone()
                depth_avg = float(r[0]) if r else 0.0
                spread_avg = float(r[1]) if r else 0.0
                # oi delta
                await cur.execute(SQL_OI_DELTA_24H, (sym,))
                r = await cur.fetchone()
                oi_delta_pct = float(r[0]) if r and r[0] is not None else 0.0

                sc = score_activity(turnover, trades, depth_avg, spread_avg, oi_delta_pct)
                entries.append((sym, sc, turnover, trades, depth_avg, spread_avg, oi_delta_pct))

    # сортируем по score убыв.
    entries.sort(key=lambda x: x[1], reverse=True)
    entries = entries[:TOP_N]

    lines = ["🔍 Активность+ (композит за ~24ч)", ""]
    rank = 1
    for sym, sc, turnover, trades, depth, spread, oi_d in entries:
        lines.append(
            f"{rank}) {sym}  score {sc:+.2f}  | turnover ~ {fmt_usd(turnover)} | trades ~ {fmt_usd(trades)} | "
            f"depth≈${fmt_usd(depth)} | spread≈{fmt_bps(spread)} | OIΔ {oi_d:+.1f}%"
        )
        rank += 1

    await message.answer("\n".join(lines))

async def on_text(message: Message):
    t = (message.text or "").strip().lower()
    if "активность+" in t or "activity+" in t or "activity2" in t:
        return await cmd_activity2(message)
    if "status" in t or "статус" in t:
        return await cmd_status(message)
    return await cmd_start(message)

# ----------------------------
# HTTP ручки
# ----------------------------
async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "innertrade-screener", "bot_id": bot.id if bot else None})

async def root(request: web.Request) -> web.Response:
    return web.Response(text="Innertrade Screener Bot is running.\n")

# TG webhook
async def handle_webhook(request: web.Request) -> web.Response:
    global dp, bot
    if request.match_info.get("secret") != WEBHOOK_SECRET:
        return web.Response(status=404, text="Not found")
    raw = await request.text()
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    upd = Update.model_validate(data)
    await dp.feed_update(bot, upd)
    return web.json_response({"ok": True})

# Вспомогательные ПРОКСИ-ручки, чтобы дергать Telegram API С СЕРВЕРА (у тебя с телефона блокируется api.telegram.org)
async def tg_whinfo(request: web.Request) -> web.Response:
    if not TELEGRAM_TOKEN:
        return web.json_response({"ok": False, "error": "TELEGRAM_TOKEN missing"}, status=500)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getWebhookInfo"
    async with aiohttp.ClientSession() as s:
        async with s.get(url, timeout=15) as r:
            try:
                data = await r.json()
            except Exception:
                txt = await r.text()
                data = {"ok": False, "status": r.status, "text": txt}
            return web.json_response(data)

async def tg_whset(request: web.Request) -> web.Response:
    if not TELEGRAM_TOKEN:
        return web.json_response({"ok": False, "error": "TELEGRAM_TOKEN missing"}, status=500)
    wh_url = f"{SERVICE_URL}/webhook/{WEBHOOK_SECRET}"
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
    params = {
        "url": wh_url,
        "allowed_updates": '["message","callback_query"]',
        "max_connections": "40",
    }
    async with aiohttp.ClientSession() as s:
        async with s.get(api, params=params, timeout=15) as r:
            try:
                data = await r.json()
            except Exception:
                txt = await r.text()
                data = {"ok": False, "status": r.status, "text": txt}
            return web.json_response({"request": {"url": wh_url}, "telegram": data})

async def tg_send(request: web.Request) -> web.Response:
    if not TELEGRAM_TOKEN:
        return web.json_response({"ok": False, "error": "TELEGRAM_TOKEN missing"}, status=500)
    chat_id = request.query.get("chat_id")
    text = request.query.get("text", "ping from server")
    if not chat_id:
        return web.json_response({"ok": False, "error": "chat_id query param required"}, status=400)
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    params = {"chat_id": chat_id, "text": text}
    async with aiohttp.ClientSession() as s:
        async with s.get(api, params=params, timeout=15) as r:
            try:
                data = await r.json()
            except Exception:
                txt = await r.text()
                data = {"ok": False, "status": r.status, "text": txt}
            return web.json_response(data)

# ----------------------------
# Сборка приложения
# ----------------------------
def build_app() -> web.Application:
    application = web.Application()
    # HTTP
    application.router.add_get("/", root)
    application.router.add_head("/", root)
    application.router.add_get("/health", health)
    application.router.add_head("/health", health)
    application.router.add_post(f"/webhook/{{secret}}", handle_webhook)
    # Служебные
    application.router.add_get("/tg/whinfo", tg_whinfo)
    application.router.add_get("/tg/whset", tg_whset)
    application.router.add_get("/tg/send", tg_send)  # опционально

    return application

# ----------------------------
# Инициализация бота/роутов
# ----------------------------
def setup_bot() -> Dispatcher:
    global bot, dp
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is empty")
    bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_now, Command("now"))
    dp.message.register(cmd_activity2, Command("activity2"))
    dp.message.register(cmd_diag_trades, Command("diag_trades"))
    dp.message.register(cmd_diag_ob, Command("diag_ob"))
    dp.message.register(cmd_diag_oi, Command("diag_oi"))
    dp.message.register(on_text, F.text)

    return dp

# ----------------------------
# Старт/главный run
# ----------------------------
async def set_webhook_from_server():
    # Пытаемся сразу поставить вебхук с сервера, чтобы не зависеть от клиента
    try:
        wh_url = f"{SERVICE_URL}/webhook/{WEBHOOK_SECRET}"
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
        params = {"url": wh_url, "allowed_updates": '["message","callback_query"]', "max_connections": "40"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params, timeout=15) as r:
                txt = await r.text()
                log.info(f"Webhook set to {wh_url}: HTTP {r.status} body={txt[:200]}")
    except Exception as e:
        log.warning(f"set_webhook attempt failed: {e}")

async def on_startup(app: web.Application):
    global pool
    # DB
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty")
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, max_idle=300)
    log.info("DB ready")

    # Bot/DP
    setup_bot()

    # Фоново: ws + опросы
    app["ws_task"] = asyncio.create_task(ws_consumer())
    if ENABLE_OI_POLL:
        app["oi_task"] = asyncio.create_task(poll_oi())
    if ENABLE_PRICE_POLL:
        app["price_task"] = asyncio.create_task(poll_prices())

    # Ставим вебхук с сервера
    await set_webhook_from_server()

async def on_cleanup(app: web.Application):
    for key in ("ws_task", "oi_task", "price_task"):
        task = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    if pool:
        await pool.close()

def run():
    application = build_app()
    application.on_startup.append(on_startup)
    application.on_cleanup.append(on_cleanup)
    port = int(os.getenv("PORT", "10000"))
    log.info("======== Running on http://0.0.0.0:%d ========", port)
    web.run_app(application, host="0.0.0.0", port=port)

# ---------------
# entrypoint
# ---------------
if __name__ == "__main__":
    import contextlib
    run()
