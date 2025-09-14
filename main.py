# main.py  — v1.11.1-vol-fallback-ga
# Требуемые ENV (каноничные имена, без сюрпризов):
# TELEGRAM_TOKEN          — токен бота
# WEBHOOK_SECRET          — секретный хвост пути вебхука (только символы/цифры)
# PUBLIC_BASE_URL         — публичный URL сервиса без завершающего "/"
# PORT                    — порт (Render передаёт сам), по умолчанию 10000
# DATABASE_URL            — postgres URL
# POOL_SIZE               — размер пула (по умолчанию 5)
# SYMBOLS                 — через запятую, например: BTCUSDT,ETHUSDT,...
# BYBIT_WS_URL            — wss для public linear (дефолт: wss://stream.bybit.com/v5/public/linear)
# BYBIT_REST_BASE         — https://api.bytick.com
# BYBIT_REST_FALLBACK     — https://api.bybit.com
# ENABLE_OI_POLL          — true/false (по умолчанию true)
# OI_POLL_INTERVAL_SEC    — 90
# OI_POLL_WINDOW_MIN      — 5
# ENABLE_PRICE_POLL       — true/false (для фоновой загрузки цен; по умолчанию true)
# PRICE_POLL_INTERVAL_SEC — 1800
# PRICE_POLL_LIMIT        — сколько свечей тянуть в фоновой задаче (по умолчанию 200)
# PRICE_FALLBACK_BINANCE  — true/false (включить каскад Binance-хостов; по умолчанию true)
# !!! OUTBOUND_PROXY — НЕ ИСПОЛЬЗУЕМ. Должен отсутствовать или быть пустым.

import os
import json
import asyncio
import logging
import signal
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import aiohttp
from aiohttp import web
from psycopg_pool import AsyncConnectionPool
import psycopg

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("innertrade")

# ----------------- ENV -----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

DATABASE_URL = os.getenv("DATABASE_URL", "")
POOL_SIZE = int(os.getenv("POOL_SIZE", "5"))

SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT"
).split(",") if s.strip()]

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bytick.com").rstrip("/")
BYBIT_REST_FALLBACK = os.getenv("BYBIT_REST_FALLBACK", "https://api.bybit.com").rstrip("/")

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "true").lower() in ("1", "true", "yes")
OI_POLL_INTERVAL_SEC = int(os.getenv("OI_POLL_INTERVAL_SEC", "90"))
OI_POLL_WINDOW_MIN = int(os.getenv("OI_POLL_WINDOW_MIN", "5"))

ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "true").lower() in ("1", "true", "yes")
PRICE_POLL_INTERVAL_SEC = int(os.getenv("PRICE_POLL_INTERVAL_SEC", "1800"))
PRICE_POLL_LIMIT = int(os.getenv("PRICE_POLL_LIMIT", "200"))
PRICE_FALLBACK_BINANCE = os.getenv("PRICE_FALLBACK_BINANCE", "true").lower() in ("1", "true", "yes")

# Не используем прокси. Если переменная окружения есть — игнорируем и предупреждаем.
if os.getenv("OUTBOUND_PROXY"):
    log.warning("OUTBOUND_PROXY is set but will be ignored. Remove it from ENV to avoid confusion.")

# ----------------- HTTP с заголовками -----------------
DEFAULT_HEADERS = {
    # Без UA некоторые CDN/вендоры режут (403/451)
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Binance хосты с разными anycast/CDN
BINANCE_HOSTS = [
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
    # публичный data CDN (без auth)
    "https://data-api.binance.vision",  # формат такой же /api/v3/klines
]

# ----------------- SQL -----------------
SQL_UPSERT_TICKER = """
INSERT INTO ws_ticker(symbol, last, price24h_pcnt, turnover24h, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (symbol) DO UPDATE SET
  last = EXCLUDED.last,
  price24h_pcnt = EXCLUDED.price24h_pcnt,
  turnover24h = EXCLUDED.turnover24h,
  updated_at = NOW()
"""
SQL_GET_TICKER = """
SELECT symbol, COALESCE(last,0), COALESCE(price24h_pcnt,0), COALESCE(turnover24h,0), updated_at
FROM ws_ticker
WHERE symbol = %s
"""
SQL_TOP_TURNOVER = """
SELECT symbol, COALESCE(price24h_pcnt,0) AS p24, COALESCE(turnover24h,0) AS turn
FROM ws_ticker
ORDER BY turn DESC NULLS LAST
LIMIT %s
"""
SQL_TRADES_SUM_24H = """
SELECT COALESCE(SUM(trades_count),0)::bigint, COALESCE(SUM(qty_sum),0)::float8
FROM trades_1m
WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours'
"""
SQL_TRADES_LATEST_N = """
SELECT ts, trades_count, qty_sum
FROM trades_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""
SQL_ORDERBOOK_AGGR_24H = """
SELECT AVG(spread_bps)::float8, AVG(depth_usd)::float8
FROM orderbook_1m
WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours'
"""
SQL_ORDERBOOK_LATEST_N = """
SELECT ts, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
FROM orderbook_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""
SQL_OI_DELTA_24H = """
WITH d AS (
  SELECT ts, oi_usd FROM oi_1m
  WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours'
  ORDER BY ts ASC
)
SELECT
  COALESCE((SELECT oi_usd FROM d ORDER BY ts ASC  LIMIT 1), NULL),
  COALESCE((SELECT oi_usd FROM d ORDER BY ts DESC LIMIT 1), NULL)
"""
SQL_OI_LATEST_N = """
SELECT ts, oi_usd
FROM oi_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""
SQL_PRICE_LATEST_N = """
SELECT ts, close
FROM kline_1h
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""

# ----------------- Глобальные -----------------
pool: Optional[AsyncConnectionPool] = None
session: Optional[aiohttp.ClientSession] = None
ws_task: Optional[asyncio.Task] = None
poll_tasks: List[asyncio.Task] = []

# ----------------- Утилиты -----------------
def fmt_money(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "0"
    s = f"{int(round(v)):,}".replace(",", " ")
    return s

def fmt_bps(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}bps"

def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:+.2f}%"

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ----------------- DB -----------------
async def get_pool() -> AsyncConnectionPool:
    global pool
    if pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is empty")
        pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=POOL_SIZE, open=False)
        await pool.open()
        log.info("DB ready")
    return pool

# ----------------- WS тикеры -----------------
async def upsert_ticker(symbol: str, last: float, p24: float, turnover: float):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT_TICKER, (symbol, last, p24, turnover))

async def ws_consumer():
    subs = [{"op": "subscribe", "args": [f"tickers.{sym}" for sym in SYMBOLS]}]
    backoff = 1
    while True:
        try:
            log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
            async with session.ws_connect(BYBIT_WS_URL, heartbeat=20) as ws:
                await ws.send_json(subs[0])
                log.info(f"WS subscribed: {len(SYMBOLS)} topics")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        topic = data.get("topic", "")
                        if topic.startswith("tickers."):
                            sym = topic.split(".", 1)[1].upper()
                            d = data.get("data", {})
                            last = float(d.get("lastPrice", 0) or 0)
                            p24  = float(d.get("price24hPcnt", 0) or 0) * 100.0
                            turn = float(d.get("turnover24h", 0) or 0)
                            await upsert_ticker(sym, last, p24, turn)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            backoff = 1
        except asyncio.CancelledError:
            log.info("WS consumer cancelled")
            return
        except Exception as e:
            log.exception("WS consumer failed: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            log.info("WS consumer finished")

# ----------------- REST цены (Bybit + Binance каскад) -----------------
async def http_get_json(url: str, params: dict, timeout: int = 12) -> Tuple[int, Optional[dict]]:
    try:
        async with session.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout) as resp:
            if resp.status == 200:
                try:
                    return resp.status, await resp.json()
                except Exception:
                    # Binance Vision иногда возвращает текст; попробуем json.loads
                    txt = await resp.text()
                    return resp.status, json.loads(txt)
            else:
                return resp.status, None
    except Exception as e:
        log.debug("HTTP error %s: %s", url, e)
        return -1, None

async def fetch_bybit_klines(symbol: str, limit: int) -> Tuple[str, List[Tuple[int, float]]]:
    params = {"category": "linear", "symbol": symbol, "interval": "60", "limit": limit}
    for base in (BYBIT_REST_BASE, BYBIT_REST_FALLBACK):
        url = f"{base}/v5/market/kline"
        st, data = await http_get_json(url, params)
        if st == 200 and data:
            result = data.get("result", {})
            lst = result.get("list", [])
            rows = []
            for it in lst:
                # [startTime, open, high, low, close, volume, turnover]
                ts_ms = int(it[0])
                close = float(it[4])
                rows.append((ts_ms, close))
            if rows:
                return "bybit", rows
        else:
            if st > 0:
                log.warning("REST %s -> HTTP %s", url, st)
    return "none", []

async def fetch_binance_klines(symbol: str, limit: int) -> Tuple[str, List[Tuple[int, float]]]:
    # Binance формат: /api/v3/klines?symbol=BTCUSDT&interval=1h&limit=200
    params = {"symbol": symbol, "interval": "1h", "limit": min(max(limit, 1), 1000)}
    for host in BINANCE_HOSTS:
        url = f"{host}/api/v3/klines"
        st, data = await http_get_json(url, params)
        if st == 200 and isinstance(data, list) and data:
            rows = []
            for k in data:
                # [ openTime, o,h,l,c, vol, closeTime, ... ]
                ts_ms = int(k[0])
                close = float(k[4])
                rows.append((ts_ms, close))
            if rows:
                return "binance", rows
        else:
            if st > 0:
                log.warning("REST %s -> HTTP %s", url, st)
    return "none", []

async def upsert_kline_rows(symbol: str, rows: List[Tuple[int, float]]) -> int:
    if not rows:
        return 0
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO kline_1h(ts, symbol, close)
                VALUES (TO_TIMESTAMP(%s/1000.0), %s, %s)
                ON CONFLICT (ts, symbol) DO UPDATE
                  SET close = EXCLUDED.close
                """,
                [(ts_ms, symbol, close) for ts_ms, close in rows]
            )
    return len(rows)

async def poll_prices_once(limit: int):
    total_upserts = 0
    for sym in SYMBOLS:
        src, rows = await fetch_bybit_klines(sym, limit)
        if src == "none" and PRICE_FALLBACK_BINANCE:
            src, rows = await fetch_binance_klines(sym, limit)
        up = await upsert_kline_rows(sym, rows)
        total_upserts += up
        log.info("Price poll %s: src=%s rows=%d upserted≈%d", sym, src, len(rows), up)
    log.info("Price poll upserts: %d", total_upserts)

async def price_poll_loop():
    log.info(
        "Price polling enabled: every %ds (last-N by limit=%d, fallback=%s)",
        PRICE_POLL_INTERVAL_SEC, PRICE_POLL_LIMIT,
        "binance" if PRICE_FALLBACK_BINANCE else "none"
    )
    while True:
        try:
            await poll_prices_once(PRICE_POLL_LIMIT)
        except Exception:
            log.exception("Price poll cycle error")
        finally:
            log.info("Price poll cycle done")
        await asyncio.sleep(PRICE_POLL_INTERVAL_SEC)

# ----------------- Волатильность -----------------
def realized_vol(prices: List[float]) -> Optional[float]:
    # простая реал-вола: stdev лог-доходностей * sqrt(24) для часовых
    import math
    if len(prices) < 2:
        return None
    rets = []
    for i in range(1, len(prices)):
        if prices[i-1] <= 0 or prices[i] <= 0:
            continue
        rets.append(math.log(prices[i] / prices[i-1]))
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    vol = (var ** 0.5) * (24 ** 0.5) * 100  # в %
    return vol

# ----------------- Telegram -----------------
TG_API = lambda: f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

async def tg_call(method: str, payload: dict) -> dict:
    async with session.post(f"{TG_API()}/{method}", json=payload, timeout=15) as resp:
        return await resp.json()

async def send_text(chat_id: int, text: str):
    await tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"})

async def set_webhook():
    if not PUBLIC_BASE_URL or not WEBHOOK_SECRET:
        log.warning("Skip set_webhook: PUBLIC_BASE_URL/WEBHOOK_SECRET not set")
        return
    url = f"{PUBLIC_BASE_URL}/webhook/{WEBHOOK_SECRET}"
    payload = {
        "url": url,
        "allowed_updates": ["message", "callback_query"],
        "max_connections": 40,
    }
    for i in range(1, 4):
        try:
            async with session.post(f"{TG_API()}/setWebhook", data=payload, timeout=10) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("ok"):
                    log.info("Webhook set to %s", url)
                    return
                log.warning("set_webhook attempt %d failed: HTTP %s %s", i, resp.status, data)
        except Exception as e:
            log.warning("set_webhook attempt %d failed: %s", i, e)
        await asyncio.sleep(1)

# ----------------- Команды -----------------
async def cmd_start(chat_id: int):
    await send_text(chat_id,
        "🧭 Market mood\n"
        "Innertrade Screener v1.11.1-vol-fallback-ga\n\n"
        "Команды:\n"
        "/status\n"
        "/now [SYMBOL]\n"
        "/activity2\n"
        "/diag_trades SYMBOL [N]\n"
        "/diag_ob SYMBOL [N]\n"
        "/diag_oi SYMBOL [N]\n"
        "/diag_price SYMBOL [N]\n"
        "/pull_prices [SYMBOL] [LIMIT]\n"
        "/vol SYMBOL [HOURS]\n"
    )

async def cmd_status(chat_id: int):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM ws_ticker")
            rows = (await cur.fetchone())[0]
    await send_text(chat_id,
        "Status\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (local)\n"
        "Source: Bybit (WS) + REST Prices (Bybit→Binance cascade)\n"
        "Version: v1.11.1-vol-fallback-ga\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
        f"DB rows (ws_ticker): {rows}\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} "
        f"(every {PRICE_POLL_INTERVAL_SEC}s, limit={PRICE_POLL_LIMIT}, "
        f"fallback={'binance' if PRICE_FALLBACK_BINANCE else 'none'})\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_POLL_WINDOW_MIN}min, every {OI_POLL_INTERVAL_SEC}s)\n"
    )

async def cmd_now(chat_id: int, symbol: str = "BTCUSDT"):
    symbol = symbol.upper()
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_GET_TICKER, (symbol,))
            row = await cur.fetchone()
    if not row:
        await send_text(chat_id, f"{symbol}\nнет данных")
        return
    sym, last, p24, turn, updated = row
    await send_text(chat_id,
        f"{sym}\nlast: {last}\n24h%: {p24}\nturnover24h: {turn}\nupdated_at: {updated}\n"
    )

async def cmd_diag_trades(chat_id: int, symbol: str, n: int):
    symbol = symbol.upper()
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_TRADES_LATEST_N, (symbol, n))
            rows = await cur.fetchall()
    if not rows:
        await send_text(chat_id, f"trades_1m {symbol}: пусто")
        return
    lines = [f"trades_1m {symbol} (latest {n})"]
    for ts, cnt, qty in rows:
        lines.append(f"{ts.isoformat()}  count={cnt}  qty_sum={qty}")
    await send_text(chat_id, "\n".join(lines))

async def cmd_diag_ob(chat_id: int, symbol: str, n: int):
    symbol = symbol.upper()
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_ORDERBOOK_LATEST_N, (symbol, n))
            rows = await cur.fetchall()
    if not rows:
        await send_text(chat_id, f"orderbook_1m {symbol}: пусто")
        return
    lines = [f"orderbook_1m {symbol} (latest {n})"]
    for ts, bb, ba, bq, aq, sp, depth in rows:
        lines.append(
            f"{ts.isoformat()}  bid={bb} ask={ba}  bq={bq} aq={aq}  spread={sp:.2f}bps  depth≈{fmt_money(depth)}"
        )
    await send_text(chat_id, "\n".join(lines))

async def cmd_diag_oi(chat_id: int, symbol: str, n: int):
    symbol = symbol.upper()
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_OI_LATEST_N, (symbol, n))
            rows = await cur.fetchall()
    if not rows:
        await send_text(chat_id, f"oi_1m {symbol}: пусто")
        return
    lines = [f"oi_1m {symbol} (latest {n})"]
    for ts, oi in rows:
        lines.append(f"{ts.isoformat()}  oi≈${fmt_money(oi)}")
    await send_text(chat_id, "\n".join(lines))

async def cmd_diag_price(chat_id: int, symbol: str, n: int):
    symbol = symbol.upper()
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_LATEST_N, (symbol, n))
            rows = await cur.fetchall()
    if not rows:
        await send_text(chat_id, f"kline_1h {symbol}: пусто (попробуй /pull_prices {symbol} 120)")
        return
    lines = [f"kline_1h {symbol} (latest {n})"]
    for ts, close in rows:
        lines.append(f"{ts.isoformat()}  close={close}")
    await send_text(chat_id, "\n".join(lines))

async def cmd_pull_prices(chat_id: int, symbol: Optional[str], limit: int):
    syms = [symbol.upper()] if symbol else SYMBOLS
    total_rows = 0
    total_upserts = 0
    for sym in syms:
        src, rows = await fetch_bybit_klines(sym, limit)
        if src == "none" and PRICE_FALLBACK_BINANCE:
            src, rows = await fetch_binance_klines(sym, limit)
        up = await upsert_kline_rows(sym, rows)
        total_rows += len(rows)
        total_upserts += up
        await send_text(chat_id, f"Загрузка клоузов {sym}: src={src} rows={len(rows)} upserted≈{up}")
    await send_text(chat_id, f"Итого: symbols={len(syms)}, limit={limit}, rows={total_rows}, upserted≈{total_upserts}")

async def cmd_vol(chat_id: int, symbol: str, hours: int):
    symbol = symbol.upper()
    # берём N=hours+1 закрытий 1h
    n = max(2, hours + 1)
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_LATEST_N, (symbol, n))
            rows = await cur.fetchall()
    if not rows or len(rows) < 2:
        await send_text(chat_id, f"Волатильность {symbol}: нет достаточных данных за {hours}ч (используй /pull_prices {symbol} 200).")
        return
    closes = [float(c) for _, c in sorted(rows, key=lambda x: x[0])]
    v = realized_vol(closes)
    if v is None:
        await send_text(chat_id, f"Волатильность {symbol}: не удалось рассчитать (недостаточно валидных точек).")
        return
    await send_text(chat_id, f"Волатильность {symbol} за {hours}ч ≈ {v:.2f}% (часовые закрытия, условная RV).")

# ----------------- HTTP handlers -----------------
async def root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")

async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "ts": now_utc_iso()})

async def handle_webhook(request: web.Request) -> web.Response:
    secret = request.match_info.get("secret", "")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return web.json_response({"ok": False, "error": "bad secret"}, status=403)
    try:
        update = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)

    message = update.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return web.json_response({"ok": True})

    if text.startswith("/start"):
        await cmd_start(chat_id)
    elif text.startswith("/status"):
        await cmd_status(chat_id)
    elif text.startswith("/now"):
        parts = text.split()
        await cmd_now(chat_id, parts[1] if len(parts) > 1 else "BTCUSDT")
    elif text.startswith("/diag_trades"):
        parts = text.split()
        if len(parts) >= 2:
            sym = parts[1]
            n = int(parts[2]) if len(parts) >= 3 else 10
            await cmd_diag_trades(chat_id, sym, n)
        else:
            await send_text(chat_id, "Usage: /diag_trades SYMBOL [N]")
    elif text.startswith("/diag_ob"):
        parts = text.split()
        if len(parts) >= 2:
            sym = parts[1]
            n = int(parts[2]) if len(parts) >= 3 else 5
            await cmd_diag_ob(chat_id, sym, n)
        else:
            await send_text(chat_id, "Usage: /diag_ob SYMBOL [N]")
    elif text.startswith("/diag_oi"):
        parts = text.split()
        if len(parts) >= 2:
            sym = parts[1]
            n = int(parts[2]) if len(parts) >= 3 else 10
            await cmd_diag_oi(chat_id, sym, n)
        else:
            await send_text(chat_id, "Usage: /diag_oi SYMBOL [N]")
    elif text.startswith("/diag_price"):
        parts = text.split()
        if len(parts) >= 2:
            sym = parts[1]
            n = int(parts[2]) if len(parts) >= 3 else 12
            await cmd_diag_price(chat_id, sym, n)
        else:
            await send_text(chat_id, "Usage: /diag_price SYMBOL [N]")
    elif text.startswith("/pull_prices"):
        parts = text.split()
        sym = parts[1] if len(parts) >= 2 and parts[1].upper() != "ALL" else None
        limit = int(parts[2]) if len(parts) >= 3 else 200
        await cmd_pull_prices(chat_id, sym, limit)
    elif text.startswith("/vol"):
        parts = text.split()
        if len(parts) >= 2:
            sym = parts[1]
            hours = int(parts[2]) if len(parts) >= 3 else 24
            await cmd_vol(chat_id, sym, hours)
        else:
            await send_text(chat_id, "Usage: /vol SYMBOL [HOURS]")
    elif text in ("📊 Активность", "Активность+", "🔍 Активность+", "/activity2"):
        await cmd_activity2(chat_id)  # см. ниже
    else:
        low = text.lower()
        if "активност" in low:
            await cmd_activity2(chat_id)
        else:
            await send_text(chat_id, "Команды: /status /now [SYMBOL] /activity2 /diag_trades /diag_ob /diag_oi /diag_price /pull_prices [/vol]")

    return web.json_response({"ok": True})

# -------- Активность (как было у нас ранее) --------
async def cmd_activity2(chat_id: int):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(SQL_TOP_TURNOVER, (10,))
            top = await cur.fetchall()
    if not top:
        await send_text(chat_id, "Нет данных для Активность+")
        return
    lines = ["🔍 Активность+ (композит за ~24ч)\n"]
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            for r in top:
                sym = r["symbol"]
                p24 = float(r["p24"] or 0)
                turn = float(r["turn"] or 0)
                await cur.execute(SQL_TRADES_SUM_24H, (sym,))
                trow = await cur.fetchone()
                trades_cnt = int(trow[0] or 0); qty_sum = float(trow[1] or 0)
                await cur.execute(SQL_ORDERBOOK_AGGR_24H, (sym,))
                orow = await cur.fetchone()
                avg_spread = orow[0]; avg_depth = orow[1]
                await cur.execute(SQL_OI_DELTA_24H, (sym,))
                oirow = await cur.fetchone()
                first_oi = oirow[0]; last_oi = oirow[1]
                oi_delta_pct = None
                if first_oi and last_oi and first_oi != 0:
                    oi_delta_pct = (last_oi - first_oi) / first_oi * 100.0
                score = 0.0
                score += (p24 / 5.0)
                if trades_cnt > 0:
                    score += min(trades_cnt / 400000.0, 1.0)
                if avg_depth:
                    score += min((avg_depth or 0) / 1_000_000.0, 1.0) * 0.5
                if avg_spread is not None:
                    score += max(0.0, (0.5 - min(avg_spread, 0.5)))
                if oi_delta_pct is not None:
                    score += (oi_delta_pct / 10.0)
                lines.append(
                    f"{sym}  score {score:+.2f}  | "
                    f"turnover ~ {fmt_money(turn)} | trades ~ {fmt_money(trades_cnt)} | "
                    f"depth≈${fmt_money(avg_depth or 0)} | spread≈{fmt_bps(avg_spread)} | "
                    f"OIΔ {fmt_pct(oi_delta_pct)}"
                )
    await send_text(chat_id, "\n".join(lines))

# ----------------- AIOHTTP APP -----------------
def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_post("/webhook/{secret}", handle_webhook)
    return app

# ----------------- Lifecycle -----------------
async def on_startup(app: web.Application):
    global session, ws_task, poll_tasks
    session = aiohttp.ClientSession(headers=DEFAULT_HEADERS)
    await get_pool()
    await set_webhook()
    ws_task = asyncio.create_task(ws_consumer())
    if ENABLE_PRICE_POLL:
        poll_tasks.append(asyncio.create_task(price_poll_loop()))
    if ENABLE_OI_POLL:
        # если нужно — можно вернуть цикл OI; сейчас фокус на ценах/воле
        pass

async def on_cleanup(app: web.Application):
    global session, ws_task, poll_tasks, pool
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try: await ws_task
        except asyncio.CancelledError: pass
    for t in poll_tasks:
        if not t.done():
            t.cancel()
    for t in poll_tasks:
        try: await t
        except asyncio.CancelledError: pass
    poll_tasks.clear()
    if session:
        await session.close(); session = None
    if pool:
        await pool.close(); pool = None

def run():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is empty")
    if not PUBLIC_BASE_URL:
        log.warning("PUBLIC_BASE_URL is empty — setWebhook будет пропущен")
    app = build_app()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(app.shutdown()))
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    run()
