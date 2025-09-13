import os
import json
import asyncio
import logging
import signal
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional

import aiohttp
from aiohttp import web
from psycopg_pool import AsyncConnectionPool
import psycopg

# =========================
# ЛОГИ
# =========================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# =========================
# ENV (каноничные имена)
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()  # секретный хвост пути вебхука
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")  # https://innertrade-...onrender.com
PORT = int(os.getenv("PORT", "10000"))

DATABASE_URL = os.getenv("DATABASE_URL", "")  # postgres://.../...
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

ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "false").lower() in ("1", "true", "yes")
PRICE_POLL_INTERVAL_SEC = int(os.getenv("PRICE_POLL_INTERVAL_SEC", "1800"))  # раз в 30 мин
PRICE_POLL_HOURS_BACK = int(os.getenv("PRICE_POLL_HOURS_BACK", "192"))  # 8 суток

# =========================
# SQL
# =========================

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

# trades_1m: ts, symbol, trades_count, qty_sum
SQL_TRADES_SUM_24H = """
SELECT
  COALESCE(SUM(trades_count),0)::bigint AS trades_cnt,
  COALESCE(SUM(qty_sum),0)::float8     AS qty_sum
FROM trades_1m
WHERE symbol = %s
  AND ts >= NOW() - INTERVAL '24 hours'
"""

SQL_TRADES_LATEST_N = """
SELECT ts, trades_count, qty_sum
FROM trades_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""

# orderbook_1m: ts, symbol, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
SQL_ORDERBOOK_AGGR_24H = """
SELECT
  AVG(spread_bps)::float8 AS avg_spread_bps,
  AVG(depth_usd)::float8  AS avg_depth_usd
FROM orderbook_1m
WHERE symbol = %s
  AND ts >= NOW() - INTERVAL '24 hours'
"""

SQL_ORDERBOOK_LATEST_N = """
SELECT ts, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
FROM orderbook_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""

# oi_1m: ts, symbol, oi_usd
SQL_OI_DELTA_24H = """
WITH d AS (
  SELECT ts, oi_usd
  FROM oi_1m
  WHERE symbol = %s
    AND ts >= NOW() - INTERVAL '24 hours'
  ORDER BY ts ASC
)
SELECT
  COALESCE((SELECT oi_usd FROM d ORDER BY ts ASC  LIMIT 1), NULL)  AS first_oi,
  COALESCE((SELECT oi_usd FROM d ORDER BY ts DESC LIMIT 1), NULL)  AS last_oi
"""

SQL_OI_LATEST_N = """
SELECT ts, oi_usd
FROM oi_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""

# kline_1h: ts, symbol, close
SQL_PRICE_LATEST_N = """
SELECT ts, close
FROM kline_1h
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s
"""

# =========================
# ГЛОБАЛЬНОЕ
# =========================
pool: Optional[AsyncConnectionPool] = None
session: Optional[aiohttp.ClientSession] = None
ws_task: Optional[asyncio.Task] = None
poll_tasks: List[asyncio.Task] = []

# =========================
# УТИЛИТЫ
# =========================

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

# =========================
# DB
# =========================

async def get_pool() -> AsyncConnectionPool:
    global pool
    if pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is empty")
        pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=POOL_SIZE, open=False)
        await pool.open()
        log.info("DB ready")
    return pool

# =========================
# Bybit WS (tickers)
# =========================

async def upsert_ticker(symbol: str, last: float, p24: float, turnover: float):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT_TICKER, (symbol, last, p24, turnover))

async def ws_consumer():
    """
    Слушаем public WS tickers (instrument_info или tickers).
    Тут используем топики: tickers. (Bybit v5 public linear)
    """
    url = BYBIT_WS_URL
    subs = [{"op": "subscribe", "args": [f"tickers.{sym}" for sym in SYMBOLS]}]

    backoff = 1
    while True:
        try:
            log.info(f"Bybit WS connecting: {url}")
            async with session.ws_connect(url, heartbeat=20) as ws:
                # subscribe
                await ws.send_json(subs[0])
                log.info(f"WS subscribed: {len(SYMBOLS)} topics")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        # ожидаем формат: {"topic":"tickers.SYMBOL","data":{...}}
                        topic = data.get("topic", "")
                        if topic.startswith("tickers."):
                            sym = topic.split(".", 1)[1].upper()
                            d = data.get("data", {})
                            last = float(d.get("lastPrice", 0) or 0)
                            p24  = float(d.get("price24hPcnt", 0) or 0) * 100.0  # bybit отдаёт долю
                            turn = float(d.get("turnover24h", 0) or 0)
                            await upsert_ticker(sym, last, p24, turn)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            backoff = 1  # успешная сессия — сброс бэкоффа
        except asyncio.CancelledError:
            log.info("WS consumer cancelled")
            return
        except Exception as e:
            log.exception("WS consumer failed: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            log.info("WS consumer finished")

# =========================
# REST polling (OI / Klines)
# =========================

async def bybit_get_json(url: str, params: dict) -> Optional[dict]:
    # пробуем базу, потом fallback
    for base in (BYBIT_REST_BASE, BYBIT_REST_FALLBACK):
        full = f"{base}{url}"
        try:
            async with session.get(full, params=params, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    log.warning("REST base %s -> HTTP %d", full, resp.status)
        except Exception:
            log.warning("REST base %s -> network error", full)
    return None

async def poll_oi_once():
    # /v5/market/open-interest?category=linear&symbol=BTCUSDT&interval=5min
    for sym in SYMBOLS:
        params = {
            "category": "linear",
            "symbol": sym,
            "interval": f"{OI_POLL_WINDOW_MIN}min",
        }
        ok = False
        # Base
        full_b = f"{BYBIT_REST_BASE}/v5/market/open-interest"
        try:
            async with session.get(full_b, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ok = True
                else:
                    log.warning("REST base %s -> HTTP %d", full_b, resp.status)
        except Exception:
            log.warning("REST base %s -> network error", full_b)

        if not ok:
            # fallback
            full_f = f"{BYBIT_REST_FALLBACK}/v5/market/open-interest"
            try:
                async with session.get(full_f, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        ok = True
                    else:
                        log.warning("REST fallback %s -> HTTP %d", full_f, resp.status)
            except Exception:
                log.warning("REST fallback %s -> network error", full_f)

        if not ok:
            log.warning("OI %s http 403", sym)
            continue

        try:
            result = data.get("result", {})
            list_ = result.get("list", [])
            if not list_:
                continue
            # Берём последний элемент
            last = list_[-1]
            ts_ms = int(last.get("timestamp", 0))
            oi_usd = float(last.get("openInterestUsd", 0) or 0)

            p = await get_pool()
            async with p.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO oi_1m(ts, symbol, oi_usd) VALUES (TO_TIMESTAMP(%s/1000.0), %s, %s)",
                        (ts_ms, sym, oi_usd)
                    )
        except Exception:
            log.exception("OI parse/save failed for %s", sym)

async def poll_prices_once():
    # /v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&start=...&end=...
    end_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ts = int((datetime.now(tz=timezone.utc) - timedelta(hours=PRICE_POLL_HOURS_BACK)).timestamp() * 1000)
    for sym in SYMBOLS:
        ok = False
        params = {
            "category": "linear",
            "symbol": sym,
            "interval": "60",
            "start": start_ts,
            "end": end_ts
        }
        # base
        full_b = f"{BYBIT_REST_BASE}/v5/market/kline"
        try:
            async with session.get(full_b, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ok = True
                else:
                    log.warning("REST base %s -> HTTP %d", full_b, resp.status)
        except Exception:
            log.warning("REST base %s -> network error", full_b)

        if not ok:
            full_f = f"{BYBIT_REST_FALLBACK}/v5/market/kline"
            try:
                async with session.get(full_f, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        ok = True
                    else:
                        log.warning("REST fallback %s -> HTTP %d", full_f, resp.status)
            except Exception:
                log.warning("REST fallback %s -> network error", full_f)

        if not ok:
            log.warning("Kline %s fetch failed (both)", sym)
            continue

        try:
            result = data.get("result", {})
            list_ = result.get("list", [])
            if not list_:
                continue

            rows = []
            for item in list_:
                # Bybit v5 kline list format: [startTime, open, high, low, close, volume, turnover]
                start_ms = int(item[0])
                close = float(item[4])
                rows.append((start_ms, sym, close))

            if rows:
                p = await get_pool()
                async with p.connection() as conn:
                    async with conn.cursor() as cur:
                        # UPSERT по (ts, symbol)
                        await cur.executemany(
                            """
                            INSERT INTO kline_1h(ts, symbol, close)
                            VALUES (TO_TIMESTAMP(%s/1000.0), %s, %s)
                            ON CONFLICT (ts, symbol) DO UPDATE
                              SET close = EXCLUDED.close
                            """,
                            rows
                        )
        except Exception:
            log.exception("Kline parse/save failed for %s", sym)

async def oi_poll_loop():
    log.info("OI polling enabled: every %ds, interval=%dmin", OI_POLL_INTERVAL_SEC, OI_POLL_WINDOW_MIN)
    while True:
        try:
            await poll_oi_once()
        except Exception:
            log.exception("OI poll cycle error")
        finally:
            log.info("OI poll cycle done in %.2fs", 0.0)
        await asyncio.sleep(OI_POLL_INTERVAL_SEC)

async def price_poll_loop():
    log.info("Price polling enabled: every %ds (1h candles ~%dh back)", PRICE_POLL_INTERVAL_SEC, PRICE_POLL_HOURS_BACK)
    while True:
        try:
            await poll_prices_once()
        except Exception:
            log.exception("Price poll cycle error")
        finally:
            log.info("Price poll cycle done")
        await asyncio.sleep(PRICE_POLL_INTERVAL_SEC)

# =========================
# TELEGRAM
# =========================

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

async def tg_call(method: str, payload: dict) -> dict:
    async with session.post(f"{TG_API}/{method}", json=payload, timeout=15) as resp:
        return await resp.json()

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
    # Несколько попыток — иногда Telegram отвечает таймаутом
    for i in range(1, 4):
        try:
            async with session.post(f"{TG_API}/setWebhook", data=payload, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        log.info("Webhook set to %s", url)
                        return
                    else:
                        log.warning("set_webhook attempt %d failed: %s", i, data)
                else:
                    log.warning("set_webhook attempt %d failed: HTTP %d", i, resp.status)
        except Exception as e:
            log.warning("set_webhook attempt %d failed: %s", i, e)
        await asyncio.sleep(1)

# =========================
# TELEGRAM HANDLERS (минимум)
# =========================

async def send_text(chat_id: int, text: str):
    await tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"})

def buttons_text() -> str:
    return "🧭 Market mood\n📊 Активность\n⚡ Волатильность\n📈 Тренд\n🔍 Активность+"

async def cmd_start(chat_id: int):
    text = (
        "🧭 Market mood\n"
        f"Добро пожаловать в Innertrade Screener v1.9.0-unified (WS tickers + OI/Prices via REST).\n\n"
        "Команды:\n"
        "/status – состояние\n"
        "/now [SYMBOL] – текущие данные по символу (например, /now BTCUSDT)\n"
        "/activity2 – Активность+ (композит)\n"
        "/diag_trades SYMBOL [N]\n"
        "/diag_ob SYMBOL [N]\n"
        "/diag_oi SYMBOL [N]\n"
    )
    await send_text(chat_id, text)

async def cmd_status(chat_id: int):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM ws_ticker")
            rows = (await cur.fetchone())[0]
    text = (
        "Status\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (local)\n"
        f"Source: Bybit (public WS + REST OI/Prices)\n"
        "Version: v1.9.0-unified\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"  # Упрощённо
        f"DB rows (ws_ticker): {rows}\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_POLL_WINDOW_MIN}min, every {OI_POLL_INTERVAL_SEC}s)\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} (every {PRICE_POLL_INTERVAL_SEC}s, ~{PRICE_POLL_HOURS_BACK}h back)\n"
    )
    await send_text(chat_id, text)

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
    text = (
        f"{sym}\n"
        f"last: {last}\n"
        f"24h%: {p24}\n"
        f"turnover24h: {turn}\n"
        f"updated_at: {updated}\n"
    )
    await send_text(chat_id, text)

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
    lines = [f"ob_1m {symbol} (latest {n})"]
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

async def cmd_activity2(chat_id: int):
    """
    Композит: turnover (из ws_ticker) + trades_1m (sum 24h) + orderbook_1m (avg 24h) + OI Δ
    Список топ-10 по обороту и расчёт простого score.
    """
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

                # trades
                await cur.execute(SQL_TRADES_SUM_24H, (sym,))
                trow = await cur.fetchone()
                trades_cnt = int(trow[0] or 0)
                qty_sum = float(trow[1] or 0)

                # orderbook
                await cur.execute(SQL_ORDERBOOK_AGGR_24H, (sym,))
                orow = await cur.fetchone()
                avg_spread = orow[0]
                avg_depth = orow[1]

                # oi delta
                await cur.execute(SQL_OI_DELTA_24H, (sym,))
                oirow = await cur.fetchone()
                first_oi = oirow[0]
                last_oi = oirow[1]
                oi_delta_pct = None
                if first_oi and last_oi and first_oi != 0:
                    oi_delta_pct = (last_oi - first_oi) / first_oi * 100.0

                # score (упрощённо): нормируем несколько признаков
                score = 0.0
                score += (p24 / 5.0)  # 5% ~ +1
                if trades_cnt > 0:
                    score += min(trades_cnt / 400000.0, 1.0)  # 400k trades ~ +1
                if avg_depth:
                    score += min(avg_depth / 1_000_000.0, 1.0) * 0.5  # 1M depth ~ +0.5
                if avg_spread is not None:
                    score += max(0.0, (0.5 - min(avg_spread, 0.5)))  # лучше маленький спред
                if oi_delta_pct is not None:
                    score += (oi_delta_pct / 10.0)  # +10% OI ~ +1

                lines.append(
                    f"{sym}  score {score:+.2f}  | "
                    f"turnover ~ {fmt_money(turn)} | trades ~ {fmt_money(trades_cnt)} | "
                    f"depth≈${fmt_money(avg_depth or 0)} | spread≈{fmt_bps(avg_spread)} | "
                    f"OIΔ {fmt_pct(oi_delta_pct)}"
                )

    await send_text(chat_id, "\n".join(lines))

# =========================
# AIOHTTP APP / WEBHOOK
# =========================

async def root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")

async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "ts": now_utc_iso()})

async def handle_webhook(request: web.Request) -> web.Response:
    # проверка секрета в path
    secret = request.match_info.get("secret", "")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return web.json_response({"ok": False, "error": "bad secret"}, status=403)

    try:
        update = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)

    # простейшая обработка
    message = update.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    text = (message.get("text") or "").strip()

    # команды
    if text.startswith("/start"):
        await cmd_start(chat_id)
    elif text.startswith("/status"):
        await cmd_status(chat_id)
    elif text.startswith("/now"):
        parts = text.split()
        sym = parts[1] if len(parts) > 1 else "BTCUSDT"
        await cmd_now(chat_id, sym)
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
    elif text in ("📊 Активность", "Активность+", "🔍 Активность+", "/activity2"):
        await cmd_activity2(chat_id)
    else:
        # кнопки в тексте
        low = text.lower()
        if "активност" in low:
            await cmd_activity2(chat_id)
        else:
            # help
            await send_text(chat_id, "Команды: /status /now [SYMBOL] /activity2 /diag_trades /diag_ob /diag_oi")

    return web.json_response({"ok": True})

def build_app() -> web.Application:
    app = web.Application()
    # Роуты: НЕ регистрируем HEAD вручную, aiohttp сам отдаёт его для GET
    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_post("/webhook/{secret}", handle_webhook)
    return app

# =========================
# LIFECYCLE
# =========================

async def on_startup(app: web.Application):
    global session, ws_task, poll_tasks
    session = aiohttp.ClientSession()
    await get_pool()

    # webhook
    await set_webhook()

    # bg tasks
    ws_task = asyncio.create_task(ws_consumer())

    if ENABLE_OI_POLL:
        poll_tasks.append(asyncio.create_task(oi_poll_loop()))
    if ENABLE_PRICE_POLL:
        poll_tasks.append(asyncio.create_task(price_poll_loop()))

async def on_cleanup(app: web.Application):
    global session, ws_task, poll_tasks, pool
    # stop ws
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    # stop polls
    for t in poll_tasks:
        if not t.done():
            t.cancel()
    for t in poll_tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
    poll_tasks.clear()
    # session
    if session:
        await session.close()
        session = None
    # db
    if pool:
        await pool.close()
        pool = None

def run():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is empty")
    if not PUBLIC_BASE_URL:
        log.warning("PUBLIC_BASE_URL is empty — setWebhook будет пропущен")

    app = build_app()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # корректное завершение
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(app.shutdown()))
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    run()
