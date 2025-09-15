import os
import json
import asyncio
import logging
import signal
from math import sqrt
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Sequence

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

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "false").lower() in ("1", "true", "yes")
OI_POLL_INTERVAL_SEC = int(os.getenv("OI_POLL_INTERVAL_SEC", "90"))
OI_POLL_WINDOW_MIN = int(os.getenv("OI_POLL_WINDOW_MIN", "5"))

ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "true").lower() in ("1", "true", "yes")
PRICE_POLL_INTERVAL_SEC = int(os.getenv("PRICE_POLL_INTERVAL_SEC", "1800"))
PRICE_POLL_LIMIT = int(os.getenv("PRICE_POLL_LIMIT", "200"))
PRICE_FALLBACK_BINANCE = os.getenv("PRICE_FALLBACK_BINANCE", "true").lower() in ("1", "true", "yes")

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

SQL_PRICE_UPSERT = """
INSERT INTO kline_1h(ts, symbol, close)
VALUES (TO_TIMESTAMP(%s/1000.0), %s, %s)
ON CONFLICT (ts, symbol) DO UPDATE
  SET close = EXCLUDED.close
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
    url = BYBIT_WS_URL
    subs = [{"op": "subscribe", "args": [f"tickers.{sym}" for sym in SYMBOLS]}]

    backoff = 1
    while True:
        try:
            log.info(f"Bybit WS connecting: {url}")
            async with session.ws_connect(url, heartbeat=20) as ws:
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

# =========================
# REST helpers (prices)
# =========================

async def http_get_json(url: str, params: dict) -> Tuple[int, Optional[dict]]:
    try:
        async with session.get(url, params=params, timeout=12) as resp:
            if resp.status == 200:
                return resp.status, await resp.json()
            return resp.status, None
    except Exception as e:
        log.warning("REST %s -> network error: %s", url, e)
        return 0, None

async def fetch_bybit_klines(symbol: str, limit: int) -> List[Tuple[int, float]]:
    # v5 market/kline last-N by limit (hourly interval=60)
    params = {"category": "linear", "symbol": symbol, "interval": "60", "limit": str(limit)}
    rows: List[Tuple[int, float]] = []

    for base in (BYBIT_REST_BASE, BYBIT_REST_FALLBACK):
        code, data = await http_get_json(f"{base}/v5/market/kline", params)
        if code == 200 and data:
            try:
                lst = (data.get("result", {}) or {}).get("list", []) or []
                for item in lst:
                    # [startTime, open, high, low, close, volume, turnover]
                    ts_ms = int(item[0])
                    close = float(item[4])
                    rows.append((ts_ms, close))
                break
            except Exception:
                log.exception("Bybit kline parse fail %s", symbol)
        elif code:
            log.warning("REST %s -> HTTP %d", f"{base}/v5/market/kline", code)
    return rows

async def fetch_binance_klines(symbol: str, limit: int) -> List[Tuple[int, float]]:
    # Binance 1h klines
    # 451 может возникать, но иногда один из доменов даёт; пробуем пул эндпоинтов
    bases = [
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
        "https://api-gcp.binance.com",
        "https://api.binance.com",
    ]
    params = {"symbol": symbol, "interval": "1h", "limit": str(limit)}
    for base in bases:
        code, data = await http_get_json(f"{base}/api/v3/klines", params)
        if code == 200 and isinstance(data, list):
            out: List[Tuple[int, float]] = []
            try:
                for item in data:
                    # [ openTime, open, high, low, close, volume, ... ]
                    ts_ms = int(item[0])
                    close = float(item[4])
                    out.append((ts_ms, close))
                return out
            except Exception:
                log.exception("Binance kline parse fail %s", symbol)
        elif code:
            log.warning("REST %s -> HTTP %d", f"{base}/api/v3/klines", code)
    return []

async def upsert_prices(symbol: str, rows: Sequence[Tuple[int, float]]) -> int:
    if not rows:
        return 0
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(SQL_PRICE_UPSERT, [(ts, symbol, close) for ts, close in rows])
    return len(rows)

# =========================
# Poll prices loop
# =========================

async def price_poll_once(symbols: Sequence[str], limit: int) -> int:
    total_upserts = 0
    for sym in symbols:
        src = "none"
        rows = await fetch_bybit_klines(sym, limit)
        if rows:
            src = "bybit"
        elif PRICE_FALLBACK_BINANCE:
            rows = await fetch_binance_klines(sym, limit)
            src = "binance" if rows else "none"

        if not rows:
            log.info("Price poll %s: src=%s rows=0 upserted≈0", sym, src)
            continue

        # нормализуем порядок (по возрастанию времени)
        rows.sort(key=lambda x: x[0])
        up = await upsert_prices(sym, rows)
        total_upserts += up
        log.info("Price poll %s: src=%s rows=%d upserted≈%d", sym, src, len(rows), up)
    return total_upserts

async def price_poll_loop():
    mode = f"last-N by limit={PRICE_POLL_LIMIT}, fallback={'binance' if PRICE_FALLBACK_BINANCE else 'none'}"
    log.info("Price polling enabled: every %ds (%s)", PRICE_POLL_INTERVAL_SEC, mode)
    while True:
        try:
            up = await price_poll_once(SYMBOLS, PRICE_POLL_LIMIT)
            log.info("Price poll upserts: %d", up)
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
# Команды
# =========================

def buttons_text() -> str:
    return "🧭 Market mood\n📊 Активность\n⚡ Волатильность\n📈 Тренд\n🔍 Активность+"

async def cmd_start(chat_id: int):
    text = (
        "🧭 Market mood\n"
        "Добро пожаловать в Innertrade Screener v1.12.0-trend-volfix.\n\n"
        "Команды:\n"
        "/status – состояние\n"
        "/now [SYMBOL] – текущие данные по символу (например, /now BTCUSDT)\n"
        "/activity2 – Активность+ (композит)\n"
        "/diag_trades SYMBOL [N]\n"
        "/diag_ob SYMBOL [N]\n"
        "/diag_oi SYMBOL [N]\n"
        "/diag_price SYMBOL [N]\n"
        "/pull_prices SYMBOL [LIMIT]\n"
        "/vol [SYMBOL] [HOURS]\n"
        "/trend [SYMBOL] [HOURS]\n"
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
        "Version: v1.12.0-trend-volfix\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
        f"DB rows (ws_ticker): {rows}\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_POLL_WINDOW_MIN}min, every {OI_POLL_INTERVAL_SEC}s)\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} (every {PRICE_POLL_INTERVAL_SEC}s, limit={PRICE_POLL_LIMIT}, fallback={'binance' if PRICE_FALLBACK_BINANCE else 'none'})\n"
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
                trades_cnt = int(trow[0] or 0)
                qty_sum = float(trow[1] or 0)

                await cur.execute(SQL_ORDERBOOK_AGGR_24H, (sym,))
                orow = await cur.fetchone()
                avg_spread = orow[0]
                avg_depth = orow[1]

                await cur.execute(SQL_OI_DELTA_24H, (sym,))
                oirow = await cur.fetchone()
                first_oi = oirow[0]
                last_oi = oirow[1]
                oi_delta_pct = None
                if first_oi and last_oi and first_oi != 0:
                    oi_delta_pct = (last_oi - first_oi) / first_oi * 100.0

                score = 0.0
                score += (p24 / 5.0)
                if trades_cnt > 0:
                    score += min(trades_cnt / 400000.0, 1.0)
                if avg_depth:
                    score += min(avg_depth / 1_000_000.0, 1.0) * 0.5
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

async def cmd_pull_prices(chat_id: int, symbols: List[str], limit: int):
    symbols = [s.upper() for s in symbols]
    up_all = await price_poll_once(symbols, limit)
    await send_text(chat_id, f"Загрузка клоузов: symbols={len(symbols)}, limit={limit}, rows≈{limit if symbols else 0}, upserted≈{up_all}")

# -------- Волатильность --------

def stdev(xs: Sequence[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs)/n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    return sqrt(var)

async def cmd_vol(chat_id: int, symbol: str, hours: int):
    symbol = symbol.upper()
    need = max(2, hours)  # минимум две точки
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_LATEST_N, (symbol, need))
            rows = await cur.fetchall()
    if len(rows) < need:
        await send_text(chat_id, f"Волатильность {symbol}: нет достаточных данных за {hours}ч (используй /pull_prices {symbol} 120).")
        return

    closes = [float(c) for _, c in reversed(rows)]  # по возрастанию времени
    rets = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            rets.append((closes[i] / closes[i-1]) - 1.0)
    if len(rets) < 2:
        await send_text(chat_id, f"Волатильность {symbol}: недостаточно точек.")
        return

    rv_hourly = stdev(rets) * 100.0  # в %
    msg = f"Волатильность {symbol} за {hours}ч ≈ {rv_hourly:.2f}% (часовые закрытия, условная RV)."
    await send_text(chat_id, msg)

# -------- Тренд --------

def sma(xs: Sequence[float], w: int) -> List[float]:
    out: List[float] = []
    s = 0.0
    for i, x in enumerate(xs):
        s += x
        if i >= w:
            s -= xs[i-w]
        if i >= w-1:
            out.append(s / w)
        else:
            out.append(None)  # type: ignore
    return out

def rsi_like(closes: Sequence[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, period+1):
        ch = closes[-i] - closes[-i-1]
        if ch >= 0:
            gains.append(ch)
        else:
            losses.append(-ch)
    avg_gain = sum(gains)/period
    avg_loss = sum(losses)/period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain/avg_loss
    return 100.0 - (100.0/(1.0+rs))

def linreg_slope(xs: Sequence[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    # x = 0..n-1
    sx = (n - 1) * n / 2
    sxx = (n - 1) * n * (2*n - 1) / 6
    sy = sum(xs)
    sxy = sum(i * xs[i] for i in range(n))
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0
    slope = (n * sxy - sx * sy) / denom
    return slope

async def cmd_trend(chat_id: int, symbol: str, hours: int):
    symbol = symbol.upper()
    need = max(20, hours)  # возьмём минимум 20 свечей, чтобы метрики были адекватны
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_LATEST_N, (symbol, need))
            rows = await cur.fetchall()

    if len(rows) < 20:
        await send_text(chat_id, f"Тренд {symbol}: недостаточно данных (нужно ≥20 1h свечей). Попробуй /pull_prices {symbol} 200.")
        return

    # по времени вперёд
    rows = list(reversed(rows))
    closes = [float(c) for _, c in rows]

    # метрики
    w_sma = min(20, len(closes))
    s = sma(closes, w_sma)
    last = closes[-1]
    last_sma = s[-1] if s[-1] is not None else None
    dev_pct = None
    if last_sma and last_sma > 0:
        dev_pct = (last / last_sma - 1) * 100.0

    # наклон (в “$ на свечу”), нормализуем в %
    slope = linreg_slope(closes[-hours:]) if len(closes) >= hours else linreg_slope(closes)
    norm_slope_pct = (slope / last) * 100.0 if last else 0.0

    # RSI-light
    rsi_val = rsi_like(closes, 14)

    # простой сводный скор (только для ориентировки)
    score = 0.0
    if dev_pct is not None:
        score += max(min(dev_pct / 2.0, 2.0), -2.0)   # ±2 балла за отклонение от SMA
    score += max(min(norm_slope_pct * 10.0, 2.0), -2.0)  # усиленный вклад наклона
    if rsi_val is not None:
        score += (rsi_val - 50.0) / 25.0   # RSI 50 — нейтрально, 75 ~ +1, 25 ~ -1

    lines = [
        f"📈 Тренд {symbol} (1h, последние {min(hours, len(closes))}ч)",
        f"Last close: {last:.2f}",
        f"SMA{w_sma}: {last_sma:.2f}" if last_sma else f"SMA{w_sma}: —",
        f"Δ от SMA: {fmt_pct(dev_pct)}",
        f"Наклон (норм.): {norm_slope_pct:+.3f}%/ч",
        f"RSI≈ {rsi_val:.1f}" if rsi_val is not None else "RSI≈ —",
        f"Сводный score: {score:+.2f}",
    ]
    await send_text(chat_id, "\n".join(lines))

# =========================
# AIOHTTP APP / WEBHOOK
# =========================

async def root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")

async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "ts": now_utc_iso()})

async def handle_webhook(request: web.Request) -> web.Response:
    # проверка секрета
    secret = request.match_info.get("secret", "")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return web.json_response({"ok": False, "error": "bad secret"}, status=403)

    try:
        update = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)

    message = update.get("message") or {}
    chat = (message.get("chat") or {})
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()

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
        if len(parts) >= 2:
            sym = parts[1].upper()
            limit = int(parts[2]) if len(parts) >= 3 else PRICE_POLL_LIMIT
            await cmd_pull_prices(chat_id, [sym], limit)
        else:
            await send_text(chat_id, "Usage: /pull_prices SYMBOL [LIMIT]")

    elif text.startswith("/vol"):
        parts = text.split()
        sym = parts[1] if len(parts) >= 2 else "BTCUSDT"
        hours = int(parts[2]) if len(parts) >= 3 else 24
        await cmd_vol(chat_id, sym.upper(), hours)

    elif text.startswith("/trend"):
        parts = text.split()
        sym = parts[1] if len(parts) >= 2 else "BTCUSDT"
        hours = int(parts[2]) if len(parts) >= 3 else 72
        await cmd_trend(chat_id, sym.upper(), hours)

    elif text in ("📊 Активность", "Активность+", "🔍 Активность+", "/activity2"):
        await cmd_activity2(chat_id)

    else:
        low = text.lower()

        # КНОПКА "⚡ Волатильность" или свободный текст с "волатил"
        if ("волатил" in low) or (text.strip() == "⚡ Волатильность"):
            await cmd_vol(chat_id, "BTCUSDT", 24)

        # КНОПКА "📈 Тренд" или упоминание "тренд"
        elif ("тренд" in low) or (text.strip() == "📈 Тренд"):
            await cmd_trend(chat_id, "BTCUSDT", 72)

        elif "активност" in low:
            await cmd_activity2(chat_id)

        else:
            await send_text(
                chat_id,
                "Команды: /status /now [SYMBOL] /activity2 /diag_trades /diag_ob /diag_oi /diag_price /pull_prices [/vol SYMBOL [HOURS]] [/trend SYMBOL [HOURS]]"
            )

    return web.json_response({"ok": True})

def build_app() -> web.Application:
    app = web.Application()
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

    await set_webhook()

    ws_task = asyncio.create_task(ws_consumer())

    if ENABLE_OI_POLL:
        poll_tasks.append(asyncio.create_task(oi_poll_loop_dummy()))  # см. ниже
    if ENABLE_PRICE_POLL:
        poll_tasks.append(asyncio.create_task(price_poll_loop()))

async def on_cleanup(app: web.Application):
    global session, ws_task, poll_tasks, pool
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    for t in poll_tasks:
        if not t.done():
            t.cancel()
    for t in poll_tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
    poll_tasks.clear()
    if session:
        await session.close()
        session = None
    if pool:
        await pool.close()
        pool = None

# ---- Заглушка OI poll (мы оставили как раньше: выбор Bybit REST может давать 403; твоя база oi_1m уже наполняется внешним сборщиком)
async def oi_poll_loop_dummy():
    log.info("OI polling enabled: every %ds, interval=%dmin", OI_POLL_INTERVAL_SEC, OI_POLL_WINDOW_MIN)
    while True:
        try:
            pass  # здесь можно подключить твой рабочий сборщик, если нужно
        except Exception:
            log.exception("OI poll cycle error")
        finally:
            log.info("OI poll cycle done")
        await asyncio.sleep(OI_POLL_INTERVAL_SEC)

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
