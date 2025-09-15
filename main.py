import os
import asyncio
import logging
import signal
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict

import aiohttp
from aiohttp import web
from psycopg_pool import AsyncConnectionPool
import psycopg
from math import sqrt

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

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "false").lower() in ("1", "true", "yes")
OI_POLL_INTERVAL_SEC = int(os.getenv("OI_POLL_INTERVAL_SEC", "90"))
OI_POLL_WINDOW_MIN = int(os.getenv("OI_POLL_WINDOW_MIN", "5"))

ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "true").lower() in ("1", "true", "yes")
PRICE_POLL_INTERVAL_SEC = int(os.getenv("PRICE_POLL_INTERVAL_SEC", "1800"))  # раз в 30 мин
PRICE_POLL_LIMIT = int(os.getenv("PRICE_POLL_LIMIT", "200"))  # сколько последних 1h свечей тащим
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

SQL_PRICE_RANGE = """
SELECT ts, close
FROM kline_1h
WHERE symbol = %s
  AND ts >= %s
ORDER BY ts ASC
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
    """
    Слушаем public WS tickers (Bybit v5 public linear): topic "tickers.SYMBOL"
    """
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
# REST polling (OI / Prices)
# =========================

async def poll_oi_once():
    # /v5/market/open-interest?category=linear&symbol=BTCUSDT&interval=5min
    for sym in SYMBOLS:
        params = {"category": "linear", "symbol": sym, "interval": f"{OI_POLL_WINDOW_MIN}min"}
        ok = False
        # base
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
            last = list_[-1]
            ts_ms = int(last.get("timestamp", 0))
            oi_usd = float(last.get("openInterestUsd", 0) or 0)
            p = await get_pool()
            async with p.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO oi_1m(ts, symbol, oi_usd) VALUES (TO_TIMESTAMP(%s/1000.0), %s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (ts_ms, sym, oi_usd)
                    )
        except Exception:
            log.exception("OI parse/save failed for %s", sym)

async def _binance_klines(symbol: str, limit: int) -> List[Tuple[int, float]]:
    """
    Пытаемся обойти 451, перебирая несколько хостов Binance.
    Возвращает список (start_ms, close).
    """
    if not PRICE_FALLBACK_BINANCE:
        return []
    hosts = [
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
        "https://api-gcp.binance.com",
        "https://api.binance.com",
    ]
    params = {"symbol": symbol, "interval": "1h", "limit": limit}
    for h in hosts:
        url = f"{h}/api/v3/klines"
        try:
            async with session.get(url, params=params, timeout=12) as resp:
                if resp.status == 200:
                    arr = await resp.json()
                    out = []
                    for it in arr:
                        # [openTime,open,high,low,close,volume,closeTime,...]
                        out.append((int(it[0]), float(it[4])))
                    return out
                else:
                    log.warning("REST %s -> HTTP %d", url, resp.status)
        except Exception:
            log.warning("REST %s -> network error", url)
    return []

async def _bybit_klines(symbol: str, limit: int) -> List[Tuple[int, float]]:
    """
    Bybit v5 клайны (1h) без start/end — берём последние N.
    Возвращает список (start_ms, close).
    """
    out = []
    for base in (BYBIT_REST_BASE, BYBIT_REST_FALLBACK):
        url = f"{base}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": "60", "limit": limit}
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = data.get("result", {}) if isinstance(data, dict) else {}
                    lst = result.get("list", []) or []
                    for item in lst:
                        # [startTime, open, high, low, close, volume, turnover]
                        out.append((int(item[0]), float(item[4])))
                    return list(sorted(out, key=lambda x: x[0]))
                else:
                    log.warning("REST %s -> HTTP %d", url, resp.status)
        except Exception:
            log.warning("REST %s -> network error", url)
    return []

async def pull_prices_once(limit: int) -> int:
    """
    Тянем 1h клоузы для всех SYMBOLS.
    Порядок: сначала Bybit, при пустом ответе — Binance fallback.
    Возвращает примерное число upsert'ов.
    """
    total_upserts = 0
    for sym in SYMBOLS:
        rows: List[Tuple[int, float]] = []

        # Bybit
        r_bybit = await _bybit_klines(sym, limit)
        if r_bybit:
            rows = r_bybit
            src = "bybit"
        else:
            # Binance fallback
            r_bin = await _binance_klines(sym, limit)
            rows = r_bin
            src = "binance" if r_bin else "none"

        upserts = 0
        if rows:
            p = await get_pool()
            async with p.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(SQL_PRICE_UPSERT, [(ms, sym, close) for ms, close in rows])
                    upserts = len(rows)

        log.info("Price poll %s: src=%s rows=%d upserted≈%d", sym, src, len(rows), upserts)
        total_upserts += upserts

    log.info("Price poll upserts: %d", total_upserts)
    return total_upserts

async def price_poll_loop():
    log.info(
        "Price polling enabled: every %ds (last-N by limit=%d, fallback=%s)",
        PRICE_POLL_INTERVAL_SEC, PRICE_POLL_LIMIT, "binance" if PRICE_FALLBACK_BINANCE else "none"
    )
    while True:
        try:
            await pull_prices_once(PRICE_POLL_LIMIT)
        except Exception:
            log.exception("Price poll cycle error")
        finally:
            log.info("Price poll cycle done")
        await asyncio.sleep(PRICE_POLL_INTERVAL_SEC)

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

# =========================
# TELEGRAM
# =========================

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

async def tg_call(method: str, payload: dict) -> dict:
    async with session.post(f"{TG_API}/{method}", json=payload, timeout=15) as resp:
        return await resp.json()

async def send_text(chat_id: int, text: str):
    await tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"})

def main_menu_kb() -> Dict:
    # 3 кнопки одной строкой или двумя — пусть телега сама перенесёт
    return {
        "keyboard": [
            [{"text": "📊 Активность"}, {"text": "⚡ Волатильность"}, {"text": "📈 Тренд"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }

async def send_menu(chat_id: int, text: str):
    await tg_call("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": main_menu_kb()
    })

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
# ВОЛАТИЛЬНОСТЬ / ТРЕНД метрики
# =========================

def calc_rv_pct(closes: List[float]) -> Optional[float]:
    """
    Условная реализованная волатильность по 1h клоузам:
    std(ln(C_t/C_{t-1})) * sqrt(N) * 100%, где N=len-1
    Для 24ч — берём ~24 точки => N~=23
    """
    if len(closes) < 2:
        return None
    rets = []
    for i in range(1, len(closes)):
        if closes[i-1] <= 0:
            continue
        rets.append((closes[i] / closes[i-1]) - 1.0)
    if not rets:
        return None
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / max(1, len(rets) - 1)
    std = sqrt(max(var, 0.0))
    ann = std * sqrt(len(rets)) * 100.0  # нормировка на длину окна
    return ann

def sma(vals: List[float], n: int) -> Optional[float]:
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n

def rsi(vals: List[float], n: int = 14) -> Optional[float]:
    if len(vals) < n + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(-n, 0):
        ch = vals[i] - vals[i-1]
        if ch > 0:
            gains += ch
        else:
            losses -= ch
    if losses == 0:
        return 100.0
    rs = (gains / n) / (losses / n)
    return 100.0 - (100.0 / (1 + rs))

def linreg_slope(vals: List[float]) -> Optional[float]:
    """
    Нормированный наклон: регрессия цены на индекс, вернём % в час относительно среднего уровня.
    """
    n = len(vals)
    if n < 3:
        return None
    x = list(range(n))
    xm = sum(x) / n
    ym = sum(vals) / n
    num = sum((x[i] - xm) * (vals[i] - ym) for i in range(n))
    den = sum((x[i] - xm) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    slope = num / den  # Δ price per 1 step (1h)
    if ym != 0:
        return (slope / ym) * 100.0  # %/h
    return 0.0

# =========================
# КОМАНДЫ
# =========================

async def cmd_start(chat_id: int):
    text = (
        "🧭 Innertrade Screener\n"
        "Версия: v1.11.0-summarized\n\n"
        "Кнопки внизу: «Активность», «Волатильность», «Тренд».\n\n"
        "Команды:\n"
        "/status – состояние\n"
        "/now [SYMBOL] – текущие данные по символу\n"
        "/activity – список активных монет (объединённая «Активность+»)\n"
        "/vol [SYMBOL] [H=24] – волатильность за H часов или топ по всем\n"
        "/trend [SYMBOL] [H=72] – тренд по символу или топ по всем\n"
        "/diag_trades SYMBOL [N]\n"
        "/diag_ob SYMBOL [N]\n"
        "/diag_oi SYMBOL [N]\n"
        "/diag_price SYMBOL [N]\n"
        "/pull_prices [SYMBOL or ALL] [LIMIT<=200]\n"
    )
    await send_menu(chat_id, text)

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
        "Version: v1.11.0-summarized\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
        f"DB rows (ws_ticker): {rows}\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} ({OI_POLL_WINDOW_MIN}min, every {OI_POLL_INTERVAL_SEC}s)\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} (every {PRICE_POLL_INTERVAL_SEC}s, limit={PRICE_POLL_LIMIT}, fallback={'binance' if PRICE_FALLBACK_BINANCE else 'none'})\n"
    )
    await send_menu(chat_id, text)

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
    for ts, close in rows[::-1]:
        lines.append(f"{ts.isoformat()}  close={close}")
    await send_text(chat_id, "\n".join(lines))

async def cmd_pull_prices(chat_id: int, arg_symbol: Optional[str], limit: int):
    limit = max(1, min(limit, 200))
    if arg_symbol and arg_symbol.upper() != "ALL":
        syms = [arg_symbol.upper()]
    else:
        syms = SYMBOLS[:]

    total_upserts = 0
    for sym in syms:
        rows_bybit = await _bybit_klines(sym, limit)
        rows = rows_bybit
        src = "bybit" if rows_bybit else "binance"
        if not rows_bybit:
            rows = await _binance_klines(sym, limit)
        upserts = 0
        if rows:
            p = await get_pool()
            async with p.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(SQL_PRICE_UPSERT, [(ms, sym, close) for ms, close in rows])
                    upserts = len(rows)
        total_upserts += upserts
        await send_text(chat_id, f"Загрузка клоузов {sym}: src={src} rows={len(rows)} upserted≈{upserts}")
    await send_text(chat_id, f"Готово. Всего upserted≈{total_upserts}")

# -------- Активность (объединённая «Активность+») --------

async def cmd_activity(chat_id: int, top_n: int = 10):
    # Берём топ по обороту из ws_ticker и дополняем метриками (trades/ob/oiΔ).
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(SQL_TOP_TURNOVER, (top_n,))
            top = await cur.fetchall()

    if not top:
        await send_text(chat_id, "Нет данных для Активности")
        return

    lines = ["📊 Активность (композит за ~24ч)\n"]

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

# -------- Волатильность --------

async def cmd_volatility(chat_id: int, symbol: Optional[str], hours: int = 24, top_n: int = 10):
    hours = max(4, min(hours, 240))
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    p = await get_pool()

    if symbol:
        sym = symbol.upper()
        async with p.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SQL_PRICE_RANGE, (sym, since))
                rows = await cur.fetchall()
        closes = [float(c) for _, c in rows]
        if len(closes) < 2:
            await send_text(chat_id, f"Волатильность {sym}: нет достаточных данных за {hours}ч (попробуй /pull_prices {sym} 120).")
            return
        rv = calc_rv_pct(closes)
        txt = f"Волатильность {sym} за {hours}ч ≈ {rv:.2f}% (часовые закрытия, условная RV)."
        await send_text(chat_id, txt)
        return

    # Топ-лист по всем SYMBOLS
    res = []
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            for sym in SYMBOLS:
                await cur.execute(SQL_PRICE_RANGE, (sym, since))
                rows = await cur.fetchall()
                closes = [float(c) for _, c in rows]
                if len(closes) >= 12:  # какое-то минимальное окно
                    rv = calc_rv_pct(closes)
                    if rv is not None:
                        res.append((sym, rv))
    if not res:
        await send_text(chat_id, "Нет достаточных ценовых данных для расчёта волатильности (используй /pull_prices ALL 200).")
        return
    # сортируем по убыванию RV
    res.sort(key=lambda x: x[1], reverse=True)
    res = res[:top_n]
    lines = [f"⚡ Волатильность (топ за {hours}ч)\n"]
    for sym, rv in res:
        lines.append(f"{sym}  RV≈{rv:.2f}%")
    await send_text(chat_id, "\n".join(lines))

# -------- Тренд --------

async def trend_for_symbol(sym: str, hours: int = 72) -> Optional[dict]:
    hours = max(8, min(hours, 240))
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_RANGE, (sym, since))
            rows = await cur.fetchall()
    if len(rows) < 20:
        return None
    closes = [float(c) for _, c in rows]
    last = closes[-1]
    s20 = sma(closes, 20)
    rsi_v = rsi(closes, 14)
    slope = linreg_slope(closes)  # %/h
    delta_sma = None if not s20 else (last - s20) / s20 * 100.0

    # композитный score: 0.5*наклон + 0.4*ΔSMA норм. + 0.1*(RSI-50)/50
    score = 0.0
    if slope is not None:
        score += 0.5 * slope
    if delta_sma is not None:
        score += 0.4 * (delta_sma / 2.0)  # 2% ~ +0.4
    if rsi_v is not None:
        score += 0.1 * ((rsi_v - 50.0) / 50.0)

    return {
        "sym": sym,
        "last": last,
        "sma20": s20,
        "delta_sma": delta_sma,
        "slope": slope,
        "rsi": rsi_v,
        "score": score,
        "n": len(closes),
    }

async def cmd_trend(chat_id: int, symbol: Optional[str], hours: int = 72, top_n: int = 10):
    if symbol:
        sym = symbol.upper()
        t = await trend_for_symbol(sym, hours)
        if not t:
            await send_text(chat_id, f"📈 Тренд {sym}: недостаточно данных (попробуй /pull_prices {sym} 120).")
            return
        txt = (
            f"📈 Тренд {sym} (1h, последние {hours}ч)\n"
            f"Last close: {t['last']:.2f}\n"
            f"SMA20: {t['sma20']:.2f}\n"
            f"Δ от SMA: {fmt_pct(t['delta_sma'])}\n"
            f"Наклон (норм.): {t['slope']:+.3f}%/ч\n"
            f"RSI≈ {t['rsi']:.1f}\n"
            f"Сводный score: {t['score']:+.2f}"
        )
        await send_text(chat_id, txt)
        return

    # Топ по всем
    out = []
    for sym in SYMBOLS:
        t = await trend_for_symbol(sym, hours)
        if t:
            out.append(t)
    if not out:
        await send_text(chat_id, "Нет достаточных ценовых данных для тренда (используй /pull_prices ALL 200).")
        return
    # сортируем по score
    out.sort(key=lambda d: d["score"], reverse=True)
    lines = [f"📈 Тренд (топ за {hours}ч)\n"]
    for t in out[:top_n]:
        lines.append(
            f"{t['sym']}  score {t['score']:+.2f} | ΔSMA {fmt_pct(t['delta_sma'])} | slope {t['slope']:+.2f}%/ч | RSI≈{t['rsi']:.1f}"
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

    # Команды
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
        sym = parts[1] if len(parts) >= 2 else "ALL"
        limit = int(parts[2]) if len(parts) >= 3 else PRICE_POLL_LIMIT
        await cmd_pull_prices(chat_id, sym, limit)
    elif text.startswith("/activity") or text in ("📊 Активность", "Активность", "Активность+"):
        await cmd_activity(chat_id, 10)
    elif text.startswith("/vol"):
        parts = text.split()
        if len(parts) == 1:
            # Топ по всем за 24ч
            await cmd_volatility(chat_id, None, 24, 10)
        else:
            sym = parts[1].upper()
            h = int(parts[2]) if len(parts) >= 3 else 24
            await cmd_volatility(chat_id, sym, h, 10)
    elif text.startswith("/trend") or text == "📈 Тренд":
        parts = text.split()
        if len(parts) == 1:
            # Топ по всем за 72ч
            await cmd_trend(chat_id, None, 72, 10)
        else:
            sym = parts[1].upper()
            h = int(parts[2]) if len(parts) >= 3 else 72
            await cmd_trend(chat_id, sym, h, 10)
    elif text == "⚡ Волатильность":
        await cmd_volatility(chat_id, None, 24, 10)
    else:
        low = text.lower()
        if "активност" in low:
            await cmd_activity(chat_id, 10)
        elif "волатил" in low:
            await cmd_volatility(chat_id, None, 24, 10)
        elif "тренд" in low:
            await cmd_trend(chat_id, None, 72, 10)
        else:
            await send_menu(chat_id, "Команды: /status /now [SYMBOL] /activity /vol [/trend] /diag_trades /diag_ob /diag_oi /diag_price /pull_prices")

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
        poll_tasks.append(asyncio.create_task(oi_poll_loop()))
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
