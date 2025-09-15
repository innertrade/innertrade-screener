import os
import json
import asyncio
import logging
import signal
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Set

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
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")  # https://...onrender.com
PORT = int(os.getenv("PORT", "10000"))

DATABASE_URL = os.getenv("DATABASE_URL", "")
POOL_SIZE = int(os.getenv("POOL_SIZE", "5"))

# Ручной список — теперь трактуем как "обязательные"
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

# Динамический универс — опциональные
UNIVERSE_MODE = os.getenv("UNIVERSE_MODE", "auto").lower()  # auto|static
UNIVERSE_MAX = int(os.getenv("UNIVERSE_MAX", "30"))
UNIVERSE_REFRESH_MIN = int(os.getenv("UNIVERSE_REFRESH_MIN", "15"))

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

# Динамический универс
_current_symbols: Set[str] = set(SYMBOLS) if SYMBOLS else set()
_target_symbols: Set[str] = set(_current_symbols)
_universe_changed = asyncio.Event()

# WS
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
# Bybit REST helpers
# =========================

async def rest_get_json(path: str, params: dict) -> Optional[dict]:
    for base in (BYBIT_REST_BASE, BYBIT_REST_FALLBACK):
        url = f"{base}{path}"
        try:
            async with session.get(url, params=params, timeout=12) as resp:
                if resp.status == 200:
                    return await resp.json()
                log.warning("REST %s -> HTTP %d", url, resp.status)
        except Exception as e:
            log.warning("REST %s -> network error: %s", url, e)
    return None

# =========================
# WS Tickers
# =========================

async def upsert_ticker(symbol: str, last: float, p24: float, turnover: float):
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT_TICKER, (symbol, last, p24, turnover))

async def _ws_connect_and_consume(symbols: List[str]):
    url = BYBIT_WS_URL
    subs = [{"op": "subscribe", "args": [f"tickers.{s}" for s in symbols]}]
    log.info("Bybit WS connecting: %s (subs=%d)", url, len(symbols))
    async with session.ws_connect(url, heartbeat=20) as ws:
        await ws.send_json(subs[0])
        log.info("WS subscribed: %d topics", len(symbols))
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

async def ws_runner():
    """Следит за изменением целевого универса и пересоздаёт WS при изменениях/ошибках."""
    backoff = 1
    while True:
        try:
            # берём срез текущей цели
            symbols = sorted(_target_symbols)
            if not symbols:
                # чтобы не крутиться впустую
                await asyncio.sleep(5)
                continue
            # запуск сессии
            await _ws_connect_and_consume(symbols)
            backoff = 1
        except asyncio.CancelledError:
            log.info("WS runner cancelled")
            return
        except Exception as e:
            log.exception("WS runner failed: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            # если за время работы изменили цель — просто продолжаем цикл, и новая попытка возьмёт новый список
            pass

# =========================
# REST polling (OI / Klines)
# =========================

async def poll_oi_once():
    syms = list(_target_symbols) if _target_symbols else list(_current_symbols) or SYMBOLS
    for sym in syms:
        params = {"category": "linear", "symbol": sym, "interval": f"{OI_POLL_WINDOW_MIN}min"}
        data = await rest_get_json("/v5/market/open-interest", params)
        if not data:
            log.warning("OI %s fetch failed", sym)
            continue
        try:
            result = data.get("result", {})
            lst = result.get("list", [])
            if not lst:
                continue
            last = lst[-1]
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
    syms = list(_target_symbols) if _target_symbols else list(_current_symbols) or SYMBOLS
    total_upserts = 0
    for sym in syms:
        rows = []
        src = "none"

        # 1) Bybit last-N (по limit)
        data = await rest_get_json("/v5/market/kline", {
            "category": "linear", "symbol": sym, "interval": "60", "limit": PRICE_POLL_LIMIT
        })
        if data and data.get("result", {}).get("list"):
            src = "bybit"
            for item in data["result"]["list"]:
                start_ms = int(item[0]); close = float(item[4])
                rows.append((start_ms, sym, close))
        # 2) fallback: Binance (если включен и Bybit пуст)
        elif PRICE_FALLBACK_BINANCE:
            # Пытаемся обойти 451: разные домены. Любой, кто ответит 200.
            bin_domains = [
                "https://api1.binance.com", "https://api2.binance.com",
                "https://api3.binance.com", "https://api-gcp.binance.com",
                "https://api.binance.com",
            ]
            params = {"symbol": sym, "interval": "1h", "limit": PRICE_POLL_LIMIT}
            for dom in bin_domains:
                url = f"{dom}/api/v3/klines"
                try:
                    async with session.get(url, params=params, timeout=12) as resp:
                        if resp.status == 200:
                            arr = await resp.json()
                            src = "binance"
                            for it in arr:
                                start_ms = int(it[0]); close = float(it[4])
                                rows.append((start_ms, sym, close))
                            break
                        else:
                            log.warning("REST %s -> HTTP %d", url, resp.status)
                except Exception:
                    log.warning("REST %s -> HTTP 451/blocked likely", url)

        upserts = 0
        if rows:
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
                        rows
                    )
                    upserts = len(rows)
        log.info("Price poll %s: src=%s rows=%d upserted≈%d", sym, src, len(rows), upserts)
        total_upserts += upserts

    log.info("Price poll upserts: %d", total_upserts)

async def oi_poll_loop():
    log.info("OI polling enabled: every %ds, interval=%dmin", OI_POLL_INTERVAL_SEC, OI_POLL_WINDOW_MIN)
    while True:
        try:
            await poll_oi_once()
        except Exception:
            log.exception("OI poll cycle error")
        finally:
            await asyncio.sleep(OI_POLL_INTERVAL_SEC)

async def price_poll_loop():
    log.info("Price polling enabled: every %ds (last-N by limit=%d, fallback=%s)",
             PRICE_POLL_INTERVAL_SEC, PRICE_POLL_LIMIT, "binance" if PRICE_FALLBACK_BINANCE else "none")
    while True:
        try:
            await poll_prices_once()
        except Exception:
            log.exception("Price poll cycle error")
        finally:
            log.info("Price poll cycle done")
            await asyncio.sleep(PRICE_POLL_INTERVAL_SEC)

# =========================
# DYNAMIC UNIVERSE
# =========================

def _merge_universe(dynamic: List[str]) -> Set[str]:
    # ручные обязательные + динамический топ
    merged = set(s.upper() for s in SYMBOLS) | set(dynamic)
    return set(sorted(list(merged)))  # нормализуем

async def fetch_dynamic_universe() -> List[str]:
    """Тянем топ по обороту c Bybit tickers (linear, USDT)."""
    data = await rest_get_json("/v5/market/tickers", {"category": "linear"})
    if not data:
        return []
    result = data.get("result", {})
    list_ = result.get("list", []) or []
    # фильтруем только USDT-квоты и нормальные символы
    pairs = []
    for d in list_:
        sym = (d.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        try:
            turn = float(d.get("turnover24h", 0) or 0.0)
        except Exception:
            turn = 0.0
        pairs.append((sym, turn))
    pairs.sort(key=lambda x: x[1], reverse=True)
    top = [sym for sym, _ in pairs[:UNIVERSE_MAX]]
    return top

async def universe_loop():
    """Периодически обновляет целевой универс и сигналит WS-раннеру."""
    global _current_symbols, _target_symbols
    if UNIVERSE_MODE != "auto":
        log.info("Universe mode: static (use SYMBOLS only)")
        _current_symbols = set(SYMBOLS)
        _target_symbols = set(SYMBOLS)
        return

    log.info("Universe mode: auto (max=%d, refresh=%dmin)", UNIVERSE_MAX, UNIVERSE_REFRESH_MIN)
    while True:
        try:
            top_dyn = await fetch_dynamic_universe()
            if not top_dyn and not _current_symbols:
                # нет данных — оставим как было
                await asyncio.sleep(UNIVERSE_REFRESH_MIN * 60)
                continue
            merged = _merge_universe(top_dyn)
            if merged != _target_symbols:
                log.info("Universe changed: %d -> %d symbols", len(_target_symbols), len(merged))
                _target_symbols = merged
                _universe_changed.set()
            _current_symbols = merged  # текущая видимость
        except Exception:
            log.exception("Universe refresh error")
        finally:
            await asyncio.sleep(UNIVERSE_REFRESH_MIN * 60)

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
# METRICS / COMMANDS
# =========================

async def send_text(chat_id: int, text: str):
    await tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"})

def menu_text() -> str:
    return "🧭 Market mood\n📊 Активность\n⚡ Волатильность\n📈 Тренд\n🔍 Активность+"

def compute_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(-period, 0):
        ch = closes[i] - closes[i-1]
        if ch > 0:
            gains += ch
        else:
            losses -= ch
    if gains == 0 and losses == 0:
        return 50.0
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))

async def cmd_start(chat_id: int):
    text = (
        "🧭 Market mood\n"
        "Innertrade Screener v1.12.0-dyn (WS tickers + OI/Prices via REST + Dynamic Universe)\n\n"
        "Команды:\n"
        "/status – состояние\n"
        "/now [SYMBOL] – текущие данные по символу (например, /now BTCUSDT)\n"
        "/activity2 – Активность+ (композит)\n"
        "/vol [SYMBOL] [H] – волатильность\n"
        "/trend [SYMBOL] [H] – тренд\n"
        "/diag_trades SYMBOL [N]\n"
        "/diag_ob SYMBOL [N]\n"
        "/diag_oi SYMBOL [N]\n"
        "/diag_price SYMBOL [N]\n"
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
        "Source: Bybit (public WS + REST OI/Prices)\n"
        "Version: v1.12.0-dyn\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
        f"DB rows (ws_ticker): {rows}\n"
        f"Universe mode: {UNIVERSE_MODE} | current={len(_current_symbols)} target={len(_target_symbols)}\n"
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

async def get_closes(symbol: str, hours: int) -> List[Tuple[datetime, float]]:
    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PRICE_LATEST_N, (symbol, hours))
            rows = await cur.fetchall()
    rows = list(reversed(rows))  # по возрастанию времени
    return rows

async def cmd_vol(chat_id: int, symbol: Optional[str], hours: int):
    if symbol:
        sym = symbol.upper()
        rows = await get_closes(sym, hours)
        if len(rows) < max(12, hours//2):
            await send_text(chat_id, f"Волатильность {sym}: нет достаточных данных за {hours}ч.")
            return
        closes = [float(c) for _, c in rows]
        # условная RV: std(log-returns)*sqrt(24/hours)*100
        import math
        rets = []
        for i in range(1, len(closes)):
            if closes[i-1] > 0:
                rets.append(math.log(closes[i]/closes[i-1]))
        if not rets:
            await send_text(chat_id, f"Волатильность {sym}: нет достаточных данных за {hours}ч.")
            return
        mean = sum(rets)/len(rets)
        var = sum((r-mean)**2 for r in rets)/max(1, len(rets)-1)
        std = math.sqrt(var)
        annualizer = math.sqrt(24/ (hours if hours>0 else 24))
        rv_pct = std * annualizer * 100
        await send_text(chat_id, f"Волатильность {sym} за {hours}ч ≈ {rv_pct:.2f}% (часовые закрытия, условная RV).")
    else:
        # топ по RV за 24ч по текущему универсy
        syms = sorted(_current_symbols) or SYMBOLS
        out = []
        for sym in syms:
            rows = await get_closes(sym, 24)
            if len(rows) < 12:
                continue
            import math
            closes = [float(c) for _, c in rows]
            rets = []
            for i in range(1, len(closes)):
                if closes[i-1] > 0:
                    rets.append(math.log(closes[i]/closes[i-1]))
            if not rets:
                continue
            mean = sum(rets)/len(rets)
            var = sum((r-mean)**2 for r in rets)/max(1, len(rets)-1)
            std = math.sqrt(var)
            rv_pct = std * 100
            out.append((sym, rv_pct))
        if not out:
            await send_text(chat_id, "⚡ Волатильность: недостаточно данных.")
            return
        out.sort(key=lambda x: x[1], reverse=True)
        lines = ["⚡ Волатильность (топ за 24ч)\n"]
        for sym, rv in out[:10]:
            lines.append(f"{sym}  RV≈{rv:.2f}%")
        await send_text(chat_id, "\n".join(lines))

async def cmd_trend(chat_id: int, symbol: Optional[str], hours: int):
    # Если symbol=None — топ (72ч) по текущему универсy
    async def trend_metrics(sym: str, hours: int):
        rows = await get_closes(sym, hours)
        if len(rows) < max(24, hours//2):
            return None
        closes = [float(c) for _, c in rows]
        last = closes[-1]
        # SMA20
        if len(closes) < 20:
            return None
        sma20 = sum(closes[-20:]) / 20.0
        delta_pct = (last - sma20)/sma20*100.0 if sma20 else 0.0
        # норм. линейный наклон (на 1ч в %)
        import numpy as np
        y = np.array(closes[-hours:], dtype=float)
        x = np.arange(len(y), dtype=float)
        # Линрег y ~ ax + b
        denom = (x - x.mean())
        denom = (denom*denom).sum()
        if denom == 0:
            slope = 0.0
        else:
            a = ((x - x.mean())*(y - y.mean())).sum()/denom
            slope = (a / y[-1]) * 100.0 if y[-1] else 0.0
        # RSI
        rsi = compute_rsi(closes, 14)
        # сводный
        score = 0.0
        score += delta_pct/5.0
        score += slope/0.5
        if rsi is not None:
            score += (rsi-50.0)/50.0
        return {
            "last": last, "sma20": sma20, "delta_pct": delta_pct,
            "slope": slope, "rsi": rsi, "score": score
        }

    if symbol:
        sym = symbol.upper()
        m = await trend_metrics(sym, hours)
        if not m:
            await send_text(chat_id, f"📈 Тренд {sym}: недостаточно данных (попробуй /pull_prices {sym} 120).")
            return
        lines = [
            f"📈 Тренд {sym} (1h, последние {hours}ч)",
            f"Last close: {m['last']}",
            f"SMA20: {m['sma20']:.2f}",
            f"Δ от SMA: {m['delta_pct']:+.2f}%",
            f"Наклон (норм.): {m['slope']:+.3f}%/ч",
            f"RSI≈ {m['rsi']:.1f}" if m['rsi'] is not None else "RSI: —",
            f"Сводный score: {m['score']:+.2f}",
        ]
        await send_text(chat_id, "\n".join(lines))
    else:
        syms = sorted(_current_symbols) or SYMBOLS
        out = []
        for sym in syms:
            m = await trend_metrics(sym, 72)
            if m:
                out.append((sym, m))
        if not out:
            await send_text(chat_id, "📈 Тренд: недостаточно данных.")
            return
        # Сортируем по score
        out.sort(key=lambda x: x[1]["score"], reverse=True)
        lines = ["📈 Тренд (топ за 72ч)\n"]
        for sym, m in out[:10]:
            lines.append(f"{sym}  score {m['score']:+.2f} | ΔSMA {m['delta_pct']:+.2f}% | slope {m['slope']:+.2f}%/ч | RSI≈{m['rsi']:.1f}")
        await send_text(chat_id, "\n".join(lines))

async def cmd_activity2(chat_id: int):
    # Композит теперь строим по текущему универсy (что уже слушаем WS)
    syms = sorted(_current_symbols) or SYMBOLS
    p = await get_pool()
    lines = ["📊 Активность (композит за ~24ч)\n"]
    async with p.connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # Берём top по turnover прямо из ws_ticker, но фильтруем по syms
            await cur.execute("SELECT symbol, COALESCE(price24h_pcnt,0) AS p24, COALESCE(turnover24h,0) AS turn FROM ws_ticker")
            all_rows = await cur.fetchall()
            rows = [r for r in all_rows if r["symbol"] in syms]
    if not rows:
        await send_text(chat_id, "Нет данных для Активность+")
        return
    # сортируем по обороту
    rows.sort(key=lambda r: float(r["turn"] or 0.0), reverse=True)
    rows = rows[:10]

    p = await get_pool()
    async with p.connection() as conn:
        async with conn.cursor() as cur:
            for r in rows:
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

# =========================
# AIOHTTP APP / WEBHOOK
# =========================

async def root(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")

async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "ts": now_utc_iso()})

async def handle_webhook(request: web.Request) -> web.Response:
    if WEBHOOK_SECRET and request.match_info.get("secret", "") != WEBHOOK_SECRET:
        return web.json_response({"ok": False, "error": "bad secret"}, status=403)

    try:
        update = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)

    message = update.get("message") or {}
    chat = message.get("chat") or {}
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
            rows = await get_closes(sym.upper(), n)
            if not rows:
                await send_text(chat_id, f"kline_1h {sym.upper()}: пусто")
            else:
                lines = [f"kline_1h {sym.upper()} (latest {n})"]
                for ts, c in rows:
                    lines.append(f"{ts.isoformat()}  close={c}")
                await send_text(chat_id, "\n".join(lines))
        else:
            await send_text(chat_id, "Usage: /diag_price SYMBOL [N]")
    elif text.startswith("/vol"):
        parts = text.split()
        if len(parts) == 1:
            await cmd_vol(chat_id, None, 24)
        elif len(parts) == 2:
            await cmd_vol(chat_id, parts[1], 24)
        else:
            await cmd_vol(chat_id, parts[1], int(parts[2]))
    elif text.startswith("/trend"):
        parts = text.split()
        if len(parts) == 1:
            await cmd_trend(chat_id, None, 72)
        elif len(parts) == 2:
            await cmd_trend(chat_id, parts[1], 72)
        else:
            await cmd_trend(chat_id, parts[1], int(parts[2]))
    elif text in ("📊 Активность", "Активность", "Активность+", "🔍 Активность+", "/activity2"):
        await cmd_activity2(chat_id)
    elif text in ("⚡ Волатильность",):
        await cmd_vol(chat_id, None, 24)
    elif text in ("📈 Тренд",):
        await cmd_trend(chat_id, None, 72)
    else:
        low = text.lower()
        if "активност" in low:
            await cmd_activity2(chat_id)
        elif "волат" in low:
            await cmd_vol(chat_id, None, 24)
        elif "тренд" in low:
            await cmd_trend(chat_id, None, 72)
        else:
            await send_text(chat_id, "Команды: /status /now [SYMBOL] /activity2 /vol [/trend] /diag_trades /diag_ob /diag_oi /diag_price")

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
    global session, ws_task, poll_tasks, _current_symbols, _target_symbols
    session = aiohttp.ClientSession()
    await get_pool()

    # webhook
    await set_webhook()

    # Universe
    if UNIVERSE_MODE == "auto":
        # инициализация первичного универса до старта WS
        try:
            top_dyn = await fetch_dynamic_universe()
            merged = _merge_universe(top_dyn)
            _current_symbols = merged
            _target_symbols = merged
        except Exception:
            log.exception("Initial universe fetch failed; fallback to SYMBOLS")
            _current_symbols = set(SYMBOLS)
            _target_symbols = set(SYMBOLS)
        # старт лупа автообновления
        poll_tasks.append(asyncio.create_task(universe_loop()))
    else:
        _current_symbols = set(SYMBOLS)
        _target_symbols = set(SYMBOLS)

    # WS
    ws_task = asyncio.create_task(ws_runner())

    # Polls
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
