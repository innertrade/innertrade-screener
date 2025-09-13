# main.py — v1.8.4-schema-fix (psycopg3 %s + trades_count/ts)

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import (
    KeyboardButton,
    ReplyKeyboardMarkup,
    Message,
)
from aiogram.filters import Command

import psycopg
from psycopg_pool import AsyncConnectionPool

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("screener")

# -------------------- ENV --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()  # https://your-app.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))
TZ = os.getenv("TZ", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear").strip()
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT").split(",")

# Optional REST (могут отдавать 403 для некоторых эндпоинтов без ключей)
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bytick.com").rstrip("/")
BYBIT_REST_FALLBACK = os.getenv("BYBIT_REST_FALLBACK", "https://api.bybit.com").rstrip("/")

ENABLE_OI_POLL = os.getenv("ENABLE_OI_POLL", "true").lower() in ("1", "true", "yes")
ENABLE_PRICE_POLL = os.getenv("ENABLE_PRICE_POLL", "false").lower() in ("1", "true", "yes")
OI_POLL_SECONDS = int(os.getenv("OI_POLL_SECONDS", "90"))       # период опроса OI
PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "1800"))  # период опроса свечей 1h

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")
if not WEBHOOK_BASE or not WEBHOOK_BASE.startswith("http"):
    raise RuntimeError("WEBHOOK_BASE is required (https://...)")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required (any non-empty string)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required (postgres)")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"

# -------------------- Globals --------------------
bot: Bot | None = None
dp: Dispatcher | None = None
app: web.Application | None = None

db_pool: AsyncConnectionPool | None = None
ws_session: aiohttp.ClientSession | None = None
ws_task: asyncio.Task | None = None
oi_task: asyncio.Task | None = None
price_task: asyncio.Task | None = None

ws_cache: dict[str, dict] = {}  # фолбэк, пока БД не прогрелась
_last_ws_msg_iso: str | None = None

# -------------------- SQL --------------------
SQL_CREATE_WS = """
CREATE TABLE IF NOT EXISTS ws_ticker (
  symbol           text PRIMARY KEY,
  last             double precision,
  price24h_pcnt    double precision,
  turnover24h      double precision,
  updated_at       timestamptz NOT NULL DEFAULT now()
);
"""

# важное: psycopg3 placeholder style -> %s
SQL_UPSERT = """
INSERT INTO ws_ticker (symbol, last, price24h_pcnt, turnover24h, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (symbol) DO UPDATE
SET last = EXCLUDED.last,
    price24h_pcnt = EXCLUDED.price24h_pcnt,
    turnover24h = EXCLUDED.turnover24h,
    updated_at = NOW();
"""

SQL_SELECT_SORTED_FMT = """
SELECT symbol,
       COALESCE(price24h_pcnt, 0) AS p24,
       COALESCE(turnover24h, 0)   AS tov,
       COALESCE(last, 0)          AS last
FROM ws_ticker
ORDER BY {order_by} {direction}
LIMIT %s;
"""

SQL_COUNT = "SELECT COUNT(*) FROM ws_ticker;"

# Диагностики и композит под фактическую схему:
# trades_1m: ts, symbol, trades_count, qty_sum
SQL_TRADES_LATEST = """
SELECT ts, trades_count, qty_sum
FROM trades_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_OB_LATEST = """
SELECT ts, best_bid, best_ask, bid_qty, ask_qty, spread_bps, depth_usd
FROM ob_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_OI_LATEST = """
SELECT ts, oi_usd
FROM oi_1m
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

SQL_PRICE_LATEST = """
SELECT ts, close
FROM price_1h
WHERE symbol = %s
ORDER BY ts DESC
LIMIT %s;
"""

# агрегаты за 24ч для activity+ (используем trades_count, не "count")
SQL_TRADES_24H_SUM = """
SELECT
  COALESCE(SUM(trades_count), 0) AS tcnt,
  COALESCE(SUM(qty_sum), 0)      AS qsum
FROM trades_1m
WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours';
"""

SQL_TRADES_24H_SUM_ALL = """
SELECT symbol,
       COALESCE(SUM(trades_count), 0) AS tcnt,
       COALESCE(SUM(qty_sum), 0)      AS qsum
FROM trades_1m
WHERE ts >= NOW() - INTERVAL '24 hours'
GROUP BY symbol;
"""

SQL_OB_24H_STATS = """
SELECT
  AVG(depth_usd)   AS depth_avg,
  AVG(spread_bps)  AS spread_avg
FROM ob_1m
WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours';
"""

SQL_OI_DELTA_24H = """
WITH w AS (
  SELECT oi_usd
  FROM oi_1m
  WHERE symbol = %s AND ts >= NOW() - INTERVAL '24 hours'
  ORDER BY ts
)
SELECT
  CASE
    WHEN COUNT(*) >= 2 THEN
      (MAX(oi_usd) - MIN(oi_usd)) / NULLIF(MIN(oi_usd), 0) * 100.0
    ELSE NULL
  END AS oi_delta_pct
FROM w;
"""

# -------------------- DB helpers --------------------
async def init_db():
    global db_pool
    db_pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=4)
    await db_pool.open()
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_CREATE_WS)
            await conn.commit()
    log.info("DB ready")

async def upsert_ticker(symbol: str, last: float | None, p24: float | None, turnover: float | None):
    if db_pool is None:
        return
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPSERT, (symbol, last, p24, turnover))
            await conn.commit()

async def select_sorted(order_by: str, limit: int = 10, desc: bool = True):
    if db_pool is None:
        return []
    # белый список алиасов из SELECT
    ob = "last"
    if order_by == "turnover":
        ob = "tov"
    elif order_by in ("p24", "price24h_pcnt"):
        ob = "p24"
    direction = "DESC" if desc else "ASC"
    sql = SQL_SELECT_SORTED_FMT.format(order_by=ob, direction=direction)
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
            return rows

async def count_rows() -> int:
    if db_pool is None:
        return 0
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_COUNT)
            n = (await cur.fetchone())[0]
            return int(n or 0)

# -------------------- Bybit WS ingest (tickers only) --------------------
async def ws_consumer():
    global ws_session, _last_ws_msg_iso
    ws_session = aiohttp.ClientSession()
    log.info(f"Bybit WS connecting: {BYBIT_WS_URL}")
    try:
        async with ws_session.ws_connect(BYBIT_WS_URL, heartbeat=30) as ws:
            args = [f"tickers.{s}" for s in SYMBOLS]
            await ws.send_json({"op": "subscribe", "args": args})
            log.info(f"WS subscribed: {len(args)} topics")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json(loads=json.loads)
                    topic = data.get("topic") or ""
                    if topic.startswith("tickers."):
                        payload = data.get("data")
                        items = [payload] if isinstance(payload, dict) else (payload or [])
                        for it in items:
                            symbol = (it.get("symbol") or "").upper()
                            try:
                                last = float(it.get("lastPrice") or 0)
                            except Exception:
                                last = 0.0
                            try:
                                p24 = float(it.get("price24hPcnt") or 0) * 100.0
                            except Exception:
                                p24 = 0.0
                            try:
                                turnover = float(it.get("turnover24h") or 0)
                            except Exception:
                                turnover = 0.0

                            ws_cache[symbol] = {
                                "last": last,
                                "p24": p24,
                                "tov": turnover,
                                "ts": datetime.now(timezone.utc).isoformat(),
                            }
                            await upsert_ticker(symbol, last, p24, turnover)
                        _last_ws_msg_iso = datetime.now(timezone.utc).isoformat()
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error("WS error: %s", msg.data)
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                    log.warning("WS closed by server")
                    break
    except Exception as e:
        log.exception("WS consumer failed: %s", e)
    finally:
        if ws_session:
            await ws_session.close()
        log.info("WS consumer finished")

# -------------------- (Опционально) простые REST pollers --------------------
async def rest_get_json(session: aiohttp.ClientSession, path: str, params: dict) -> tuple[int, dict | None]:
    """Опробуем base и fallback. Возвращаем (status, json_or_none)"""
    url1 = f"{BYBIT_REST_BASE}{path}"
    url2 = f"{BYBIT_REST_FALLBACK}{path}"
    for url in (url1, url2):
        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status == 200:
                    return 200, await r.json()
                else:
                    log.warning("REST %s %s -> HTTP %s", "base" if url==url1 else "fallback", url, r.status)
        except Exception:
            log.warning("REST %s request failed", url, exc_info=False)
    return 403, None

async def oi_poll_loop():
    if not ENABLE_OI_POLL:
        return
    log.info("OI polling enabled: every %ss, interval=5min", OI_POLL_SECONDS)
    async with aiohttp.ClientSession() as session:
        while True:
            t0 = datetime.now()
            for sym in SYMBOLS:
                status, js = await rest_get_json(session, "/v5/market/open-interest", {
                    "category": "linear",
                    "symbol": sym,
                    "intervalTime": "5min",
                    "limit": 1,
                })
                if status != 200 or not js or js.get("retCode") != 0:
                    log.warning("OI %s http %s", sym, status)
                    continue
                try:
                    rows = js["result"]["list"]
                    if not rows:
                        continue
                    row = rows[0]
                    oi_usd = float(row.get("openInterestUsd", 0) or 0)
                    ts_str = row.get("timestamp") or row.get("ts")
                    ts = datetime.fromtimestamp(int(ts_str)//1000, tz=timezone.utc) if ts_str else datetime.now(timezone.utc)
                except Exception:
                    continue
                # вставим в oi_1m, если нужно — сделайте upsert
                # здесь простая вставка через ON CONFLICT DO NOTHING по (symbol, ts)
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                        CREATE TABLE IF NOT EXISTS oi_1m (
                          ts timestamptz NOT NULL,
                          symbol text NOT NULL,
                          oi_usd double precision,
                          PRIMARY KEY (symbol, ts)
                        );
                        """)
                        await cur.execute("""
                        INSERT INTO oi_1m (ts, symbol, oi_usd)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, ts) DO UPDATE
                        SET oi_usd = EXCLUDED.oi_usd;
                        """, (ts, sym, oi_usd))
                        await conn.commit()
            log.info("OI poll cycle done in %.2fs", (datetime.now() - t0).total_seconds())
            await asyncio.sleep(OI_POLL_SECONDS)

async def price_poll_loop():
    if not ENABLE_PRICE_POLL:
        return
    log.info("Price polling enabled: every %ss (1h candles ~192h back)", PRICE_POLL_SECONDS)
    async with aiohttp.ClientSession() as session:
        while True:
            for sym in SYMBOLS:
                status, js = await rest_get_json(session, "/v5/market/kline", {
                    "category": "linear",
                    "symbol": sym,
                    "interval": "60",
                    "limit": 192,
                })
                if status != 200 or not js or js.get("retCode") != 0:
                    log.warning("Kline %s fetch failed (both)", sym)
                    continue
                try:
                    kl = js["result"]["list"]  # [[start,open,high,low,close,vol,turnover], ...]
                    if not kl:
                        continue
                    rows = []
                    for item in kl:
                        ts = datetime.fromtimestamp(int(item[0])//1000, tz=timezone.utc)
                        close = float(item[4])
                        rows.append((ts, sym, close))
                except Exception:
                    continue
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                        CREATE TABLE IF NOT EXISTS price_1h (
                          ts timestamptz NOT NULL,
                          symbol text NOT NULL,
                          close double precision,
                          PRIMARY KEY (symbol, ts)
                        );
                        """)
                        await cur.executemany("""
                        INSERT INTO price_1h (ts, symbol, close)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, ts) DO UPDATE
                        SET close = EXCLUDED.close;
                        """, rows)
                        await conn.commit()
            log.info("Price poll cycle done")
            await asyncio.sleep(PRICE_POLL_SECONDS)

# -------------------- Bot UI --------------------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="🔍 Активность+")]
        ],
        resize_keyboard=True
    )

def fmt_activity(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | turnover24h ~ {int(tov):,}".replace(",", " "))
    return "🔥 Активность (Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_volatility(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  24h% {p24:.2f}  | last {last}")
    return "⚡ Волатильность (24h %, Bybit WS + DB)\n" + "\n".join(lines[:10])

def fmt_trend(rows: list[tuple]) -> str:
    if not rows:
        return "Нет данных."
    lines = []
    for i, r in enumerate(rows, 1):
        sym, p24, tov, last = r
        lines.append(f"{i}) {sym}  ≈  24h% {p24:.2f}  | last {last}")
    return "📈 Тренд (упрощённо по 24h%, Bybit WS + DB)\n" + "\n".join(lines[:10])

# -------------------- Handlers --------------------
async def cmd_start(message: Message):
    await message.answer(
        "🧭 Market mood\n"
        "Добро пожаловать в Innertrade Screener v1.8.4-schema-fix "
        "(WS tickers + trades/orderbook/OI/price diagnostics).",
        reply_markup=kb_main()
    )

async def cmd_status(message: Message):
    n = await count_rows()
    await message.answer(
        "Status\n"
        f"Time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S (%Z)')}\n"
        "Source: Bybit (public WS + REST OI/Prices)\n"
        "Version: v1.8.4-schema-fix\n"
        f"Bybit WS: {BYBIT_WS_URL}\n"
        f"WS connected: True\n"
        f"WS last msg: {_last_ws_msg_iso or 'n/a'}\n"
        f"DB rows (ws_ticker): {n}\n"
        f"OI poll: {'enabled' if ENABLE_OI_POLL else 'disabled'} "
        f"(5min, every {OI_POLL_SECONDS}s)\n"
        f"Price poll: {'enabled' if ENABLE_PRICE_POLL else 'disabled'} "
        f"(every {PRICE_POLL_SECONDS}s, ~192h back)\n"
    )

async def cmd_diag(message: Message):
    n = await count_rows()
    sample = list(ws_cache.items())[:3]
    sample_txt = "\n".join(
        f"{k}: p24={v['p24']:.2f} last={v['last']} tov~{int(v['tov']):,}".replace(",", " ")
        for k, v in sample
    ) or "—"
    await message.answer(
        "diag\n"
        f"rows={n}\n"
        f"cache_sample:\n{sample_txt}"
    )

async def cmd_activity(message: Message):
    rows = await select_sorted("turnover", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: z[2], reverse=True
        )[:10]
    await message.answer(fmt_activity(rows))

async def cmd_volatility(message: Message):
    rows = await select_sorted("p24", 50, desc=True)
    rows = sorted(rows, key=lambda r: abs(r[1]), reverse=True)[:10]
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: abs(z[1]), reverse=True
        )[:10]
    await message.answer(fmt_volatility(rows))

async def cmd_trend(message: Message):
    rows = await select_sorted("p24", 10, desc=True)
    if not rows and ws_cache:
        rows = sorted(
            [(s, v["p24"], v["tov"], v["last"]) for s, v in ws_cache.items()],
            key=lambda z: z[1], reverse=True
        )[:10]
    await message.answer(fmt_trend(rows))

def _fmt_money(x: float | None) -> str:
    try:
        return f"{int(x):,}".replace(",", " ")
    except Exception:
        return "0"

async def cmd_activity2(message: Message):
    # Композит по 24ч: оборот из ws_ticker, сумма trades_count/qty_sum из trades_1m,
    # ликвидность/спред из ob_1m, OIΔ из oi_1m. Итоговое ранжирование — простая нормировка.
    out = []
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            # 1) берем топ по обороту (ws_ticker)
            await cur.execute("""
                SELECT symbol,
                       COALESCE(turnover24h,0) AS tov,
                       COALESCE(price24h_pcnt,0) AS p24
                FROM ws_ticker
                ORDER BY tov DESC
                LIMIT 50;
            """)
            tick = await cur.fetchall()
            sym_list = [r[0] for r in tick] or SYMBOLS

            # предварительно притащим агрегаты по всем символам за 24ч (trades_1m)
            await cur.execute(SQL_TRADES_24H_SUM_ALL)
            tmap: dict[str, tuple[float, float]] = {r[0]: (float(r[1] or 0), float(r[2] or 0)) for r in await cur.fetchall()}

            for sym, tov, p24 in tick:
                # trades
                tcnt, qsum = tmap.get(sym, (0.0, 0.0))

                # ob stats
                await cur.execute(SQL_OB_24H_STATS, (sym,))
                obrow = await cur.fetchone()
                depth_avg = float((obrow[0] or 0)) if obrow else 0.0
                spread_avg = float((obrow[1] or 0)) if obrow else 0.0

                # oi delta
                await cur.execute(SQL_OI_DELTA_24H, (sym,))
                oi_delta = (await cur.fetchone())[0] if cur.rowcount != 0 else None
                oi_delta = 0.0 if oi_delta is None else float(oi_delta)

                # простой скор (веса можно потом калибровать)
                score = 0.0
                # нормировки грубые: делим на «типичные» масштабы
                score += (tov / 1e9) * 0.6
                score += (tcnt / 1e5) * 0.3
                score += (depth_avg / 5e5) * 0.2
                score -= (spread_avg / 1.0) * 0.2
                score += (oi_delta / 10.0) * 0.2
                # добавим слабый бонус за п24>0
                score += (max(p24, 0) / 10.0) * 0.1

                out.append((sym, score, tov, tcnt, depth_avg, spread_avg, oi_delta))

    out.sort(key=lambda z: z[1], reverse=True)
    lines = ["🔍 Активность+ (композит за ~24ч)\n"]
    for i, (sym, score, tov, tcnt, depth, spr, oi_d) in enumerate(out[:10], 1):
        lines.append(
            f"{i}) {sym}  score {score:+.2f}  | turnover ~ {_fmt_money(tov)} | "
            f"trades ~ {_fmt_money(tcnt)} | depth≈${_fmt_money(depth)} | "
            f"spread≈{spr:.1f}bps | OIΔ {oi_d:+.1f}%"
        )
    await message.answer("\n".join(lines))

async def cmd_diag_trades(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    limit = int(parts[2]) if len(parts) > 2 else 10
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_TRADES_LATEST, (sym, limit))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"trades_1m {sym}: нет данных")
        return
    lines = [f"trades_1m {sym} (latest {len(rows)})"]
    for ts, tcnt, qsum in rows:
        lines.append(f"{ts.isoformat()}  count={tcnt}  qty_sum={qsum}")
    await message.answer("\n".join(lines))

async def cmd_diag_ob(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    limit = int(parts[2]) if len(parts) > 2 else 5
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_OB_LATEST, (sym, limit))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"ob_1m {sym}: нет данных")
        return
    lines = [f"ob_1m {sym} (latest {len(rows)})"]
    for ts, bb, ba, bq, aq, sp, depth in rows:
        lines.append(
            f"{ts.isoformat()}  bid={bb} ask={ba}  bq={bq} aq={aq}  "
            f"spread={sp:.2f}bps  depth≈{_fmt_money(depth)}"
        )
    await message.answer("\n".join(lines))

async def cmd_diag_oi(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    limit = int(parts[2]) if len(parts) > 2 else 10
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_OI_LATEST, (sym, limit))
            rows = await cur.fetchall()
    if not rows:
        await message.answer(f"{sym}: нет строк в oi_1m (ожидайте цикл опроса).")
        return
    lines = [f"oi_1m {sym} (latest {len(rows)})"]
    for ts, oi_usd in rows:
        lines.append(f"{ts.isoformat()}  oi≈${_fmt_money(oi_usd)}")
    await message.answer("\n".join(lines))

async def cmd_now(message: Message):
    parts = (message.text or "").split()
    sym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
            SELECT symbol, last, price24h_pcnt, turnover24h, updated_at
            FROM ws_ticker WHERE symbol=%s;
            """, (sym,))
            row = await cur.fetchone()
    if not row:
        await message.answer(f"{sym}: нет в таблице ws_ticker")
        return
    _, last, p24, tov, updated = row
    await message.answer(
        f"{sym}\nlast: {last}\n24h%: {p24}\nturnover24h: {tov}\nupdated_at: {updated}"
    )

async def on_text(message: Message):
    t = (message.text or "").strip()
    if t == "📊 Активность":
        await cmd_activity(message)
    elif t == "⚡ Волатильность":
        await cmd_volatility(message)
    elif t == "📈 Тренд":
        await cmd_trend(message)
    elif t == "🫧 Bubbles":
        await message.answer("WS Bubbles (24h %, size~turnover24h)")
    elif t == "🔍 Активность+":
        await cmd_activity2(message)
    else:
        await message.answer(
            "Команды: /start /status /activity /volatility /trend /activity2 "
            "/diag_trades BTCUSDT 5 /diag_ob BTCUSDT 5 /diag_oi BTCUSDT 10 /now BTCUSDT"
        )

# -------------------- Webhook server --------------------
async def handle_health(request: web.Request):
    return web.Response(text="ok")

async def handle_root(request: web.Request):
    return web.Response(text="Innertrade screener is alive")

async def handle_webhook(request: web.Request):
    assert bot and dp
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="bad json")
    from aiogram.types import Update
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return web.Response(text="ok")

def build_app() -> web.Application:
    global bot, dp, app
    tg_session = AiohttpSession()
    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"), session=tg_session)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_activity, Command("activity"))
    dp.message.register(cmd_volatility, Command("volatility"))
    dp.message.register(cmd_trend, Command("trend"))
    dp.message.register(cmd_activity2, Command("activity2"))
    dp.message.register(cmd_diag_trades, Command("diag_trades"))
    dp.message.register(cmd_diag_ob, Command("diag_ob"))
    dp.message.register(cmd_diag_oi, Command("diag_oi"))
    dp.message.register(cmd_now, Command("now"))
    dp.message.register(on_text, F.text)

    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app

async def on_startup():
    global ws_task, oi_task, price_task
    await init_db()
    ws_task = asyncio.create_task(ws_consumer())
    if ENABLE_OI_POLL:
        oi_task = asyncio.create_task(oi_poll_loop())
    if ENABLE_PRICE_POLL:
        price_task = asyncio.create_task(price_poll_loop())
    assert bot
    url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
    # на старте вебхук может не установиться с первого раза (таймаут) — это ок, бот сам попробует еще раз при рестарте
    try:
        await bot.set_webhook(url, allowed_updates=["message", "callback_query"])
        log.info("Webhook set to %s", url)
    except Exception as e:
        log.warning("set_webhook attempt failed: %s", e)

async def on_shutdown():
    global ws_task, ws_session, db_pool, bot, oi_task, price_task
    if bot:
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass
        await bot.session.close()
    for task in (ws_task, oi_task, price_task):
        if task:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
    if ws_session:
        await ws_session.close()
    if db_pool:
        await db_pool.close()

def run():
    application = build_app()
    application.on_startup.append(lambda app: on_startup())
    application.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(application, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    import contextlib
    try:
        run()
    except KeyboardInterrupt:
        pass
