import os
import json
import math
import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pytz
from aiohttp import web, ClientSession, ClientWebSocketResponse, WSMsgType
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.client.default import DefaultBotProperties

# DB
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# =========================
# CONFIG
# =========================

VERSION = "v1.0.0-passport-m5"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
TZ = os.getenv("TZ", "Europe/Moscow")
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# список символов по умолчанию (USDT перпы)
SYMBOLS = [
    s.strip()
    for s in os.getenv(
        "SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,TONUSDT",
    ).split(",")
    if s.strip()
]

# Вебхук путь — всегда начинается с /
WEBHOOK_PATH = f"/webhook/{(TELEGRAM_TOKEN.split(':')[0] if ':' in TELEGRAM_TOKEN else 'bot')}"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}" if BASE_URL else ""

# Клавиатура
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Активность"), KeyboardButton(text="⚡ Волатильность")],
        [KeyboardButton(text="📈 Тренд"), KeyboardButton(text="🫧 Bubbles")],
        [KeyboardButton(text="📰 Новости"), KeyboardButton(text="🧮 Калькулятор")],
        [KeyboardButton(text="⭐ Watchlist"), KeyboardButton(text="⚙️ Настройки")],
    ],
    resize_keyboard=True,
)

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("screener")

# =========================
# STATE / DATA MODELS
# =========================

@dataclass
class TickerWS:
    symbol: str
    last: Optional[float] = None
    chg24h_pct: Optional[float] = None   # 24h % change (e.g., -1.42)
    turnover24h: Optional[float] = None  # in quote currency

@dataclass
class Kline:
    # 5m bars
    symbol: str
    ts: int  # open time (ms)
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


class DataHub:
    """Хранит кэш по тикерам и 5m свечам, сохраняет свечи в Postgres."""
    def __init__(self, pool: Optional[AsyncConnectionPool]):
        self.pool = pool
        self.tickers: Dict[str, TickerWS] = {}
        # in-memory: последние N свечей на символ
        self.klines_5m: Dict[str, List[Kline]] = {}
        self.max_cache_bars = 3000  # ~ 10 дней 5m, 288*10

    async def init_db(self):
        if not self.pool:
            return
        async with self.pool.connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS klines_5m (
                    symbol TEXT NOT NULL,
                    ts BIGINT NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY(symbol, ts)
                );
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS klines_5m_symbol_ts_idx ON klines_5m(symbol, ts);")

    async def load_recent_klines(self, symbol: str, limit: int = 1500):
        """Подгрузить последние свечи из БД в память (после рестарта)."""
        if not self.pool:
            return
        async with self.pool.connection() as conn:
            rows = await conn.execute(
                "SELECT symbol, ts, open, high, low, close, volume "
                "FROM klines_5m WHERE symbol=%s ORDER BY ts DESC LIMIT %s;",
                (symbol, limit),
            )
            rows = await rows.fetchall()
        lst = [
            Kline(
                symbol=r["symbol"],
                ts=int(r["ts"]),
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]) if r["volume"] is not None else None,
            )
            for r in rows
        ]
        lst.reverse()
        self.klines_5m[symbol] = lst

    async def upsert_kline(self, k: Kline):
        """Сохранить и обновить кэш."""
        # память
        buf = self.klines_5m.setdefault(k.symbol, [])
        if buf and buf[-1].ts == k.ts:
            buf[-1] = k
        else:
            buf.append(k)
            if len(buf) > self.max_cache_bars:
                del buf[: len(buf) - self.max_cache_bars]

        # база
        if self.pool:
            async with self.pool.connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO klines_5m (symbol, ts, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(symbol, ts) DO UPDATE
                    SET open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        volume=EXCLUDED.volume;
                    """,
                    (k.symbol, k.ts, k.open, k.high, k.low, k.close, k.volume),
                )

    def get_ticker_list(self) -> List[TickerWS]:
        return list(self.tickers.values())

    def set_ticker(self, t: TickerWS):
        self.tickers[t.symbol] = t

    def get_klines(self, symbol: str, n: int = 600) -> List[Kline]:
        buf = self.klines_5m.get(symbol, [])
        return buf[-n:] if n < len(buf) else buf[:]


# =========================
# BYBIT WS CLIENT
# =========================

class BybitWSClient:
    def __init__(self, url: str, symbols: List[str], hub: DataHub):
        self.url = url
        self.symbols = symbols
        self.hub = hub
        self.ws: Optional[ClientWebSocketResponse] = None
        self.session: Optional[ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._connected = asyncio.Event()

    async def start(self):
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session:
            await self.session.close()

    async def _run(self):
        backoff = 1
        while True:
            try:
                if self.session is None or self.session.closed:
                    self.session = ClientSession()
                log.info("Bybit WS connecting: %s", self.url)
                async with self.session.ws_connect(self.url, heartbeat=20) as ws:
                    self.ws = ws
                    await self._subscribe(ws)
                    self._connected.set()
                    backoff = 1
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            await self._handle(json.loads(msg.data))
                        elif msg.type == WSMsgType.ERROR:
                            log.warning("WS error: %s", msg.data)
                            break
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE):
                            break
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("WS loop error: %s", e)
            finally:
                self._connected.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _subscribe(self, ws: ClientWebSocketResponse):
        # v5 topics:
        # - tickers: "tickers.<symbol>"
        # - kline 5m: "kline.5.<symbol>"
        tick_args = [f"tickers.{s}" for s in self.symbols]
        kl_args = [f"kline.5.{s}" for s in self.symbols]
        sub = {"op": "subscribe", "args": tick_args + kl_args}
        await ws.send_json(sub)
        log.info("WS subscribed: %d topics", len(tick_args) + len(kl_args))

    async def wait_ready(self, timeout: float = 10.0) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _handle(self, payload: dict):
        topic = payload.get("topic")
        typ = payload.get("type")
        if not topic:
            return
        # tickers
        if topic.startswith("tickers."):
            data = payload.get("data") or {}
            symbol = data.get("symbol") or topic.split(".", 1)[-1]
            last = data.get("lastPrice")
            if isinstance(last, str):
                try:
                    last = float(last)
                except:
                    last = None
            chg = data.get("price24hPcnt")  # often as string like "-0.0123"
            if isinstance(chg, str):
                try:
                    chg = float(chg) * 100.0
                except:
                    chg = None
            elif isinstance(chg, (int, float)):
                chg = float(chg) * 100.0
            turnover = data.get("turnover24h")
            if isinstance(turnover, str):
                try:
                    turnover = float(turnover)
                except:
                    turnover = None
            self.hub.set_ticker(TickerWS(symbol=symbol, last=last, chg24h_pct=chg, turnover24h=turnover))
            return

        # kline
        if topic.startswith("kline.5."):
            # v5 returns {"data":[{ "start":ms, "open":"", "high":"", "low":"", "close":"", "volume":"", ...}], "topic": "kline.5.SYMBOL"}
            arr = payload.get("data") or []
            for bar in arr:
                try:
                    symbol = topic.split(".")[-1]
                    ts = int(bar.get("start"))
                    open_ = float(bar.get("open"))
                    high = float(bar.get("high"))
                    low = float(bar.get("low"))
                    close = float(bar.get("close"))
                    vol = bar.get("volume")
                    vol = float(vol) if vol is not None else None
                    await self.hub.upsert_kline(Kline(symbol=symbol, ts=ts, open=open_, high=high, low=low, close=close, volume=vol))
                except Exception as e:
                    log.debug("kline parse err: %s / %s", e, bar)
            return


# =========================
# PASSPORT METRICS (M5)
# =========================

def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = []
    ema = values[0]
    out.append(ema)
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
        out.append(ema)
    return out

def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    m = n // 2
    if n % 2:
        return ys[m]
    return 0.5 * (ys[m - 1] + ys[m])

def calc_atr_pct_approx(bars: List[Kline]) -> float:
    # приблизительно ATR% через медианный (high-low)/close
    if not bars:
        return 0.0
    vals = []
    for b in bars[-600:]:
        if b.close > 0:
            vals.append((b.high - b.low) / b.close * 100.0)
    return _median(vals) if vals else 0.0

def calc_breakout_follow_through(bars: List[Kline], ema_period: int = 20, look_ahead: int = 6) -> float:
    # пробой ema20 вверх => медианный импульс в ближайшие 6 свечей
    if len(bars) < ema_period + look_ahead + 2:
        return 0.0
    closes = [b.close for b in bars]
    ema20 = _ema(closes, ema_period)
    res = []
    for i in range(1, len(bars) - look_ahead):
        if closes[i - 1] < ema20[i - 1] and closes[i] >= ema20[i] and (ema20[i] - ema20[i - 1]) >= 0:
            c0 = closes[i]
            mx = max(b.high for b in bars[i + 1 : i + 1 + look_ahead])
            if c0 > 0:
                res.append((mx - c0) / c0 * 100.0)
    return _median(res) if res else 0.0

def calc_zone_bounce_success(bars: List[Kline], ema_period: int = 50, horizon: int = 12, target_mult: float = 0.5) -> Tuple[int, int, float]:
    # зона = ema50 -/+ (0.75 * ATR_abs). Сигнал: low <= нижняя граница и close > нижней => "отбой".
    # успех: за 12 свечей high достигает close + 0.5*ATR_abs
    if len(bars) < ema_period + horizon + 5:
        return 0, 0, 0.0
    closes = [b.close for b in bars]
    ema50 = _ema(closes, ema_period)
    atr_pct = calc_atr_pct_approx(bars)
    succ = 0
    tot = 0
    for i in range(ema_period, len(bars) - horizon):
        c = bars[i].close
        atr_abs = atr_pct / 100.0 * c
        lower = ema50[i] - 0.75 * atr_abs
        # "касание зоны и закрытие выше"
        if bars[i].low <= lower and bars[i].close > lower:
            tot += 1
            target = c + target_mult * atr_abs
            hh = max(b.high for b in bars[i + 1 : i + 1 + horizon])
            if hh >= target:
                succ += 1
    rate = (succ / tot * 100.0) if tot else 0.0
    return succ, tot, rate

def calc_staircase_down(bars: List[Kline]) -> Tuple[float, float]:
    # средняя длина "лестницы" (серий из >=3 подряд красных свечей с понижающимися high/low)
    # и среднее падение (%) за серию
    if len(bars) < 50:
        return 0.0, 0.0
    runs = []
    i = 1
    while i < len(bars):
        j = i
        # начало серии: красная и high/low ниже предыдущей
        def is_down(k):
            return bars[k].close < bars[k - 1].close and bars[k].high <= bars[k - 1].high and bars[k].low <= bars[k - 1].low
        if is_down(j):
            while j < len(bars) and is_down(j):
                j += 1
            length = j - i + 1  # включительно i-1 -> j-1? Упростим:
            start = bars[i - 1].close
            end = bars[j - 1].close
            if length >= 3 and start > 0:
                drop = (start - end) / start * 100.0
                runs.append((length, drop))
            i = j
        else:
            i += 1
    if not runs:
        return 0.0, 0.0
    avg_len = sum(r[0] for r in runs) / len(runs)
    avg_drop = sum(r[1] for r in runs) / len(runs)
    return avg_len, avg_drop

def calc_rangeiness(bars: List[Kline], window: int = 288) -> Tuple[float, int]:
    # Диапазон за сутки (288 свечей): (HH-LL)/close% и сколько свечей заняло
    if len(bars) < 10:
        return 0.0, 0
    w = min(window, len(bars))
    tail = bars[-w:]
    hh = max(b.high for b in tail)
    ll = min(b.low for b in tail)
    c = tail[-1].close
    rng_pct = ((hh - ll) / c * 100.0) if c > 0 else 0.0
    return rng_pct, w


def format_passport(symbol: str, bars: List[Kline]) -> str:
    if len(bars) < 50:
        return f"Паспорт {symbol} (M5)\nНедостаточно данных. Оставь бота работать, наберём историю."

    atr_pct = calc_atr_pct_approx(bars)
    bo_ft = calc_breakout_follow_through(bars)
    succ, tot, rate = calc_zone_bounce_success(bars)
    avg_len, avg_drop = calc_staircase_down(bars)
    rng, w = calc_rangeiness(bars)

    lines = [
        f"📄 <b>Паспорт {symbol} (M5)</b>",
        f"• ATR% (мед.): {atr_pct:.2f}",
        f"• Импульс после пробоя EMA20: +{bo_ft:.2f}%",
        f"• Отбой от зоны (EMA50±0.75*ATR): {succ}/{tot} успешных ({rate:.1f}%)",
        f"• «Лестница вниз»: ср. длина {avg_len:.1f} св., ср. падение {avg_drop:.2f}%",
        f"• Диапазон за {w} свечей: {rng:.2f}%",
    ]
    return "\n".join(lines)


# =========================
# TELEGRAM
# =========================

bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Инлайн callback для паспорта
PASSPORT_CB_PREFIX = "passport:"

def make_list_inline(symbols: List[str], prefix: str = PASSPORT_CB_PREFIX) -> InlineKeyboardMarkup:
    # по 2 на ряд
    rows = []
    row = []
    for s in symbols:
        row.append(InlineKeyboardButton(text=s, callback_data=f"{prefix}{s}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(CommandStart())
async def cmd_start(m: Message):
    await m.answer("🧭 <b>Market mood</b>\nBTC.D: 54.1% (+0.3) | Funding avg: +0.012% | F&G: 34 (-3)")
    await m.answer(f"Добро пожаловать в <b>Innertrade Screener {VERSION}</b> (Bybit WS).", reply_markup=MAIN_KB)


@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    await m.answer("Клавиатура восстановлена.", reply_markup=MAIN_KB)


@dp.message(Command("status"))
async def cmd_status(m: Message):
    await m.answer(
        "Status\n"
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} ({TZ})\n"
        "Mode: active | Quiet: False\n"
        "Source: Bybit (public WS)\n"
        f"Version: {VERSION}\n"
        f"Bybit WS: {BYBIT_WS}"
    )


@dp.message(Command("health"))
async def cmd_health(m: Message):
    await m.answer("✅ Bot online (webhook).")


@dp.message(Command("diag"))
async def cmd_diag(m: Message):
    ws_ok = WS_CLIENT is not None and WS_READY_EVENT.is_set()
    cnt_syms = len(HUB.tickers)
    await m.answer(
        "diag\n"
        f"ws_ok={ws_ok} | symbols_cached={cnt_syms}\n"
        f"symbols: {', '.join(sorted(HUB.tickers.keys())[:10])}"
    )


@dp.message(F.text == "📊 Активность")
async def on_activity(m: Message):
    # сортировка по turnover24h
    lst = [t for t in HUB.get_ticker_list() if t.turnover24h is not None]
    if not lst:
        await m.answer("🔥 Активность\nПодбираю данные (WS)…")
        return
    lst.sort(key=lambda x: x.turnover24h or 0.0, reverse=True)
    top = lst[:10]
    lines = ["🔥 <b>Активность (Bybit WS)</b>"]
    for i, t in enumerate(top, 1):
        chg = f"{t.chg24h_pct:.2f}" if t.chg24h_pct is not None else "n/a"
        to = int(t.turnover24h) if t.turnover24h is not None else 0
        lines.append(f"{i}) {t.symbol}  24h% {chg}  | turnover24h ~ {to:,}".replace(",", " "))
    text = "\n".join(lines)
    kb = make_list_inline([t.symbol for t in top])
    await m.answer(text)
    await m.answer("Нажми на тикер, чтобы открыть паспорт (M5).", reply_markup=kb)


@dp.message(F.text == "⚡ Волатильность")
async def on_vol(m: Message):
    lst = [t for t in HUB.get_ticker_list() if t.chg24h_pct is not None]
    if not lst:
        await m.answer("⚡ Волатильность\nПодбираю данные (WS)…")
        return
    lst.sort(key=lambda x: abs(x.chg24h_pct or 0.0), reverse=True)
    top = lst[:10]
    lines = ["⚡ <b>Волатильность (24h %, Bybit WS)</b>"]
    for i, t in enumerate(top, 1):
        last = f"{t.last:.4f}" if t.last else "n/a"
        lines.append(f"{i}) {t.symbol}  24h% {t.chg24h_pct:.2f}  | last {last}")
    text = "\n".join(lines)
    kb = make_list_inline([t.symbol for t in top])
    await m.answer(text)
    await m.answer("Нажми на тикер, чтобы открыть паспорт (M5).", reply_markup=kb)


@dp.message(F.text == "📈 Тренд")
async def on_trend(m: Message):
    lst = [t for t in HUB.get_ticker_list() if t.chg24h_pct is not None]
    if not lst:
        await m.answer("📈 Тренд\nПодбираю данные…")
        return
    lst.sort(key=lambda x: (x.chg24h_pct or 0.0), reverse=True)
    top = lst[:10]
    lines = ["📈 <b>Тренд (упрощённо по 24h%, Bybit WS)</b>"]
    for i, t in enumerate(top, 1):
        last = f"{t.last:.4f}" if t.last else "n/a"
        lines.append(f"{i}) {t.symbol}  ≈  24h% {t.chg24h_pct:.2f}  | last {last}")
    text = "\n".join(lines)
    kb = make_list_inline([t.symbol for t in top])
    await m.answer(text)
    await m.answer("Нажми на тикер, чтобы открыть паспорт (M5).", reply_markup=kb)


@dp.message(F.text == "🫧 Bubbles")
async def on_bubbles(m: Message):
    await m.answer("WS Bubbles (24h %, size~turnover24h)")


@dp.message(F.text == "📰 Новости")
async def on_news(m: Message):
    await m.answer("📰 Макро (последний час)\n• demo headline")


@dp.message(F.text == "🧮 Калькулятор")
async def on_calc(m: Message):
    await m.answer("Шаблон риск-менеджмента (встроенный Excel добавим позже).")


@dp.message(F.text == "⭐ Watchlist")
async def on_watchlist(m: Message):
    await m.answer("Watchlist пуст. Добавь /add SYMBOL (например, /add SOLUSDT)")


@dp.message(F.text == "⚙️ Настройки")
async def on_settings(m: Message):
    await m.answer(
        "⚙️ Настройки\n"
        "Биржа: Bybit (USDT perp, WS)\n"
        "Режим: active | Quiet: False\n"
        "Watchlist: —\n\n"
        "Команды:\n"
        "• /add SYMBOL  — добавить (например, /add SOLUSDT)\n"
        "• /rm SYMBOL   — удалить\n"
        "• /watchlist   — показать лист\n"
        "• /passive     — автосводки/сигналы ON\n"
        "• /active      — автосводки/сигналы OFF\n"
        "• /menu        — восстановить клавиатуру"
    )


@dp.callback_query(F.data.startswith(PASSPORT_CB_PREFIX))
async def on_passport_click(cq: CallbackQuery):
    symbol = cq.data.split(":", 1)[-1]
    bars = HUB.get_klines(symbol, n=1200)  # ~ 4 суток
    text = format_passport(symbol, bars)
    await cq.message.answer(text)
    await cq.answer()


# =========================
# BOOTSTRAP
# =========================

HUB: DataHub
WS_CLIENT: Optional[BybitWSClient] = None
WS_READY_EVENT = asyncio.Event()
POOL: Optional[AsyncConnectionPool] = None


async def ws_runner():
    # ожидание подключения
    ready = await WS_CLIENT.wait_ready(timeout=15.0)
    if ready:
        WS_READY_EVENT.set()
    else:
        WS_READY_EVENT.clear()


async def on_startup_app(app: web.Application):
    # DB pool
    global POOL
    if DATABASE_URL:
        POOL = AsyncConnectionPool(conninfo=DATABASE_URL, max_size=5, kwargs={"autocommit": True})
        await POOL.open()
        log.info("DB pool opened")
        await HUB.init_db()
        # подгрузим историю из БД
        for s in SYMBOLS:
            await HUB.load_recent_klines(s, limit=1500)
            await asyncio.sleep(0)  # уступить цикл
    else:
        log.warning("DATABASE_URL not set — паспорт будет работать только после накопления истории в памяти")

    # WS
    await WS_CLIENT.start()
    asyncio.create_task(ws_runner())


async def on_cleanup_app(app: web.Application):
    if WS_CLIENT:
        await WS_CLIENT.stop()
    if POOL:
        await POOL.close()


def build_app() -> web.Application:
    app = web.Application()
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app


async def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    if not BASE_URL:
        raise RuntimeError("BASE_URL is required (public https URL of this service)")

    # init globals
    global HUB, WS_CLIENT
    HUB = DataHub(pool=None)  # временно, позже подключим pool после on_startup
    if DATABASE_URL:
        # Pool создаётся в on_startup, но хаб уже нужен:
        HUB = DataHub(pool=None)  # заменим pool после открытия
    WS_CLIENT = BybitWSClient(BYBIT_WS, SYMBOLS, hub=HUB)

    # выставим вебхук
    await bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
    log.info("Webhook set to %s", WEBHOOK_URL)

    # aiohttp app
    app = build_app()

    # если в on_startup мы откроем pool — надо присвоить его в HUB
    async def late_bind_pool(app: web.Application):
        # ждём пока on_startup откроет POOL
        await asyncio.sleep(0.1)
        if DATABASE_URL and POOL:
            HUB.pool = POOL

    app.on_startup.append(late_bind_pool)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("App started on 0.0.0.0:%d", PORT)

    # табличное "вечное ожидание"
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down...")
