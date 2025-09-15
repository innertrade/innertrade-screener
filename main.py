#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Бот-скринер (main.py) — монолитный файл
======================================
Назначение
----------
Единый исполняемый скрипт для сбора цен по универсу крипто-символов
с приоритетом Bybit/Bytick (V5) и фолбэком на Binance. Реализует:
- Полную CLI/ENV-конфигурацию (без переменной Symbols; используем Universe*);
- Домены: ByBitRestBase = "bytick.com", ByBitRestFallback = "bybit.com";
- Переменные: PriceFallbackBinance, UniverseMax, UniverseMode, UniverseRefreshMin;
- Экспоненциальные ретраи, таймауты, backoff с джиттером;
- Ротацию логов и CSV-журнал;
- Сохранение в SQLite;
- Кэширование с TTL;
- Ограничение скорости (token bucket);
- Горячую подгрузку конфигурации (JSON) без рестарта;
- Грейсфул-шатдаун по Ctrl+C/SIGTERM;
- Параллельные запросы (ThreadPoolExecutor);
- Алерты по порогам (конфигурируемые), уведомления по Telegram/Webhook;
- Мини-вебсервер здоровья/метрик (/health, /metrics, /config);
- Нормализация тикеров, единый пайплайн;
- Возможность одноразового прогона (--once) или демона (--loop).

ВНИМАНИЕ: Скрипт не требует внешних зависимостей, кроме `requests`.
Для Telegram-уведомлений нужен токен/чат. Для Webhook — URL.

Примеры запуска
---------------
# демо, дефолтные параметры
python3 main.py --loop

# одноразовая выборка по указанному универсу
python3 main.py --list BTCUSDT,ETHUSDT,SOLUSDT --once

# TOP с лимитом 30, обновление каждые 5 минут, лог-уровень DEBUG
python3 main.py --mode TOP --max 30 --refresh 5 --level DEBUG --loop

# включить вебсервер метрик на порту 8080
python3 main.py --http 8080 --loop

# горяча подгрузка конфигурации из JSON (пороги, алерты, универс и т.д.)
python3 main.py --config ./runtime-config.json --loop


Формат runtime-config.json
--------------------------
{
  "universe": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
  "thresholds": {
    "BTCUSDT": {"above": 70000, "below": 60000},
    "ETHUSDT": {"above": 4000}
  },
  "alerts": {
    "telegram": {"token": "123:AAA", "chat_id": 12345678, "enabled": true},
    "webhook": {"url": "https://hook.example", "enabled": false}
  },
  "universe_mode": "TOP",           # override CLI
  "universe_max": 25,               # override CLI
  "universe_refresh_min": 5         # override CLI
}

"""

import os
import sys
import time
import csv
import json
import math
import signal
import queue
import types
import sqlite3
import threading
import argparse
import logging
import traceback
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


# =======================
# Конфигурация (дефолт)
# =======================

@dataclass
class AlertsConfig:
    telegram_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    telegram_enabled: bool = False
    webhook_url: Optional[str] = None
    webhook_enabled: bool = False


@dataclass
class Config:
    # Биржевые домены
    ByBitRestBase: str = "bytick.com"
    ByBitRestFallback: str = "bybit.com"
    PriceFallbackBinance: str = "binance.com"

    # Универс
    UniverseMax: int = 50
    UniverseMode: str = "TOP"               # "TOP" | "ALL"
    UniverseRefreshMin: int = 15             # период итерации в минутах
    UniverseList: Optional[List[str]] = None # явный список символов

    # Сетевая часть
    RequestTimeout: int = 10
    MaxRetries: int = 3
    BackoffFactor: float = 0.6
    Jitter: float = 0.25                     # доля джиттера для бэкоффа
    Category: str = "linear"                 # Bybit V5: linear | inverse | option
    Concurrency: int = 12                    # потоков для фетчинга
    RatePerSecond: float = 20.0              # ограничение скорости запросов

    # Файлы и вывод
    LogFile: str = "bot.log"
    CsvFile: str = "prices.csv"
    DbFile: str = "prices.sqlite3"
    LogLevel: str = "INFO"
    PrintOnly: bool = False

    # Режим исполнения
    Once: bool = False
    Loop: bool = False

    # Веб-сервер метрик/здоровья
    HttpPort: Optional[int] = None

    # Горячая конфигурация из JSON
    RuntimeConfigPath: Optional[str] = None

    # Кэш
    CacheTTL: int = 15                       # секунд

    # Алерты
    Alerts: AlertsConfig = field(default_factory=AlertsConfig)


# =======================
# Глобальные константы
# =======================

DEFAULT_TOP = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "TONUSDT", "TRXUSDT", "LINKUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "NEARUSDT", "SUIUSDT",
    "LTCUSDT", "MATICUSDT", "ETCUSDT", "ATOMUSDT", "AAVEUSDT",
]

DEFAULT_ALL = DEFAULT_TOP + [
    "EOSUSDT", "XLMUSDT", "FILUSDT", "INJUSDT", "WLDUSDT",
    "PEPEUSDT", "SHIBUSDT", "FTMUSDT", "KASUSDT", "RUNEUSDT",
    "SEIUSDT", "PYTHUSDT", "TIAUSDT", "ORDIUSDT", "JUPUSDT",
]

_SHUTDOWN = False


# =======================
# Утилиты
# =======================

def env_or_default(name: str, default, cast: Optional[Callable]=None):
    v = os.getenv(name)
    if v is None:
        return default
    if cast is None:
        return v
    try:
        return cast(v)
    except Exception:
        return default


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def jittered_backoff(base: float, jitter: float) -> float:
    # Простой джиттер ±jitter*base
    return max(0.0, base * (1.0 + (2.0 * (os.urandom(1)[0] / 255.0 - 0.5) * jitter)))


def normalize_symbol_for_bybit(symbol: str) -> str:
    return symbol.replace("-", "").upper()


def normalize_symbol_for_binance(symbol: str) -> str:
    return symbol.replace("-", "").upper()


# =======================
# Парсинг аргументов
# =======================

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Бот-скринер по Bybit/Bytick с фолбэком на Binance")

    # Режим
    p.add_argument("--once", action="store_true", help="Одноразовый прогон")
    p.add_argument("--loop", action="store_true", help="Циклический режим")

    # Универс
    p.add_argument("--mode", dest="UniverseMode", default=env_or_default("UNIVERSE_MODE", "TOP"),
                   choices=["TOP", "ALL"], help="Режим универса")
    p.add_argument("--max", dest="UniverseMax", type=int, default=env_or_default("UNIVERSE_MAX", 50, int),
                   help="Ограничение размера универса")
    p.add_argument("--refresh", dest="UniverseRefreshMin", type=int,
                   default=env_or_default("UNIVERSE_REFRESH_MIN", 15, int),
                   help="Интервал обновления в минутах")
    p.add_argument("--list", dest="UniverseList", type=str,
                   default=env_or_default("UNIVERSE_LIST", None),
                   help="Явный список символов через запятую (пример: BTCUSDT,ETHUSDT,SOLUSDT)")

    # Сеть
    p.add_argument("--timeout", dest="RequestTimeout", type=int,
                   default=env_or_default("REQUEST_TIMEOUT", 10, int), help="Таймаут HTTP")
    p.add_argument("--retries", dest="MaxRetries", type=int,
                   default=env_or_default("MAX_RETRIES", 3, int), help="Максимум ретраев")
    p.add_argument("--backoff", dest="BackoffFactor", type=float,
                   default=env_or_default("BACKOFF_FACTOR", 0.6, float), help="Фактор бэкоффа")
    p.add_argument("--jitter", dest="Jitter", type=float,
                   default=env_or_default("JITTER", 0.25, float), help="Доля джиттера")
    p.add_argument("--category", dest="Category", type=str,
                   default=env_or_default("BYBIT_CATEGORY", "linear"),
                   choices=["linear", "inverse", "option"], help="Категория для Bybit V5")
    p.add_argument("--concurrency", dest="Concurrency", type=int,
                   default=env_or_default("CONCURRENCY", 12, int), help="Количество потоков")
    p.add_argument("--rps", dest="RatePerSecond", type=float,
                   default=env_or_default("RATE_PER_SECOND", 20.0, float), help="Лимит запросов/сек")

    # Файлы/логирование
    p.add_argument("--log", dest="LogFile", type=str, default=env_or_default("LOG_FILE", "bot.log"),
                   help="Путь к лог-файлу")
    p.add_argument("--csv", dest="CsvFile", type=str, default=env_or_default("CSV_FILE", "prices.csv"),
                   help="Путь к CSV-файлу")
    p.add_argument("--db", dest="DbFile", type=str, default=env_or_default("DB_FILE", "prices.sqlite3"),
                   help="Путь к SQLite БД")
    p.add_argument("--level", dest="LogLevel", type=str,
                   default=env_or_default("LOG_LEVEL", "INFO"),
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Уровень логирования")
    p.add_argument("--print-only", dest="PrintOnly", action="store_true",
                   default=env_or_default("PRINT_ONLY", "false").lower() == "true",
                   help="Не писать в файлы, только консоль")

    # Домены
    p.add_argument("--bybit-base", dest="ByBitRestBase", type=str,
                   default=env_or_default("BYBIT_REST_BASE", "bytick.com"),
                   help="Домен основного Bybit REST")
    p.add_argument("--bybit-fallback", dest="ByBitRestFallback", type=str,
                   default=env_or_default("BYBIT_REST_FALLBACK", "bybit.com"),
                   help="Домен fallback Bybit REST")
    p.add_argument("--binance", dest="PriceFallbackBinance", type=str,
                   default=env_or_default("PRICE_FALLBACK_BINANCE", "binance.com"),
                   help="Домен Binance API")

    # Вебсервер метрик/здоровья
    p.add_argument("--http", dest="HttpPort", type=int,
                   default=env_or_default("HTTP_PORT", None, int),
                   help="HTTP порт для /health и /metrics")

    # Горячая конфигурация
    p.add_argument("--config", dest="RuntimeConfigPath", type=str,
                   default=env_or_default("RUNTIME_CONFIG", None),
                   help="Путь к runtime JSON конфигу (горячая подгрузка)")

    args = p.parse_args()

    cfg = Config(
        ByBitRestBase=args.ByBitRestBase,
        ByBitRestFallback=args.ByBitRestFallback,
        PriceFallbackBinance=args.PriceFallbackBinance,
        UniverseMax=args.UniverseMax,
        UniverseMode=args.UniverseMode,
        UniverseRefreshMin=args.UniverseRefreshMin,
        UniverseList=[s.strip() for s in args.UniverseList.split(",")] if args.UniverseList else None,
        RequestTimeout=args.RequestTimeout,
        MaxRetries=args.MaxRetries,
        BackoffFactor=args.BackoffFactor,
        Jitter=args.Jitter,
        Category=args.Category,
        Concurrency=args.Concurrency,
        RatePerSecond=args.RatePerSecond,
        LogFile=args.LogFile,
        CsvFile=args.CsvFile,
        DbFile=args.DbFile,
        LogLevel=args.LogLevel,
        PrintOnly=args.PrintOnly,
        Once=args.once,
        Loop=args.loop,
        HttpPort=args.HttpPort,
        RuntimeConfigPath=args.RuntimeConfigPath,
    )
    return cfg


# =======================
# Логирование
# =======================

def setup_logger(cfg: Config) -> logging.Logger:
    logger = logging.getLogger("bot")
    logger.setLevel(getattr(logging, cfg.LogLevel.upper(), logging.INFO))

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(sh)

    if not cfg.PrintOnly:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(cfg.LogFile, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


# =======================
# Ограничение скорости
# =======================

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = float(rate_per_sec)
        self.capacity = capacity if capacity is not None else self.rate
        self.tokens = self.capacity
        self.last = time.perf_counter()
        self.lock = threading.Lock()

    def consume(self, amount: float = 1.0):
        while True:
            with self.lock:
                self._add_new_tokens()
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
            # недобор — спим немного
            time.sleep(0.001)

    def _add_new_tokens(self):
        now = time.perf_counter()
        delta = now - self.last
        self.last = now
        self.tokens = min(self.capacity, self.tokens + delta * self.rate)


# =======================
# Кэш с TTL
# =======================

class TTLCache:
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self.data: Dict[str, Tuple[float, float, str]] = {}  # sym -> (price, ts, source)
        self.lock = threading.Lock()

    def get(self, sym: str) -> Optional[Tuple[float, float, str]]:
        with self.lock:
            row = self.data.get(sym)
            if not row:
                return None
            price, ts, source = row
            if time.time() - ts <= self.ttl:
                return row
            else:
                self.data.pop(sym, None)
                return None

    def put(self, sym: str, price: float, source: str):
        with self.lock:
            self.data[sym] = (price, time.time(), source)


# =======================
# База данных SQLite
# =======================

class PriceDB:
    def __init__(self, path: str, logger: logging.Logger):
        self.path = path
        self.logger = logger
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.path)
        try:
            c = conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS prices(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                price REAL NOT NULL
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS alerts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rule TEXT NOT NULL,
                price REAL NOT NULL,
                details TEXT
            )""")
            conn.commit()
        finally:
            conn.close()

    def insert_price(self, ts: str, symbol: str, source: str, price: float):
        try:
            conn = sqlite3.connect(self.path)
            c = conn.cursor()
            c.execute("INSERT INTO prices(ts, symbol, source, price) VALUES(?,?,?,?)",
                      (ts, symbol.upper(), source, float(price)))
            conn.commit()
        except Exception as e:
            self.logger.error(f"SQLite insert_price error: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def insert_alert(self, ts: str, symbol: str, rule: str, price: float, details: str = ""):
        try:
            conn = sqlite3.connect(self.path)
            c = conn.cursor()
            c.execute("INSERT INTO alerts(ts, symbol, rule, price, details) VALUES(?,?,?,?,?)",
                      (ts, symbol.upper(), rule, float(price), details))
            conn.commit()
        except Exception as e:
            self.logger.error(f"SQLite insert_alert error: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


# =======================
# HTTP-сессия с ретраями
# =======================

def build_session(cfg: Config) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=cfg.MaxRetries,
        backoff_factor=cfg.BackoffFactor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "ScreenerBot/2.0 (+https://example.local)"
    })
    return session


# =======================
# API-обёртки
# =======================

def fetch_from_bybit(session: requests.Session, cfg: Config, symbol: str) -> Optional[float]:
    sym = normalize_symbol_for_bybit(symbol)
    url = f"https://api.{cfg.ByBitRestBase}/v5/market/tickers"
    params = {"category": cfg.Category, "symbol": sym}
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    return float(data["result"]["list"][0]["lastPrice"])


def fetch_from_bybit_fallback(session: requests.Session, cfg: Config, symbol: str) -> Optional[float]:
    sym = normalize_symbol_for_bybit(symbol)
    url = f"https://api.{cfg.ByBitRestFallback}/v5/market/tickers"
    params = {"category": cfg.Category, "symbol": sym}
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    return float(data["result"]["list"][0]["lastPrice"])


def fetch_from_binance(session: requests.Session, cfg: Config, symbol: str) -> Optional[float]:
    sym = normalize_symbol_for_binance(symbol)
    url = f"https://api.{cfg.PriceFallbackBinance}/api/v3/ticker/price"
    params = {"symbol": sym}
    r = session.get(url, params=params, timeout=cfg.RequestTimeout)
    r.raise_for_status()
    data = r.json()
    return float(data["price"])


def get_price_chain(session: requests.Session, cfg: Config, symbol: str, logger: logging.Logger) -> Optional[Tuple[str, float]]:
    """
    Порядок: Bybit base -> Bybit fallback -> Binance
    Возвращает (source, price) или None
    """
    try:
        p = fetch_from_bybit(session, cfg, symbol)
        return ("bytick", p)
    except Exception as e:
        logger.debug(f"Bytick fail {symbol}: {e}")

    try:
        p = fetch_from_bybit_fallback(session, cfg, symbol)
        return ("bybit", p)
    except Exception as e:
        logger.debug(f"Bybit fallback fail {symbol}: {e}")

    try:
        p = fetch_from_binance(session, cfg, symbol)
        return ("binance", p)
    except Exception as e:
        logger.debug(f"Binance fail {symbol}: {e}")

    return None


# =======================
# Горячая конфигурация
# =======================

class RuntimeConfig:
    def __init__(self, path: Optional[str], logger: logging.Logger):
        self.path = path
        self.logger = logger
        self._mtime = 0.0
        self.lock = threading.Lock()
        self.data: Dict[str, Any] = {}
        if path:
            self._load(initial=True)

    def _load(self, initial: bool=False):
        if not self.path:
            return
        try:
            st = os.stat(self.path)
            if initial or st.st_mtime > self._mtime:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                with self.lock:
                    self.data = data
                    self._mtime = st.st_mtime
                self.logger.info(f"Runtime config reloaded from {self.path}")
        except FileNotFoundError:
            if initial:
                self.logger.warning(f"Runtime config file not found: {self.path}")
        except Exception as e:
            self.logger.error(f"Runtime config load error: {e}")

    def maybe_reload(self):
        if self.path:
            self._load()

    def get(self, key: str, default=None):
        with self.lock:
            return self.data.get(key, default)

    def get_thresholds(self) -> Dict[str, Dict[str, float]]:
        with self.lock:
            return self.data.get("thresholds", {}) or {}

    def get_universe_override(self) -> Optional[List[str]]:
        with self.lock:
            unv = self.data.get("universe")
            if isinstance(unv, list) and unv:
                return [str(s).upper() for s in unv]
            return None

    def apply_overrides(self, cfg: Config):
        with self.lock:
            m = self.data.get("universe_mode")
            if m in ("TOP", "ALL"):
                cfg.UniverseMode = m
            mx = self.data.get("universe_max")
            if isinstance(mx, int) and mx > 0:
                cfg.UniverseMax = mx
            rf = self.data.get("universe_refresh_min")
            if isinstance(rf, int) and rf > 0:
                cfg.UniverseRefreshMin = rf
            # alerts
            alerts = self.data.get("alerts", {})
            if isinstance(alerts, dict):
                tg = alerts.get("telegram", {})
                wh = alerts.get("webhook", {})
                cfg.Alerts.telegram_token = tg.get("token") or cfg.Alerts.telegram_token
                cfg.Alerts.telegram_chat_id = tg.get("chat_id") or cfg.Alerts.telegram_chat_id
                cfg.Alerts.telegram_enabled = bool(tg.get("enabled", cfg.Alerts.telegram_enabled))
                cfg.Alerts.webhook_url = wh.get("url") or cfg.Alerts.webhook_url
                cfg.Alerts.webhook_enabled = bool(wh.get("enabled", cfg.Alerts.webhook_enabled))


# =======================
# Универс
# =======================

def get_universe(cfg: Config, rtx: RuntimeConfig, logger: logging.Logger) -> List[str]:
    # приоритет: runtime-config -> CLI --list -> режим
    override = rtx.get_universe_override()
    if override:
        unv = override[: cfg.UniverseMax]
        logger.debug(f"Universe from runtime-config: {unv}")
        return unv

    if cfg.UniverseList and len(cfg.UniverseList) > 0:
        universe = [s.strip().upper() for s in cfg.UniverseList if s.strip()]
        logger.debug(f"Universe from CLI/ENV: {universe}")
        return universe[: cfg.UniverseMax]

    if cfg.UniverseMode.upper() == "TOP":
        return DEFAULT_TOP[: cfg.UniverseMax]
    elif cfg.UniverseMode.upper() == "ALL":
        return DEFAULT_ALL[: cfg.UniverseMax]
    else:
        logger.warning(f"Unknown UniverseMode={cfg.UniverseMode}, returning empty list.")
        return []


# =======================
# CSV журнал
# =======================

def ensure_csv_header(path: str, print_only: bool):
    if print_only:
        return
    if not os.path.isfile(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "symbol", "source", "price"])


def append_csv(path: str, row: List, print_only: bool):
    if print_only:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(row)


# =======================
# Алёрты
# =======================

def tg_send(token: str, chat_id: str, text: str, logger: logging.Logger):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"Telegram send non-200: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.error(f"Telegram send error: {e}")


def webhook_send(url: str, payload: Dict[str, Any], logger: logging.Logger):
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code >= 300:
            logger.warning(f"Webhook non-2xx: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.error(f"Webhook send error: {e}")


def check_thresholds(symbol: str, price: float, thresholds: Dict[str, Dict[str, float]], notify: Callable[[str,str,str,float], None]):
    th = thresholds.get(symbol.upper()) or {}
    above = th.get("above")
    below = th.get("below")
    if isinstance(above, (int, float)) and price >= float(above):
        notify(symbol, "above", str(above), price)
    if isinstance(below, (int, float)) and price <= float(below):
        notify(symbol, "below", str(below), price)


# =======================
# HTTP сервер для health/metrics
# =======================

class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.stats = {
            "ok": 0,
            "fail": 0,
            "last_cycle_start": "",
            "last_cycle_end": "",
            "inflight": 0,
        }
        self.last_config: Dict[str, Any] = {}

    def set_metric(self, key: str, val: Any):
        with self.lock:
            self.stats[key] = val

    def inc(self, key: str, v: int = 1):
        with self.lock:
            self.stats[key] = self.stats.get(key, 0) + v

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return dict(self.stats)

    def set_last_config(self, cfg: Dict[str, Any]):
        with self.lock:
            self.last_config = cfg

    def get_last_config(self) -> Dict[str, Any]:
        with self.lock:
            return dict(self.last_config)


GLOBAL_STATE = State()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/health"):
            snap = GLOBAL_STATE.snapshot()
            body = json.dumps({"status": "ok", "stats": snap}, ensure_ascii=False).encode("utf-8")
            self._send(200, body, "application/json")
        elif self.path.startswith("/metrics"):
            snap = GLOBAL_STATE.snapshot()
            lines = []
            for k, v in snap.items():
                if isinstance(v, (int, float)):
                    lines.append(f"bot_{k} {v}")
            body = ("\n".join(lines) + "\n").encode("utf-8")
            self._send(200, body, "text/plain; version=0.0.4")
        elif self.path.startswith("/config"):
            body = json.dumps(GLOBAL_STATE.get_last_config(), ensure_ascii=False, indent=2).encode("utf-8")
            self._send(200, body, "application/json")
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, format, *args):
        # не засоряем stdout
        return

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_http_server(port: int, stop_event: threading.Event, logger: logging.Logger):
    httpd = HTTPServer(("0.0.0.0", port), Handler)
    httpd.timeout = 1.0
    logger.info(f"HTTP metrics/health server started on :{port}")
    while not stop_event.is_set():
        httpd.handle_request()
    logger.info("HTTP server stopped")


# =======================
# Главный цикл
# =======================

def install_signal_handlers(logger: logging.Logger):
    def _handler(signum, frame):
        global _SHUTDOWN
        _SHUTDOWN = True
        logger.info(f"Signal {signum} received, shutting down...")
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def run_once(cfg: Config, rtx: RuntimeConfig, logger: logging.Logger, cache: TTLCache,
             session: requests.Session, db: PriceDB, bucket: TokenBucket):
    ts = now_str()
    GLOBAL_STATE.set_metric("last_cycle_start", ts)
    symbols = get_universe(cfg, rtx, logger)
    logger.info(f"=== Universe ({len(symbols)}) @ {ts} ===")

    thresholds = rtx.get_thresholds()

    # Уведомитель
    def notify(symbol: str, rule: str, level: str, price: float):
        msg = f"[ALERT] {symbol} {rule} {level} | price={price} @ {now_str()}"
        logger.warning(msg)
        db.insert_alert(now_str(), symbol, f"{rule}:{level}", price, "")
        # Telegram
        if cfg.Alerts.telegram_enabled and cfg.Alerts.telegram_token and cfg.Alerts.telegram_chat_id:
            try:
                tg_send(cfg.Alerts.telegram_token, str(cfg.Alerts.telegram_chat_id), msg, logger)
            except Exception as e:
                logger.error(f"Telegram notify error: {e}")
        # Webhook
        if cfg.Alerts.webhook_enabled and cfg.Alerts.webhook_url:
            try:
                webhook_send(cfg.Alerts.webhook_url, {"event": "alert", "symbol": symbol,
                                                      "rule": rule, "level": level, "price": price, "ts": now_str()}, logger)
            except Exception as e:
                logger.error(f"Webhook notify error: {e}")

    ensure_csv_header(cfg.CsvFile, cfg.PrintOnly)

    results: Dict[str, Tuple[str, float]] = {}
    errors: Dict[str, str] = {}

    # Пул потоков
    with ThreadPoolExecutor(max_workers=max(1, cfg.Concurrency)) as ex:
        futures = {}
        for sym in symbols:
            # кэш? — используем
            cached = cache.get(sym)
            if cached:
                price, ts_cached, source = cached
                results[sym] = (source, price)
                continue

            # иначе планируем фетч
            def worker(symbol=sym):
                bucket.consume(1.0)  # ограничение скорости
                try:
                    out = get_price_chain(session, cfg, symbol, logger)
                    if out is None:
                        raise RuntimeError("all sources failed")
                    source, price = out
                    cache.put(symbol, price, source)
                    return (symbol, source, price, None)
                except Exception as e:
                    return (symbol, "", 0.0, str(e))

            futures[ex.submit(worker)] = sym

        for f in as_completed(futures):
            sym = futures[f]
            try:
                symbol, source, price, err = f.result()
                if err:
                    errors[sym] = err
                else:
                    results[sym] = (source, price)
            except Exception as e:
                errors[sym] = str(e)

    ok = 0
    fail = 0
    for sym in symbols:
        if sym in results:
            source, price = results[sym]
            line = f"{sym}: {price}  [{source}]"
            print(line)
            logger.info(line)
            append_csv(cfg.CsvFile, [ts, sym, source, f"{price:.10g}"], cfg.PrintOnly)
            db.insert_price(ts, sym, source, price)
            ok += 1
            # thresholds
            check_thresholds(sym, price, thresholds, notify)
        else:
            msg = f"{sym}: нет данных ({errors.get(sym, 'unknown error')})"
            print(msg)
            logger.warning(msg)
            fail += 1

    GLOBAL_STATE.inc("ok", ok)
    GLOBAL_STATE.inc("fail", fail)
    GLOBAL_STATE.set_metric("last_cycle_end", now_str())


def run_loop(cfg: Config, rtx: RuntimeConfig, logger: logging.Logger):
    # подготовка инфраструктуры
    session = build_session(cfg)
    db = PriceDB(cfg.DbFile, logger)
    cache = TTLCache(cfg.CacheTTL)
    bucket = TokenBucket(cfg.RatePerSecond)

    # HTTP сервер опционально
    http_stop = threading.Event()
    http_thread = None
    if cfg.HttpPort:
        http_thread = threading.Thread(target=run_http_server, args=(cfg.HttpPort, http_stop, logger), daemon=True)
        http_thread.start()

    # Основной цикл
    logger.info("Скринер запущен")
    logger.info(f"Mode={cfg.UniverseMode} Max={cfg.UniverseMax} Refresh={cfg.UniverseRefreshMin}m Category={cfg.Category}")
    logger.info(f"Domains: base={cfg.ByBitRestBase} fallback={cfg.ByBitRestFallback} binance={cfg.PriceFallbackBinance}")
    logger.info(f"Concurrency={cfg.Concurrency} RPS={cfg.RatePerSecond} CacheTTL={cfg.CacheTTL}s HTTP={cfg.HttpPort or 'off'}")

    try:
        while not _SHUTDOWN:
            # Runtime overrides
            rtx.maybe_reload()
            rtx.apply_overrides(cfg)
            # Отображаем последнюю эффективную конфигурацию для /config
            GLOBAL_STATE.set_last_config({
                "ByBitRestBase": cfg.ByBitRestBase,
                "ByBitRestFallback": cfg.ByBitRestFallback,
                "PriceFallbackBinance": cfg.PriceFallbackBinance,
                "UniverseMax": cfg.UniverseMax,
                "UniverseMode": cfg.UniverseMode,
                "UniverseRefreshMin": cfg.UniverseRefreshMin,
                "Category": cfg.Category,
                "Concurrency": cfg.Concurrency,
                "RatePerSecond": cfg.RatePerSecond,
                "CacheTTL": cfg.CacheTTL,
                "HttpPort": cfg.HttpPort,
                "Alerts": {
                    "telegram_enabled": cfg.Alerts.telegram_enabled,
                    "webhook_enabled": cfg.Alerts.webhook_enabled,
                }
            })

            run_once(cfg, rtx, logger, cache, session, db, bucket)
            if cfg.Once:
                break

            # спим по секундам, чтобы быстро реагировать на SIGTERM
            total = max(1, int(cfg.UniverseRefreshMin * 60))
            for _ in range(total):
                if _SHUTDOWN:
                    break
                time.sleep(1)
    finally:
        if http_thread:
            http_stop.set()
            # деликатное завершение
            for _ in range(50):
                if not http_thread.is_alive():
                    break
                time.sleep(0.1)
        logger.info("Скринер остановлен")


# =======================
# Точка входа
# =======================

def main():
    cfg = parse_args()
    logger = setup_logger(cfg)
    install_signal_handlers(logger)

    # runtime config
    rtx = RuntimeConfig(cfg.RuntimeConfigPath, logger)

    if not cfg.Loop and not cfg.Once:
        # По умолчанию — циклический режим
        cfg.Loop = True

    try:
        if cfg.Loop:
            run_loop(cfg, rtx, logger)
        else:
            # одноразовый прогон
            session = build_session(cfg)
            db = PriceDB(cfg.DbFile, logger)
            cache = TTLCache(cfg.CacheTTL)
            bucket = TokenBucket(cfg.RatePerSecond)
            run_once(cfg, rtx, logger, cache, session, db, bucket)
    except Exception as e:
        logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
