#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRE/CONFIRM forwarder:
- тянет /signals с движка (innertrade-api)
- делает отбор по порогам Z/VOLX/24h и по OI (flat/confirm)
- шлёт текст в TVOI gateway (/tvoi)
Все пороги — ТОЛЬКО через ENV.
"""

from __future__ import annotations
import os, time, json, math, logging
from typing import Any, Dict, List, Optional
import requests

HOST         = os.getenv("HOST", "http://127.0.0.1:8088").rstrip("/")
TVOI_URL     = os.getenv("TVOI_URL", "http://127.0.0.1:8787/tvoi").rstrip("/")

MIN_Z        = float(os.getenv("MIN_Z", "1.8"))
MIN_VOL_X    = float(os.getenv("MIN_VOL_X", "1.3"))
MIN_V24_USD  = float(os.getenv("MIN_V24_USD", "3000000"))

# OI-логика
FLAT_ONLY    = os.getenv("FLAT_ONLY", "1") == "1"
FLAT_BAND    = float(os.getenv("FLAT_BAND", "1.0"))     # |oi_z| <= FLAT_BAND -> flat
MIN_OI_Z     = float(os.getenv("MIN_OI_Z", "0.8"))      # |oi_z| >= MIN_OI_Z -> CONFIRM

COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "600"))    # защита от спама одинаковыми символами
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "8"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

session = requests.Session()
session.headers.update({"User-Agent": "InnerTradeForwarder/1.0"})

_last_sent: Dict[str, int] = {}   # symbol -> ts

def _fetch_signals() -> List[Dict[str, Any]]:
    try:
        r = session.get(f"{HOST}/signals", timeout=HTTP_TIMEOUT)
        if not r.ok:
            logging.info(f"signals HTTP {r.status_code}: {r.text[:160]}")
            return []
        j = r.json()
        return j.get("data", [])
    except Exception as e:
        logging.info(f"fetch fail: {e}")
        return []

def _fmt_money(x: Optional[float]) -> str:
    try:
        if x is None: return "—"
        if x >= 1e9:  return f"{x/1e9:.1f}B"
        if x >= 1e6:  return f"{x/1e6:.1f}M"
        if x >= 1e3:  return f"{x/1e3:.1f}K"
        return f"{x:.0f}"
    except: return "—"

def _pass_base_filters(s: Dict[str, Any]) -> bool:
    z  = s.get("zprice")
    vx = s.get("vol_mult")
    v24= s.get("vol24h_usd")

    if z is None or vx is None or v24 is None:
        return False
    if z < MIN_Z: return False
    if vx < MIN_VOL_X: return False
    if v24 < MIN_V24_USD: return False
    return True

def _classify_oi(oi_z: Optional[float]) -> str:
    if oi_z is None:
        # без OI — пропускаем, форвардер у нас основан на OI-режиме
        return "skip"
    a = abs(oi_z)
    if FLAT_ONLY and a <= FLAT_BAND:
        return "flat"     # PRE-кандидат
    if a >= MIN_OI_Z:
        return "confirm"  # CONFIRM
    return "skip"

def _cooldown_ok(symbol: str, now: int) -> bool:
    ts = _last_sent.get(symbol, 0)
    return (now - ts) >= COOLDOWN_SEC

def _mark_sent(symbol: str, now: int) -> None:
    _last_sent[symbol] = now

def _compose_text(kind: str, s: Dict[str, Any]) -> str:
    # kind: "PRE" или "CONFIRMED"
    sym = s["symbol"]
    z   = s.get("zprice")
    vx  = s.get("vol_mult")
    v24 = s.get("vol24h_usd")
    oi  = s.get("oi_z")
    ts  = s.get("ts")
    return (
        f"🟢 {sym}  ({'LONG' if z>=0 else 'SHORT'})\n"
        f"{kind}: z={z:+.2f} | vol×{vx:.2f} | "
        f"OI z={oi:+.2f if oi is not None else float('nan'):.2f}\n"
        f"24h volume ≈ ${_fmt_money(v24)}\n"
        f"{ts}"
    )

def _post_tvoi(text: str) -> bool:
    try:
        r = session.post(TVOI_URL, json={"text": text}, timeout=HTTP_TIMEOUT)
        if r.ok:
            return True
        logging.info(f"TVOI {r.status_code}: {r.text[:160]}")
    except Exception as e:
        logging.info(f"TVOI send fail: {e}")
    return False

def main_loop():
    logging.info(
        f"PRE forwarder start | {HOST} "
        f"Z>={MIN_Z} Vx>={MIN_VOL_X} v24h>={MIN_V24_USD} "
        f"| OI: flat_only={FLAT_ONLY} band=±{FLAT_BAND} confirm≥{MIN_OI_Z} "
        f"| cooldown={COOLDOWN_SEC}s"
    )
    while True:
        base = 0
        sent = 0
        now  = int(time.time())
        for s in _fetch_signals():
            if not _pass_base_filters(s):
                continue
            base += 1
            kind = _classify_oi(s.get("oi_z"))
            if kind == "skip":
                continue
            symbol = s["symbol"]
            if not _cooldown_ok(symbol, now):
                continue
            text = _compose_text("PRE" if kind == "flat" else "CONFIRMED", s)
            if _post_tvoi(text):
                _mark_sent(symbol, now)
                sent += 1
        logging.info(f"tick: base={base} sent={sent}")
        time.sleep(47)  # чуть короче 1 бара 1m/5m
if __name__ == "__main__":
    main_loop()
