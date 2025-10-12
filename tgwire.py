# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests


def _get_bool(x: Optional[str], default: bool = False) -> bool:
    if x is None:
        return default
    return str(x).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class TelegramConfig:
    token: Optional[str]
    chat_id: Optional[str]
    timeout_connect: float = float(os.getenv("FORWARD_TIMEOUT_CONNECT", "5"))
    timeout_read: float = float(os.getenv("FORWARD_TIMEOUT_READ", "10"))
    max_retries: int = int(os.getenv("FORWARD_MAX_RETRIES", "3"))
    retry_sleep: float = float(os.getenv("FORWARD_RETRY_SLEEP", "1"))
    respect_retry_after: bool = _get_bool(os.getenv("TG_RESPECT_RETRY_AFTER", "1"))
    throttle_rps: float = float(os.getenv("TG_THROTTLE_RPS", "1"))
    throttle_burst: int = int(os.getenv("TG_THROTTLE_BURST", "5"))

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)


class TelegramSender:
    def __init__(self, cfg: TelegramConfig):
        self.cfg = cfg
        self._sess = requests.Session()
        self._log = logging.getLogger("tg")
        # простейший токен-бакет для RPS
        self._tb_tokens = cfg.throttle_burst
        self._tb_last = time.monotonic()

    @classmethod
    def from_env(cls) -> "TelegramSender":
        token = os.getenv("TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
        chat = os.getenv("TG_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
        return cls(TelegramConfig(token=token, chat_id=chat))

    # --- внутреннее ---

    def _throttle(self) -> None:
        # пополняем токен-бакет с заданной скоростью
        now = time.monotonic()
        elapsed = now - self._tb_last
        self._tb_last = now
        refill = elapsed * self.cfg.throttle_rps
        self._tb_tokens = min(self.cfg.throttle_burst, self._tb_tokens + refill)
        if self._tb_tokens < 1:
            need = 1 - self._tb_tokens
            sleep_s = max(0.0, need / max(self.cfg.throttle_rps, 1e-9))
            time.sleep(sleep_s)
            self._tb_tokens = 0
            self._tb_last = time.monotonic()
        else:
            self._tb_tokens -= 1

    def _post(self, method: str, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> bool:
        if not self.cfg.enabled:
            self._log.warning("TG disabled: no token/chat")
            return False

        url = f"https://api.telegram.org/bot{self.cfg.token}/{method}"
        last_resp = None
        backoff = self.cfg.retry_sleep

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                self._throttle()
                resp = self._sess.post(
                    url,
                    data=data,
                    files=files,
                    timeout=(self.cfg.timeout_connect, self.cfg.timeout_read),
                )
                last_resp = resp

                if resp.status_code == 429 and self.cfg.respect_retry_after:
                    try:
                        j = resp.json()
                        ra = j.get("parameters", {}).get("retry_after") or 1
                    except Exception:
                        ra = 1
                    self._log.info("TG 429 retry_after=%s", ra)
                    time.sleep(max(1, int(ra)))
                    continue

                if 200 <= resp.status_code < 300:
                    try:
                        j = resp.json()
                    except Exception:
                        j = {}
                    if j.get("ok"):
                        mid = j.get("result", {}).get("message_id")
                        self._log.info("sent to TG (message_id=%s)", mid)
                        return True
                    else:
                        self._log.error("TG error payload: %s", json.dumps(j)[:300])
                else:
                    self._log.warning("TG HTTP %s: %s", resp.status_code, str(resp.text)[:300])

            except Exception as e:
                self._log.warning("TG post error (attempt %s/%s): %s", attempt, self.cfg.max_retries, e)

            time.sleep(backoff)
            backoff = min(backoff * 2, 8.0)

        if last_resp is None:
            self._log.error("TG failed: no response after retries")
        else:
            self._log.error("TG failed: last HTTP=%s body=%s", last_resp.status_code, str(last_resp.text)[:300])
        return False

    # --- публичное API ---

    def send_message(self, text: str, parse_mode: Optional[str] = None, disable_notification: bool = True) -> bool:
        data = {
            "chat_id": self.cfg.chat_id,
            "text": text,
            "disable_notification": "true" if disable_notification else "false",
        }
        if parse_mode:
            data["parse_mode"] = parse_mode
        return self._post("sendMessage", data)

    def send_document(self, caption: str, filepath: str, disable_notification: bool = True) -> bool:
        data = {
            "chat_id": self.cfg.chat_id,
            "caption": caption,
            "disable_notification": "true" if disable_notification else "false",
        }
        with open(filepath, "rb") as f:
            files = {"document": (os.path.basename(filepath), f)}
            return self._post("sendDocument", data, files=files)
