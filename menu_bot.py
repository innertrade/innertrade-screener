from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from state_manager import (
    ensure_state_file,
    get_push_enabled,
    get_trend_enabled,
    set_push_enabled,
    set_trend_enabled,
    toggle_push_enabled,
    toggle_trend_enabled,
)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MSK = timezone(timedelta(hours=3))

LOGGER = logging.getLogger(__name__)

# --- UI strings ---
BTN_DASHBOARD = "📊 Дашборд баблы"
BTN_THERMAL = "🌡 Температура рынка"
BTN_NEWS = "📰 Новости"
BTN_CALC = "🧮 Калькулятор"
BTN_SIGNALS = "📡 Сигналы"
BTN_TREND = "📈 TRND"
STATE_ON = "✅ ON"
STATE_OFF = "⛔ OFF"
CMD_ON = "ON"
CMD_OFF = "OFF"
CMD_TRND = "TRND"
CMD_TREND_ON = "TRND ON"
CMD_TREND_OFF = "TRND OFF"


def _render_toggle(label: str, enabled: bool) -> str:
    return f"{label}: {STATE_ON if enabled else STATE_OFF}"


def _build_keyboard() -> ReplyKeyboardMarkup:
    push = get_push_enabled()
    trend = get_trend_enabled()
    keyboard = [
        [BTN_DASHBOARD, BTN_THERMAL],
        [BTN_NEWS, BTN_CALC],
        [
            _render_toggle(BTN_SIGNALS, push),
            _render_toggle(BTN_TREND, trend),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def _reply_with_confirmation(update: Update, text: str) -> None:
    try:
        await update.message.reply_text(text, reply_markup=_build_keyboard())
    except Exception as exc:  # pragma: no cover - telegram runtime guards
        LOGGER.error("Failed to send confirmation: %s", exc)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_state_file()
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S MSK")
    await update.message.reply_text(
        f"Привет! Время: {now}", reply_markup=_build_keyboard()
    )


async def _handle_push_toggle(update: Update) -> None:
    new_state = toggle_push_enabled()
    status = "Рассылка включена" if new_state else "Рассылка приостановлена"
    await _reply_with_confirmation(update, status)


async def _handle_trend_toggle(update: Update) -> None:
    new_state = toggle_trend_enabled()
    status = "TRND-поток включён" if new_state else "TRND-поток приостановлен"
    await _reply_with_confirmation(update, status)


async def _set_push_state(update: Update, enabled: bool) -> None:
    set_push_enabled(enabled)
    status = "Рассылка включена" if enabled else "Рассылка приостановлена"
    await _reply_with_confirmation(update, status)


async def _set_trend_state(update: Update, enabled: bool) -> None:
    set_trend_enabled(enabled)
    status = "TRND-поток включён" if enabled else "TRND-поток приостановлен"
    await _reply_with_confirmation(update, status)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    normalized = " ".join(text.upper().split())
    push_label = _render_toggle(BTN_SIGNALS, get_push_enabled())
    trend_label = _render_toggle(BTN_TREND, get_trend_enabled())

    if text == push_label:
        await _handle_push_toggle(update)
        return
    if text == trend_label:
        await _handle_trend_toggle(update)
        return

    if normalized == CMD_ON:
        await _set_push_state(update, True)
        return
    if normalized == CMD_OFF:
        await _set_push_state(update, False)
        return
    if normalized == CMD_TRND:
        await _handle_trend_toggle(update)
        return
    if normalized == CMD_TREND_ON:
        await _set_trend_state(update, True)
        return
    if normalized == CMD_TREND_OFF:
        await _set_trend_state(update, False)
        return

    await update.message.reply_text(
        f"Вы нажали: {text}", reply_markup=_build_keyboard()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ensure_state_file()
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    main()
