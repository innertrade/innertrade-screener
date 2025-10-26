from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Any

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from push_state import get_push_enabled, set_push_enabled

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

MSK = timezone(timedelta(hours=3))


def _build_keyboard(push_enabled: bool) -> ReplyKeyboardMarkup:
    buttons = [["ON", "OFF"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)


def _authorized(obj: Any) -> bool:
    if CHAT_ID == 0:
        return True
    chat = None
    if isinstance(obj, Update):
        chat = obj.effective_chat
    else:
        chat = getattr(obj, "chat", None) or getattr(obj, "effective_chat", None)
    if not chat:
        return False
    return chat.id == CHAT_ID


async def _reply_not_authorized(message):
    await message.reply_text("Недостаточно прав для управления рассылкой.")


async def _reply_state(message, push_enabled: bool):
    await message.reply_text(
        f"Рассылка {'включена (ON)' if push_enabled else 'выключена (OFF)'}.",
        reply_markup=_build_keyboard(push_enabled),
    )


async def _handle_toggle(message, enable: bool):
    if not _authorized(message):
        await _reply_not_authorized(message)
        return

    set_push_enabled(enable, source="menu_bot")
    await _reply_state(message, enable)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await _reply_not_authorized(message)
        return
    push_enabled = get_push_enabled()
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S MSK")
    await message.reply_text(
        f"Привет! Время: {now}\nРассылка сейчас {'ON' if push_enabled else 'OFF'}.",
        reply_markup=_build_keyboard(push_enabled),
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await _reply_not_authorized(message)
        return
    txt = (message.text or "").strip().upper()
    if txt == "ON":
        await _handle_toggle(message, True)
        return
    if txt == "OFF":
        await _handle_toggle(message, False)
        return

    push_enabled = get_push_enabled()
    await message.reply_text(
        f"Используйте кнопки ON или OFF. Текущее состояние: {'ON' if push_enabled else 'OFF'}.",
        reply_markup=_build_keyboard(push_enabled),
    )


async def cmd_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    await _handle_toggle(message, True)


async def cmd_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    await _handle_toggle(message, False)


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", cmd_on))
    app.add_handler(CommandHandler("off", cmd_off))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    main()
