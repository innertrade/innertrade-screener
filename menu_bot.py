from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from push_state import get_push_enabled, set_push_enabled

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

MSK = timezone(timedelta(hours=3))


def _build_keyboard(push_enabled: bool) -> ReplyKeyboardMarkup:
    status_label = "✅ ON" if push_enabled else "⛔ OFF"
    return ReplyKeyboardMarkup(
        [
            ["📊 Дашборд баблы", "🌡 Температура рынка"],
            ["📰 Новости", "🧮 Калькулятор"],
            [f"📡 Сигналы: {status_label}", "ℹ️ Команды: /on /off"],
        ],
        resize_keyboard=True,
    )


def _authorized(update: Update) -> bool:
    if CHAT_ID == 0:
        return True
    if not update.effective_chat:
        return False
    return update.effective_chat.id == CHAT_ID


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await message.reply_text("Недостаточно прав для управления ботом.")
        return
    push_enabled = get_push_enabled()
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S MSK")
    await message.reply_text(
        f"Привет! Время: {now}",
        reply_markup=_build_keyboard(push_enabled),
    )


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await message.reply_text("Недостаточно прав для управления ботом.")
        return
    push_enabled = get_push_enabled()
    txt = (update.message.text or "").strip()
    await message.reply_text(
        f"Вы нажали: {txt}\nТекущее состояние рассылки: {'ON' if push_enabled else 'OFF'}",
        reply_markup=_build_keyboard(push_enabled),
    )


async def cmd_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await message.reply_text("Недостаточно прав для управления рассылкой.")
        return
    set_push_enabled(True, source="menu_bot")
    await message.reply_text(
        "Рассылка включена ✅",
        reply_markup=_build_keyboard(True),
    )


async def cmd_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    if not _authorized(update):
        await message.reply_text("Недостаточно прав для управления рассылкой.")
        return
    set_push_enabled(False, source="menu_bot")
    await message.reply_text(
        "Рассылка приостановлена ⏸️",
        reply_markup=_build_keyboard(False),
    )


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", cmd_on))
    app.add_handler(CommandHandler("off", cmd_off))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    app.run_polling()


if __name__ == "__main__":
    main()
