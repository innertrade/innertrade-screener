#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
InnerTrade Menu Bot (reply-keyboard version)
— главное меню и обработка нажатий
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)

# ---------------- ENV ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ---------------- LOG ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("menu_bot")

# ---------------- MENU ----------------
# две строки, по три кнопки в каждой
MAIN_MENU = [
    [
        KeyboardButton("📊 Дашборд баблы"),
        KeyboardButton("🌡 Температура рынка"),
        KeyboardButton("📰 Новости"),
    ],
    [
        KeyboardButton("🧮 Калькулятор"),
        KeyboardButton("📡 Сигналы 1: ✅ ON"),
        KeyboardButton("⚙️ Сигналы 2: ⛔ OFF"),
    ],
]

menu_markup = ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True)

# ---------------- HANDLERS ----------------
def _now_msk() -> str:
    tz_msk = timezone(timedelta(hours=3))
    return datetime.now(tz=tz_msk).strftime("%H:%M:%S MSK")

def start(update: Update, context: CallbackContext):
    user = update.effective_user.first_name or ""
    text = (
        f"👋 Привет, {user}!\n"
        f"🕓 Время: {_now_msk()}\n\n"
        "Выбери действие из меню 👇"
    )
    update.message.reply_text(text, reply_markup=menu_markup)

def handle_message(update: Update, context: CallbackContext):
    text = (update.message.text or "").strip()

    if text == "📊 Дашборд баблы":
        update.message.reply_text("📊 Дашборд — в разработке (будет топ-100 по 24h объёму).")
    elif text == "🌡 Температура рынка":
        update.message.reply_text("🌡 Температура рынка — доступно в следующем обновлении.")
    elif text == "📰 Новости":
        update.message.reply_text("📰 Новости — будут агрегированы из открытых источников.")
    elif text == "🧮 Калькулятор":
        update.message.reply_text("🧮 Калькулятор — готовится к добавлению (позже в меню).")
    elif text.startswith("📡 Сигналы 1"):
        if "ON" in text:
            new_btn = "📡 Сигналы 1: ⛔ OFF"
            update.message.reply_text("📡 Сигналы выключены.")
        else:
            new_btn = "📡 Сигналы 1: ✅ ON"
            update.message.reply_text("📡 Сигналы включены.")
        # переключаем состояние кнопки
        MAIN_MENU[1][1] = KeyboardButton(new_btn)
        new_markup = ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True)
        update.message.reply_text("Меню обновлено ↩️", reply_markup=new_markup)
    elif text == "⚙️ Сигналы 2: ⛔ OFF":
        update.message.reply_text("⚙️ Сигналы 2 — зарезервировано для второй логики (RSI/уровни).")
    else:
        update.message.reply_text("❓ Неизвестная команда. Пожалуйста, выбери в меню.")

# ---------------- MAIN ----------------
def main():
    if not TELEGRAM_BOT_TOKEN:
        log.error("No TELEGRAM_BOT_TOKEN set.")
        return

    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    log.info("Menu bot started.")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
