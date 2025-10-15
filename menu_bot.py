from __future__ import annotations
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID","0"))

MSK = timezone(timedelta(hours=3))

KB = ReplyKeyboardMarkup(
    [
        ["📊 Дашборд баблы", "🌡 Температура рынка"],
        ["📰 Новости", "🧮 Калькулятор"],
        ["📡 Сигналы 1: ✅ ON", "⚙️ Сигналы 2: ⛔ OFF"],
    ],
    resize_keyboard=True
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S MSK")
    await update.message.reply_text(f"Привет! Время: {now}", reply_markup=KB)

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    await update.message.reply_text(f"Вы нажали: {txt}", reply_markup=KB)

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    app.run_polling()

if __name__ == "__main__":
    main()
