#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv

# Если у вас python-telegram-bot:
# v20+
try:
    from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
    PTB_VERSION = 20
except Exception:
    # v13 fallback (не меняем требований проекта — просто поддерживаем оба API)
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
    PTB_VERSION = 13

load_dotenv()

# --- Логирование ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("menu_bot")

# --- Пути/константы ---
BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
STATE_FILE = STATE_DIR / "push_enabled.json"

MENU_BOT_TOKEN = os.getenv("MENU_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
ADMINS = set(
    [a.strip() for a in (os.getenv("ADMINS", "")).split(",") if a.strip()]
)

# --- Утилиты состояния ---
DEFAULT_STATE = {"tvoi": True, "trnd": False}


def _safe_read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return dict(default)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # миграции старых форматов: bool или {"enabled": bool}
        if isinstance(data, bool):
            return {"tvoi": bool(data), "trnd": bool(data)}
        if isinstance(data, dict):
            if "enabled" in data and ("tvoi" not in data or "trnd" not in data):
                en = bool(data.get("enabled"))
                data = {"tvoi": en, "trnd": en}
            # гарантируем поля
            for k, v in DEFAULT_STATE.items():
                data.setdefault(k, v)
            return data
        return dict(default)
    except Exception as e:
        logger.exception("Failed to read state: %s", e)
        return dict(default)


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def get_state() -> Dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return _safe_read_json(STATE_FILE, DEFAULT_STATE)


def set_state(new_state: Dict[str, Any]) -> Dict[str, Any]:
    # нормализуем и пишем атомарно
    st = get_state()
    st.update({k: bool(v) for k, v in new_state.items() if k in DEFAULT_STATE})
    _atomic_write_json(STATE_FILE, st)
    return st


def build_keyboard(st: Dict[str, Any]) -> InlineKeyboardMarkup:
    # Отображаем текущее состояние
    tvoi_label = f"📡 TVOI: {'ON' if st['tvoi'] else 'OFF'}"
    trnd_label = f"📈 TRND: {'ON' if st['trnd'] else 'OFF'}"
    keyboard = [
        [InlineKeyboardButton(tvoi_label, callback_data="toggle_tvoi")],
        [InlineKeyboardButton(trnd_label, callback_data="toggle_trnd")],
        [InlineKeyboardButton("ℹ️ Команды", callback_data="show_help")],
    ]
    return InlineKeyboardMarkup(keyboard)


HELP_TEXT = (
    "Команды:\n"
    "/start — показать панель управления\n"
    "/status — текущее состояние флагов\n"
    "/on_tvoi, /off_tvoi — включить/выключить TVOI\n"
    "/on_trnd, /off_trnd — включить/выключить TRND\n"
    "\n"
    "Кнопки: переключают режимы независимо."
)


# --- Проверка админов (мягкая) ---
def _is_admin(user_id: int) -> bool:
    if not ADMINS:
        # если список пуст — не ограничиваем (как раньше)
        return True
    return str(user_id) in ADMINS


# --- Обработчики PTB v20 ---
async def _start_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = get_state()
    await update.message.reply_text("InnerTrade Screener — управление сигналами", reply_markup=build_keyboard(st))


async def _status_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = get_state()
    msg = f"📡 TVOI: {'ON' if st['tvoi'] else 'OFF'}\n📈 TRND: {'ON' if st['trnd'] else 'OFF'}"
    await update.message.reply_text(msg, reply_markup=build_keyboard(st))


async def _help_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    await update.message.reply_text(HELP_TEXT)


async def _cb_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    user = q.from_user
    if not user or not _is_admin(user.id):
        await q.answer("Нет доступа")
        return

    st = get_state()
    if q.data == "toggle_tvoi":
        st = set_state({"tvoi": not st["tvoi"]})
        await q.answer(f"TVOI → {'ON' if st['tvoi'] else 'OFF'}")
    elif q.data == "toggle_trnd":
        st = set_state({"trnd": not st["trnd"]})
        await q.answer(f"TRND → {'ON' if st['trnd'] else 'OFF'}")
    elif q.data == "show_help":
        await q.answer("Команды")
        await q.message.reply_text(HELP_TEXT)
    else:
        await q.answer("Неизвестная команда")

    # Обновляем клавиатуру в том же сообщении
    try:
        await q.message.edit_reply_markup(reply_markup=build_keyboard(st))
    except Exception:
        # если нельзя отредактировать — просто отправим новое
        await q.message.reply_text("Обновлено", reply_markup=build_keyboard(st))


async def _on_tvoi_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = set_state({"tvoi": True})
    await update.message.reply_text("TVOI включен", reply_markup=build_keyboard(st))


async def _off_tvoi_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = set_state({"tvoi": False})
    await update.message.reply_text("TVOI выключен", reply_markup=build_keyboard(st))


async def _on_trnd_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = set_state({"trnd": True})
    await update.message.reply_text("TRND включен", reply_markup=build_keyboard(st))


async def _off_trnd_v20(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update.effective_user.id):
        return
    st = set_state({"trnd": False})
    await update.message.reply_text("TRND выключен", reply_markup=build_keyboard(st))


def run_v20():
    if not MENU_BOT_TOKEN:
        logger.error("MENU_BOT_TOKEN is not set")
        sys.exit(1)
    app = Application.builder().token(MENU_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", _start_v20))
    app.add_handler(CommandHandler("status", _status_v20))
    app.add_handler(CommandHandler("help", _help_v20))
    app.add_handler(CommandHandler("on_tvoi", _on_tvoi_v20))
    app.add_handler(CommandHandler("off_tvoi", _off_tvoi_v20))
    app.add_handler(CommandHandler("on_trnd", _on_trnd_v20))
    app.add_handler(CommandHandler("off_trnd", _off_trnd_v20))
    app.add_handler(CallbackQueryHandler(_cb_v20))

    logger.info("menu_bot started (PTB v20)")
    app.run_polling(drop_pending_updates=True)


# --- Обработчики PTB v13 (fallback) ---
def _start_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = get_state()
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="InnerTrade Screener — управление сигналами",
                             reply_markup=build_keyboard(st))


def _status_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = get_state()
    msg = f"📡 TVOI: {'ON' if st['tvoi'] else 'OFF'}\n📈 TRND: {'ON' if st['trnd'] else 'OFF'}"
    context.bot.send_message(chat_id=update.effective_chat.id, text=msg, reply_markup=build_keyboard(st))


def _help_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    context.bot.send_message(chat_id=update.effective_chat.id, text=HELP_TEXT)


def _cb_v13(update, context):
    q = update.callback_query
    if not q:
        return
    user = q.from_user
    if not user or not _is_admin(user.id):
        context.bot.answer_callback_query(q.id, text="Нет доступа")
        return

    st = get_state()
    if q.data == "toggle_tvoi":
        st = set_state({"tvoi": not st["tvoi"]})
        context.bot.answer_callback_query(q.id, text=f"TVOI → {'ON' if st['tvoi'] else 'OFF'}")
    elif q.data == "toggle_trnd":
        st = set_state({"trnd": not st["trnd"]})
        context.bot.answer_callback_query(q.id, text=f"TRND → {'ON' if st['trnd'] else 'OFF'}")
    elif q.data == "show_help":
        context.bot.answer_callback_query(q.id, text="Команды")
        context.bot.send_message(chat_id=update.effective_chat.id, text=HELP_TEXT)
    else:
        context.bot.answer_callback_query(q.id, text="Неизвестная команда")

    # обновляем клавиатуру
    try:
        context.bot.edit_message_reply_markup(
            chat_id=update.effective_chat.id,
            message_id=q.message.message_id,
            reply_markup=build_keyboard(st)
        )
    except Exception:
        context.bot.send_message(chat_id=update.effective_chat.id, text="Обновлено", reply_markup=build_keyboard(st))


def _on_tvoi_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = set_state({"tvoi": True})
    context.bot.send_message(chat_id=update.effective_chat.id, text="TVOI включен", reply_markup=build_keyboard(st))


def _off_tvoi_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = set_state({"tvoi": False})
    context.bot.send_message(chat_id=update.effective_chat.id, text="TVOI выключен", reply_markup=build_keyboard(st))


def _on_trnd_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = set_state({"trnd": True})
    context.bot.send_message(chat_id=update.effective_chat.id, text="TRND включен", reply_markup=build_keyboard(st))


def _off_trnd_v13(update, context):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    st = set_state({"trnd": False})
    context.bot.send_message(chat_id=update.effective_chat.id, text="TRND выключен", reply_markup=build_keyboard(st))


def run_v13():
    if not MENU_BOT_TOKEN:
        logger.error("MENU_BOT_TOKEN is not set")
        sys.exit(1)
    updater = Updater(token=MENU_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", _start_v13))
    dp.add_handler(CommandHandler("status", _status_v13))
    dp.add_handler(CommandHandler("help", _help_v13))
    dp.add_handler(CommandHandler("on_tvoi", _on_tvoi_v13))
    dp.add_handler(CommandHandler("off_tvoi", _off_tvoi_v13))
    dp.add_handler(CommandHandler("on_trnd", _on_trnd_v13))
    dp.add_handler(CommandHandler("off_trnd", _off_trnd_v13))
    dp.add_handler(CallbackQueryHandler(_cb_v13))

    logger.info("menu_bot started (PTB v13)")
    updater.start_polling(drop_pending_updates=True)
    updater.idle()


if __name__ == "__main__":
    logger.info("Starting menu_bot …")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    # ensure state exists
    if not STATE_FILE.exists():
        _atomic_write_json(STATE_FILE, DEFAULT_STATE)
        logger.info("State initialized: %s", DEFAULT_STATE)

    # запуск под подходящую версию PTB
    if PTB_VERSION >= 20:
        run_v20()
    else:
        run_v13()
