from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, __version__ as TG_VER

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("menu_bot")

STATE_DEFAULT: Dict[str, bool] = {"tvoi": True, "trnd": False}
BASE_DIR = Path(__file__).resolve().parent

MENU_BOT_TOKEN = os.getenv("MENU_BOT_TOKEN", "")
PUSH_STATE_FILE = os.getenv("PUSH_STATE_FILE", "state/push_enabled.json")
ADMINS_RAW = os.getenv("ADMINS", "").strip()

BTN_TVOI = "📡 TVOI"
BTN_TRND = "📈 TRND"
BTN_HELP = "ℹ️ Команды"
STATE_ON = "ON"
STATE_OFF = "OFF"

HELP_TEXT = (
    "Доступные команды:\n"
    "/start — показать текущее состояние и клавиатуру.\n"
    "/status — текущее состояние рассылок.\n"
    "/on_tvoi /off_tvoi — включить/выключить TVOI.\n"
    "/on_trnd /off_trnd — включить/выключить TRND.\n"
    "/help — справка."
)

FLAG_LABELS = {"tvoi": "TVOI", "trnd": "TRND"}

try:  # python-telegram-bot v20+
    from telegram.ext import (
        Application,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
    )

    PTB_V20 = True
except ImportError:  # pragma: no cover - executed on legacy installs
    from telegram.ext import CallbackContext, CallbackQueryHandler, CommandHandler, Updater

    PTB_V20 = False
    ContextTypes = None  # type: ignore

ADMIN_IDS = {
    int(part.strip())
    for part in ADMINS_RAW.split(",")
    if part.strip()
    and part.strip().lstrip("-+").isdigit()
}


def _state_path() -> Path:
    configured = Path(PUSH_STATE_FILE)
    if not configured.is_absolute():
        configured = (BASE_DIR / configured).resolve()
    return configured


def _normalize_state(data: object) -> Tuple[Dict[str, bool], bool]:
    state = STATE_DEFAULT.copy()
    migrated = False
    if isinstance(data, bool):
        state = {"tvoi": data, "trnd": data}
        migrated = True
    elif isinstance(data, dict):
        if "tvoi" in data or "trnd" in data:
            state["tvoi"] = bool(data.get("tvoi", state["tvoi"]))
            state["trnd"] = bool(data.get("trnd", state["trnd"]))
        elif "enabled" in data:
            val = bool(data.get("enabled"))
            state = {"tvoi": val, "trnd": val}
            migrated = True
        else:
            if "push_enabled" in data:
                state["tvoi"] = bool(data.get("push_enabled"))
                migrated = True
            if "trend_enabled" in data:
                state["trnd"] = bool(data.get("trend_enabled"))
                migrated = True
    else:
        migrated = True
    return state, migrated


def _read_state() -> Dict[str, bool]:
    path = _state_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        LOGGER.info("State file missing, creating default at %s", path)
        _write_state(STATE_DEFAULT)
        return STATE_DEFAULT.copy()
    except json.JSONDecodeError:
        LOGGER.warning("Corrupted state file at %s, resetting to defaults", path)
        _write_state(STATE_DEFAULT)
        return STATE_DEFAULT.copy()
    state, migrated = _normalize_state(data)
    if migrated:
        LOGGER.info("Migrated state format at %s", path)
        _write_state(state)
    return state


def _write_state(state: Dict[str, bool]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"tvoi": bool(state.get("tvoi", False)), "trnd": bool(state.get("trnd", False))}
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def get_state() -> Dict[str, bool]:
    return _read_state()


def set_flag(flag: str, value: bool) -> Dict[str, bool]:
    state = get_state()
    state[flag] = bool(value)
    _write_state(state)
    return state


def toggle_flag(flag: str) -> Tuple[bool, Dict[str, bool]]:
    state = get_state()
    state[flag] = not state.get(flag, False)
    _write_state(state)
    return state[flag], state


def _format_state(value: bool) -> str:
    return STATE_ON if value else STATE_OFF


def build_keyboard(state: Dict[str, bool]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(f"{BTN_TVOI}: {_format_state(state['tvoi'])}", callback_data="toggle_tvoi"),
                InlineKeyboardButton(f"{BTN_TRND}: {_format_state(state['trnd'])}", callback_data="toggle_trnd"),
            ],
            [InlineKeyboardButton(BTN_HELP, callback_data="show_help")],
        ]
    )


def _status_text(state: Dict[str, bool]) -> str:
    return (
        "Состояние рассылок:\n"
        f"{BTN_TVOI}: {_format_state(state['tvoi'])}\n"
        f"{BTN_TRND}: {_format_state(state['trnd'])}"
    )


def _confirmation(flag: str, enabled: bool) -> str:
    label = FLAG_LABELS.get(flag, flag.upper())
    if enabled:
        return f"Рассылка {label} включена"
    return f"Рассылка {label} приостановлена"


def _is_admin(user_id: Optional[int]) -> bool:
    if not ADMIN_IDS:
        return True
    if user_id is None:
        return False
    return user_id in ADMIN_IDS


async def _send_message_async(update: Update, text: str, state: Dict[str, bool]) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(text, reply_markup=build_keyboard(state))


def _send_message_sync(update: Update, text: str, state: Dict[str, bool]) -> None:
    message = update.effective_message
    if message is None:
        return
    message.reply_text(text, reply_markup=build_keyboard(state))


async def start_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    state = get_state()
    await _send_message_async(update, _status_text(state), state)


def start_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    state = get_state()
    _send_message_sync(update, _status_text(state), state)


async def status_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    state = get_state()
    await _send_message_async(update, _status_text(state), state)


def status_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    state = get_state()
    _send_message_sync(update, _status_text(state), state)


async def help_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    state = get_state()
    await _send_message_async(update, HELP_TEXT, state)


def help_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    state = get_state()
    _send_message_sync(update, HELP_TEXT, state)


async def _handle_toggle_async(update: Update, flag: str) -> None:
    query = update.callback_query
    user_id = query.from_user.id if query and query.from_user else None
    if not _is_admin(user_id):
        if query:
            await query.answer("Недостаточно прав", show_alert=True)
        LOGGER.warning("Unauthorized toggle attempt | flag=%s | user=%s", flag, user_id)
        return
    enabled, state = toggle_flag(flag)
    LOGGER.info("Flag toggled | flag=%s | enabled=%s | user=%s", flag, enabled, user_id)
    text = _status_text(state)
    if query:
        await query.answer(_confirmation(flag, enabled))
        if query.message:
            try:
                await query.message.edit_text(text, reply_markup=build_keyboard(state))
            except Exception as exc:  # pragma: no cover - telegram API race
                LOGGER.warning("Failed to edit message, sending new one | err=%s", exc)
                await query.message.reply_text(text, reply_markup=build_keyboard(state))


def _handle_toggle_sync(update: Update, flag: str) -> None:
    query = update.callback_query
    user_id = query.from_user.id if query and query.from_user else None
    if not _is_admin(user_id):
        if query:
            query.answer("Недостаточно прав", show_alert=True)
        LOGGER.warning("Unauthorized toggle attempt | flag=%s | user=%s", flag, user_id)
        return
    enabled, state = toggle_flag(flag)
    LOGGER.info("Flag toggled | flag=%s | enabled=%s | user=%s", flag, enabled, user_id)
    text = _status_text(state)
    if query:
        query.answer(_confirmation(flag, enabled))
        if query.message:
            try:
                query.message.edit_text(text, reply_markup=build_keyboard(state))
            except Exception as exc:  # pragma: no cover - telegram API race
                LOGGER.warning("Failed to edit message, sending new one | err=%s", exc)
                query.message.reply_text(text, reply_markup=build_keyboard(state))


async def callback_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    query = update.callback_query
    if not query or not query.data:
        return
    if query.data == "toggle_tvoi":
        await _handle_toggle_async(update, "tvoi")
        return
    if query.data == "toggle_trnd":
        await _handle_toggle_async(update, "trnd")
        return
    if query.data == "show_help":
        await query.answer("Подсказка отправлена")
        state = get_state()
        if query.message:
            await query.message.reply_text(HELP_TEXT, reply_markup=build_keyboard(state))
        return
    await query.answer()


def callback_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    query = update.callback_query
    if not query or not query.data:
        return
    if query.data == "toggle_tvoi":
        _handle_toggle_sync(update, "tvoi")
        return
    if query.data == "toggle_trnd":
        _handle_toggle_sync(update, "trnd")
        return
    if query.data == "show_help":
        query.answer("Подсказка отправлена")
        state = get_state()
        if query.message:
            query.message.reply_text(HELP_TEXT, reply_markup=build_keyboard(state))
        return
    query.answer()


async def _command_toggle_async(update: Update, flag: str, value: bool) -> None:
    user = update.effective_user
    if not _is_admin(user.id if user else None):
        await _send_message_async(update, "Недостаточно прав", get_state())
        LOGGER.warning("Unauthorized command toggle | flag=%s | user=%s", flag, user.id if user else None)
        return
    state = set_flag(flag, value)
    LOGGER.info("Flag set | flag=%s | value=%s | user=%s", flag, value, user.id if user else None)
    await _send_message_async(update, _confirmation(flag, value), state)


def _command_toggle_sync(update: Update, flag: str, value: bool) -> None:
    user = update.effective_user
    if not _is_admin(user.id if user else None):
        _send_message_sync(update, "Недостаточно прав", get_state())
        LOGGER.warning("Unauthorized command toggle | flag=%s | user=%s", flag, user.id if user else None)
        return
    state = set_flag(flag, value)
    LOGGER.info("Flag set | flag=%s | value=%s | user=%s", flag, value, user.id if user else None)
    _send_message_sync(update, _confirmation(flag, value), state)


async def on_tvoi_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    await _command_toggle_async(update, "tvoi", True)


def on_tvoi_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    _command_toggle_sync(update, "tvoi", True)


async def off_tvoi_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    await _command_toggle_async(update, "tvoi", False)


def off_tvoi_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    _command_toggle_sync(update, "tvoi", False)


async def on_trnd_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    await _command_toggle_async(update, "trnd", True)


def on_trnd_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    _command_toggle_sync(update, "trnd", True)


async def off_trnd_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[attr-defined]
    await _command_toggle_async(update, "trnd", False)


def off_trnd_sync(update: Update, context: "CallbackContext") -> None:  # type: ignore[valid-type]
    _command_toggle_sync(update, "trnd", False)


def _log_startup(state: Dict[str, bool]) -> None:
    LOGGER.info(
        "Menu bot starting | telegram_version=%s | admins=%s | state=%s",
        TG_VER,
        sorted(ADMIN_IDS) if ADMIN_IDS else "all",
        state,
    )


def main() -> None:
    if not MENU_BOT_TOKEN:
        raise RuntimeError("MENU_BOT_TOKEN is not configured")
    state = get_state()
    _log_startup(state)
    if PTB_V20:
        application = Application.builder().token(MENU_BOT_TOKEN).build()
        application.add_handler(CommandHandler("start", start_async))
        application.add_handler(CommandHandler("status", status_async))
        application.add_handler(CommandHandler("help", help_async))
        application.add_handler(CommandHandler("on_tvoi", on_tvoi_async))
        application.add_handler(CommandHandler("off_tvoi", off_tvoi_async))
        application.add_handler(CommandHandler("on_trnd", on_trnd_async))
        application.add_handler(CommandHandler("off_trnd", off_trnd_async))
        application.add_handler(CallbackQueryHandler(callback_async))
        application.run_polling()
    else:  # pragma: no cover - PTB v13 compatibility path
        updater = Updater(token=MENU_BOT_TOKEN, use_context=True)
        dispatcher = updater.dispatcher
        dispatcher.add_handler(CommandHandler("start", start_sync))
        dispatcher.add_handler(CommandHandler("status", status_sync))
        dispatcher.add_handler(CommandHandler("help", help_sync))
        dispatcher.add_handler(CommandHandler("on_tvoi", on_tvoi_sync))
        dispatcher.add_handler(CommandHandler("off_tvoi", off_tvoi_sync))
        dispatcher.add_handler(CommandHandler("on_trnd", on_trnd_sync))
        dispatcher.add_handler(CommandHandler("off_trnd", off_trnd_sync))
        dispatcher.add_handler(CallbackQueryHandler(callback_sync))
        updater.start_polling()
        updater.idle()


if __name__ == "__main__":
    main()
