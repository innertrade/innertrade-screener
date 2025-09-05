import os
import asyncio
import logging
from datetime import datetime, timedelta
from io import BytesIO

import pytz
import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL", "")
TZ = os.getenv("TZ", "Europe/Moscow")
VERSION = "v0.4"

logging.basicConfig(level=logging.INFO)

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

BYBIT_API = "https://api.bybit.com"
SEM_LIMIT = 3
REQUEST_TIMEOUT = 25

# runtime state (memory only)
USER_STATE = {}  # user_id: {mode, passive, watchlist, alert_vol, alert_pct, last_alerts}
DEFAULT_USER = {
    "exchange": "bybit",
    "passive": False,
    "watchlist": [],
    "alert_vol": 1.5,
    "alert_pct": 3.0,
    "last_alerts": {}
}

SYMBOLS_BYBIT = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
                 "DOGEUSDT","ADAUSDT","LINKUSDT","TRXUSDT","TONUSDT"]

# ---------------- UTILS ----------------
def ensure_user(uid:int):
    if uid not in USER_STATE:
        USER_STATE[uid] = DEFAULT_USER.copy()
        USER_STATE[uid]["watchlist"] = []
        USER_STATE[uid]["last_alerts"] = {}
    return USER_STATE[uid]

async def http_get_json(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as r:
            if r.status==200:
                return await r.json()
            else:
                return None
    except Exception as e:
        logging.warning(f"http_get_json fail: {e}")
        return None

async def render_header_text() -> str:
    # Заглушка с фиктивными числами (можно подключить реальные источники позже)
    btc_dom = "54.1% (+0.3)"
    funding = "+0.012%"
    fg = "34 (-3)"
    return f"🧭 Market mood\nBTC.D: {btc_dom} | Funding avg: {funding} | F&G: {fg}"

# ---------------- SCREENER MOCK ----------------
async def render_activity():
    return "🔥 Активность\n— пока тестовый вывод"

async def render_volatility():
    return "⚡ Волатильность\n— пока тестовый вывод"

async def render_trend():
    return "📈 Тренд\n— пока тестовый вывод"

# ---------------- BUBBLES ----------------
async def bybit_ticker(session, symbol):
    url = f"{BYBIT_API}/v5/market/tickers"
    params = {"category":"linear","symbol":symbol}
    return await http_get_json(session,url,params)

async def build_bubbles_data():
    async with aiohttp.ClientSession() as s:
        tasks = [asyncio.create_task(bybit_ticker(s,sym)) for sym in SYMBOLS_BYBIT]
        raws = await asyncio.gather(*tasks)
    out=[]
    for sym,data in zip(SYMBOLS_BYBIT, raws):
        try:
            lst = (data or {}).get("result",{}).get("list",[])
            if not lst: continue
            it = lst[0]
            pct=float(it.get("price24hPcnt",0.0))*100.0
            turnover=float(it.get("turnover24h",0.0))
            out.append({"symbol":sym,"pct":pct,"turnover":turnover})
        except: continue
    return out

def render_bubbles_png(items):
    buf=BytesIO()
    if not items:
        fig=plt.figure(figsize=(8,4),dpi=160)
        ax=fig.add_subplot(111); ax.axis("off")
        ax.text(0.5,0.5,"Нет данных для пузырьков",ha="center",va="center",fontsize=16)
        fig.savefig(buf,format="png"); plt.close(fig); return buf.getvalue()
    turnovers=np.array([max(1.0,it["turnover"]) for it in items],dtype=float)
    sizes=np.sqrt(turnovers); k=(8000.0/sizes.max()) if sizes.max()>0 else 1.0; s=sizes*k
    n=len(items); cols=int(np.ceil(np.sqrt(n))); rows=int(np.ceil(n/cols))
    xs,ys=[],[]
    for i in range(n): r=i//cols; c=i%cols; xs.append(c); ys.append(rows-1-r)
    xs=np.array(xs); ys=np.array(ys)
    colors=[]
    for it in items:
        if it["pct"]>0.5: colors.append("#16a34a")
        elif it["pct"]<-0.5: colors.append("#dc2626")
        else: colors.append("#6b7280")
    labels=[f"{it['symbol']}\n{it['pct']:+.1f}%" for it in items]
    fig=plt.figure(figsize=(12,7),dpi=160); ax=fig.add_subplot(111)
    ax.set_facecolor("#0b1020"); fig.patch.set_facecolor("#0b1020")
    ax.scatter(xs,ys,s=s,c=colors,alpha=0.85)
    for x,y,lab in zip(xs,ys,labels):
        ax.text(x,y,lab,ha="center",va="center",color="white",fontsize=9,weight="bold")
    ax.set_xticks([]); ax.set_yticks([]); ax.set_xlim(-0.8,cols-0.2); ax.set_ylim(-0.2,rows-0.2)
    ax.set_title("Daily Bubbles (Bybit, 24h % | size ~ turnover)",color="white",fontsize=14)
    fig.savefig(buf,format="png",facecolor=fig.get_facecolor()); plt.close(fig)
    return buf.getvalue()

async def render_bubbles_message():
    data=await build_bubbles_data()
    header=await render_header_text()
    png=render_bubbles_png(data)
    return header,png

# ---------------- EXCEL CALC ----------------
async def build_risk_excel_template()->bytes:
    wb=Workbook(); ws=wb.active; ws.title="RiskCalc"
    headers=["Equity (USDT)","Risk %","Side","Entry","Stop",
             "Risk $","Stop Dist","Qty","Leverage","Notional $","TP1","TP2","TP3"]
    ws.append(headers)
    ws["A2"]=10000; ws["B2"]=0.01; ws["C2"]="LONG"; ws["D2"]=100.0; ws["E2"]=95.0; ws["I2"]=5
    ws["F2"]="=A2*B2"
    ws["G2"]='=ABS(IF(UPPER(C2)="LONG",D2-E2,E2-D2))'
    ws["H2"]="=IF(G2>0,F2/G2,0)"
    ws["J2"]="=H2*D2"
    ws["K2"]='=IF(UPPER(C2)="LONG",D2+G2,D2-G2)'
    ws["L2"]='=IF(UPPER(C2)="LONG",D2+2*G2,D2-2*G2)'
    ws["M2"]='=IF(UPPER(C2)="LONG",D2+3*G2,D2-3*G2)'
    bio=BytesIO(); wb.save(bio); return bio.getvalue()

# ---------------- MENUS ----------------
def bottom_menu_kb()->ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Активность"),KeyboardButton(text="⚡ Волатильность")],
            [KeyboardButton(text="📈 Тренд"),KeyboardButton(text="🫧 Bubbles")],
            [KeyboardButton(text="📰 Новости"),KeyboardButton(text="🧮 Калькулятор")],
            [KeyboardButton(text="⭐ Watchlist"),KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,is_persistent=True,
        input_field_placeholder="Выберите раздел…",
    )

# ---------------- HANDLERS ----------------
@dp.message(F.text=="📊 Активность")
async def on_activity(m:Message):
    header=await render_header_text(); body=await render_activity()
    await m.answer(header+"\n"+body,reply_markup=bottom_menu_kb())

@dp.message(F.text=="⚡ Волатильность")
async def on_vol(m:Message):
    header=await render_header_text(); body=await render_volatility()
    await m.answer(header+"\n"+body,reply_markup=bottom_menu_kb())

@dp.message(F.text=="📈 Тренд")
async def on_trend(m:Message):
    header=await render_header_text(); body=await render_trend()
    await m.answer(header+"\n"+body,reply_markup=bottom_menu_kb())

@dp.message(F.text=="🫧 Bubbles")
async def on_bubbles(m:Message):
    try:
        header,png=await render_bubbles_message()
        await m.answer_photo(photo=png,caption=header,reply_markup=bottom_menu_kb())
    except: await m.answer("Не удалось построить Bubbles.")

@dp.message(F.text=="🧮 Калькулятор")
async def on_calc(m:Message):
    data=await build_risk_excel_template()
    await m.answer_document(document=("risk_calc.xlsx",data),caption="Шаблон риск-менеджмента")

@dp.message(F.text=="⭐ Watchlist")
async def on_watchlist(m:Message):
    u=ensure_user(m.from_user.id)
    if not u["watchlist"]:
        await m.answer("Watchlist пуст. Добавь командой /add SYMBOL (например /add SOLUSDT)")
    else:
        text="⭐ Твой Watchlist:\n"+"\n".join(["• "+x for x in u["watchlist"]])
        await m.answer(text)

@dp.message(F.text=="📰 Новости")
async def on_news(m:Message):
    header=await render_header_text()
    await m.answer(header+"\n\n📰 Макро (последний час)\n• CPI (US) 3.1% vs 3.2% прогноз — риск-он")

@dp.message(F.text=="⚙️ Настройки")
async def on_settings(m:Message):
    u=ensure_user(m.from_user.id)
    await m.answer(f"Настройки:\nБиржа: {u['exchange']}\n"
                   f"Пассивный режим: {u['passive']}\n"
                   f"Алерты: Vol>{u['alert_vol']} | |pct|>{u['alert_pct']}")

# watchlist commands
@dp.message(F.text.startswith("/add"))
async def cmd_add(m:Message):
    parts=m.text.split()
    if len(parts)<2: await m.answer("Формат: /add SYMBOL"); return
    sym=parts[1].upper()
    u=ensure_user(m.from_user.id)
    if sym not in u["watchlist"]: u["watchlist"].append(sym)
    await m.answer(f"Добавил {sym} в Watchlist.")

@dp.message(F.text.startswith("/rm"))
async def cmd_rm(m:Message):
    parts=m.text.split()
    if len(parts)<2: await m.answer("Формат: /rm SYMBOL"); return
    sym=parts[1].upper()
    u=ensure_user(m.from_user.id)
    if sym in u["watchlist"]: u["watchlist"].remove(sym)
    await m.answer(f"Убрал {sym} из Watchlist.")

@dp.message(F.text=="/watchlist")
async def cmd_watchlist(m:Message):
    u=ensure_user(m.from_user.id)
    if not u["watchlist"]: await m.answer("Watchlist пуст.")
    else: await m.answer("⭐ Watchlist:\n"+"\n".join(["• "+x for x in u["watchlist"]]))

# passive toggle
@dp.message(F.text=="/passive")
async def cmd_passive(m:Message):
    u=ensure_user(m.from_user.id); u["passive"]=True
    await m.answer("Пассивный режим включен.")

@dp.message(F.text=="/active")
async def cmd_active(m:Message):
    u=ensure_user(m.from_user.id); u["passive"]=False
    await m.answer("Пассивный режим выключен.")

@dp.message(F.text=="/status")
async def cmd_status(m:Message):
    u=ensure_user(m.from_user.id)
    await m.answer(f"Status\nTime: {datetime.now(pytz.timezone(TZ))}\n"
                   f"Mode: {'passive' if u['passive'] else 'active'}\n"
                   f"Exchange: {u['exchange']}\nWatchlist: {u['watchlist']}\nVersion: {VERSION}")

@dp.message(F.text=="/diag")
async def cmd_diag(m:Message):
    await m.answer("Diag OK — Bybit sources reachable (mock).")

# ---------------- BACKGROUND JOB ----------------
async def passive_loop():
    while True:
        for uid,u in USER_STATE.items():
            if not u["passive"]: continue
            try:
                header=await render_header_text()
                msg="Автосигнал (demo)\n— пока что просто заглушка."
                await bot.send_message(uid,header+"\n"+msg)
            except Exception as e:
                logging.warning(f"passive_loop error: {e}")
        await asyncio.sleep(900)  # 15 min

# ---------------- STARTUP ----------------
@dp.message(F.text=="/start")
async def cmd_start(m:Message):
    ensure_user(m.from_user.id)
    header=await render_header_text()
    await m.answer(header+"\n\nДобро пожаловать в <b>Innertrade Screener</b>!",
                   reply_markup=bottom_menu_kb())

async def main():
    asyncio.create_task(passive_loop())
    await dp.start_polling(bot)

if __name__=="__main__":
    asyncio.run(main())
