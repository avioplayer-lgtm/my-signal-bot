import os
import time
import uuid
import logging
import threading
import requests
import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("BOT_TOKEN and CHAT_ID must be set as environment variables.")
CAPITAL = float(os.environ.get("CAPITAL", 30000))
RISK_PCT = 0.005
MAX_DAILY_LOSS = CAPITAL * 0.02
MIN_CONFIDENCE = 6
IST = pytz.timezone("Asia/Kolkata")
SYMBOLS = {
    "NIFTY": {"yahoo": "^NSEI", "interval": 50, "lot": 75, "expiry_day": 1},
    "BANKNIFTY": {"yahoo": "^NSEBANK", "interval": 100, "lot": 35, "expiry_day": None},
}
STRATEGY_RANK = {"TRENDING": 3, "VOLATILE": 2, "SIDEWAYS": 1}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()],
)
log = logging.getLogger("SignalBot")

_lock = threading.Lock()
regime_state = {name: {"last": None, "count": 0} for name in SYMBOLS}
state = {
    "active_trade": None,
    "pending_signals": {},
    "daily_loss": 0.0,
    "current_day": None,
    "rules_sent": {"open": False, "mid": False, "close": False},
    "last_heartbeat_hour": -1,
    "holiday_sent": False,
}

def get_st(key):
    with _lock:
        return state[key]

def set_st(key, val):
    with _lock:
        state[key] = val

def _tg(endpoint, payload):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
            json=payload, timeout=10,
        )
        return r.json()
    except Exception as e:
        log.error(f"Telegram error ({endpoint}): {e}")
        return {}

def send_text(text):
    res = _tg("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
    return res.get("result", {}).get("message_id")

def send_with_buttons(text, signal_id):
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "Take Trade", "callback_data": f"take|{signal_id}"},
                {"text": "Skip", "callback_data": f"skip|{signal_id}"},
            ],
            [{"text": "Remind in 5 min", "callback_data": f"remind|{signal_id}"}],
        ]
    }
    res = _tg("sendMessage", {
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "Markdown", "reply_markup": keyboard,
    })
    return res.get("result", {}).get("message_id")

def edit_message(message_id, text, keep_buttons=False):
    payload = {"chat_id": CHAT_ID, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
    if not keep_buttons:
        payload["reply_markup"] = {"inline_keyboard": []}
    _tg("editMessageText", payload)

def answer_callback(callback_id, text=""):
    _tg("answerCallbackQuery", {"callback_query_id": callback_id, "text": text, "show_alert": False})

def handle_callback(query):
    cb_id = query["id"]
    data = query.get("data", "")
    message_id = query.get("message", {}).get("message_id")
    if "|" not in data:
        answer_callback(cb_id, "Unknown action")
        return
    action, signal_id = data.split("|", 1)
    pending = get_st("pending_signals")
    signal = pending.get(signal_id)
    if not signal:
        answer_callback(cb_id, "Signal expired")
        if message_id:
            edit_message(message_id, "Signal expired - already handled or timed out.")
        return
    if action == "take":
        with _lock:
            if state["active_trade"]:
                ex = state["active_trade"]
                answer_callback(cb_id, "Trade already open!")
                edit_message(message_id,
                    f"Blocked - already have open trade:\n"
                    f"{ex['symbol']} {ex['atm_strike']} {ex['direction']}\n"
                    f"Close that first.")
                return
            state["active_trade"] = signal
            del state["pending_signals"][signal_id]
        answer_callback(cb_id, "Trade logged!")
        s = signal
        edit_message(message_id,
            f"*Trade Taken*\n\n"
            f"*{s['symbol']}* {s['atm_strike']} {s['direction']}\n"
            f"Regime: {s['regime']}\nStrategy: {s['strategy']}\n\n"
            f"Entry est : Rs.{s['atm_prem']} per unit\n"
            f"Stop Loss : Rs.{s['sl_prem']}\n"
            f"Target    : Rs.{s['tgt_prem']}\n"
            f"Lot size  : {s['lot']} units\n\n"
            f"Bot will notify when SL or Target is hit.")
        log.info(f"Trade taken: {s['symbol']} {s['atm_strike']} {s['direction']}")
    elif action == "skip":
        with _lock:
            pending.pop(signal_id, None)
        answer_callback(cb_id, "Skipped")
        s = signal
        edit_message(message_id,
            f"Skipped: {s['symbol']} {s['atm_strike']} {s['direction']}\n"
            f"Watching for next signal...")
        log.info(f"Signal skipped: {s['symbol']} {s['atm_strike']} {s['direction']}")
    elif action == "remind":
        answer_callback(cb_id, "Will remind at next scan")
        if message_id:
            edit_message(message_id,
                f"Reminder set: {signal['symbol']} {signal['atm_strike']} {signal['direction']}\n"
                f"Bot will re-alert in ~5 min.", keep_buttons=True)
    else:
        answer_callback(cb_id, "Unknown action")

def telegram_polling_thread():
    log.info("Telegram polling thread started")
    offset = 0
    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 25, "allowed_updates": ["callback_query"]},
                timeout=30,
            )
            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                if "callback_query" in upd:
                    try:
                        handle_callback(upd["callback_query"])
                    except Exception as e:
                        log.error(f"Callback error: {e}")
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"Polling thread error: {e}")
            time.sleep(5)

def compute_indicators(df):
    df = df.copy()
    df["ema9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["ema21"] = df["Close"].ewm(span=21, adjust=False).mean()
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-9)))
    df["atr"] = (df["High"] - df["Low"]).rolling(10).mean()
    cum_vol = df["Volume"].cumsum()
    cum_pv = (df["Close"] * df["Volume"]).cumsum()
    df["vwap"] = cum_pv / cum_vol.replace(0, 1e-9)
    return df

def detect_regime(df, atr, ema9, ema21):
    recent = df.iloc[-10:]
    rng = recent["High"].max() - recent["Low"].min()
    ema_diff = abs(ema9 - ema21)
    if ema_diff > atr * 0.6 and rng > atr * 4:
        return "TRENDING"
    if ema_diff < atr * 0.3 and rng < atr * 3:
        return "SIDEWAYS"
    if rng > atr * 5:
        return "VOLATILE"
    return "NORMAL"

def confirm_regime(symbol, new_regime):
    rs = regime_state[symbol]
    if rs["last"] == new_regime:
        rs["count"] += 1
    else:
        rs["last"] = new_regime
        rs["count"] = 1
    return new_regime if rs["count"] >= 2 else None

def strategy_breakout(df, atr, ema9, ema21, rsi, vwap):
    orb_high = float(df.iloc[:3]["High"].max())
    orb_low = float(df.iloc[:3]["Low"].min())
    close = float(df.iloc[-1]["Close"])
    conf = 0
    if close > orb_high or close < orb_low:
        conf += 3
    if (ema9 > ema21 and close > orb_high) or (ema9 < ema21 and close < orb_low):
        conf += 3
    if (rsi > 55 and close > orb_high) or (rsi < 45 and close < orb_low):
        conf += 2
    if conf < MIN_CONFIDENCE:
        return None
    if close > orb_high and close > vwap and ema9 > ema21:
        return "CE", close, close - atr, close + atr * 2, conf
    if close < orb_low and close < vwap and ema9 < ema21:
        return "PE", close, close + atr, close - atr * 2, conf
    return None

def strategy_range_trade(df, atr, ema9, ema21, rsi):
    recent = df.iloc[-10:]
    high = float(recent["High"].max())
    low = float(recent["Low"].min())
    close = float(df.iloc[-1]["Close"])
    buffer = atr * 0.3
    conf = 0
    if abs(ema9 - ema21) < atr * 0.3:
        conf += 3
    if close <= low + buffer or close >= high - buffer:
        conf += 3
    if (close <= low + buffer and rsi < 40) or (close >= high - buffer and rsi > 60):
        conf += 2
    if conf < MIN_CONFIDENCE:
        return None
    if close <= low + buffer:
        return "CE", close, close - atr * 0.8, close + atr * 1.2, conf
    if close >= high - buffer:
        return "PE", close, close + atr * 0.8, close - atr * 1.2, conf
    return None

def strategy_momentum(df, atr, rsi):
    last = df.iloc[-1]
    body = abs(float(last["Close"]) - float(last["Open"]))
    rng = float(last["High"]) - float(last["Low"])
    close = float(last["Close"])
    conf = 0
    
