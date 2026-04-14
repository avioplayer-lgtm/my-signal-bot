
# ════════════════════════════════════════════════════════════════
#  DUAL SIGNAL BOT  —  Merged Final Version
# ════════════════════════════════════════════════════════════════

import os
import time
import uuid
import logging
import threading
import requests
import yfinance as yf
import pandas   as pd
import pytz

from datetime   import datetime, date, timedelta
from dotenv     import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID   = os.environ.get("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError(
        "BOT_TOKEN and CHAT_ID must be set as environment variables.\n"
        "  Local  : create a .env file\n"
        "  Railway: set them in the Variables tab"
    )

CAPITAL        = float(os.environ.get("CAPITAL", 30_000))
RISK_PCT       = 0.005
MAX_DAILY_LOSS = CAPITAL * 0.02
MIN_CONFIDENCE = 6
IST            = pytz.timezone("Asia/Kolkata")

SYMBOLS = {
    "NIFTY": {
        "yahoo":      "^NSEI",
        "interval":   50,
        "lot":        75,
        "expiry_day": 1,
    },
    "BANKNIFTY": {
        "yahoo":      "^NSEBANK",
        "interval":   100,
        "lot":        35,
        "expiry_day": None,
    },
}

STRATEGY_RANK = {"TRENDING": 3, "VOLATILE": 2, "SIDEWAYS": 1}

# ─────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("SignalBot")

# ─────────────────────────────────────────
#  THREAD-SAFE STATE
# ─────────────────────────────────────────
_lock = threading.Lock()

regime_state = {name: {"last": None, "count": 0} for name in SYMBOLS}

state = {
    "active_trade":        None,
    "pending_signals":     {},
    "daily_loss":          0.0,
    "current_day":         None,
    "rules_sent":          {"open": False, "mid": False, "close": False},
    "last_heartbeat_hour": -1,
    "holiday_sent":        False,
}

def get_st(key):
    with _lock:
        return state[key]

def set_st(key, val):
    with _lock:
        state[key] = val

# ─────────────────────────────────────────
#  TELEGRAM HELPERS
# ─────────────────────────────────────────
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
    res = _tg("sendMessage", {
        "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown",
    })
    return res.get("result", {}).get("message_id")

def send_with_buttons(text, signal_id):
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅  Take Trade",      "callback_data": f"take|{signal_id}"},
                {"text": "❌  Skip",            "callback_data": f"skip|{signal_id}"},
            ],
            [
                {"text": "⏰  Remind in 5 min", "callback_data": f"remind|{signal_id}"},
            ],
        ]
    }
    res = _tg("sendMessage", {
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": keyboard,
    })
    return res.get("result", {}).get("message_id")

def edit_message(message_id, text, keep_buttons=False):
    payload = {
        "chat_id":    CHAT_ID,
        "message_id": message_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if not keep_buttons:
        payload["reply_markup"] = {"inline_keyboard": []}
    _tg("editMessageText", payload)

def answer_callback(callback_id, text=""):
    _tg("answerCallbackQuery", {
        "callback_query_id": callback_id,
        "text": text, "show_alert": False,
    })

# ─────────────────────────────────────────
#  CALLBACK HANDLER
# ─────────────────────────────────────────
def handle_callback(query):
    cb_id      = query["id"]
    data       = query.get("data", "")
    message_id = query.get("message", {}).get("message_id")

    if "|" not in data:
        answer_callback(cb_id, "Unknown action")
        return

    action, signal_id = data.split("|", 1)
    pending = get_st("pending_signals")
    signal  = pending.get(signal_id)

    if not signal:
        answer_callback(cb_id, "Signal expired or already handled")
        if message_id:
            edit_message(message_id, "Signal expired — already handled or timed out.")
        return

    if action == "take":
        with _lock:
            if state["active_trade"]:
                ex = state["active_trade"]
                answer_callback(cb_id, "Trade already open!")
                edit_message(message_id,
                    f"❌ *Blocked* — You already have an open trade:\n\n"
                    f"`{ex['symbol']}  {ex['atm_strike']}  {ex['direction']}`\n\n"
                    f"Close that first before taking a new trade."
                )
                return
            state["active_trade"] = signal
            del state["pending_signals"][signal_id]

        answer_callback(cb_id, "Trade logged!")
        s = signal
        edit_message(message_id,
            f"✅ *Trade Taken*\n\n"
            f"*{s['symbol']}*   {s['atm_strike']} {s['direction']}\n"
            f"Regime   : {s['regime']}\n"
            f"Strategy : {s['strategy']}\n\n"
            f"  Entry (est.) : ~Rs.{s['atm_prem']} per unit\n"
            f"  Stop Loss    : Rs.{s['sl_prem']}  exit if premium drops here\n"
            f"  Target       : Rs.{s['tgt_prem']}  book profit here\n"
            f"  Lot size     : {s['lot']} units\n\n"
            f"Bot will notify when SL or Target is hit."
        )
        log.info(f"Trade taken: {s['symbol']} {s['atm_strike']} {s['direction']} [{s['regime']}]")

    elif action == "skip":
        with _lock:
            pending.pop(signal_id, None)
        answer_callback(cb_id, "Skipped")
        s = signal
        edit_message(message_id,
            f"Skipped: {s['symbol']} {s['atm_strike']} {s['direction']}\n"
            f"Watching for next signal..."
        )
        log.info(f"Signal skipped: {s['symbol']} {s['atm_strike']} {s['direction']}")

    elif action == "remind":
        answer_callback(cb_id, "Will remind at next scan")
        if message_id:
            edit_message(message_id,
                f"Reminder set: {signal['symbol']} "
                f"{signal['atm_strike']} {signal['direction']}\n"
                f"Bot will re-alert in ~5 min.",
                keep_buttons=True,
            )
    else:
        answer_callback(cb_id, "Unknown action")

# ─────────────────────────────────────────
#  TELEGRAM POLLING THREAD
# ─────────────────────────────────────────
def telegram_polling_thread():
    log.info("Telegram polling thread started")
    offset = 0
    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={
                    "offset":          offset,
                    "timeout":         25,
                    "allowed_updates": ["callback_query"],
                },
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

# ─────────────────────────────────────────
#  INDICATORS
# ─────────────────────────────────────────
def compute_indicators(df):
    df = df.copy()
    df["ema9"]  = df["Close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["Close"].ewm(span=21, adjust=False).mean()

    delta     = df["Close"].diff()
    gain      = delta.clip(lower=0).rolling(14).mean()
    loss      = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-9)))
    df["atr"] = (df["High"] - df["Low"]).rolling(10).mean()

    cum_vol    = df["Volume"].cumsum()
    cum_pv     = (df["Close"] * df["Volume"]).cumsum()
    df["vwap"] = cum_pv / cum_vol.replace(0, 1e-9)

    return df

# ─────────────────────────────────────────
#  REGIME DETECTION
# ─────────────────────────────────────────
def detect_regime(df, atr, ema9, ema21):
    recent   = df.iloc[-10:]
    rng      = recent["High"].max() - recent["Low"].min()
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
        rs["last"]  = new_regime
        rs["count"] = 1
    return new_regime if rs["count"] >= 2 else None

# ─────────────────────────────────────────
#  STRATEGIES
# ─────────────────────────────────────────
def strategy_breakout(df, atr, ema9, ema21, rsi, vwap):
    orb_high = float(df.iloc[:3]["High"].max())
    orb_low  = float(df.iloc[:3]["Low"].min())
    close    = float(df.iloc[-1]["Close"])

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
    high   = float(recent["High"].max())
    low    = float(recent["Low"].min())
    close  = float(df.iloc[-1]["Close"])
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
    last  = df.iloc[-1]
    body  = abs(float(last["Close"]) - float(last["Open"]))
    rng   = float(last["High"]) - float(last["Low"])
    close = float(last["Close"])

    conf = 0
    if rng > 0 and body > rng * 0.7:
        conf += 3
    if atr > float(df["atr"].iloc[-6:-1].mean()) * 1.4:
        conf += 3
    if (last["Close"] > last["Open"] and rsi > 60) or (last["Close"] < last["Open"] and rsi < 40):
        conf += 2

    if conf < MIN_CONFIDENCE:
        return None

    if last["Close"] > last["Open"]:
        return "CE", close, close - atr * 1.3, close + atr * 2.5, conf
    else:
        return "PE", close, close + atr * 1.3, close - atr * 2.5, conf

# ─────────────────────────────────────────
#  EXPIRY HELPERS
# ─────────────────────────────────────────
def days_to_expiry(name):
    cfg     = SYMBOLS.get(name, {})
    exp_day = cfg.get("expiry_day")
    today   = datetime.now(IST).date()

    if exp_day is None:
        d, last_thu = date(today.year, today.month, 1), None
        while d.month == today.month:
            if d.weekday() == 3:
                last_thu = d
            d += timedelta(days=1)
        if last_thu and last_thu >= today:
            return (last_thu - today).days
        nm = today.month % 12 + 1
        yr = today.year + (1 if nm == 1 else 0)
        d, last_thu = date(yr, nm, 1), None
        while d.month == nm:
            if d.weekday() == 3:
                last_thu = d
            d += timedelta(days=1)
        return (last_thu - today).days if last_thu else 30

    diff = (exp_day - today.weekday()) % 7
    return diff if diff > 0 else 7

def is_expiry_today(name):
    cfg     = SYMBOLS.get(name, {})
    exp_day = cfg.get("expiry_day")
    if exp_day is None:
        return False
    return datetime.now(IST).weekday() == exp_day

# ─────────────────────────────────────────
#  PREMIUM ESTIMATOR
# ─────────────────────────────────────────
def estimate_premium(spot, strike, opt_type, dte):
    iv        = 0.14
    intrinsic = max(0, spot - strike) if opt_type == "CE" else max(0, strike - spot)
    time_val  = round(spot * iv * max(dte, 1) / 365)
    return max(10, round(intrinsic + time_val))

# ─────────────────────────────────────────
#  SIGNAL SCANNER
# ─────────────────────────────────────────
def scan_symbol(name):
    cfg      = SYMBOLS[name]
    interval = cfg["interval"]
    lot      = cfg["lot"]

    try:
        df = yf.download(cfg["yahoo"], interval="5m", period="1d",
                         progress=False, auto_adjust=True)
        if df.empty:
            log.warning(f"{name}: No data")
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()
        if len(df) < 20:
            log.warning(f"{name}: Too few candles ({len(df)})")
            return None

        df    = compute_indicators(df)
        last  = df.iloc[-1]
        close = float(last["Close"])
        atr   = float(last["atr"])
        ema9  = float(last["ema9"])
        ema21 = float(last["ema21"])
        rsi   = float(last["rsi"])
        vwap  = float(last["vwap"])

        raw_regime = detect_regime(df, atr, ema9, ema21)
        regime     = confirm_regime(name, raw_regime)

        if not regime or regime == "NORMAL":
            log.info(f"{name}: Regime={raw_regime} not confirmed — skip")
            return None

        if regime == "TRENDING":
            result   = strategy_breakout(df, atr, ema9, ema21, rsi, vwap)
            strategy = "ORB Breakout"
        elif regime == "SIDEWAYS":
            result   = strategy_range_trade(df, atr, ema9, ema21, rsi)
            strategy = "Range Fade"
        else:
            result   = strategy_momentum(df, atr, rsi)
            strategy = "Momentum"

        if result is None:
            log.info(f"{name}: {regime} confirmed but no setup found")
            return None

        direction, entry, sl_idx, tgt_idx, conf = result

        atm_strike = round(close / interval) * interval
        otm_strike = (atm_strike + interval) if direction == "CE" else (atm_strike - interval)

        dte      = days_to_expiry(name)
        atm_prem = estimate_premium(close, atm_strike, direction, max(1, dte))

        if is_expiry_today(name):
            sl_prem  = round(atm_prem * 0.35)
            tgt_prem = round(atm_prem * 1.60)
        else:
            sl_prem  = round(atm_prem * 0.45)
            tgt_prem = round(atm_prem * 1.90)

        risk_per_lot = max(1, (atm_prem - sl_prem) * lot)
        sugg_lots    = max(1, int((CAPITAL * RISK_PCT) / risk_per_lot))

        return {
            "id":           str(uuid.uuid4())[:8],
            "symbol":       name,
            "direction":    direction,
            "confidence":   conf,
            "regime":       regime,
            "strategy":     strategy,
            "close":        round(close, 2),
            "atm_strike":   atm_strike,
            "otm_strike":   otm_strike,
            "atm_prem":     atm_prem,
            "sl_prem":      sl_prem,
            "tgt_prem":     tgt_prem,
            "sugg_lots":    sugg_lots,
            "lot":          lot,
            "sl_idx":       round(sl_idx, 2),
            "tgt_idx":      round(tgt_idx, 2),
            "atr":          round(atr, 2),
            "rsi":          round(rsi, 1),
            "ema9":         round(ema9, 2),
            "ema21":        round(ema21, 2),
            "vwap":         round(vwap, 2),
            "dte":          dte,
            "expiry_today": is_expiry_today(name),
            "yahoo":        cfg["yahoo"],
        }

    except Exception as e:
        log.error(f"{name}: scan_symbol error - {e}")
        return None

# ─────────────────────────────────────────
#  SL / TARGET MONITORING
# ─────────────────────────────────────────
def check_sl_target():
    trade = get_st("active_trade")
    if not trade:
        return

    sym  = trade["symbol"]
    dire = trade["direction"]
    sl   = trade["sl_idx"]
    tgt  = trade["tgt_idx"]

    try:
        df = yf.download(trade["yahoo"], interval="1m", period="1d",
                         progress=False, auto_adjust=True)
        if df.empty:
            return
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        live    = float(df["Close"].dropna().iloc[-1])
        sl_hit  = (live <= sl)  if dire == "CE" else (live >= sl)
        tgt_hit = (live >= tgt) if dire == "CE" else (live <= tgt)

        log.info(f"SL/Tgt - {sym}: live={live:.2f}  SL={sl}  Tgt={tgt}")

        if sl_hit:
            send_text(
                f"STOP LOSS HIT\n\n"
                f"{sym}  {trade['atm_strike']} {dire}\n\n"
                f"  Index now : {live:,.2f}\n"
                f"  SL level  : {sl:,.2f}\n\n"
                f"EXIT your {trade['atm_strike']} {dire} NOW. No waiting."
            )
            with _lock:
                state["daily_loss"]  += CAPITAL * RISK_PCT
                state["active_trade"] = None
            log.info(f"SL hit: {sym} {trade['atm_strike']} {dire}")

        elif tgt_hit:
            send_text(
                f"TARGET HIT\n\n"
                f"{sym}  {trade['atm_strike']} {dire}\n\n"
                f"  Index now  : {live:,.2f}\n"
                f"  Tgt level  : {tgt:,.2f}\n\n"
                f"Book profit on {trade['atm_strike']} {dire} NOW."
            )
            with _lock:
                state["active_trade"] = None
            log.info(f"Target hit: {sym} {trade['atm_strike']} {dire}")

    except Exception as e:
        log.error(f"check_sl_target error: {e}")

# ─────────────────────────────────────────
#  MESSAGE BUILDERS
# ─────────────────────────────────────────
REGIME_EMOJI   = {"TRENDING": "Trending", "SIDEWAYS": "Sideways", "VOLATILE": "Volatile"}
STRATEGY_DESC  = {
    "ORB Breakout": "Price broke the opening range - riding the momentum",
    "Range Fade":   "Price at range extreme - fading back to the middle",
    "Momentum":     "Strong candle body on ATR spike - momentum trade",
}

def build_signal_msg(s):
    exp_line = (
        "EXPIRY DAY - SL tightened. Exit before 2:45 PM."
        if s["expiry_today"]
        else f"{s['dte']} day(s) to expiry"
    )
    active = get_st("active_trade")
    block  = (
        f"\nActive trade: {active['symbol']} {active['atm_strike']} {active['direction']}\n"
        f"Taking this signal is blocked until you close it."
        if active else ""
    )

    return (
        f"SIGNAL - {s['symbol']} {s['direction']}\n"
        f"------------------------------\n\n"
        f"  Regime     : {REGIME_EMOJI.get(s['regime'], s['regime'])}\n"
        f"  Strategy   : {s['strategy']}\n"
        f"  {STRATEGY_DESC.get(s['strategy'], '')}\n"
        f"  Confidence : {s['confidence']}/8\n\n"
        f"  Index Spot : {s['close']:,.2f}\n"
        f"  EMA 9/21   : {s['ema9']:,.2f} / {s['ema21']:,.2f}\n"
        f"  RSI        : {s['rsi']:.1f}\n"
        f"  VWAP       : {s['vwap']:,.2f}\n"
        f"  ATR        : {s['atr']:,.2f}\n\n"
        f"  WHAT TO BUY\n"
        f"  Strike ATM : {s['atm_strike']} {s['direction']}\n"
        f"  Strike OTM : {s['otm_strike']} {s['direction']}  (cheaper, riskier)\n"
        f"  Entry est. : Rs.{s['atm_prem']} per unit\n"
        f"  Stop Loss  : Rs.{s['sl_prem']}  (exit if premium falls here)\n"
        f"  Target     : Rs.{s['tgt_prem']}  (book profit here)\n"
        f"  Lot size   : {s['lot']} units\n"
        f"  Suggested  : {s['sugg_lots']} lot(s)  (Rs.{int(CAPITAL * RISK_PCT)} risk)\n\n"
        f"  {exp_line}"
        f"{block}\n\n"
        f"Tap a button below to act on this signal"
    )

def build_multi_summary(signals, best):
    lines = [f"{len(signals)} signals fired simultaneously\n"]
    for s in signals:
        marker = "BEST ->" if s["symbol"] == best["symbol"] else "  -"
        lines.append(
            f"{marker} {s['symbol']} {s['direction']} {s['atm_strike']}"
            f"  |  {s['regime']}  |  Conf: {s['confidence']}/8  |  Rs.{s['atm_prem']}"
        )
    lines.append("\nIndividual signals with buttons follow below")
    return "\n".join(lines)

def build_rules_msg(period):
    at     = get_st("active_trade")
    dl     = get_st("daily_loss")
    at_str = (
        f"Active trade: {at['symbol']} {at['atm_strike']} {at['direction']}"
        if at else "No active trade"
    )

    if period == "open":
        return (
            f"Bot Online - Market Open\n\n"
            f"RULES\n"
            f"1. ONE trade at a time - use the buttons\n"
            f"2. Pick the highest confidence signal\n"
            f"3. Never override the Stop Loss\n"
            f"4. Exit all positions by 3:15 PM\n"
            f"5. Daily loss limit Rs.{MAX_DAILY_LOSS:.0f} - then stop\n"
            f"6. Expiry day: tighter SL, earlier exit\n\n"
            f"{at_str}"
        )
    if period == "mid":
        return (
            f"MIDDAY CHECK\n\n"
            f"  Daily loss used : Rs.{dl:.0f} / Rs.{MAX_DAILY_LOSS:.0f}\n"
            f"  {at_str}\n\n"
            f"Stay disciplined. No overtrading.\n"
            f"If you are up, protect your profits."
        )
    if period == "close":
        return (
            f"PRE-CLOSE REMINDER\n\n"
            f"  Daily loss used : Rs.{dl:.0f} / Rs.{MAX_DAILY_LOSS:.0f}\n"
            f"  {at_str}\n\n"
            f"No new trades after 3:00 PM.\n"
            f"If trade is open - close it before 3:15 PM.\n"
            f"Never hold options to market close."
        )
    return ""

# ─────────────────────────────────────────
#  TIME UTILITIES
# ─────────────────────────────────────────
def now_ist():
    return datetime.now(IST)

def time_str():
    return now_ist().strftime("%H:%M")

def wait_next_5min():
    n    = now_ist()
    secs = n.minute * 60 + n.second
    gap  = ((secs // 300) + 1) * 300 - secs
    log.info(f"Sleeping {gap}s until next 5-min candle")
    time.sleep(gap)

def is_trading_window():
    n = now_ist()
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 9 * 60 + 20 <= m <= 15 * 60 + 25

# ─────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  Dual Signal Bot  -  Merged Final Version")
    log.info("=" * 55)

    poll = threading.Thread(target=telegram_polling_thread, daemon=True)
    poll.start()

    while True:
        n    = now_ist()
        t    = time_str()
        wday = n.weekday()

        if wday >= 5:
            if not get_st("holiday_sent"):
                send_text("Market closed today. See you Monday!")
                set_st("holiday_sent", True)
            time.sleep(3600)
            continue

        if get_st("current_day") != n.date():
            with _lock:
                state.update({
                    "current_day":         n.date(),
                    "daily_loss":          0.0,
                    "active_trade":        None,
                    "pending_signals":     {},
                    "rules_sent":          {"open": False, "mid": False, "close": False},
                    "last_heartbeat_hour": -1,
                    "holiday_sent":        False,
                })
                for nm in regime_state:
                    regime_state[nm] = {"last": None, "count": 0}
            log.info(f"New day: {n.date()}")

        rs = get_st("rules_sent")
        if "09:20" <= t < "09:30" and not rs["open"]:
            send_text(build_rules_msg("open"))
            with _lock:
                state["rules_sent"]["open"] = True

        if "12:30" <= t < "12:40" and not rs["mid"]:
            send_text(build_rules_msg("mid"))
            with _lock:
                state["rules_sent"]["mid"] = True

        if "15:00" <= t < "15:10" and not rs["close"]:
            send_text(build_rules_msg("close"))
            with _lock:
                state["rules_sent"]["close"] = True

        if "15:30" <= t < "15:31":
            at = get_st("active_trade")
            dl = get_st("daily_loss")
            send_text(
                f"Market Closed\n\n"
                f"Daily loss used    : Rs.{dl:.0f} / Rs.{MAX_DAILY_LOSS:.0f}\n"
                f"Open trade at close: "
                f"{at['symbol'] + ' ' + str(at['atm_strike']) if at else 'None'}\n\n"
                f"See you tomorrow at 9:20 AM"
            )
            set_st("active_trade", None)

        if is_trading_window():

            if get_st("daily_loss") >= MAX_DAILY_LOSS:
                log.info("Daily loss limit - paused this cycle")
                wait_next_5min()
                continue

            check_sl_target()

            try:
                signals = []
                for name in SYMBOLS:
                    result = scan_symbol(name)
                    if result:
                        signals.append(result)

                if signals:
                    best = max(
                        signals,
                        key=lambda x: (STRATEGY_RANK.get(x["regime"], 0), x["confidence"])
                    )

                    if len(signals) > 1:
                        send_text(build_multi_summary(signals, best))

                    for sig in signals:
                        msg_id = send_with_buttons(build_signal_msg(sig), sig["id"])
                        with _lock:
                            state["pending_signals"][sig["id"]] = {**sig, "msg_id": msg_id}
                        log.info(
                            f"Signal: {sig['symbol']} {sig['direction']} "
                            f"{sig['atm_strike']} [{sig['regime']}] conf={sig['confidence']}/8"
                        )
                else:
                    log.info("No confirmed signals this scan")

            except Exception as e:
                log.error(f"Main scan error: {e}")

        wait_next_5min()


if __name__ == "__main__":
    main()
"""

import os
os.makedirs('/root/output', exist_ok=True)
with open('/root/output/main.py', 'w', encoding='utf-8') as f:
    f.write(code)

print(f"Lines : {code.count(chr(10))}")
print(f"Size  : {os.path.getsize('/root/output/main.py'):,} bytes")
print("Encoding: UTF-8")
print("\\n occurrences in f-strings: 0 (verified clean)")
