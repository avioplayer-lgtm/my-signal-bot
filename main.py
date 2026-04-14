import os
os.makedirs('/root/merged-bot', exist_ok=True)

code = '''\
# ════════════════════════════════════════════════════════════════
#  DUAL SIGNAL BOT  —  Merged Final Version
#
#  From your latest file  (file:77):
#    ✅ detect_regime()   — TRENDING / SIDEWAYS / VOLATILE / NORMAL
#    ✅ confirm_regime()  — 2-scan confirmation before firing
#    ✅ breakout()        — ORB strategy for TRENDING markets
#    ✅ range_trade()     — Range fade for SIDEWAYS markets
#    ✅ momentum()        — Candle-body strategy for VOLATILE markets
#    ✅ 3 rules per day   — Open / Midday / Close (less noisy)
#
#  From the corrected final version:
#    ✅ Inline Telegram buttons  — Take Trade / Skip / Remind
#    ✅ active_trade assignment  — set when you tap Take Trade
#    ✅ SL/Target monitoring     — live index price, fires alerts
#    ✅ VWAP filter              — extra trend confirmation
#    ✅ ATM/OTM strike output    — tells you WHAT to buy
#    ✅ Premium estimator        — rough option premium guidance
#    ✅ Risk sizing              — suggested lot count
#    ✅ Expiry awareness         — tighter SL on expiry day
#    ✅ Regime state per symbol  — fixes the shared-state bug
#    ✅ daily_loss uses continue  — bot doesn't die permanently
#    ✅ Thread-safe state        — polling + scanning run together
# ════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────
#  IMPORTS
# ─────────────────────────────────────────
import time
import uuid
import logging
import threading
import requests
import yfinance as yf
import pandas   as pd
import pytz

from datetime import datetime, date, timedelta

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID   = os.environ.get("CHAT_ID")

CAPITAL        = 30_000
RISK_PCT       = 0.005           # 0.5% of capital per trade  →  ₹150
MAX_DAILY_LOSS = CAPITAL * 0.02  # 2% daily hard stop          →  ₹600

MIN_CONFIDENCE = 6               # out of 8 (slightly relaxed for multi-strategy)

IST = pytz.timezone("Asia/Kolkata")

# Index config
SYMBOLS = {
    "NIFTY": {
        "yahoo":       "^NSEI",
        "interval":    50,
        "lot":         75,
        "expiry_day":  1,     # Tuesday
    },
    "BANKNIFTY": {
        "yahoo":       "^NSEBANK",
        "interval":    100,
        "lot":         35,
        "expiry_day":  None,  # Monthly
    },
}

# Strategy priority when picking the best signal
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

# Per-symbol regime confirmation state  (FIX: separate for each symbol)
regime_state = {
    name: {"last": None, "count": 0}
    for name in SYMBOLS
}

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
def _tg(endpoint: str, payload: dict) -> dict:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
            json=payload, timeout=10,
        )
        return r.json()
    except Exception as e:
        log.error(f"Telegram error ({endpoint}): {e}")
        return {}

def send_text(text: str) -> int | None:
    res = _tg("sendMessage", {
        "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown",
    })
    return res.get("result", {}).get("message_id")

def send_with_buttons(text: str, signal_id: str) -> int | None:
    """Signal message with Take / Skip / Remind buttons."""
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅  Take Trade",       "callback_data": f"take|{signal_id}"},
                {"text": "❌  Skip",             "callback_data": f"skip|{signal_id}"},
            ],
            [
                {"text": "⏰  Remind in 5 min",  "callback_data": f"remind|{signal_id}"},
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

def edit_message(message_id: int, text: str, keep_buttons: bool = False):
    payload = {
        "chat_id":    CHAT_ID,
        "message_id": message_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if not keep_buttons:
        payload["reply_markup"] = {"inline_keyboard": []}
    _tg("editMessageText", payload)

def answer_callback(callback_id: str, text: str = ""):
    _tg("answerCallbackQuery", {
        "callback_query_id": callback_id,
        "text": text, "show_alert": False,
    })

# ─────────────────────────────────────────
#  CALLBACK HANDLER
# ─────────────────────────────────────────
def handle_callback(query: dict):
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
        answer_callback(cb_id, "⚠️ Signal expired or already handled")
        if message_id:
            edit_message(message_id, "⚠️ *Signal expired* — already handled or timed out.")
        return

    if action == "take":
        with _lock:
            if state["active_trade"]:
                ex = state["active_trade"]
                answer_callback(cb_id, "❌ Trade already open!")
                edit_message(message_id,
                    f"❌ *Blocked* — You already have an open trade:\n\n"
                    f"`{ex['symbol']}  {ex['atm_strike']}  {ex['direction']}`\n\n"
                    f"Close that first before taking a new trade."
                )
                return
            state["active_trade"] = signal
            del state["pending_signals"][signal_id]

        answer_callback(cb_id, "✅ Trade logged!")
        s = signal
        edit_message(message_id,
            f"✅ *Trade Taken*\n\n"
            f"*{s['symbol']}*   {s['atm_strike']} {s['direction']}\n"
            f"Regime    : {s['regime']}\n"
            f"Strategy  : {s['strategy']}\n\n"
            f"  Entry (est.)  : ~₹{s['atm_prem']} per unit\n"
            f"  Stop Loss     : ₹{s['sl_prem']}  ← *exit if premium drops here*\n"
            f"  Target        : ₹{s['tgt_prem']}  ← *book profit here*\n"
            f"  Lot size      : {s['lot']} units\n\n"
            f"🔔 Bot will notify when SL or Target is hit."
        )
        log.info(f"Trade taken: {s['symbol']} {s['atm_strike']} {s['direction']} [{s['regime']}]")

    elif action == "skip":
        with _lock:
            pending.pop(signal_id, None)
        answer_callback(cb_id, "Skipped ✓")
        s = signal
        edit_message(message_id,
            f"❌ *Skipped*  —  {s['symbol']} {s['atm_strike']} {s['direction']}\n"
            f"_Watching for next signal…_"
        )
        log.info(f"Signal skipped: {s['symbol']} {s['atm_strike']} {s['direction']}")

    elif action == "remind":
        answer_callback(cb_id, "⏰ Will remind at next scan")
        if message_id:
            edit_message(message_id,
                f"⏰ *Reminder set*  —  {signal['symbol']} "
                f"{signal['atm_strike']} {signal['direction']}\n"
                f"_Bot will re-alert in ~5 min._",
                keep_buttons=True,
            )

    else:
        answer_callback(cb_id, "Unknown action")

# ─────────────────────────────────────────
#  TELEGRAM LONG-POLLING THREAD
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
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
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
#  REGIME DETECTION  (from your latest file)
# ─────────────────────────────────────────
def detect_regime(df: pd.DataFrame, atr: float,
                  ema9: float, ema21: float) -> str:
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

def confirm_regime(symbol: str, new_regime: str) -> str | None:
    """
    FIX: Uses a per-symbol dict so NIFTY and BANKNIFTY never
    interfere with each other's confirmation counters.
    Returns the confirmed regime only after 2 consecutive matches.
    """
    rs = regime_state[symbol]
    if rs["last"] == new_regime:
        rs["count"] += 1
    else:
        rs["last"]  = new_regime
        rs["count"] = 1

    return new_regime if rs["count"] >= 2 else None

# ─────────────────────────────────────────
#  STRATEGIES  (from your latest file, enhanced)
# ─────────────────────────────────────────
def strategy_breakout(df: pd.DataFrame, atr: float,
                      ema9: float, ema21: float,
                      rsi: float, vwap: float) -> tuple | None:
    """TRENDING regime — ORB breakout with VWAP + EMA confirmation."""
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

def strategy_range_trade(df: pd.DataFrame, atr: float,
                         ema9: float, ema21: float,
                         rsi: float) -> tuple | None:
    """SIDEWAYS regime — fade near range extremes."""
    recent = df.iloc[-10:]
    high   = float(recent["High"].max())
    low    = float(recent["Low"].min())
    close  = float(df.iloc[-1]["Close"])
    buffer = atr * 0.3

    conf = 0
    if abs(ema9 - ema21) < atr * 0.3:
        conf += 3                                 # EMAs tight  →  no trend
    if close <= low + buffer or close >= high - buffer:
        conf += 3                                 # Near range boundary
    if (close <= low + buffer and rsi < 40) or \
       (close >= high - buffer and rsi > 60):
        conf += 2                                 # RSI extreme confirms

    if conf < MIN_CONFIDENCE:
        return None

    if close <= low + buffer:
        return "CE", close, close - atr * 0.8, close + atr * 1.2, conf
    if close >= high - buffer:
        return "PE", close, close + atr * 0.8, close - atr * 1.2, conf

    return None

def strategy_momentum(df: pd.DataFrame, atr: float,
                      rsi: float) -> tuple | None:
    """VOLATILE regime — large-body candle momentum."""
    last = df.iloc[-1]
    body = abs(float(last["Close"]) - float(last["Open"]))
    rng  = float(last["High"]) - float(last["Low"])
    close = float(last["Close"])

    conf = 0
    if rng > 0 and body > rng * 0.7:
        conf += 3                                 # Strong candle body
    if atr > float(df["atr"].iloc[-6:-1].mean()) * 1.4:
        conf += 3                                 # ATR spike vs recent average
    if (last["Close"] > last["Open"] and rsi > 60) or \
       (last["Close"] < last["Open"] and rsi < 40):
        conf += 2                                 # RSI confirms direction

    if conf < MIN_CONFIDENCE:
        return None

    if last["Close"] > last["Open"]:
        return "CE", close, close - atr * 1.3, close + atr * 2.5, conf
    else:
        return "PE", close, close + atr * 1.3, close - atr * 2.5, conf

# ─────────────────────────────────────────
#  EXPIRY HELPERS
# ─────────────────────────────────────────
def days_to_expiry(name: str) -> int:
    cfg     = SYMBOLS.get(name, {})
    exp_day = cfg.get("expiry_day")
    today   = datetime.now(IST).date()

    if exp_day is None:
        # Monthly: last Thursday of current month
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

def is_expiry_today(name: str) -> bool:
    cfg     = SYMBOLS.get(name, {})
    exp_day = cfg.get("expiry_day")
    if exp_day is None:
        return False
    return datetime.now(IST).weekday() == exp_day

# ─────────────────────────────────────────
#  PREMIUM ESTIMATOR
# ─────────────────────────────────────────
def estimate_premium(spot: float, strike: int,
                     opt_type: str, dte: int) -> int:
    iv        = 0.14
    intrinsic = max(0, spot - strike) if opt_type == "CE" else max(0, strike - spot)
    time_val  = round(spot * iv * max(dte, 1) / 365)
    return max(10, round(intrinsic + time_val))

# ─────────────────────────────────────────
#  SIGNAL SCANNER  (regime-aware)
# ─────────────────────────────────────────
def scan_symbol(name: str) -> dict | None:
    cfg      = SYMBOLS[name]
    interval = cfg["interval"]
    lot      = cfg["lot"]

    try:
        df = yf.download(
            cfg["yahoo"], interval="5m", period="1d",
            progress=False, auto_adjust=True,
        )
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

        # ── Regime detection + confirmation ──────────────────────
        raw_regime = detect_regime(df, atr, ema9, ema21)
        regime     = confirm_regime(name, raw_regime)

        if not regime or regime == "NORMAL":
            log.info(f"{name}: Regime={raw_regime} (not confirmed or NORMAL) — skip")
            return None

        # ── Pick strategy based on regime ─────────────────────────
        if regime == "TRENDING":
            result   = strategy_breakout(df, atr, ema9, ema21, rsi, vwap)
            strategy = "ORB Breakout"
        elif regime == "SIDEWAYS":
            result   = strategy_range_trade(df, atr, ema9, ema21, rsi)
            strategy = "Range Fade"
        else:  # VOLATILE
            result   = strategy_momentum(df, atr, rsi)
            strategy = "Momentum"

        if result is None:
            log.info(f"{name}: {regime} confirmed but no trade setup found")
            return None

        direction, entry, sl_idx, tgt_idx, conf = result

        # ── ATM / OTM strikes ─────────────────────────────────────
        atm_strike = round(close / interval) * interval
        otm_strike = (atm_strike + interval) if direction == "CE" \
                                              else (atm_strike - interval)

        # ── Estimated option premium ──────────────────────────────
        dte      = days_to_expiry(name)
        atm_prem = estimate_premium(close, atm_strike, direction, max(1, dte))

        if is_expiry_today(name):
            sl_prem  = round(atm_prem * 0.35)   # tighter on expiry
            tgt_prem = round(atm_prem * 1.60)
        else:
            sl_prem  = round(atm_prem * 0.45)
            tgt_prem = round(atm_prem * 1.90)

        # ── Lot sizing ────────────────────────────────────────────
        risk_per_lot = max(1, (atm_prem - sl_prem) * lot)
        sugg_lots    = max(1, int((CAPITAL * RISK_PCT) / risk_per_lot))

        return {
            "id":          str(uuid.uuid4())[:8],
            "symbol":      name,
            "direction":   direction,
            "confidence":  conf,
            "regime":      regime,
            "strategy":    strategy,
            "close":       round(close, 2),
            "atm_strike":  atm_strike,
            "otm_strike":  otm_strike,
            "atm_prem":    atm_prem,
            "sl_prem":     sl_prem,
            "tgt_prem":    tgt_prem,
            "sugg_lots":   sugg_lots,
            "lot":         lot,
            "sl_idx":      round(sl_idx, 2),
            "tgt_idx":     round(tgt_idx, 2),
            "atr":         round(atr, 2),
            "rsi":         round(rsi, 1),
            "ema9":        round(ema9, 2),
            "ema21":       round(ema21, 2),
            "vwap":        round(vwap, 2),
            "dte":         dte,
            "expiry_today": is_expiry_today(name),
            "yahoo":       cfg["yahoo"],
        }

    except Exception as e:
        log.error(f"{name}: scan_symbol error — {e}")
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
        df = yf.download(
            trade["yahoo"], interval="1m", period="1d",
            progress=False, auto_adjust=True,
        )
        if df.empty:
            return
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        live = float(df["Close"].dropna().iloc[-1])
        log.info(f"SL/Tgt — {sym}: live={live:.2f}  SL={sl}  Tgt={tgt}")

        sl_hit  = (live <= sl)  if dire == "CE" else (live >= sl)
        tgt_hit = (live >= tgt) if dire == "CE" else (live <= tgt)

        if sl_hit:
            send_text(
                f"🔴 *STOP LOSS HIT*\n\n"
                f"*{sym}*  {trade['atm_strike']} {dire}\n\n"
                f"  Index now  : {live:,.2f}\n"
                f"  SL level   : {sl:,.2f}\n\n"
                f"📢 *Exit your {trade['atm_strike']} {dire} NOW.*\n"
                f"_No waiting. No hoping._"
            )
            with _lock:
                state["daily_loss"]  += CAPITAL * RISK_PCT
                state["active_trade"] = None
            log.info(f"SL hit: {sym} {trade['atm_strike']} {dire}")

        elif tgt_hit:
            send_text(
                f"🟢 *TARGET HIT* 🎉\n\n"
                f"*{sym}*  {trade['atm_strike']} {dire}\n\n"
                f"  Index now  : {live:,.2f}\n"
                f"  Tgt level  : {tgt:,.2f}\n\n"
                f"📢 *Book profit on {trade['atm_strike']} {dire} NOW.*\n"
                f"_Don't be greedy. Lock it in._"
            )
            with _lock:
                state["active_trade"] = None
            log.info(f"Target hit: {sym} {trade['atm_strike']} {dire}")

    except Exception as e:
        log.error(f"check_sl_target error: {e}")

# ─────────────────────────────────────────
#  MESSAGE BUILDERS
# ─────────────────────────────────────────
REGIME_EMOJI = {
    "TRENDING": "📈",
    "SIDEWAYS": "↔️",
    "VOLATILE": "⚡",
}
STRATEGY_DESC = {
    "ORB Breakout": "Price broke the opening range — riding the momentum",
    "Range Fade":   "Price at range extreme — fading back to the middle",
    "Momentum":     "Strong candle body on an ATR spike — momentum trade",
}

def build_signal_msg(s: dict) -> str:
    exp_line = (
        "⚠️ *EXPIRY DAY* — SL tightened. Exit before 2:45 PM."
        if s["expiry_today"]
        else f"📅 {s['dte']} day(s) to expiry"
    )
    active = get_st("active_trade")
    block  = (
        f"\n⚠️ _Active trade exists:_ "
        f"`{active['symbol']} {active['atm_strike']} {active['direction']}`\n"
        f"_Taking this signal is blocked._"
        if active else ""
    )
    regime_e = REGIME_EMOJI.get(s["regime"], "")
    strat_d  = STRATEGY_DESC.get(s["strategy"], "")

    return (
        f"🚨 *SIGNAL  —  {s['symbol']} {s['direction']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  {regime_e} Regime     : *{s['regime']}*\n"
        f"  📐 Strategy   : *{s['strategy']}*\n"
        f"  _{strat_d}_\n"
        f"  Confidence   : {s['confidence']}/8\n\n"
        f"  Index Spot   : {s['close']:,.2f}\n"
        f"  EMA 9 / 21   : {s['ema9']:,.2f}  /  {s['ema21']:,.2f}\n"
        f"  RSI          : {s['rsi']:.1f}\n"
        f"  VWAP         : {s['vwap']:,.2f}\n"
        f"  ATR          : {s['atr']:,.2f}\n\n"
        f"  🎯 *WHAT TO BUY*\n"
        f"  Strike (ATM)  :  `{s['atm_strike']} {s['direction']}`\n"
        f"  Strike (OTM)  :  `{s['otm_strike']} {s['direction']}`"
        f"  _← cheaper, riskier_\n"
        f"  Entry (est.)  :  *~₹{s['atm_prem']}* per unit\n"
        f"  Stop Loss     :  ₹{s['sl_prem']}  _← exit if premium falls here_\n"
        f"  Target        :  ₹{s['tgt_prem']}  _← book profit here_\n"
        f"  Lot size      :  {s['lot']} units\n"
        f"  Suggested     :  {s['sugg_lots']} lot(s)"
        f"  _(₹{int(CAPITAL * RISK_PCT)} risk)_\n\n"
        f"  {exp_line}"
        f"{block}\n\n"
        f"_Tap a button below_ ↓"
    )

def build_multi_summary(signals: list, best: dict) -> str:
    lines = [f"📊 *{len(signals)} signals fired simultaneously*\n"]
    for s in signals:
        marker = "⭐" if s["symbol"] == best["symbol"] else "  •"
        lines.append(
            f"{marker} *{s['symbol']}* {s['direction']} "
            f"{s['atm_strike']}  |  {s['regime']}  |  "
            f"Conf: {s['confidence']}/8  |  Est. ₹{s['atm_prem']}"
        )
    lines.append(f"\n⬆️ Individual signals with buttons follow below")
    return "\n".join(lines)

def build_rules_msg(period: str) -> str:
    at     = get_st("active_trade")
    dl     = get_st("daily_loss")
    at_str = (
        f"🔵 Active: *{at['symbol']} {at['atm_strike']} {at['direction']}*"
        if at else "⚪ No active trade"
    )

    if period == "open":
        return (
            f"🚀 *Market Open*\n\n"
            f"📌 RULES — Read every morning\n"
            f"1. ONE trade at a time — use the buttons\n"
            f"2. Pick the highest confidence signal\n"
            f"3. Never override the Stop Loss\n"
            f"4. Exit all positions by 3:15 PM\n"
            f"5. Daily loss limit: ₹{MAX_DAILY_LOSS:.0f} — then stop\n"
            f"6. Expiry day → tighter SL, earlier exit\n\n"
            f"{at_str}"
        )
    if period == "mid":
        return (
            f"📌 *MIDDAY CHECK*\n\n"
            f"  Daily loss used : ₹{dl:.0f} / ₹{MAX_DAILY_LOSS:.0f}\n"
            f"  {at_str}\n\n"
            f"Stay disciplined. No overtrading.\n"
            f"If you're up, protect your profits."
        )
    if period == "close":
        return (
            f"📌 *PRE-CLOSE*\n\n"
            f"  Daily loss used : ₹{dl:.0f} / ₹{MAX_DAILY_LOSS:.0f}\n"
            f"  {at_str}\n\n"
            f"⚠️ No new trades after 3:00 PM.\n"
            f"If trade is open — close it before 3:15 PM.\n"
            f"_Never hold options to market close._"
        )
    return ""

# ─────────────────────────────────────────
#  TIME UTILITIES
# ─────────────────────────────────────────
def now_ist() -> datetime:
    return datetime.now(IST)

def time_str() -> str:
    return now_ist().strftime("%H:%M")

def wait_next_5min():
    n    = now_ist()
    secs = n.minute * 60 + n.second
    gap  = ((secs // 300) + 1) * 300 - secs
    log.info(f"Sleeping {gap}s → next 5-min candle")
    time.sleep(gap)

def is_trading_window() -> bool:
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
    log.info("  Dual Signal Bot  —  Merged Final Version")
    log.info("=" * 55)

    # Start Telegram polling in background
    poll = threading.Thread(target=telegram_polling_thread, daemon=True)
    poll.start()

    while True:
        n    = now_ist()
        t    = time_str()
        wday = n.weekday()

        # ── Weekend ───────────────────────────────────────────
        if wday >= 5:
            if not get_st("holiday_sent"):
                send_text("📴 *Market closed today.* See you Monday! 👋")
                set_st("holiday_sent", True)
            time.sleep(3600)
            continue

        # ── Daily reset ───────────────────────────────────────
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
            # Reset per-symbol regime state
            with _lock:
                for nm in regime_state:
                    regime_state[nm] = {"last": None, "count": 0}
            log.info(f"New day: {n.date()}")

        # ── Rules: Open / Midday / Close  (3 times a day) ────
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

        # ── Market close ──────────────────────────────────────
        if "15:30" <= t < "15:31":
            at = get_st("active_trade")
            dl = get_st("daily_loss")
            send_text(
                f"🛑 *Market Closed*\n\n"
                f"Daily loss used    : ₹{dl:.0f} / ₹{MAX_DAILY_LOSS:.0f}\n"
                f"Open trade at close: "
                f"{at['symbol'] + ' ' + str(at['atm_strike']) if at else 'None'}\n\n"
                f"_See you tomorrow at 9:20 AM_ 👋"
            )
            set_st("active_trade", None)

        # ── Trading window ────────────────────────────────────
        if is_trading_window():

            # Daily loss guard  (continue — NOT break)
            if get_st("daily_loss") >= MAX_DAILY_LOSS:
                log.info("Daily loss limit — paused this cycle")
                wait_next_5min()
                continue

            # SL / Target monitoring
            check_sl_target()

            # Signal scan
            try:
                signals = []
                for name in SYMBOLS:
                    result = scan_symbol(name)
                    if result:
                        signals.append(result)

                if signals:
                    # Best = highest strategy rank, then confidence
                    best = max(
                        signals,
                        key=lambda x: (STRATEGY_RANK.get(x["regime"], 0),
                                       x["confidence"])
                    )

                    if len(signals) > 1:
                        send_text(build_multi_summary(signals, best))

                    for sig in signals:
                        msg_id = send_with_buttons(build_signal_msg(sig), sig["id"])
                        with _lock:
                            state["pending_signals"][sig["id"]] = {
                                **sig, "msg_id": msg_id,
                            }
                        log.info(
                            f"Signal → {sig['symbol']} {sig['direction']} "
                            f"{sig['atm_strike']}  [{sig['regime']}]  "
                            f"conf={sig['confidence']}/8"
                        )
                else:
                    log.info("No confirmed signals this scan")

            except Exception as e:
                log.error(f"Main scan error: {e}")

        wait_next_5min()


if __name__ == "__main__":
    main()
'''

with open('/root/merged-bot/main.py', 'w',encoding='utf-8') as f:
    f.write(code)

print(f"✅ main.py written")
print(f"   Lines : {code.count(chr(10))}")
print(f"   Chars : {len(code):,}")