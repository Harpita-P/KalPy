"""
BTC 15-minute UP/DOWN predictor for Kalshi (read-only, no trading).

What it does
------------
Kalshi's KXBTC15M markets ask "BTC price up in next 15 mins?" where:
  - YES  = BTC spot >= the round's opening/target price at close  (UP)
  - NO   = BTC spot <  the round's opening/target price at close  (DOWN)

These markets settle on **CF Benchmarks' BRTI** (Bitcoin Real-Time Index):
the simple average of the BRTI over the **final 60 seconds** before close is
compared to the strike. The model below mirrors that mechanic.

This script watches the active round and, once 4 minutes or less remain,
picks ONE side (UP or DOWN) by combining several free signals:

  1. BRTI-proxy price:    robust median of BRTI's constituent exchanges
                          (Coinbase, Kraken, Bitstamp, Gemini) instead of a
                          single venue -> closely tracks the settlement index.
  2. Settlement mechanics: settlement = 60-second average of BRTI before close,
                          so the model uses the variance of that *time-average*
                          (not a single point at close).
  3. Volatility model:    recent realized volatility (1-min Coinbase candles)
                          -> random-walk (martingale) digital-option
                          probability that the gap survives to settlement.
                          Being ahead now does NOT imply settling ahead; the
                          volatility term quantifies the flip probability.
  4. Market wisdom:       Kalshi order book implied probability, weighted by
                          how liquid/tight the book is.

The model probability and the (liquidity-weighted) market-implied probability
are blended into a single P(UP). It then checks the settlement result and logs
running accuracy to CSV.

No API keys required: every endpoint used here is public.
"""

import csv
import math
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dateutil import parser as dateutil_parser

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
COINBASE_CANDLES = "https://api.exchange.coinbase.com/products/BTC-USD/candles"

# BRTI constituent exchanges (public spot endpoints). Median approximates BRTI.
EXCHANGE_SPOT_SOURCES = [
    ("Coinbase", "https://api.coinbase.com/v2/prices/BTC-USD/spot",
     lambda j: j["data"]["amount"]),
    ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
     lambda j: list(j["result"].values())[0]["c"][0]),
    ("Bitstamp", "https://www.bitstamp.net/api/v2/ticker/btcusd/",
     lambda j: j["last"]),
    ("Gemini", "https://api.gemini.com/v1/pubticker/btcusd",
     lambda j: j["last"]),
]

# --- Tunables ---------------------------------------------------------------
PREDICT_WINDOW_SECONDS = 240   # Pick ONE side when <= 4 minutes remain
SETTLEMENT_AVG_SECONDS = 60    # Kalshi averages BRTI over the final 60s
SPOT_SAMPLES = 2               # Median-of-exchanges reads to denoise spot
VOL_REFRESH_SECONDS = 60       # How often to refresh the volatility estimate
VOL_LOOKBACK_MINUTES = 15      # 1-min candles used for volatility (recent regime)
DRIFT_LOOKBACK_MINUTES = 5     # 1-min candles used for short-term drift
DRIFT_WEIGHT = 0.15            # Short-horizon crypto ~ martingale; keep drift small
# Liquidity-aware blend: market weight scales between these by book quality.
MARKET_WEIGHT_MIN = 0.30       # Illiquid/wide book -> trust model more
MARKET_WEIGHT_MAX = 0.80       # Liquid/tight book -> trust market more
TIGHT_SPREAD = 0.02            # <= 2c spread counts as tight
WIDE_SPREAD = 0.10             # >= 10c spread counts as wide
GOOD_SIZE_FP = 2000.0         # Contracts at top-of-book considered "deep"
REQUEST_TIMEOUT = 12

LOG_PATH = Path("csv_trading_logs/btc_updown_predictions.csv")
LOG_HEADERS = [
    "round_close",
    "ticker",
    "strike",
    "final_spot",
    "gap_dollars",
    "seconds_left_at_lock",
    "sigma_per_min_usd",
    "model_prob_up",
    "market_prob_up",
    "market_weight",
    "blended_prob_up",
    "prediction",
    "confidence_pct",
    "result",
    "correct",
    "logged_at",
]


def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def fmt_pct(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.1f}%"


def fmt_usd(amount: float | None) -> str:
    if amount is None:
        return "N/A"
    return f"${amount:,.2f}"


# --- Data sources -----------------------------------------------------------
def fetch_exchange_prices() -> dict:
    """Fetch BTC spot from each BRTI constituent exchange. Returns {name: price}."""
    prices = {}
    for name, url, parse in EXCHANGE_SPOT_SOURCES:
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT,
                             headers={"User-Agent": "btc-updown-predictor"})
            r.raise_for_status()
            price = safe_float(parse(r.json()))
            if price and price > 0:
                prices[name] = price
        except Exception:
            continue
    return prices


def fetch_brti_proxy() -> tuple[float | None, dict]:
    """Robust BRTI proxy = median of constituent exchanges, sampled & averaged.

    Returns (proxy_price, last_sample_prices).
    """
    samples = []
    last_prices: dict = {}
    for _ in range(max(1, SPOT_SAMPLES)):
        prices = fetch_exchange_prices()
        if prices:
            last_prices = prices
            samples.append(statistics.median(prices.values()))
        if SPOT_SAMPLES > 1:
            time.sleep(0.4)
    if not samples:
        return None, {}
    return statistics.median(samples), last_prices


def fetch_1m_candles() -> list | None:
    """Coinbase 1-min candles: each row is [time, low, high, open, close, volume]."""
    try:
        r = requests.get(
            COINBASE_CANDLES,
            params={"granularity": 60},
            headers={"User-Agent": "btc-updown-predictor"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        candles = r.json()
        # API returns newest-first; sort oldest-first for return calcs.
        candles.sort(key=lambda c: c[0])
        return candles
    except Exception:
        return None


def estimate_vol_and_drift(candles: list, spot: float) -> tuple[float, float]:
    """Return (sigma_per_min_usd, drift_per_min_usd) from 1-min closes."""
    if not candles or len(candles) < 5:
        # Fallback: ~0.04%/min typical BTC short-term vol.
        return spot * 0.0004, 0.0

    closes = [safe_float(c[4]) for c in candles if safe_float(c[4]) is not None]
    rets = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev and prev > 0:
            rets.append(closes[i] / prev - 1.0)

    if len(rets) < 3:
        return spot * 0.0004, 0.0

    vol_rets = rets[-VOL_LOOKBACK_MINUTES:]
    mean = sum(vol_rets) / len(vol_rets)
    var = sum((r - mean) ** 2 for r in vol_rets) / max(1, len(vol_rets) - 1)
    sigma_per_min = math.sqrt(var) * spot
    if sigma_per_min <= 0:
        sigma_per_min = spot * 0.0004

    drift_rets = rets[-DRIFT_LOOKBACK_MINUTES:]
    drift_per_min = (sum(drift_rets) / len(drift_rets)) * spot

    return sigma_per_min, drift_per_min


def get_active_btc15m_market() -> dict | None:
    """Return the soonest-closing active KXBTC15M market dict, or None."""
    try:
        r = requests.get(
            KALSHI_BASE + "/markets",
            params={"series_ticker": "KXBTC15M", "status": "open", "limit": 20},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        markets = r.json().get("markets", [])
    except Exception as e:
        print(f"Could not fetch markets: {e}")
        return None

    now = datetime.now(timezone.utc)
    upcoming = []
    for m in markets:
        ct = m.get("close_time")
        if not ct:
            continue
        close_dt = dateutil_parser.parse(ct)
        if close_dt.tzinfo is None:
            close_dt = close_dt.replace(tzinfo=timezone.utc)
        if close_dt > now:
            upcoming.append((close_dt, m))

    if not upcoming:
        return None
    upcoming.sort(key=lambda x: x[0])
    return upcoming[0][1]


def get_market_result(ticker: str) -> str | None:
    """Return 'yes'/'no' once settled, else None."""
    try:
        r = requests.get(KALSHI_BASE + f"/markets/{ticker}", timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("market", {}).get("result") or None
    except Exception:
        return None


def market_implied_prob_up(ticker: str) -> tuple[float | None, float]:
    """YES (UP) mid-probability and a liquidity-based trust weight in [0,1].

    Uses the fresh market quote (yes bid/ask + top-of-book sizes). The weight
    is high when the book is tight and deep, low when wide/thin.
    """
    try:
        r = requests.get(KALSHI_BASE + f"/markets/{ticker}", timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        m = r.json().get("market", {})
    except Exception:
        return None, 0.0

    yes_bid = safe_float(m.get("yes_bid_dollars"))
    yes_ask = safe_float(m.get("yes_ask_dollars"))
    bid_size = safe_float(m.get("yes_bid_size_fp")) or 0.0
    ask_size = safe_float(m.get("yes_ask_size_fp")) or 0.0

    if yes_bid is None or yes_ask is None or yes_bid <= 0 or yes_ask <= 0:
        # Fall back to last traded price if no two-sided quote.
        last = safe_float(m.get("last_price_dollars"))
        return (last, 0.25) if last else (None, 0.0)

    mid = (yes_bid + yes_ask) / 2.0
    spread = max(0.0, yes_ask - yes_bid)

    # Spread quality: 1.0 at <=TIGHT_SPREAD, 0.0 at >=WIDE_SPREAD.
    if spread <= TIGHT_SPREAD:
        spread_q = 1.0
    elif spread >= WIDE_SPREAD:
        spread_q = 0.0
    else:
        spread_q = (WIDE_SPREAD - spread) / (WIDE_SPREAD - TIGHT_SPREAD)

    # Depth quality: top-of-book size relative to GOOD_SIZE_FP.
    depth = min(bid_size, ask_size)
    depth_q = max(0.0, min(1.0, depth / GOOD_SIZE_FP))

    quality = 0.65 * spread_q + 0.35 * depth_q
    weight = MARKET_WEIGHT_MIN + (MARKET_WEIGHT_MAX - MARKET_WEIGHT_MIN) * quality
    return mid, weight


# --- Model ------------------------------------------------------------------
def settlement_horizon_variance(seconds_remaining: float,
                                sigma_per_sec_sq: float) -> float:
    """Variance of the settlement value vs now.

    Kalshi settles on the *average* of BRTI over the final
    SETTLEMENT_AVG_SECONDS before close. For a driftless random walk with
    per-second variance s2, the variance of that time-average relative to the
    current price is:

        tau >= w:   s2 * (tau - w/3 - ... )  -> simplifies to s2 * (tau - w/3*... )
    Using A = (1/w)∫ X dt over [tau-w, tau] (tau = seconds left, w = window):
        Var(A) = s2 * ( (tau - w) + w/3 )      for tau >= w
        Var(A) = s2 * ( tau / 3 )              for tau <  w   (window already open)

    Both branches agree at tau == w (= s2 * w/3), so the function is continuous.
    Averaging shrinks the effective horizon vs a single point at close
    (which would be s2 * tau), correctly reflecting the settlement mechanic.
    """
    w = float(SETTLEMENT_AVG_SECONDS)
    tau = max(seconds_remaining, 0.0)
    if tau >= w:
        eff = (tau - w) + w / 3.0
    else:
        eff = tau / 3.0
    return sigma_per_sec_sq * max(eff, 1e-6)


def model_prob_up(gap: float, sigma_per_min: float, drift_per_min: float,
                  seconds_remaining: float) -> float:
    """Probability the 60s-average settlement finishes >= strike.

    Driftless random walk (martingale) is the statistically honest assumption
    at this horizon: leading now does NOT imply settling there. The variance
    term quantifies how likely the gap is to flip before settlement.
    """
    sigma_per_sec_sq = (sigma_per_min ** 2) / 60.0
    var = settlement_horizon_variance(seconds_remaining, sigma_per_sec_sq)
    sigma_total = math.sqrt(var)
    if sigma_total <= 0:
        return 1.0 if gap >= 0 else 0.0
    # Small momentum tilt; centered on the middle of the settlement window.
    eff_minutes = max((seconds_remaining - SETTLEMENT_AVG_SECONDS / 2.0) / 60.0, 0.0)
    drift_total = drift_per_min * eff_minutes * DRIFT_WEIGHT
    z = (gap + drift_total) / sigma_total
    return norm_cdf(z)


def blend(model_p: float, market_p: float | None, market_weight: float) -> float:
    """Liquidity-weighted blend of model and market probabilities."""
    if market_p is None:
        return model_p
    w = max(0.0, min(1.0, market_weight))
    return w * market_p + (1.0 - w) * model_p


def build_prediction(market: dict, seconds_remaining: float,
                     vol_cache: dict) -> dict | None:
    strike = safe_float(market.get("floor_strike"))
    if strike is None:
        strike = safe_float(market.get("cap_strike"))
    spot, exch_prices = fetch_brti_proxy()
    if spot is None or strike is None:
        return None

    now_mono = time.monotonic()
    if (now_mono - vol_cache.get("ts", 0)) >= VOL_REFRESH_SECONDS or "sigma" not in vol_cache:
        candles = fetch_1m_candles()
        sigma, drift = estimate_vol_and_drift(candles, spot)
        vol_cache["sigma"] = sigma
        vol_cache["drift"] = drift
        vol_cache["ts"] = now_mono
    sigma = vol_cache["sigma"]
    drift = vol_cache["drift"]

    gap = spot - strike
    m_prob = model_prob_up(gap, sigma, drift, seconds_remaining)
    mkt_prob, mkt_weight = market_implied_prob_up(market["ticker"])
    blended = blend(m_prob, mkt_prob, mkt_weight)

    prediction = "UP" if blended >= 0.5 else "DOWN"
    confidence = abs(blended - 0.5) * 2.0

    return {
        "spot": spot,
        "strike": strike,
        "gap": gap,
        "sigma_per_min": sigma,
        "drift_per_min": drift,
        "model_prob_up": m_prob,
        "market_prob_up": mkt_prob,
        "market_weight": mkt_weight,
        "exchanges": exch_prices,
        "blended_prob_up": blended,
        "prediction": prediction,
        "confidence": confidence,
    }


# --- Logging ----------------------------------------------------------------
def ensure_log():
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not LOG_PATH.exists():
        with open(LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow(LOG_HEADERS)


def log_result(market: dict, locked: dict, seconds_left_at_lock: float,
               result: str | None):
    ensure_log()
    result_updown = ""
    correct = ""
    if result in ("yes", "no"):
        result_updown = "UP" if result == "yes" else "DOWN"
        correct = "YES" if result_updown == locked["prediction"] else "NO"
    row = [
        market.get("close_time", ""),
        market.get("ticker", ""),
        round(locked["strike"], 2),
        round(locked["spot"], 2),
        round(locked["gap"], 2),
        round(seconds_left_at_lock, 1),
        round(locked["sigma_per_min"], 2),
        round(locked["model_prob_up"], 4),
        round(locked["market_prob_up"], 4) if locked["market_prob_up"] is not None else "",
        round(locked.get("market_weight", 0.0), 3),
        round(locked["blended_prob_up"], 4),
        locked["prediction"],
        round(locked["confidence"] * 100, 1),
        result_updown,
        correct,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ]
    with open(LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow(row)


def running_accuracy() -> str:
    if not LOG_PATH.exists():
        return "no history yet"
    total = 0
    correct = 0
    with open(LOG_PATH, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("correct") in ("YES", "NO"):
                total += 1
                if r["correct"] == "YES":
                    correct += 1
    if total == 0:
        return "no settled predictions yet"
    return f"{correct}/{total} correct ({correct / total * 100:.1f}%)"


# --- Round loop -------------------------------------------------------------
def run_round(market: dict):
    ticker = market["ticker"]
    close_dt = dateutil_parser.parse(market["close_time"])
    if close_dt.tzinfo is None:
        close_dt = close_dt.replace(tzinfo=timezone.utc)
    strike = safe_float(market.get("floor_strike")) or safe_float(market.get("cap_strike"))

    print("\n" + "=" * 78)
    print(f"BTC 15M UP/DOWN PREDICTOR  |  {ticker}")
    print(f"Target/strike price: {fmt_usd(strike)}")
    print(f"Round closes: {market['close_time']}")
    print(f"Will pick ONE side at <= {PREDICT_WINDOW_SECONDS // 60} min remaining")
    print("=" * 78)

    # Wait quietly until the prediction window opens (<= 4 minutes remaining).
    while True:
        now = datetime.now(timezone.utc)
        seconds_remaining = (close_dt - now).total_seconds()
        if seconds_remaining <= 0:
            print("Round closed before a prediction was made.")
            return
        if seconds_remaining <= PREDICT_WINDOW_SECONDS:
            break
        wait = min(seconds_remaining - PREDICT_WINDOW_SECONDS, 30)
        time.sleep(max(wait, 1))

    # Make exactly one prediction. Retry briefly only if data is unavailable.
    vol_cache: dict = {}
    locked: dict | None = None
    locked_seconds_left: float | None = None
    while locked is None:
        now = datetime.now(timezone.utc)
        seconds_remaining = (close_dt - now).total_seconds()
        if seconds_remaining <= 0:
            print("Round closed before a prediction could be made (data unavailable).")
            return
        pred = build_prediction(market, seconds_remaining, vol_cache)
        if pred is not None:
            locked = pred
            locked_seconds_left = seconds_remaining
            break
        print("  data unavailable, retrying in 3s...")
        time.sleep(3)

    arrow = "UP" if locked["prediction"] == "UP" else "DOWN"
    exch = locked.get("exchanges") or {}
    exch_str = ", ".join(f"{k} {v:,.0f}" for k, v in exch.items()) or "n/a"
    mkt_p = locked["market_prob_up"]
    print("\n" + "-" * 78)
    print(f"PREDICTION ({int(locked_seconds_left)}s left): >>> {arrow} <<<  "
          f"P(UP)={fmt_pct(locked['blended_prob_up'])} "
          f"(confidence {fmt_pct(locked['confidence'])})")
    print(f"  BRTI-proxy spot {fmt_usd(locked['spot'])} vs strike "
          f"{fmt_usd(locked['strike'])} (gap {locked['gap']:+,.2f})")
    print(f"  vol {locked['sigma_per_min']:.1f} $/min  |  "
          f"settlement = 60s avg before close")
    print(f"  model {fmt_pct(locked['model_prob_up'])} | "
          f"market {fmt_pct(mkt_p) if mkt_p is not None else 'n/a'} "
          f"(trust {fmt_pct(locked['market_weight'])})")
    print(f"  sources: {exch_str}")
    print("-" * 78)

    # Wait for settlement and verify.
    print("Waiting for settlement to verify...")
    deadline = datetime.now(timezone.utc) + timedelta(seconds=300)
    result = None
    while datetime.now(timezone.utc) < deadline:
        result = get_market_result(ticker)
        if result in ("yes", "no"):
            break
        time.sleep(5)

    if result in ("yes", "no"):
        result_updown = "UP" if result == "yes" else "DOWN"
        hit = result_updown == locked["prediction"]
        print(f"RESULT: {result_updown}  ->  prediction was {'CORRECT' if hit else 'WRONG'}")
    else:
        print("RESULT: could not confirm settlement in time.")

    log_result(market, locked, locked_seconds_left or 0.0, result)
    print(f"Running accuracy: {running_accuracy()}")


def main():
    print("BTC 15-minute UP/DOWN predictor (read-only)")
    print(f"Logging predictions to: {LOG_PATH}")
    last_ticker = None
    while True:
        market = get_active_btc15m_market()
        if not market:
            print("No active BTC 15-minute market found. Retrying in 20s...")
            time.sleep(20)
            continue

        if market["ticker"] == last_ticker:
            # Already handled this round; wait briefly for the next one to open.
            time.sleep(10)
            continue

        last_ticker = market["ticker"]
        try:
            run_round(market)
        except Exception as e:
            print(f"Error during round: {e}")
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
