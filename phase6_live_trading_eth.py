import asyncio
import base64
import csv
import json
import os
import socket
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests
import websockets
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
BASE_URL = os.getenv("KALSHI_BASE_URL")

if not API_KEY_ID or not PRIVATE_KEY_PATH or not BASE_URL:
    raise ValueError("Missing environment variables. Check your .env file.")

# Trading parameters
ENTRY_TRIGGER = 0.999  # Enter when ask >= 99.9 cents
EXIT_TRIGGER = 0.30   # Stop: sell when held-side ask <= 30 cents
POSITION_SIZE_PCT = 0.40  # Use 40% of account balance
TRADING_DELAY_MINUTES = 2.333  # Only trade when 2 min 20 sec (2:20) or less remain
NO_ENTRY_FINAL_SECONDS = 25  # Don't place new entry orders in final 25 seconds
# Final 2 minutes: compare spot ETH to market strike; within $3 blocks new entries (flat) or triggers exit (position)
FINAL_PHASE_ETH_RULE_SECONDS = 120
ETH_TARGET_PROXIMITY_DOLLARS = 0.10
ETH_SPOT_POLL_INTERVAL_SECONDS = 5
VELOCITY_WINDOW_SECONDS = 60        # Look-back window for spot velocity measurement
# 0.20% in 60s is calibrated from actual loss events:
#   ETH Jun10: +0.26%/60s before bad NO entry (would have blocked it)
#   BTC Jun7:  +0.24%/57s spike after NO entry (would have triggered exit)
#   BTC Jun18: +0.33%/60s spike during position (would have triggered exit)
# Normal ETH volatility is ~0.04-0.07%/min, so 0.20% gives 3-5x buffer vs noise.
RAPID_MOVE_THRESHOLD_PCT = 0.20
MINIMUM_ACCOUNT_BALANCE = 2.00  # Minimum balance required to continue trading
PRINT_TICK_UPDATES = False
ENTRY_ORDER_TIMEOUT_SECONDS = 60
MIN_ENTRY_FILL_CENTS = 97
EXIT_LADDER_INTERVAL_SECONDS = 3
EXIT_LADDER_STEP_CENTS = 1
EXIT_LADDER_RULE1_FLOOR_CENTS = 95
EXIT_LADDER_TIME_EXIT_FLOOR_CENTS = 95
EXIT_LADDER_STOP_FLOOR_CENTS = 40
# NO COOLDOWNS - Can trade continuously


def base_url_to_ws_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/trade-api/ws/v2"


WS_URL = base_url_to_ws_url(BASE_URL)

# CSV logging setup — separate files from the BTC bot
CSV_LOG_PATH = Path("csv_trading_logs/eth_trades_log.csv")
CSV_HEADERS = [
    "session_number", "session_date", "session_start_time", "session_end_time",
    "market_ticker", "starting_balance", "ending_balance", "session_pnl", "session_pnl_percent",
    "trade_number", "side", "buy_time", "buy_price", "buy_quantity", "buy_cost", "buy_order_id",
    "sold", "sell_time", "sell_price", "sell_quantity", "sell_proceeds", "sell_order_id",
    "exit_type", "trade_pnl", "trade_pnl_percent", "hold_duration_seconds", "outcome",
    "emergency_exit", "emergency_exit_time", "emergency_exit_price", "boundary_difference",
    "market_outcome"
]

DAILY_NET_LOG_PATH = Path("csv_trading_logs/eth_daily_net_changes.csv")
DAILY_NET_START_DATE = "2026-05-31"
DAILY_NET_HEADERS = [
    "date",
    "starting_balance",
    "ending_balance",
    "net_change",
    "number_rounds_traded",
    "rounds_sold_after_buying",
    "logged_at",
]

# Session JSON files stored separately from BTC sessions
SESSIONS_DIR = Path("trading_sessions_live_eth")


def ensure_csv_exists():
    CSV_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_LOG_PATH.exists():
        with open(CSV_LOG_PATH, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)


def log_trade_to_csv(session_info: dict, trade_info: dict):
    try:
        ensure_csv_exists()

        hold_duration = None
        if trade_info.get("buy_time") and trade_info.get("sell_time"):
            try:
                buy_dt = datetime.strptime(trade_info["buy_time"], "%Y-%m-%d %H:%M:%S")
                sell_dt = datetime.strptime(trade_info["sell_time"], "%Y-%m-%d %H:%M:%S")
                hold_duration = int((sell_dt - buy_dt).total_seconds())
            except Exception:
                hold_duration = None

        outcome = "OPEN"
        if trade_info.get("sold"):
            outcome = "WIN" if trade_info.get("trade_pnl", 0) >= 0 else "LOSS"

        session_date = session_info["session_start_time"].split()[0] if session_info.get("session_start_time") else ""

        row = [
            session_info.get("session_number", ""),
            session_date,
            session_info.get("session_start_time", ""),
            session_info.get("session_end_time", ""),
            session_info.get("market_ticker", ""),
            session_info.get("starting_balance", ""),
            session_info.get("ending_balance", ""),
            session_info.get("session_pnl", ""),
            session_info.get("session_pnl_percent", ""),
            trade_info.get("trade_number", ""),
            trade_info.get("side", "").upper(),
            trade_info.get("buy_time", ""),
            trade_info.get("buy_price", ""),
            trade_info.get("buy_quantity", ""),
            trade_info.get("buy_cost", ""),
            trade_info.get("buy_order_id", ""),
            trade_info.get("sold", False),
            trade_info.get("sell_time", ""),
            trade_info.get("sell_price", ""),
            trade_info.get("sell_quantity", ""),
            trade_info.get("sell_proceeds", ""),
            trade_info.get("sell_order_id", ""),
            trade_info.get("exit_type", ""),
            trade_info.get("trade_pnl", ""),
            trade_info.get("trade_pnl_percent", ""),
            hold_duration if hold_duration is not None else "",
            outcome,
            trade_info.get("emergency_exit", False),
            trade_info.get("emergency_exit_time", ""),
            trade_info.get("emergency_exit_price", ""),
            trade_info.get("boundary_difference", ""),
            trade_info.get("market_outcome", "")
        ]

        with open(CSV_LOG_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception as e:
        print(f"WARNING: Failed to log trade to CSV: {e}")


def ensure_daily_net_csv_exists():
    DAILY_NET_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DAILY_NET_LOG_PATH.exists():
        with open(DAILY_NET_LOG_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(DAILY_NET_HEADERS)
        return

    try:
        with open(DAILY_NET_LOG_PATH, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []

        if fieldnames == DAILY_NET_HEADERS:
            return

        updated_fieldnames = list(fieldnames)
        changed = False
        for header in DAILY_NET_HEADERS:
            if header not in updated_fieldnames:
                insert_at = DAILY_NET_HEADERS.index(header)
                updated_fieldnames.insert(min(insert_at, len(updated_fieldnames)), header)
                changed = True

        if not changed:
            return

        with open(DAILY_NET_LOG_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=updated_fieldnames)
            writer.writeheader()
            for row in rows:
                for header in updated_fieldnames:
                    row.setdefault(header, "")
                writer.writerow(row)
    except Exception as e:
        print(f"WARNING: Could not migrate daily net-change CSV headers: {e}")


def _logged_daily_net_dates() -> set[str]:
    ensure_daily_net_csv_exists()
    logged_dates = set()
    try:
        with open(DAILY_NET_LOG_PATH, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date"):
                    logged_dates.add(row["date"])
    except Exception as e:
        print(f"WARNING: Could not read daily net-change CSV: {e}")
    return logged_dates


def _parse_session_date(session: dict) -> str | None:
    start_time = session.get("start_time")
    if not start_time:
        return None
    try:
        return datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    except ValueError:
        return start_time.split()[0] if " " in start_time else None


def _load_all_sessions() -> list[dict]:
    master_log = SESSIONS_DIR / "all_sessions.json"
    if not master_log.exists():
        return []
    try:
        with open(master_log, "r") as f:
            payload = json.load(f)
        sessions = payload.get("sessions", [])
        return sessions if isinstance(sessions, list) else []
    except Exception as e:
        print(f"WARNING: Could not load sessions for daily net-change log: {e}")
        return []


def _format_daily_sold_rounds(sessions: list[dict]) -> str:
    sold_rounds = []
    for session in sessions:
        ticker = session.get("market_ticker", "")
        for trade in session.get("trades") or []:
            exit_order_id = trade.get("exit_order_id")
            if not exit_order_id:
                continue
            side = str(trade.get("side", "")).upper()
            exit_time = str(trade.get("exit_time", ""))
            exit_time_only = exit_time.split()[-1] if exit_time else ""
            exit_price = trade.get("exit_price")
            price_text = "N/A" if exit_price is None else f"{float(exit_price) * 100:.1f}c"
            sold_rounds.append(f"{ticker} {side} sold {exit_time_only} @ {price_text}")
    return "; ".join(sold_rounds)


def log_completed_daily_net_changes():
    """Log completed midnight-to-midnight daily balance changes for ETH sessions."""
    try:
        ensure_daily_net_csv_exists()
        today = datetime.now().strftime("%Y-%m-%d")
        logged_dates = _logged_daily_net_dates()
        sessions_by_date: dict[str, list[dict]] = {}

        for session in _load_all_sessions():
            session_date = _parse_session_date(session)
            if not session_date:
                continue
            if session_date < DAILY_NET_START_DATE or session_date >= today:
                continue
            sessions_by_date.setdefault(session_date, []).append(session)

        rows_to_write = []
        for session_date, sessions in sorted(sessions_by_date.items()):
            if session_date in logged_dates:
                continue
            sessions.sort(key=lambda s: s.get("start_time", ""))
            starting_balance = float(sessions[0].get("starting_balance", 0.0))
            ending_balance = float(sessions[-1].get("ending_balance", starting_balance))
            net_change = ending_balance - starting_balance
            rounds_with_trades = sum(1 for session in sessions if session.get("trades"))
            sold_rounds = _format_daily_sold_rounds(sessions)
            rows_to_write.append([
                session_date,
                round(starting_balance, 2),
                round(ending_balance, 2),
                round(net_change, 2),
                rounds_with_trades,
                sold_rounds,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ])

        if not rows_to_write:
            return

        with open(DAILY_NET_LOG_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows_to_write)

        for row in rows_to_write:
            print(
                f"ETH daily net-change logged for {row[0]}: "
                f"{fmt_dollars(row[1])} -> {fmt_dollars(row[2])} "
                f"({fmt_dollars(row[3])}), rounds={row[4]}"
            )
    except Exception as e:
        print(f"WARNING: Failed to log daily net-change summary: {e}")


class KalshiClient:
    def __init__(self, api_key_id: str, private_key_path: str, base_url: str):
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.base_url = base_url.rstrip("/")
        self.private_key = self._load_private_key()

    def _load_private_key(self):
        with open(self.private_key_path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(),
                password=None,
                backend=default_backend(),
            )

    def _timestamp_ms(self) -> str:
        return str(int(time.time() * 1000))

    def _create_signature(self, timestamp: str, method: str, sign_path: str) -> str:
        path_without_query = sign_path.split("?")[0]
        message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self, method: str, endpoint_path: str) -> dict:
        timestamp = self._timestamp_ms()
        sign_path = urlparse(self.base_url + endpoint_path).path
        signature = self._create_signature(timestamp, method, sign_path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    def ws_auth_headers(self):
        timestamp = self._timestamp_ms()
        ws_path = "/trade-api/ws/v2"
        signature = self._create_signature(timestamp, "GET", ws_path)
        return [
            ("KALSHI-ACCESS-KEY", self.api_key_id),
            ("KALSHI-ACCESS-SIGNATURE", signature),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp),
        ]

    def get(self, endpoint_path: str, params=None, auth: bool = False) -> requests.Response:
        headers = self._auth_headers("GET", endpoint_path) if auth else None
        return requests.get(
            self.base_url + endpoint_path,
            headers=headers,
            params=params,
            timeout=30,
        )

    def post(self, endpoint_path: str, data: dict, auth: bool = True) -> requests.Response:
        headers = self._auth_headers("POST", endpoint_path) if auth else None
        return requests.post(
            self.base_url + endpoint_path,
            headers=headers,
            json=data,
            timeout=30,
        )

    def delete(self, endpoint_path: str, auth: bool = True) -> requests.Response:
        headers = self._auth_headers("DELETE", endpoint_path) if auth else None
        return requests.delete(
            self.base_url + endpoint_path,
            headers=headers,
            timeout=30,
        )

    def get_balance(self) -> dict:
        response = self.get("/portfolio/balance", auth=True)
        response.raise_for_status()
        return response.json()

    def get_market(self, ticker: str) -> dict:
        response = self.get(f"/markets/{ticker}", auth=False)
        response.raise_for_status()
        return response.json()

    def get_milestones(self, category: str = "Crypto", limit: int = 200, cursor: str | None = None) -> dict:
        params = {"category": category, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        response = self.get("/milestones", params=params, auth=False)
        response.raise_for_status()
        return response.json()

    def get_event_with_markets(self, event_ticker: str) -> dict:
        response = self.get(
            f"/events/{event_ticker}",
            params={"with_nested_markets": "true"},
            auth=False,
        )
        response.raise_for_status()
        return response.json()

    def get_positions(self, ticker: str = None) -> dict:
        params = {}
        if ticker:
            params["ticker"] = ticker
        response = self.get("/portfolio/positions", params=params, auth=True)
        response.raise_for_status()
        return response.json()

    def get_orders(self, ticker: str = None, status: str = None) -> dict:
        params = {}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status
        response = self.get("/portfolio/orders", params=params, auth=True)
        response.raise_for_status()
        return response.json()

    def create_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        yes_price: int = None,
        no_price: int = None,
        yes_price_dollars: str = None,
        no_price_dollars: str = None,
    ) -> dict:
        """Create a limit order via the V2 events endpoint (see BTC bot for full docs)."""
        if yes_price_dollars is not None:
            yes_fp = float(yes_price_dollars)
        elif no_price_dollars is not None:
            yes_fp = 1.0 - float(no_price_dollars)
        elif yes_price is not None:
            yes_fp = yes_price / 100.0
        elif no_price is not None:
            yes_fp = (100 - no_price) / 100.0
        else:
            raise ValueError("Must provide a price argument")

        yes_fp = max(0.0001, min(0.9999, yes_fp))
        price_str = f"{yes_fp:.4f}"

        if action == "buy" and side == "yes":
            book_side = "bid"
        elif action == "sell" and side == "yes":
            book_side = "ask"
        elif action == "buy" and side == "no":
            book_side = "ask"
        elif action == "sell" and side == "no":
            book_side = "bid"
        else:
            raise ValueError(f"Unsupported side/action: {side}/{action}")

        order_data = {
            "ticker": ticker,
            "side": book_side,
            "count": f"{int(count)}.00",
            "price": price_str,
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
        }

        response = self.post("/portfolio/events/orders", data=order_data, auth=True)
        response.raise_for_status()
        raw = response.json()

        no_fp_str = f"{1.0 - yes_fp:.4f}"
        return {
            "order": {
                "order_id": raw.get("order_id", ""),
                "yes_price_dollars": price_str if side == "yes" else no_fp_str,
                "no_price_dollars": no_fp_str if side == "no" else price_str,
                "fill_count": raw.get("fill_count", "0.00"),
                "remaining_count": raw.get("remaining_count", f"{int(count)}.00"),
                "ts_ms": raw.get("ts_ms"),
            }
        }

    def cancel_order(self, order_id: str) -> dict:
        response = self.delete(f"/portfolio/orders/{order_id}", auth=True)
        response.raise_for_status()
        return response.json()

    def get_fills(self, ticker: str = None, limit: int = 100) -> dict:
        params = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        response = self.get("/portfolio/fills", params=params, auth=True)
        response.raise_for_status()
        return response.json()


def text_contains_eth(*values) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    keywords = ["eth", "ethereum", "kxeth"]
    return any(k in text for k in keywords)


def milestone_related_event_ticker(m: dict) -> str | None:
    related = m.get("related_event_tickers") or []
    primary = m.get("primary_event_tickers") or []

    if isinstance(related, list) and related:
        return related[0]
    if isinstance(primary, list) and primary:
        return primary[0]
    if isinstance(related, str):
        return related
    if isinstance(primary, str):
        return primary
    return None


def milestone_priority(m: dict):
    title = (m.get("title") or "").lower()

    if "15 minutes" in title or "15 minute" in title or "15 min" in title or "15" in title:
        priority = 0
    elif "hourly" in title:
        priority = 1
    elif "daily" in title:
        priority = 2
    else:
        priority = 3

    ts = (
        m.get("start_date")
        or m.get("start_time")
        or m.get("last_updated_ts")
        or "0000-01-01T00:00:00Z"
    )

    return (-priority, ts)


def find_latest_eth15m_market(client: KalshiClient) -> str | None:
    print("Finding latest ETH 15-minute market...")

    data = client.get_milestones(category="Crypto", limit=200)
    milestones = data.get("milestones", [])

    eth_milestones = [
        m for m in milestones
        if text_contains_eth(
            m.get("title"),
            m.get("category"),
            m.get("related_event_tickers"),
            m.get("primary_event_tickers"),
        )
    ]

    if not eth_milestones:
        print("No ETH milestones found.")
        return None

    eth_milestones.sort(key=milestone_priority, reverse=True)
    chosen_milestone = eth_milestones[0]

    print(f"Selected milestone: {chosen_milestone.get('title')}")

    related_event = milestone_related_event_ticker(chosen_milestone)
    if not related_event:
        print("No related event ticker found.")
        return None

    event_payload = client.get_event_with_markets(related_event)
    event_obj = event_payload.get("event", {})
    nested_markets = event_obj.get("markets", []) or event_payload.get("markets", [])

    if not nested_markets:
        print("No markets found for this event.")
        return None

    eth15m_markets = [
        m for m in nested_markets
        if (m.get("event_ticker") or "").upper().startswith("KXETH15M")
        or (m.get("ticker") or "").upper().startswith("KXETH15M")
    ]

    active_markets = [
        m for m in eth15m_markets
        if (m.get("status") or "").lower() == "active"
    ]

    if not active_markets:
        print("No active ETH 15-minute markets found.")
        return None

    active_markets.sort(
        key=lambda m: (
            m.get("close_time") or "9999-12-31T23:59:59Z",
            m.get("ticker") or "",
        )
    )

    selected = active_markets[0]
    ticker = selected["ticker"]
    print(f"Found active market: {ticker}")
    print(f"Close time: {selected.get('close_time')}")

    return ticker


def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def eth_strike_target_usd(market_info: dict) -> float | None:
    """Single reference USD level from Kalshi market strikes (for proximity vs spot ETH)."""
    floor_f = safe_float(market_info.get("floor_strike"))
    cap_f = safe_float(market_info.get("cap_strike"))
    if floor_f is not None and cap_f is not None:
        return (floor_f + cap_f) / 2.0
    if floor_f is not None:
        return floor_f
    if cap_f is not None:
        return cap_f
    return None


def fetch_eth_spot_usd_sync() -> float | None:
    """Spot ETH/USD (public Coinbase quote)."""
    try:
        response = requests.get(
            "https://api.coinbase.com/v2/prices/ETH-USD/spot",
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        amount = payload.get("data", {}).get("amount")
        return safe_float(amount)
    except Exception:
        return None


def _spot_velocity_pct(history: list, window_seconds: float) -> float | None:
    """% change of spot price over the last window_seconds.
    Positive = rising, negative = falling. Returns None if insufficient data."""
    if len(history) < 2:
        return None
    now_mono = time.monotonic()
    cutoff = now_mono - window_seconds
    oldest_in_window = None
    for ts, price in history:
        if ts >= cutoff:
            oldest_in_window = (ts, price)
            break
    if oldest_in_window is None:
        oldest_in_window = history[0]
    latest = history[-1]
    if oldest_in_window[1] <= 0:
        return None
    return (latest[1] - oldest_in_window[1]) / oldest_in_window[1] * 100.0


def fmt_dollars(amount: float) -> str:
    return f"${amount:.2f}"


def fmt_cents(price: float | None) -> str:
    if price is None:
        return "N/A"
    return f"{price * 100:.1f}¢"


async def subscribe_to_ticker(ws, market_ticker: str):
    subscribe_msg = {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
            "market_tickers": [market_ticker],
        },
    }
    await ws.send(json.dumps(subscribe_msg))


class LivePosition:
    def __init__(self, side: str, entry_price: float, quantity: int, entry_time: str, order_id: str = None):
        self.side = side
        self.entry_price = entry_price
        self.quantity = quantity
        self.entry_quantity = quantity
        self.entry_time = entry_time
        self.entry_order_id = order_id
        self.exit_price = None
        self.exit_time = None
        self.exit_order_id = None
        self.pnl = None
        self._exit_realized_qty = 0
        self._exit_realized_proceeds = 0.0

    def close(self, exit_price: float, exit_time: str, exit_order_id: str = None):
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.exit_order_id = exit_order_id
        self.pnl = (exit_price - self.entry_price) * self.quantity

    def __str__(self):
        status = "OPEN" if self.exit_price is None else "CLOSED"
        return (
            f"Position({status}): {self.side.upper()} | "
            f"Entry: {fmt_cents(self.entry_price)} x {self.quantity} @ {self.entry_time} | "
            f"Exit: {fmt_cents(self.exit_price) if self.exit_price else 'N/A'} @ {self.exit_time or 'N/A'} | "
            f"P&L: {fmt_dollars(self.pnl) if self.pnl is not None else 'N/A'}"
        )


def save_session_to_json(session_number: int, market_ticker: str, session_start_time: str, session_end_time: str,
                         session_start_balance: float, current_balance: float, position: LivePosition | None,
                         trades_log: list, outcome: str):
    session_pnl = current_balance - session_start_balance

    session_data = {
        "session_number": session_number,
        "market_ticker": market_ticker,
        "start_time": session_start_time,
        "end_time": session_end_time,
        "starting_balance": round(session_start_balance, 2),
        "ending_balance": round(current_balance, 2),
        "session_pnl": round(session_pnl, 2),
        "session_pnl_percent": round((session_pnl / session_start_balance * 100), 2) if session_start_balance > 0 else 0,
        "outcome": outcome,
        "trades": []
    }

    for trade in trades_log:
        trade_data = {
            "side": trade.side,
            "entry_price": round(trade.entry_price, 4),
            "entry_time": trade.entry_time,
            "quantity": trade.quantity,
            "entry_order_id": trade.entry_order_id,
            "exit_price": round(trade.exit_price, 4) if trade.exit_price else None,
            "exit_time": trade.exit_time,
            "exit_order_id": trade.exit_order_id,
            "pnl": round(trade.pnl, 2) if trade.pnl is not None else None,
            "status": "CLOSED" if trade.exit_price else "OPEN"
        }
        session_data["trades"].append(trade_data)

    SESSIONS_DIR.mkdir(exist_ok=True)

    session_file = SESSIONS_DIR / f"session_{session_number:03d}_{market_ticker}.json"
    with open(session_file, "w") as f:
        json.dump(session_data, f, indent=2)

    master_log = SESSIONS_DIR / "all_sessions.json"
    if master_log.exists():
        with open(master_log, "r") as f:
            all_sessions = json.load(f)
    else:
        all_sessions = {"sessions": []}

    all_sessions["sessions"].append(session_data)

    with open(master_log, "w") as f:
        json.dump(all_sessions, f, indent=2)

    print(f"Session saved to: {session_file}")
    return session_data


def print_session_summary(session_number: int, market_ticker: str, session_start_time: str, session_end_time: str,
                          session_start_balance: float, current_balance: float, position: LivePosition | None,
                          trades_log: list, outcome: str):
    print("\n" + "=" * 80)
    print(f"SESSION #{session_number} SUMMARY: {market_ticker}")
    print("=" * 80)
    print(f"Start Time:       {session_start_time}")
    print(f"End Time:         {session_end_time}")
    print(f"Starting Balance: {fmt_dollars(session_start_balance)}")
    print(f"Ending Balance:   {fmt_dollars(current_balance)}")

    session_pnl = current_balance - session_start_balance
    print(f"Session P&L:      {fmt_dollars(session_pnl)} ({'+' if session_pnl >= 0 else ''}{session_pnl/session_start_balance*100:.2f}%)")
    print(f"Outcome:          {outcome}")

    if trades_log:
        print(f"\nTrades Executed: {len(trades_log)}")
        for i, trade in enumerate(trades_log, 1):
            print(f"  Trade {i}: {trade}")
    else:
        print("\nNo trades executed this session")

    print("=" * 80 + "\n")

    save_session_to_json(session_number, market_ticker, session_start_time, session_end_time,
                         session_start_balance, current_balance, position, trades_log, outcome)


async def run_live_trading(client: KalshiClient, market_ticker: str, initial_balance: float, session_number: int):
    async def _to_thread(func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _poll_market_result(ticker: str, timeout_seconds: int = 120, interval_seconds: int = 5):
        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
        last_error = None
        while datetime.now(timezone.utc) < deadline:
            try:
                market = await _to_thread(client.get_market, ticker)
                info = market.get("market", {})
                result = info.get("result")
                if result in ("yes", "no"):
                    return result
            except Exception as e:
                last_error = e
            await asyncio.sleep(interval_seconds)
        if last_error:
            print(f"Warning: Could not fetch market result within timeout: {last_error}")
        return None

    async def _reconcile_pending_exit_fills(now: str):
        nonlocal current_balance, position, pending_exit_order_id
        if not pending_exit_order_id or position is None:
            return
        try:
            await asyncio.sleep(2)
            fills_response = await _to_thread(client.get_fills, ticker=market_ticker, limit=50)
            fills = fills_response.get("fills", [])

            total_filled = 0
            fill_price_sum = 0.0
            for fill in fills:
                if fill.get("order_id") != pending_exit_order_id:
                    continue
                count = int(float(fill.get("count_fp", "0")))
                if count <= 0:
                    continue
                total_filled += count
                if position.side == "yes":
                    fill_price = safe_float(fill.get("yes_price_dollars", "0"))
                else:
                    fill_price = safe_float(fill.get("no_price_dollars", "0"))
                if fill_price is not None:
                    fill_price_sum += fill_price * count

            if total_filled <= 0:
                return

            avg_fill_price = (fill_price_sum / total_filled) if total_filled > 0 else 0.0
            proceeds = avg_fill_price * total_filled
            current_balance += proceeds

            position._exit_realized_qty += total_filled
            position._exit_realized_proceeds += proceeds

            if total_filled >= position.quantity:
                full_qty = getattr(position, "entry_quantity", position.quantity)
                total_proceeds = position._exit_realized_proceeds
                avg_exit_price_total = (total_proceeds / full_qty) if full_qty > 0 else avg_fill_price
                position.quantity = full_qty
                position.close(avg_exit_price_total, now, pending_exit_order_id)
                trades_log.append(position)
                position = None
                pending_exit_order_id = None
            else:
                position.quantity -= total_filled
                pending_exit_order_id = None
        except Exception as e:
            print(f"Warning: Could not reconcile pending exit fills at close: {e}")

    session_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    market_data = await _to_thread(client.get_market, market_ticker)
    market_info = market_data.get("market", {})
    close_time_str = market_info.get("close_time")

    if not close_time_str:
        print(f"Warning: Could not get close time for {market_ticker}")
        return initial_balance

    close_time = dateutil_parser.parse(close_time_str)
    eth_target_usd = eth_strike_target_usd(market_info)

    print("\n" + "=" * 80)
    print("PHASE 6: LIVE TRADING SYSTEM (ETH)")
    print(f"WS URL: {WS_URL}")
    print(f"Market: {market_ticker}")
    print(f"Close Time: {close_time_str}")
    print(f"Initial Balance: {fmt_dollars(initial_balance)}")

    if initial_balance <= MINIMUM_ACCOUNT_BALANCE:
        print("\n" + "!" * 80)
        print("⛔ INSUFFICIENT BALANCE - BOT SHUTDOWN ⛔")
        print("!" * 80)
        print(f"Current Balance: {fmt_dollars(initial_balance)}")
        print(f"Minimum Required: {fmt_dollars(MINIMUM_ACCOUNT_BALANCE)}")
        print(f"\nAccount balance is too low to continue trading safely.")
        print(f"Please deposit funds to bring balance above ${MINIMUM_ACCOUNT_BALANCE:.2f}")
        print("\n⚠️  BOT SHUT DOWN - MINIMUM BALANCE REQUIREMENT NOT MET ⚠️")
        print("!" * 80 + "\n")
        raise RuntimeError(f"Insufficient balance: ${initial_balance:.2f} <= ${MINIMUM_ACCOUNT_BALANCE:.2f}")

    print(f"Position Size: {POSITION_SIZE_PCT * 100:.1f}% of balance (SAFETY MODE)")
    print(f"Entry Rule: First side whose ASK reaches >= {fmt_cents(ENTRY_TRIGGER)}")
    print(f"Exit Rule: Near strike in final {FINAL_PHASE_ETH_RULE_SECONDS // 60}m (spot vs strike) -> sell; "
          f"else stop at {fmt_cents(EXIT_TRIGGER)}; else hold to settlement")
    print(f"Trading Delay: Only trade when <= {TRADING_DELAY_MINUTES} minutes remain")
    print(f"No new entries in final {NO_ENTRY_FINAL_SECONDS} seconds")
    if eth_target_usd is not None:
        print(
            f"Final {FINAL_PHASE_ETH_RULE_SECONDS // 60}m ETH rule: spot within ${ETH_TARGET_PROXIMITY_DOLLARS:.0f} "
            f"of strike ${eth_target_usd:,.2f} -> no new entries if flat; sell if holding"
        )
    else:
        print("Final-2m ETH proximity rule: disabled (no floor/cap strike on market)")
    print("NO COOLDOWNS - Can trade continuously")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")

    session_start_balance = initial_balance
    current_balance = initial_balance
    position = None
    last_yes_ask = None
    update_count = 0
    trades_log = []
    outcome = "No trades"
    session_end_time = None
    pending_entry_order_id = None
    pending_entry_side = None
    pending_entry_quantity = None
    pending_entry_time = None
    pending_exit_order_id = None
    pending_exit_price_cents = None
    pending_exit_reason = None
    pending_exit_time = None
    pending_exit_floor_price_cents = None
    next_entry_attempt_time = datetime.min
    next_exit_attempt_time = datetime.min
    rule1_exit_triggered = False
    rule1_exit_time = None
    rule1_exit_price = None
    rule1_boundary_diff = None
    stop_loss_exit_triggered = False
    last_eth_spot_usd = None
    last_eth_spot_fetch_mono = 0.0
    spot_history: list = []          # [(monotonic_time, price)] rolling 2-min window
    spot_velocity: float | None = None
    rising_rapidly: bool = False
    falling_rapidly: bool = False

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            headers = client.ws_auth_headers()
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                open_timeout=20,
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                is_reconnect = retry_count > 0
                if retry_count > 0:
                    print(f"\n✓ Reconnected to WebSocket (attempt {retry_count + 1})")
                else:
                    print("Connected to Kalshi WebSocket.")

                await subscribe_to_ticker(ws, market_ticker)
                print(f"Subscribed to ticker updates for {market_ticker}.\n")

                if is_reconnect or pending_exit_order_id:
                    if pending_exit_order_id and position is not None:
                        print(f"\n⚠️  Checking pending exit order after reconnection: {pending_exit_order_id}")
                        try:
                            await asyncio.sleep(2)
                            fills_response = await _to_thread(client.get_fills, ticker=market_ticker, limit=20)
                            fills = fills_response.get("fills", [])

                            total_filled = 0
                            fill_prices = []
                            for fill in fills:
                                if fill.get("order_id") == pending_exit_order_id:
                                    count = int(float(fill.get("count_fp", "0")))
                                    total_filled += count
                                    if position.side == "yes":
                                        fill_price = safe_float(fill.get("yes_price_dollars", "0"))
                                    else:
                                        fill_price = safe_float(fill.get("no_price_dollars", "0"))
                                    fill_prices.append(fill_price)

                            if total_filled > 0:
                                avg_fill_price = sum(fill_prices) / len(fill_prices) if fill_prices else 0
                                proceeds = avg_fill_price * total_filled
                                current_balance += proceeds

                                print(f"✓ Exit order filled during disconnect: {total_filled} contracts @ ${avg_fill_price:.2f}")
                                print(f"✓ Proceeds: ${proceeds:.2f}")
                                print(f"✓ New balance: ${current_balance:.2f}\n")

                                position.exit_price = avg_fill_price
                                position.exit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                position.exit_order_id = pending_exit_order_id
                                position.close()

                                trades_log.append(position)
                                position = None
                                pending_exit_order_id = None
                            else:
                                print(f"⚠️  Exit order not filled yet - continuing to monitor\n")
                        except Exception as e:
                            print(f"❌ Error checking pending exit order: {e}\n")

                subscribed_printed = False

                while True:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        current_time = datetime.now(timezone.utc)
                        if current_time >= close_time:
                            session_end_time = now
                            print(f"\n[{now}] Market {market_ticker} has CLOSED (reached close time)")

                            yes_ask_f = last_yes_ask

                            if position:
                                await _reconcile_pending_exit_fills(now)
                                if position:
                                    result = await _poll_market_result(market_ticker)
                                    if result is None:
                                        outcome = "Market closed - result unavailable (position unresolved)"
                                    else:
                                        winner = "YES" if result == "yes" else "NO"
                                        settlement_price = 1.0 if position.side == result else 0.0
                                        proceeds = settlement_price * position.quantity
                                        current_balance += proceeds

                                        full_qty = getattr(position, "entry_quantity", position.quantity)
                                        total_proceeds = position._exit_realized_proceeds + proceeds
                                        avg_exit_price_total = (total_proceeds / full_qty) if full_qty > 0 else settlement_price
                                        position.quantity = full_qty
                                        position.close(avg_exit_price_total, now)

                                        print(f"Market settled: {winner} won")
                                        print(f"Position closed at settlement: {position.side.upper()} @ {fmt_cents(settlement_price)}")
                                        print(f"Proceeds: {fmt_dollars(proceeds)}")
                                        print(f"P&L: {fmt_dollars(position.pnl)}")

                                        trades_log.append(position)

                                        if position.side.upper() == winner:
                                            outcome = f"Held {position.side.upper()} to close - WON"
                                        else:
                                            outcome = f"Held {position.side.upper()} to close - LOST"

                                        trade_cost = position.entry_price * position.quantity
                                        trade_pnl_pct = (position.pnl / trade_cost) * 100 if trade_cost > 0 else 0
                                        session_pnl_pct = ((current_balance - session_start_balance) / session_start_balance) * 100 if session_start_balance > 0 else 0
                                        log_trade_to_csv(
                                            session_info={
                                                "session_number": session_number,
                                                "session_start_time": session_start_time,
                                                "session_end_time": now,
                                                "market_ticker": market_ticker,
                                                "starting_balance": session_start_balance,
                                                "ending_balance": current_balance,
                                                "session_pnl": current_balance - session_start_balance,
                                                "session_pnl_percent": session_pnl_pct
                                            },
                                            trade_info={
                                                "trade_number": len(trades_log),
                                                "side": position.side,
                                                "buy_time": position.entry_time,
                                                "buy_price": position.entry_price,
                                                "buy_quantity": position.quantity,
                                                "buy_cost": position.entry_price * position.quantity,
                                                "buy_order_id": position.entry_order_id,
                                                "sold": True,
                                                "sell_time": position.exit_time,
                                                "sell_price": position.exit_price,
                                                "sell_quantity": position.quantity,
                                                "sell_proceeds": proceeds,
                                                "sell_order_id": position.exit_order_id if position.exit_order_id else None,
                                                "exit_type": "SETTLEMENT",
                                                "trade_pnl": position.pnl,
                                                "trade_pnl_percent": trade_pnl_pct,
                                                "emergency_exit": False,
                                                "emergency_exit_time": "",
                                                "emergency_exit_price": "",
                                                "boundary_difference": "",
                                                "market_outcome": winner
                                            }
                                        )
                                else:
                                    outcome = "No position at close"
                            else:
                                outcome = "No position at close"

                            if session_end_time is None:
                                session_end_time = now

                            print_session_summary(
                                session_number, market_ticker, session_start_time, session_end_time,
                                session_start_balance, current_balance, position, trades_log, outcome,
                            )

                            return current_balance

                        continue

                    try:
                        data = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    msg_type = data.get("type")

                    if msg_type == "subscribed":
                        if not subscribed_printed:
                            print(f"[{now}] Subscription confirmed.")
                            subscribed_printed = True
                        continue

                    if msg_type == "error":
                        print(f"[{now}] WebSocket error: {data}")
                        continue

                    if msg_type != "ticker":
                        continue

                    msg = data.get("msg", {})
                    ticker = msg.get("market_ticker")
                    if ticker != market_ticker:
                        continue

                    update_count += 1

                    yes_bid = msg.get("yes_bid_dollars")
                    yes_ask = msg.get("yes_ask_dollars")

                    yes_bid_f = safe_float(yes_bid)
                    yes_ask_f = safe_float(yes_ask)

                    no_bid = (1.0 - yes_ask_f) if yes_ask_f is not None else None
                    no_ask = (1.0 - yes_bid_f) if yes_bid_f is not None else None

                    if PRINT_TICK_UPDATES:
                        yes_ask_str = f"{fmt_cents(yes_ask_f)}" if yes_ask_f is not None else "N/A"
                        no_ask_str = f"{fmt_cents(no_ask)}" if no_ask is not None else "N/A"
                        print(f"[{now}] TICKER: YES ask={yes_ask_str}, NO ask={no_ask_str}")

                    current_time = datetime.now(timezone.utc)
                    seconds_remaining = (close_time - current_time).total_seconds()
                    in_final_two_min = 0 < seconds_remaining <= FINAL_PHASE_ETH_RULE_SECONDS

                    # Always poll ETH spot (needed for velocity check and final-2m rule).
                    mono = time.monotonic()
                    if mono - last_eth_spot_fetch_mono >= ETH_SPOT_POLL_INTERVAL_SECONDS:
                        last_eth_spot_fetch_mono = mono
                        fetched = await asyncio.to_thread(fetch_eth_spot_usd_sync)
                        if fetched is not None:
                            last_eth_spot_usd = fetched
                            spot_history.append((mono, fetched))
                            keep_cutoff = mono - 120.0
                            while spot_history and spot_history[0][0] < keep_cutoff:
                                spot_history.pop(0)
                    # Recompute velocity on every tick (uses cached history).
                    spot_velocity = _spot_velocity_pct(spot_history, VELOCITY_WINDOW_SECONDS)
                    rising_rapidly = spot_velocity is not None and spot_velocity >= RAPID_MOVE_THRESHOLD_PCT
                    falling_rapidly = spot_velocity is not None and spot_velocity <= -RAPID_MOVE_THRESHOLD_PCT

                    near_target_band = (
                        in_final_two_min
                        and eth_target_usd is not None
                        and last_eth_spot_usd is not None
                        and abs(last_eth_spot_usd - eth_target_usd) <= ETH_TARGET_PROXIMITY_DOLLARS
                    )
                    block_entry_near_target = (
                        near_target_band
                        and position is None
                        and pending_entry_order_id is None
                    )

                    market_closed = current_time >= close_time

                    if market_closed:
                        session_end_time = now
                        print(f"\n[{now}] Market {market_ticker} has CLOSED (reached close time)")

                        if position:
                            await _reconcile_pending_exit_fills(now)
                            if position:
                                result = await _poll_market_result(market_ticker)
                                if result is None:
                                    outcome = "Market closed - result unavailable (position unresolved)"
                                else:
                                    winner = "YES" if result == "yes" else "NO"
                                    settlement_price = 1.0 if position.side == result else 0.0
                                    proceeds = settlement_price * position.quantity
                                    current_balance += proceeds

                                    full_qty = getattr(position, "entry_quantity", position.quantity)
                                    total_proceeds = position._exit_realized_proceeds + proceeds
                                    avg_exit_price_total = (total_proceeds / full_qty) if full_qty > 0 else settlement_price
                                    position.quantity = full_qty
                                    position.close(avg_exit_price_total, now)

                                    print(f"Market settled: {winner} won")
                                    print(f"Position closed at settlement: {position.side.upper()} @ {fmt_cents(settlement_price)}")
                                    print(f"Proceeds: {fmt_dollars(proceeds)}")
                                    print(f"P&L: {fmt_dollars(position.pnl)}")

                                    trades_log.append(position)

                                    if position.side.upper() == winner:
                                        outcome = f"Held {position.side.upper()} to close - WON"
                                    else:
                                        outcome = f"Held {position.side.upper()} to close - LOST"

                                    trade_cost = position.entry_price * position.quantity
                                    trade_pnl_pct = (position.pnl / trade_cost) * 100 if trade_cost > 0 else 0
                                    session_pnl_pct = ((current_balance - session_start_balance) / session_start_balance) * 100 if session_start_balance > 0 else 0
                                    log_trade_to_csv(
                                        session_info={
                                            "session_number": session_number,
                                            "session_start_time": session_start_time,
                                            "session_end_time": now,
                                            "market_ticker": market_ticker,
                                            "starting_balance": session_start_balance,
                                            "ending_balance": current_balance,
                                            "session_pnl": current_balance - session_start_balance,
                                            "session_pnl_percent": session_pnl_pct
                                        },
                                        trade_info={
                                            "trade_number": len(trades_log),
                                            "side": position.side,
                                            "buy_time": position.entry_time,
                                            "buy_price": position.entry_price,
                                            "buy_quantity": position.quantity,
                                            "buy_cost": position.entry_price * position.quantity,
                                            "buy_order_id": position.entry_order_id,
                                            "sold": True,
                                            "sell_time": position.exit_time,
                                            "sell_price": position.exit_price,
                                            "sell_quantity": position.quantity,
                                            "sell_proceeds": proceeds,
                                            "sell_order_id": position.exit_order_id if position.exit_order_id else None,
                                            "exit_type": "SETTLEMENT",
                                            "trade_pnl": position.pnl,
                                            "trade_pnl_percent": trade_pnl_pct,
                                            "emergency_exit": False,
                                            "emergency_exit_time": "",
                                            "emergency_exit_price": "",
                                            "boundary_difference": "",
                                            "market_outcome": winner
                                        }
                                    )
                        else:
                            outcome = "No position at close"

                        if session_end_time is None:
                            session_end_time = now

                        print_session_summary(
                            session_number, market_ticker, session_start_time, session_end_time,
                            session_start_balance, current_balance, position, trades_log, outcome,
                        )

                        return current_balance

                    # Check if pending entry has filled
                    if pending_entry_order_id and position is None:
                        try:
                            fills_response = await _to_thread(client.get_fills, ticker=market_ticker, limit=20)
                            fills = fills_response.get("fills", [])

                            total_filled = 0
                            fill_price_sum = 0.0
                            for fill in fills:
                                if fill.get("order_id") == pending_entry_order_id:
                                    count = int(float(fill.get("count_fp", "0")))
                                    total_filled += count
                                    if pending_entry_side == "yes":
                                        price = safe_float(fill.get("yes_price_dollars", "0"))
                                    else:
                                        price = safe_float(fill.get("no_price_dollars", "0"))
                                    if price is not None and count > 0:
                                        fill_price_sum += price * count

                            avg_fill_price = (fill_price_sum / total_filled) if total_filled > 0 else None
                            time_since_order = (datetime.now() - pending_entry_time).total_seconds() if pending_entry_time else 0

                            if total_filled > 0 and total_filled < pending_entry_quantity:
                                print(f"\n[{now}] ⚠️  PARTIAL FILL DETECTED ⚠️")
                                print(f"Expected: {pending_entry_quantity} contracts | Filled: {total_filled} contracts")
                                print(f"Canceling order and rejecting partial fill...")

                                try:
                                    await _to_thread(client.delete, f"/portfolio/orders/{pending_entry_order_id}", auth=True)
                                    print(f"Order {pending_entry_order_id} canceled successfully")
                                except Exception as cancel_error:
                                    print(f"Warning: Could not cancel order: {cancel_error}")

                                if avg_fill_price is None:
                                    print(f"[{now}] ERROR: Partial fill missing price; cannot safely liquidate.")
                                else:
                                    position = LivePosition(
                                        pending_entry_side, avg_fill_price, total_filled, now, pending_entry_order_id,
                                    )
                                    cost = avg_fill_price * total_filled
                                    current_balance -= cost
                                    print(f"[{now}] Continuing with partially filled position: {pending_entry_side.upper()} x {total_filled} @ {fmt_cents(avg_fill_price)}")
                                    print(f"[{now}] Remaining Balance: {fmt_dollars(current_balance)}")

                                pending_entry_order_id = None
                                pending_entry_side = None
                                pending_entry_quantity = None
                                pending_entry_time = None
                                next_entry_attempt_time = datetime.now() + timedelta(seconds=30)
                                continue

                            elif total_filled > 0 and total_filled == pending_entry_quantity:
                                position = LivePosition(pending_entry_side, avg_fill_price, total_filled, now, pending_entry_order_id)
                                cost = avg_fill_price * total_filled
                                current_balance -= cost

                                print("\n" + ">" * 80)
                                print(f"[{now}] ENTRY FILLED: BUY {pending_entry_side.upper()}")
                                print(f"Entry Price: {fmt_cents(avg_fill_price)}")
                                print(f"Quantity: {total_filled} contracts (FULL FILL)")
                                print(f"Cost: {fmt_dollars(cost)}")
                                print(f"Remaining Balance: {fmt_dollars(current_balance)}")
                                print(">" * 80 + "\n")

                                balance_before_trade = current_balance + cost
                                expected_position_value = balance_before_trade * POSITION_SIZE_PCT
                                position_value_tolerance = 0.15
                                max_allowed = expected_position_value * (1 + position_value_tolerance)

                                violations = []
                                if avg_fill_price is None:
                                    violations.append("Entry fill price missing/invalid")
                                else:
                                    fill_cents = int(round(avg_fill_price * 100))
                                    limit_cents = int(round(ENTRY_TRIGGER * 100))
                                    if fill_cents > (limit_cents + 1):
                                        violations.append(
                                            f"Entry price {fmt_cents(avg_fill_price)} is worse than limit {fmt_cents(ENTRY_TRIGGER)}"
                                        )
                                    if fill_cents < MIN_ENTRY_FILL_CENTS:
                                        violations.append(
                                            f"Entry price {fmt_cents(avg_fill_price)} is below minimum allowed {MIN_ENTRY_FILL_CENTS}c"
                                        )
                                if cost > max_allowed:
                                    violations.append(
                                        f"Position cost ${cost:.2f} exceeds allowed max ${max_allowed:.2f}"
                                    )

                                if violations:
                                    print("\n" + "!" * 80)
                                    print("⚠️  BOT VALIDATION WARNING - LIQUIDATING THIS ENTRY ⚠️")
                                    print("!" * 80)
                                    for i, violation in enumerate(violations, 1):
                                        print(f"{i}. {violation}")
                                    print("\nPlacing immediate liquidation sell...")
                                    print("!" * 80 + "\n")

                                    try:
                                        liq_cents = max(1, min(99, int(round(avg_fill_price * 100)) - 1))
                                        order_response = await _to_thread(
                                            client.create_order,
                                            ticker=market_ticker,
                                            side=position.side,
                                            action="sell",
                                            count=position.quantity,
                                            yes_price=liq_cents if position.side == "yes" else None,
                                            no_price=liq_cents if position.side == "no" else None,
                                        )
                                        order = order_response.get("order", {})
                                        pending_exit_order_id = order.get("order_id")
                                        pending_exit_price_cents = liq_cents
                                        pending_exit_reason = "BAD_ENTRY"
                                        pending_exit_time = datetime.now()
                                        pending_exit_floor_price_cents = liq_cents
                                        print(f"[{now}] Liquidation sell placed at {liq_cents}c (order {pending_exit_order_id})")
                                    except Exception as e:
                                        print(f"[{now}] ERROR placing liquidation sell: {e}")

                                    pending_entry_order_id = None
                                    pending_entry_side = None
                                    pending_entry_quantity = None
                                    pending_entry_time = None
                                    next_entry_attempt_time = datetime.now() + timedelta(seconds=30)
                                    continue

                                pending_entry_order_id = None
                                pending_entry_side = None
                                pending_entry_quantity = None
                                pending_entry_time = None

                            elif total_filled == 0 and time_since_order > ENTRY_ORDER_TIMEOUT_SECONDS:
                                print(f"\n[{now}] Order not filled after {ENTRY_ORDER_TIMEOUT_SECONDS} seconds - canceling...")
                                try:
                                    await _to_thread(client.delete, f"/portfolio/orders/{pending_entry_order_id}", auth=True)
                                    print(f"Order {pending_entry_order_id} canceled")
                                except Exception as cancel_error:
                                    print(f"Warning: Could not cancel order: {cancel_error}")

                                pending_entry_order_id = None
                                pending_entry_side = None
                                pending_entry_quantity = None
                                pending_entry_time = None

                        except Exception as e:
                            if PRINT_TICK_UPDATES:
                                print(f"DEBUG: Error checking fills: {e}")

                    # Entry logic
                    time_remaining = (close_time - current_time).total_seconds() / 60
                    trading_allowed = time_remaining <= TRADING_DELAY_MINUTES
                    current_price = None

                    if (
                        position is None
                        and pending_entry_order_id is None
                        and trading_allowed
                        and not stop_loss_exit_triggered
                        and not block_entry_near_target
                    ):
                        if seconds_remaining <= NO_ENTRY_FINAL_SECONDS:
                            continue
                        if datetime.now() < next_entry_attempt_time:
                            continue
                        if yes_ask_f is not None and yes_ask_f >= ENTRY_TRIGGER and falling_rapidly:
                            vel_str = f"{spot_velocity:+.2f}%" if spot_velocity is not None else "?"
                            print(f"[{now}] VELOCITY BLOCK (YES entry): ETH falling rapidly ({vel_str} in {VELOCITY_WINDOW_SECONDS}s) – skipping")
                            continue
                        if no_ask is not None and no_ask >= ENTRY_TRIGGER and rising_rapidly:
                            vel_str = f"{spot_velocity:+.2f}%" if spot_velocity is not None else "?"
                            print(f"[{now}] VELOCITY BLOCK (NO entry): ETH rising rapidly ({vel_str} in {VELOCITY_WINDOW_SECONDS}s) – skipping")
                            continue
                        if yes_ask_f is not None and yes_ask_f >= ENTRY_TRIGGER:
                            position_value_dollars = current_balance * POSITION_SIZE_PCT
                            desired_qty = int(position_value_dollars / ENTRY_TRIGGER)
                            max_affordable_qty = int(current_balance / ENTRY_TRIGGER)
                            quantity = min(max_affordable_qty, max(1, desired_qty)) if max_affordable_qty >= 1 else 0
                            if quantity > 0:
                                yes_price_dollars = f"{ENTRY_TRIGGER:.4f}"
                                try:
                                    print(f"[{now}] PLACING BUY ORDER: YES @ {fmt_cents(ENTRY_TRIGGER)} x {quantity} contracts")
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker, side="yes", action="buy",
                                        count=quantity, yes_price_dollars=yes_price_dollars,
                                    )
                                    order = order_response.get("order", {})
                                    pending_entry_order_id = order.get("order_id")
                                    pending_entry_side = "yes"
                                    pending_entry_quantity = quantity
                                    pending_entry_time = datetime.now()
                                except Exception as e:
                                    print(f"[{now}] ERROR placing buy order: {e}")
                                    resp = getattr(e, "response", None)
                                    if resp is not None:
                                        try:
                                            err_json = resp.json()
                                            print(f"[{now}] API response JSON: {err_json}")
                                            if isinstance(err_json, dict) and (err_json.get("error") or {}).get("code") == "insufficient_balance":
                                                next_entry_attempt_time = datetime.now() + timedelta(seconds=30)
                                        except Exception:
                                            pass

                        elif no_ask is not None and no_ask >= ENTRY_TRIGGER:
                            position_value_dollars = current_balance * POSITION_SIZE_PCT
                            desired_qty = int(position_value_dollars / ENTRY_TRIGGER)
                            max_affordable_qty = int(current_balance / ENTRY_TRIGGER)
                            quantity = min(max_affordable_qty, max(1, desired_qty)) if max_affordable_qty >= 1 else 0
                            if quantity > 0:
                                no_price_dollars = f"{ENTRY_TRIGGER:.4f}"
                                try:
                                    print(f"[{now}] PLACING BUY ORDER: NO @ {fmt_cents(ENTRY_TRIGGER)} x {quantity} contracts")
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker, side="no", action="buy",
                                        count=quantity, no_price_dollars=no_price_dollars,
                                    )
                                    order = order_response.get("order", {})
                                    pending_entry_order_id = order.get("order_id")
                                    pending_entry_side = "no"
                                    pending_entry_quantity = quantity
                                    pending_entry_time = datetime.now()
                                except Exception as e:
                                    print(f"[{now}] ERROR placing buy order: {e}")
                                    resp = getattr(e, "response", None)
                                    if resp is not None:
                                        try:
                                            err_json = resp.json()
                                            print(f"[{now}] API response JSON: {err_json}")
                                            if isinstance(err_json, dict) and (err_json.get("error") or {}).get("code") == "insufficient_balance":
                                                next_entry_attempt_time = datetime.now() + timedelta(seconds=30)
                                        except Exception:
                                            pass

                    # Position management: final-2m ETH near strike -> exit; else stop at EXIT_TRIGGER; else hold.
                    elif position is not None and pending_exit_order_id is None:
                        current_price = yes_ask_f if position.side == "yes" else no_ask

                        rapid_move_exit = (
                            current_price is not None
                            and datetime.now() >= next_exit_attempt_time
                            and (
                                (position.side == "no" and rising_rapidly)
                                or (position.side == "yes" and falling_rapidly)
                            )
                        )
                        if rapid_move_exit:
                            vel_str = f"{spot_velocity:+.2f}%" if spot_velocity is not None else "?"
                            price_cents = max(1, min(99, int(round(current_price * 100))))
                            sell_quantity = position.quantity
                            if sell_quantity <= 0:
                                print(f"[{now}] ERROR: Invalid sell quantity {sell_quantity}! Skipping rapid-move exit.")
                            else:
                                try:
                                    direction = "rising" if rising_rapidly else "falling"
                                    print(
                                        f"\n[{now}] RAPID MOVE EXIT: ETH {direction} rapidly "
                                        f"({vel_str} in {VELOCITY_WINDOW_SECONDS}s) "
                                        f"| holding {position.side.upper()} @ {fmt_cents(current_price)}"
                                    )
                                    print(f"[{now}] PLACING SELL ORDER: {position.side.upper()} @ {fmt_cents(current_price)} x {sell_quantity}")
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker,
                                        side=position.side,
                                        action="sell",
                                        count=sell_quantity,
                                        yes_price=price_cents if position.side == "yes" else None,
                                        no_price=price_cents if position.side == "no" else None,
                                    )
                                    order = order_response.get("order", {})
                                    exit_order_id = order.get("order_id")
                                    pending_exit_order_id = exit_order_id
                                    pending_exit_price_cents = price_cents
                                    pending_exit_reason = "RAPID_MOVE"
                                    pending_exit_time = datetime.now()
                                    pending_exit_floor_price_cents = EXIT_LADDER_STOP_FLOOR_CENTS
                                    stop_loss_exit_triggered = True
                                    print(f"Rapid-move sell order placed: {exit_order_id}")
                                except Exception as e:
                                    print(f"[{now}] ERROR placing rapid-move sell order: {e}")
                                    next_exit_attempt_time = datetime.now() + timedelta(seconds=10)
                        elif (
                            near_target_band
                            and current_price is not None
                            and datetime.now() >= next_exit_attempt_time
                        ):
                            price_cents = max(1, min(99, int(round(current_price * 100))))
                            sell_quantity = position.quantity
                            if sell_quantity <= 0:
                                print(f"[{now}] ERROR: Invalid sell quantity {sell_quantity}! Skipping near-target exit.")
                            else:
                                try:
                                    print(
                                        f"\n[{now}] NEAR-TARGET EXIT: spot ETH ${last_eth_spot_usd:,.2f} "
                                        f"within ${ETH_TARGET_PROXIMITY_DOLLARS:.0f} of strike ${eth_target_usd:,.2f} "
                                        f"(final {FINAL_PHASE_ETH_RULE_SECONDS // 60}m)"
                                    )
                                    print(f"[{now}] PLACING SELL ORDER: {position.side.upper()} @ {fmt_cents(current_price)} x {sell_quantity}")
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker, side=position.side, action="sell",
                                        count=sell_quantity,
                                        yes_price=price_cents if position.side == "yes" else None,
                                        no_price=price_cents if position.side == "no" else None,
                                    )
                                    order = order_response.get("order", {})
                                    pending_exit_order_id = order.get("order_id")
                                    pending_exit_price_cents = price_cents
                                    pending_exit_reason = "NEAR_TARGET"
                                    pending_exit_time = datetime.now()
                                    pending_exit_floor_price_cents = EXIT_LADDER_STOP_FLOOR_CENTS
                                    stop_loss_exit_triggered = True
                                    print(f"Near-target sell order placed: {pending_exit_order_id}")
                                except Exception as e:
                                    print(f"[{now}] ERROR placing near-target sell order: {e}")
                                    next_exit_attempt_time = datetime.now() + timedelta(seconds=10)

                        elif current_price is not None and current_price <= EXIT_TRIGGER:
                            if datetime.now() >= next_exit_attempt_time:
                                price_cents = max(1, min(99, int(round(current_price * 100))))
                                sell_quantity = position.quantity
                                if sell_quantity <= 0:
                                    print(f"[{now}] ERROR: Invalid sell quantity {sell_quantity}! Skipping stop exit.")
                                else:
                                    try:
                                        print(
                                            f"\n[{now}] STOP LOSS TRIGGERED: Entry @ {fmt_cents(position.entry_price)} "
                                            f"| Trigger @ {fmt_cents(EXIT_TRIGGER)} | Current @ {fmt_cents(current_price)}"
                                        )
                                        print(f"[{now}] Selling ALL {sell_quantity} contracts")
                                        print(f"[{now}] PLACING SELL ORDER: {position.side.upper()} @ {fmt_cents(current_price)} x {sell_quantity}")
                                        order_response = await _to_thread(
                                            client.create_order,
                                            ticker=market_ticker, side=position.side, action="sell",
                                            count=sell_quantity,
                                            yes_price=price_cents if position.side == "yes" else None,
                                            no_price=price_cents if position.side == "no" else None,
                                        )
                                        order = order_response.get("order", {})
                                        pending_exit_order_id = order.get("order_id")
                                        pending_exit_price_cents = price_cents
                                        pending_exit_reason = "STOP"
                                        pending_exit_time = datetime.now()
                                        pending_exit_floor_price_cents = EXIT_LADDER_STOP_FLOOR_CENTS
                                        stop_loss_exit_triggered = True
                                        print(f"Stop sell order placed: {pending_exit_order_id}")
                                    except Exception as e:
                                        print(f"[{now}] ERROR placing stop sell order: {e}")
                                        next_exit_attempt_time = datetime.now() + timedelta(seconds=10)

                    # Check if pending exit order has filled
                    if pending_exit_order_id and position is not None:
                        try:
                            await asyncio.sleep(2)
                            fills_response = await _to_thread(client.get_fills, ticker=market_ticker, limit=20)
                            fills = fills_response.get("fills", [])

                            total_filled = 0
                            fill_price_sum = 0.0
                            for fill in fills:
                                if fill.get("order_id") == pending_exit_order_id:
                                    count = int(float(fill.get("count_fp", "0")))
                                    total_filled += count
                                    if position.side == "yes":
                                        fill_price = safe_float(fill.get("yes_price_dollars", "0"))
                                    else:
                                        fill_price = safe_float(fill.get("no_price_dollars", "0"))
                                    if fill_price is not None and count > 0:
                                        fill_price_sum += fill_price * count

                            avg_fill_price = (fill_price_sum / total_filled) if total_filled > 0 else None

                            if total_filled == 0:
                                if pending_exit_time is None:
                                    pending_exit_time = datetime.now()
                                if pending_exit_floor_price_cents is None:
                                    pending_exit_floor_price_cents = 1

                                time_since_exit_order = (datetime.now() - pending_exit_time).total_seconds()
                                seconds_remaining = (close_time - datetime.now(timezone.utc)).total_seconds()
                                if (
                                    time_since_exit_order >= EXIT_LADDER_INTERVAL_SECONDS
                                    and pending_exit_price_cents is not None
                                    and pending_exit_price_cents > pending_exit_floor_price_cents
                                    and seconds_remaining <= 45
                                ):
                                    next_price_cents = max(pending_exit_floor_price_cents, pending_exit_price_cents - EXIT_LADDER_STEP_CENTS)
                                    try:
                                        await _to_thread(client.delete, f"/portfolio/orders/{pending_exit_order_id}", auth=True)
                                    except Exception:
                                        pass
                                    try:
                                        order_response = await _to_thread(
                                            client.create_order,
                                            ticker=market_ticker, side=position.side, action="sell",
                                            count=position.quantity,
                                            yes_price=next_price_cents if position.side == "yes" else None,
                                            no_price=next_price_cents if position.side == "no" else None,
                                        )
                                        order = order_response.get("order", {})
                                        pending_exit_order_id = order.get("order_id")
                                        pending_exit_price_cents = next_price_cents
                                        pending_exit_time = datetime.now()
                                        print(f"[{now}] Exit not filled; laddering down to {next_price_cents}c (order {pending_exit_order_id})")
                                    except Exception as e:
                                        print(f"[{now}] ERROR laddering exit order: {e}")
                                        pending_exit_time = datetime.now()
                                continue

                            if total_filled > 0 and total_filled < position.quantity:
                                remaining_qty = position.quantity - total_filled
                                proceeds = (avg_fill_price or 0.0) * total_filled
                                current_balance += proceeds
                                position._exit_realized_qty += total_filled
                                position._exit_realized_proceeds += proceeds

                                print(f"\n⚠️  PARTIAL EXIT FILL: {total_filled} sold, {remaining_qty} remaining")
                                position.quantity = remaining_qty

                                try:
                                    await _to_thread(client.delete, f"/portfolio/orders/{pending_exit_order_id}", auth=True)
                                except Exception:
                                    pass

                                price_cents = pending_exit_price_cents or int((avg_fill_price or 0.0) * 100)
                                try:
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker, side=position.side, action="sell",
                                        count=remaining_qty,
                                        yes_price=price_cents if position.side == "yes" else None,
                                        no_price=price_cents if position.side == "no" else None,
                                    )
                                    order = order_response.get("order", {})
                                    pending_exit_order_id = order.get("order_id")
                                    pending_exit_price_cents = price_cents
                                    pending_exit_time = datetime.now()
                                    print(f"New exit order placed for remaining {remaining_qty}: {pending_exit_order_id}")
                                except Exception as e:
                                    print(f"[{now}] ERROR placing follow-up exit order: {e}")
                                    pending_exit_order_id = None
                                continue

                            elif total_filled > 0 and total_filled == position.quantity:
                                proceeds = (avg_fill_price or 0.0) * total_filled
                                current_balance += proceeds
                                position._exit_realized_qty += total_filled
                                position._exit_realized_proceeds += proceeds

                                full_qty = getattr(position, "entry_quantity", position.quantity)
                                total_proceeds = position._exit_realized_proceeds
                                avg_exit_price_total = (total_proceeds / full_qty) if full_qty > 0 else (avg_fill_price or 0.0)
                                position.quantity = full_qty
                                position.close(avg_exit_price_total, now, pending_exit_order_id)

                                print("\n" + "<" * 80)
                                print(f"[{now}] EXIT FILLED: SELL {position.side.upper()}")
                                print(f"Exit Price: {fmt_cents(avg_fill_price)}")
                                print(f"Quantity: {total_filled} contracts")
                                print(f"Proceeds: {fmt_dollars(proceeds)}")
                                print(f"P&L: {fmt_dollars(position.pnl)}")
                                print(f"New Balance: {fmt_dollars(current_balance)}")
                                print("<" * 80 + "\n")

                                trades_log.append(position)
                                print(f"Trade #{len(trades_log)}: {position}\n")

                                if position.pnl >= 0:
                                    outcome = f"Exited {position.side.upper()} at stop - Small profit"
                                else:
                                    outcome = f"Exited {position.side.upper()} at stop - Loss"

                                trade_cost = position.entry_price * getattr(position, "entry_quantity", position.quantity)
                                trade_pnl_pct = (position.pnl / trade_cost) * 100 if trade_cost > 0 else 0
                                session_pnl_pct = ((current_balance - session_start_balance) / session_start_balance) * 100 if session_start_balance > 0 else 0
                                is_emergency_exit = rule1_exit_price is not None

                                log_trade_to_csv(
                                    session_info={
                                        "session_number": session_number,
                                        "session_start_time": session_start_time,
                                        "session_end_time": now,
                                        "market_ticker": market_ticker,
                                        "starting_balance": session_start_balance,
                                        "ending_balance": current_balance,
                                        "session_pnl": current_balance - session_start_balance,
                                        "session_pnl_percent": session_pnl_pct
                                    },
                                    trade_info={
                                        "trade_number": len(trades_log),
                                        "side": position.side,
                                        "buy_time": position.entry_time,
                                        "buy_price": position.entry_price,
                                        "buy_quantity": getattr(position, "entry_quantity", position.quantity),
                                        "buy_cost": position.entry_price * getattr(position, "entry_quantity", position.quantity),
                                        "buy_order_id": position.entry_order_id,
                                        "sold": True,
                                        "sell_time": position.exit_time,
                                        "sell_price": position.exit_price,
                                        "sell_quantity": getattr(position, "entry_quantity", position.quantity),
                                        "sell_proceeds": total_proceeds,
                                        "sell_order_id": position.exit_order_id,
                                        "exit_type": "MANUAL_STOP_LOSS",
                                        "trade_pnl": position.pnl,
                                        "trade_pnl_percent": trade_pnl_pct,
                                        "emergency_exit": is_emergency_exit,
                                        "emergency_exit_time": rule1_exit_time if is_emergency_exit else "",
                                        "emergency_exit_price": rule1_exit_price if is_emergency_exit else "",
                                        "boundary_difference": rule1_boundary_diff if is_emergency_exit else "",
                                        "market_outcome": ""
                                    }
                                )

                                position = None
                                pending_exit_order_id = None
                                pending_exit_price_cents = None
                                pending_exit_reason = None
                                pending_exit_time = None
                                pending_exit_floor_price_cents = None

                        except Exception as e:
                            print(f"DEBUG: Error checking exit fills: {e}")

                    last_yes_ask = yes_ask_f

        except websockets.exceptions.ConnectionClosedError as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 30)
                print(f"\n[!] WebSocket connection lost: {e}")
                print(f"Reconnecting in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"\n[!] Failed to reconnect after {max_retries} attempts")
                raise

        except Exception as e:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] DEBUG: WebSocket loop ended due to Exception: {e}")

            if isinstance(e, (socket.gaierror, OSError)):
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 30)
                    print(f"\n[!] Temporary network error: {e}")
                    print(f"Reconnecting in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue

            raise


async def main_loop():
    print("Initializing Kalshi client (ETH bot)...")
    client = KalshiClient(API_KEY_ID, PRIVATE_KEY_PATH, BASE_URL)
    print("Client initialized.\n")

    print("Fetching account balance...")
    try:
        balance_data = client.get_balance()
        balance_cents = safe_float(balance_data.get("balance")) if isinstance(balance_data, dict) else safe_float(balance_data)
        balance = (balance_cents or 1000000.0) / 100.0
    except Exception as e:
        print(f"Could not fetch balance: {e}")
        balance = 10000.0
    print(f"Account balance: {fmt_dollars(balance)}\n")

    session_count = 0

    while True:
        session_count += 1
        print(f"\n{'#' * 80}")
        print(f"STARTING SESSION #{session_count}")
        print(f"{'#' * 80}\n")

        if session_count > 1:
            print("Refreshing account balance...")
            try:
                balance_data = client.get_balance()
                balance_cents = safe_float(balance_data.get("balance")) if isinstance(balance_data, dict) else safe_float(balance_data)
                balance = (balance_cents or balance * 100) / 100.0
                print(f"Updated balance: {fmt_dollars(balance)}\n")
            except Exception as e:
                print(f"Could not refresh balance: {e}, using previous: {fmt_dollars(balance)}\n")

        ticker = find_latest_eth15m_market(client)

        if not ticker:
            print("\nCould not find an active ETH 15-minute market.")
            print("Waiting 30 seconds before trying again...\n")
            await asyncio.sleep(30)
            continue

        new_balance = await run_live_trading(client, ticker, balance, session_count)

        if new_balance is not None:
            balance = new_balance
            print(f"Rolling over balance to next session: {fmt_dollars(balance)}")

        print("\n" + "=" * 80)
        print("SESSION COMPLETE - SEARCHING FOR NEXT MARKET")
        print("=" * 80)
        print("All session data logged successfully.")
        print(f"Current balance: {fmt_dollars(balance)}")
        log_completed_daily_net_changes()
        print("Waiting 60 seconds for balance/portfolio updates before next market...\n")
        await asyncio.sleep(60)


def main():
    asyncio.run(main_loop())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nUNEXPECTED ERROR\n{type(e).__name__} {e}")
