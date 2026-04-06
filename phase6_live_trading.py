import asyncio
import base64
import csv
import json
import os
import time
from datetime import datetime
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

# Trading parameters - SAFETY FIRST: 2% position sizing
ENTRY_TRIGGER = 0.91  # Enter when ask >= 91 cents
EXIT_TRIGGER = 0.82   # Exit when price falls to <= 82 cents
POSITION_SIZE_PCT = 0.50  # Use 50% of account balance


def base_url_to_ws_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/trade-api/ws/v2"


WS_URL = base_url_to_ws_url(BASE_URL)

# CSV logging setup
CSV_LOG_PATH = Path("csv_trading_logs/trades_log.csv")
CSV_HEADERS = [
    "session_number", "session_date", "session_start_time", "session_end_time",
    "market_ticker", "starting_balance", "ending_balance", "session_pnl", "session_pnl_percent",
    "trade_number", "side", "buy_time", "buy_price", "buy_quantity", "buy_cost", "buy_order_id",
    "sold", "sell_time", "sell_price", "sell_quantity", "sell_proceeds", "sell_order_id",
    "exit_type", "trade_pnl", "trade_pnl_percent", "hold_duration_seconds", "outcome"
]


def ensure_csv_exists():
    """Create CSV file with headers if it doesn't exist"""
    CSV_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_LOG_PATH.exists():
        with open(CSV_LOG_PATH, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)


def log_trade_to_csv(session_info: dict, trade_info: dict):
    """Log a single trade to CSV"""
    try:
        ensure_csv_exists()
        
        # Calculate hold duration
        hold_duration = None
        if trade_info.get("buy_time") and trade_info.get("sell_time"):
            try:
                buy_dt = datetime.strptime(trade_info["buy_time"], "%Y-%m-%d %H:%M:%S")
                sell_dt = datetime.strptime(trade_info["sell_time"], "%Y-%m-%d %H:%M:%S")
                hold_duration = int((sell_dt - buy_dt).total_seconds())
            except:
                hold_duration = None
        
        # Determine outcome
        outcome = "OPEN"
        if trade_info.get("sold"):
            outcome = "WIN" if trade_info.get("trade_pnl", 0) >= 0 else "LOSS"
        
        # Extract session date from start time
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
            outcome
        ]
        
        with open(CSV_LOG_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception as e:
        print(f"WARNING: Failed to log trade to CSV: {e}")
        # Don't crash the bot if CSV logging fails


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
        params = {
            "category": category,
            "limit": limit,
        }
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
        """Get current positions for a market or all positions."""
        params = {}
        if ticker:
            params["ticker"] = ticker
        response = self.get("/portfolio/positions", params=params, auth=True)
        response.raise_for_status()
        return response.json()

    def get_orders(self, ticker: str = None, status: str = None) -> dict:
        """Get orders, optionally filtered by ticker and status."""
        params = {}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status
        response = self.get("/portfolio/orders", params=params, auth=True)
        response.raise_for_status()
        return response.json()

    def create_order(self, ticker: str, side: str, action: str, count: int, yes_price: int = None, no_price: int = None) -> dict:
        """
        Create a limit order.
        
        Args:
            ticker: Market ticker
            side: "yes" or "no"
            action: "buy" or "sell"
            count: Number of contracts (whole number)
            yes_price: YES price in cents (1-99), mutually exclusive with no_price
            no_price: NO price in cents (1-99), mutually exclusive with yes_price
        """
        order_data = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": "limit",
        }
        
        if yes_price is not None:
            order_data["yes_price"] = yes_price
        elif no_price is not None:
            order_data["no_price"] = no_price
        else:
            raise ValueError("Must provide either yes_price or no_price")
        
        response = self.post("/portfolio/orders", data=order_data, auth=True)
        response.raise_for_status()
        return response.json()

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an order by ID."""
        response = self.delete(f"/portfolio/orders/{order_id}", auth=True)
        response.raise_for_status()
        return response.json()

    def get_fills(self, ticker: str = None, limit: int = 100) -> dict:
        """Get recent fills (executed trades)."""
        params = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        response = self.get("/portfolio/fills", params=params, auth=True)
        response.raise_for_status()
        return response.json()


def text_contains_btc(*values) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    keywords = ["btc", "bitcoin", "kxbtc"]
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


def find_latest_btc15m_market(client: KalshiClient) -> str | None:
    print("Finding latest BTC 15-minute market...")
    
    data = client.get_milestones(category="Crypto", limit=200)
    milestones = data.get("milestones", [])
    
    btc_milestones = [
        m for m in milestones
        if text_contains_btc(
            m.get("title"),
            m.get("category"),
            m.get("related_event_tickers"),
            m.get("primary_event_tickers"),
        )
    ]
    
    if not btc_milestones:
        print("No BTC milestones found.")
        return None
    
    btc_milestones.sort(key=milestone_priority, reverse=True)
    chosen_milestone = btc_milestones[0]
    
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
    
    btc15m_markets = [
        m for m in nested_markets
        if (m.get("event_ticker") or "").upper().startswith("KXBTC15M")
        or (m.get("ticker") or "").upper().startswith("KXBTC15M")
    ]
    
    active_markets = [
        m for m in btc15m_markets
        if (m.get("status") or "").lower() == "active"
    ]
    
    if not active_markets:
        print("No active BTC15M markets found.")
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


def safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


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
    """Tracks a real position with order IDs and fill tracking."""
    def __init__(self, side: str, entry_price: float, quantity: int, entry_time: str, order_id: str = None):
        self.side = side
        self.entry_price = entry_price
        self.quantity = quantity
        self.entry_time = entry_time
        self.entry_order_id = order_id
        self.exit_price = None
        self.exit_time = None
        self.exit_order_id = None
        self.pnl = None

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
    
    sessions_dir = Path("trading_sessions_live")
    sessions_dir.mkdir(exist_ok=True)
    
    session_file = sessions_dir / f"session_{session_number:03d}_{market_ticker}.json"
    with open(session_file, "w") as f:
        json.dump(session_data, f, indent=2)
    
    master_log = sessions_dir / "all_sessions.json"
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
    headers = client.ws_auth_headers()
    session_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    market_data = client.get_market(market_ticker)
    market_info = market_data.get("market", {})
    close_time_str = market_info.get("close_time")
    
    if not close_time_str:
        print(f"Warning: Could not get close time for {market_ticker}")
        return initial_balance
    
    close_time = dateutil_parser.parse(close_time_str)

    print("\n" + "=" * 80)
    print("PHASE 6: LIVE TRADING SYSTEM")
    print(f"WS URL: {WS_URL}")
    print(f"Market: {market_ticker}")
    print(f"Close Time: {close_time_str}")
    print(f"Initial Balance: {fmt_dollars(initial_balance)}")
    print(f"Position Size: {POSITION_SIZE_PCT * 100:.1f}% of balance (SAFETY MODE)")
    print(f"Entry Rule: First side whose ASK reaches >= {fmt_cents(ENTRY_TRIGGER)}")
    print(f"Exit Rule: When price falls to <= {fmt_cents(EXIT_TRIGGER)}")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")

    session_start_balance = initial_balance
    current_balance = initial_balance
    position = None
    last_yes_ask = None
    update_count = 0
    trades_log = []
    outcome = "No trades"
    pending_entry_order_id = None
    pending_entry_side = None
    pending_exit_order_id = None

    async with websockets.connect(
        WS_URL,
        additional_headers=headers,
        open_timeout=20,
    ) as ws:
        print("Connected to Kalshi WebSocket.")
        await subscribe_to_ticker(ws, market_ticker)
        print(f"Subscribed to ticker updates for {market_ticker}.\n")

        async for raw_message in ws:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                data = json.loads(raw_message)
            except json.JSONDecodeError:
                print(f"[{now}] Non-JSON message received: {raw_message}")
                continue

            msg_type = data.get("type")

            if msg_type == "subscribed":
                print(f"[{now}] Subscription confirmed.")
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

            current_time = datetime.now(close_time.tzinfo)
            market_closed = current_time >= close_time

            if market_closed:
                session_end_time = now
                print(f"\n[{now}] Market {market_ticker} has CLOSED (reached close time)")
                
                if position:
                    await asyncio.sleep(2)
                    
                    try:
                        final_market = client.get_market(market_ticker)
                        final_info = final_market.get("market", {})
                        result = final_info.get("result")
                        
                        if result == "yes":
                            winner = "YES"
                            settlement_price = 1.0 if position.side == "yes" else 0.0
                        elif result == "no":
                            winner = "NO"
                            settlement_price = 0.0 if position.side == "yes" else 1.0
                        else:
                            print(f"Warning: Market result not yet available, using last prices")
                            if yes_ask_f and yes_ask_f >= 0.99:
                                winner = "YES"
                                settlement_price = 1.0 if position.side == "yes" else 0.0
                            else:
                                winner = "NO"
                                settlement_price = 0.0 if position.side == "yes" else 1.0
                    except Exception as e:
                        print(f"Error fetching final result: {e}")
                        if yes_ask_f and yes_ask_f >= 0.99:
                            winner = "YES"
                            settlement_price = 1.0 if position.side == "yes" else 0.0
                        else:
                            winner = "NO"
                            settlement_price = 0.0 if position.side == "yes" else 1.0
                    
                    proceeds = settlement_price * position.quantity
                    current_balance += proceeds
                    position.close(settlement_price, now)
                    
                    print(f"Market settled: {winner} won")
                    print(f"Position closed at settlement: {position.side.upper()} @ {fmt_cents(settlement_price)}")
                    print(f"Proceeds: {fmt_dollars(proceeds)}")
                    print(f"P&L: {fmt_dollars(position.pnl)}")
                    
                    trades_log.append(position)
                    
                    if position.side.upper() == winner:
                        outcome = f"Held {position.side.upper()} to close - WON"
                    else:
                        outcome = f"Held {position.side.upper()} to close - LOST"
                    
                    # Log to CSV
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
                            "trade_pnl_percent": trade_pnl_pct
                        }
                    )
                else:
                    outcome = "No position at close"
                
                print_session_summary(session_number, market_ticker, session_start_time, session_end_time,
                                    session_start_balance, current_balance, None, trades_log, outcome)
                
                return current_balance

            # Check if pending order has filled
            if pending_entry_order_id and position is None:
                try:
                    fills_response = client.get_fills(ticker=market_ticker, limit=20)
                    fills = fills_response.get("fills", [])
                    
                    # Debug: Show fills on first few checks
                    if update_count % 10 == 0 and fills:
                        print(f"DEBUG: Checking {len(fills)} fills for order {pending_entry_order_id}")
                        if fills:
                            first_fill = fills[0]
                            print(f"DEBUG: First fill order_id: {first_fill.get('order_id')}")
                            print(f"DEBUG: Expected order_id: {pending_entry_order_id}")
                            print(f"DEBUG: Match: {first_fill.get('order_id') == pending_entry_order_id}")
                    
                    total_filled = 0
                    avg_fill_price = 0
                    for fill in fills:
                        if fill.get("order_id") == pending_entry_order_id:
                            # Kalshi API returns: count_fp (string), yes_price_dollars/no_price_dollars (string in dollars)
                            count = int(float(fill.get("count_fp", "0")))
                            total_filled += count
                            if pending_entry_side == "yes":
                                price_dollars = fill.get("yes_price_dollars", "0")
                                avg_fill_price = safe_float(price_dollars)
                            else:
                                price_dollars = fill.get("no_price_dollars", "0")
                                avg_fill_price = safe_float(price_dollars)
                    
                    if total_filled > 0:
                        position = LivePosition(pending_entry_side, avg_fill_price, total_filled, now, pending_entry_order_id)
                        cost = avg_fill_price * total_filled
                        current_balance -= cost
                        
                        print("\n" + ">" * 80)
                        print(f"[{now}] ENTRY FILLED: BUY {pending_entry_side.upper()}")
                        print(f"Entry Price: {fmt_cents(avg_fill_price)}")
                        print(f"Quantity: {total_filled} contracts")
                        print(f"Cost: {fmt_dollars(cost)}")
                        print(f"Remaining Balance: {fmt_dollars(current_balance)}")
                        print(">" * 80 + "\n")
                        
                        pending_entry_order_id = None
                        pending_entry_side = None
                except Exception as e:
                    print(f"DEBUG: Error checking fills: {e}")

            changed = (yes_ask != last_yes_ask) or update_count <= 3
            if changed:
                print("-" * 80)
                print(f"[{now}] Update #{update_count} | Balance: {fmt_dollars(current_balance)}")
                print(f"YES bid/ask: {fmt_cents(yes_bid_f)} / {fmt_cents(yes_ask_f)}")
                print(f"NO  bid/ask: {fmt_cents(no_bid)} / {fmt_cents(no_ask)}")
                if position:
                    current_price = yes_ask_f if position.side == "yes" else no_ask
                    unrealized_pnl = (current_price - position.entry_price) * position.quantity if current_price else 0
                    print(f"Position: {position.side.upper()} @ {fmt_cents(position.entry_price)} x {position.quantity} | Unrealized P&L: {fmt_dollars(unrealized_pnl)}")
                elif pending_entry_order_id:
                    print(f"Pending order: {pending_entry_order_id}")

            # ENTRY LOGIC: No position and no pending order
            if position is None and pending_entry_order_id is None:
                if yes_ask_f is not None and yes_ask_f >= ENTRY_TRIGGER:
                    position_value_dollars = current_balance * POSITION_SIZE_PCT
                    quantity = max(1, int(position_value_dollars / yes_ask_f))
                    
                    if quantity > 0:
                        yes_price_cents = int(yes_ask_f * 100)
                        
                        try:
                            print(f"\n[{now}] PLACING BUY ORDER: YES @ {fmt_cents(yes_ask_f)} x {quantity} contracts")
                            print(f"Position value: {fmt_dollars(position_value_dollars)} (2% of {fmt_dollars(current_balance)})")
                            order_response = client.create_order(
                                ticker=market_ticker,
                                side="yes",
                                action="buy",
                                count=quantity,
                                yes_price=yes_price_cents
                            )
                            order = order_response.get("order", {})
                            order_id = order.get("order_id")
                            pending_entry_order_id = order_id
                            pending_entry_side = "yes"
                            print(f"Order placed: {order_id}")
                                
                        except Exception as e:
                            print(f"[{now}] ERROR placing order: {e}")
                            if hasattr(e, 'response') and e.response is not None:
                                try:
                                    error_detail = e.response.json()
                                    print(f"API Error Details: {error_detail}")
                                except:
                                    pass

                elif no_ask is not None and no_ask >= ENTRY_TRIGGER:
                    position_value_dollars = current_balance * POSITION_SIZE_PCT
                    quantity = max(1, int(position_value_dollars / no_ask))
                    
                    if quantity > 0:
                        no_price_cents = int(no_ask * 100)
                        
                        try:
                            print(f"\n[{now}] PLACING BUY ORDER: NO @ {fmt_cents(no_ask)} x {quantity} contracts")
                            print(f"Position value: {fmt_dollars(position_value_dollars)} (2% of {fmt_dollars(current_balance)})")
                            order_response = client.create_order(
                                ticker=market_ticker,
                                side="no",
                                action="buy",
                                count=quantity,
                                no_price=no_price_cents
                            )
                            order = order_response.get("order", {})
                            order_id = order.get("order_id")
                            pending_entry_order_id = order_id
                            pending_entry_side = "no"
                            print(f"Order placed: {order_id}")
                                
                        except Exception as e:
                            print(f"[{now}] ERROR placing order: {e}")
                            if hasattr(e, 'response') and e.response is not None:
                                try:
                                    error_detail = e.response.json()
                                    print(f"API Error Details: {error_detail}")
                                except:
                                    pass

            # EXIT LOGIC: Have position and price drops significantly from entry
            elif position is not None and pending_exit_order_id is None:
                # For YES position, check YES ask price
                # For NO position, check NO ask price
                if position.side == "yes":
                    current_price = yes_ask_f
                else:
                    current_price = no_ask
                
                # Exit when price drops below EXIT_TRIGGER (loss scenario)
                # This means: bought at higher price, now trading at lower price
                if current_price is not None and current_price <= EXIT_TRIGGER and current_price < position.entry_price:
                    price_cents = int(current_price * 100)
                    
                    try:
                        print(f"\n[{now}] PLACING SELL ORDER: {position.side.upper()} @ {fmt_cents(current_price)} x {position.quantity}")
                        order_response = client.create_order(
                            ticker=market_ticker,
                            side=position.side,
                            action="sell",
                            count=position.quantity,
                            yes_price=price_cents if position.side == "yes" else None,
                            no_price=price_cents if position.side == "no" else None
                        )
                        order = order_response.get("order", {})
                        exit_order_id = order.get("order_id")
                        pending_exit_order_id = exit_order_id
                        print(f"Exit order placed: {exit_order_id}")
                            
                    except Exception as e:
                        print(f"[{now}] ERROR placing exit order: {e}")
            
            # Check if pending exit order has filled
            if pending_exit_order_id and position is not None:
                try:
                    fills_response = client.get_fills(ticker=market_ticker, limit=20)
                    fills = fills_response.get("fills", [])
                    
                    total_filled = 0
                    avg_fill_price = 0
                    for fill in fills:
                        if fill.get("order_id") == pending_exit_order_id:
                            count = int(float(fill.get("count_fp", "0")))
                            total_filled += count
                            if position.side == "yes":
                                price_dollars = fill.get("yes_price_dollars", "0")
                                avg_fill_price = safe_float(price_dollars)
                            else:
                                price_dollars = fill.get("no_price_dollars", "0")
                                avg_fill_price = safe_float(price_dollars)
                    
                    if total_filled > 0:
                        proceeds = avg_fill_price * total_filled
                        current_balance += proceeds
                        position.close(avg_fill_price, now, pending_exit_order_id)
                        
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
                        
                        # Log to CSV
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
                                "sell_order_id": position.exit_order_id,
                                "exit_type": "MANUAL_STOP_LOSS",
                                "trade_pnl": position.pnl,
                                "trade_pnl_percent": trade_pnl_pct
                            }
                        )
                        
                        position = None
                        pending_exit_order_id = None
                except Exception as e:
                    print(f"DEBUG: Error checking exit fills: {e}")

            last_yes_ask = yes_ask


async def main_loop():
    print("Initializing Kalshi client...")
    client = KalshiClient(API_KEY_ID, PRIVATE_KEY_PATH, BASE_URL)
    print("Client initialized.\n")

    print("Fetching account balance...")
    try:
        balance_data = client.get_balance()
        if isinstance(balance_data, dict):
            balance_cents = safe_float(balance_data.get("balance")) or 1000000.0
        else:
            balance_cents = safe_float(balance_data) or 1000000.0
        balance = balance_cents / 100.0
    except Exception as e:
        print(f"Could not fetch balance: {e}")
        balance = 10000.0
    print(f"Account balance: {fmt_dollars(balance)}\n")

    session_count = 0
    traded_markets = set()
    
    while True:
        session_count += 1
        print(f"\n{'#' * 80}")
        print(f"STARTING SESSION #{session_count}")
        print(f"{'#' * 80}\n")
        
        # Refresh balance at start of each session
        if session_count > 1:
            print("Refreshing account balance...")
            try:
                balance_data = client.get_balance()
                if isinstance(balance_data, dict):
                    balance_cents = safe_float(balance_data.get("balance")) or balance * 100
                else:
                    balance_cents = safe_float(balance_data) or balance * 100
                balance = balance_cents / 100.0
                print(f"Updated balance: {fmt_dollars(balance)}\n")
            except Exception as e:
                print(f"Could not refresh balance: {e}, using previous: {fmt_dollars(balance)}\n")
        
        ticker = find_latest_btc15m_market(client)

        if not ticker:
            print("\nCould not find an active BTC 15-minute market.")
            print("Waiting 30 seconds before trying again...\n")
            await asyncio.sleep(30)
            continue
        
        if ticker in traded_markets:
            print(f"Already traded {ticker} - waiting for new market...")
            print("Waiting 30 seconds before checking again...\n")
            await asyncio.sleep(30)
            continue
        
        traded_markets.add(ticker)

        new_balance = await run_live_trading(client, ticker, balance, session_count)
        
        if new_balance is not None:
            balance = new_balance
            print(f"Rolling over balance to next session: {fmt_dollars(balance)}")
        
        print("Waiting 30 seconds before finding next market...\n")
        await asyncio.sleep(30)


def main():
    asyncio.run(main_loop())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nUNEXPECTED ERROR\n{type(e).__name__} {e}")
