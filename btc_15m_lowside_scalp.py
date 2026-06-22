"""
BTC 15-minute low-side scalp bot.

Strategy:
- Connect to the active KXBTC15M market immediately.
- Buy whichever side (YES/NO) has the lower ask, once per round (maker limit at bid, post_only).
- Position size: 40% of account balance.
- After entry, sell when the held side's bid rises more than +1¢ above entry price.
- Otherwise hold to settlement at market close.
"""

import asyncio
import base64
import csv
import json
import os
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import websockets
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

load_dotenv()

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
BASE_URL = os.getenv("KALSHI_BASE_URL")

if not API_KEY_ID or not PRIVATE_KEY_PATH or not BASE_URL:
    raise ValueError("Missing environment variables. Check your .env file.")

POSITION_SIZE_PCT = 0.40
PROFIT_TARGET_DOLLARS = 0.01
MINIMUM_ACCOUNT_BALANCE = 2.00
ENTRY_ORDER_TIMEOUT_SECONDS = 300  # Maker bids may take longer to fill
ENTRY_RETRY_COOLDOWN_SECONDS = 5
EXIT_ORDER_TIMEOUT_SECONDS = 120  # Maker exit orders can take longer to fill
EXIT_RETRY_COOLDOWN_SECONDS = 10
MAKER_TICK_DOLLARS = 0.01
MINIMUM_BALANCE_WAIT_SECONDS = 60
SESSION_ROLLOVER_WAIT_SECONDS = 0

CSV_LOG_PATH = Path("csv_trading_logs/btc_lowside_scalp_log.csv")
CSV_HEADERS = [
    "session_number",
    "session_date",
    "session_start_time",
    "session_end_time",
    "market_ticker",
    "starting_balance",
    "ending_balance",
    "session_pnl",
    "side",
    "buy_time",
    "buy_price",
    "buy_quantity",
    "sell_time",
    "sell_price",
    "exit_type",
    "trade_pnl",
    "market_outcome",
]


def base_url_to_ws_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/trade-api/ws/v2"


WS_URL = base_url_to_ws_url(BASE_URL)


def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def fmt_dollars(amount: float) -> str:
    return f"${amount:.2f}"


def fmt_cents(price: float | None) -> str:
    if price is None:
        return "N/A"
    return f"{price * 100:.1f}¢"


def price_dollars_str(price: float) -> str:
    return f"{price:.4f}"


def summarize_order_fills(fills: list, order_id: str, side: str) -> dict | None:
    """Aggregate fill price, fees, and totals for one order."""
    total_filled = 0
    fill_price_sum = 0.0
    fee_sum = 0.0
    for fill in fills:
        if fill.get("order_id") != order_id:
            continue
        count = int(float(fill.get("count_fp", "0")))
        if count <= 0:
            continue
        total_filled += count
        if side == "yes":
            price = safe_float(fill.get("yes_price_dollars", "0"))
        else:
            price = safe_float(fill.get("no_price_dollars", "0"))
        if price is not None:
            fill_price_sum += price * count
        fee = safe_float(fill.get("fee_cost"))
        if fee is not None:
            fee_sum += fee

    if total_filled <= 0:
        return None

    avg_fill_price = fill_price_sum / total_filled
    gross_total = fill_price_sum
    all_in_total = gross_total + fee_sum if fee_sum else gross_total
    all_in_per_contract = all_in_total / total_filled
    return {
        "quantity": total_filled,
        "avg_fill_price": avg_fill_price,
        "gross_total": gross_total,
        "fee_total": fee_sum,
        "all_in_total": all_in_total,
        "all_in_per_contract": all_in_per_contract,
    }


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
        return requests.get(self.base_url + endpoint_path, headers=headers, params=params, timeout=30)

    def post(self, endpoint_path: str, data: dict, auth: bool = True) -> requests.Response:
        headers = self._auth_headers("POST", endpoint_path) if auth else None
        return requests.post(self.base_url + endpoint_path, headers=headers, json=data, timeout=30)

    def delete(self, endpoint_path: str, auth: bool = True) -> requests.Response:
        headers = self._auth_headers("DELETE", endpoint_path) if auth else None
        return requests.delete(self.base_url + endpoint_path, headers=headers, timeout=30)

    def get_balance(self) -> dict:
        response = self.get("/portfolio/balance", auth=True)
        response.raise_for_status()
        return response.json()

    def get_market(self, ticker: str) -> dict:
        response = self.get(f"/markets/{ticker}", auth=False)
        response.raise_for_status()
        return response.json()

    def get_milestones(self, category: str = "Crypto", limit: int = 200) -> dict:
        response = self.get("/milestones", params={"category": category, "limit": limit}, auth=False)
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

    def create_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        yes_price_dollars: str = None,
        no_price_dollars: str = None,
        post_only: bool = False,
    ) -> dict:
        order_data = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": "limit",
        }
        if post_only:
            order_data["post_only"] = True
        if yes_price_dollars is not None:
            order_data["yes_price_dollars"] = yes_price_dollars
        elif no_price_dollars is not None:
            order_data["no_price_dollars"] = no_price_dollars
        else:
            raise ValueError("Must provide yes_price_dollars or no_price_dollars")

        response = self.post("/portfolio/orders", data=order_data, auth=True)
        response.raise_for_status()
        return response.json()

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

    def get_positions(self, ticker: str = None) -> dict:
        params = {}
        if ticker:
            params["ticker"] = ticker
        response = self.get("/portfolio/positions", params=params, auth=True)
        response.raise_for_status()
        return response.json()


def text_contains_btc(*values) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    return any(k in text for k in ("btc", "bitcoin", "kxbtc"))


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
    ts = m.get("start_date") or m.get("start_time") or m.get("last_updated_ts") or "0000-01-01T00:00:00Z"
    return (-priority, ts)


def find_latest_btc15m_market(client: KalshiClient) -> str | None:
    print("Finding latest BTC 15-minute market...")
    data = client.get_milestones(category="Crypto", limit=200)
    milestones = data.get("milestones", [])

    btc_milestones = [
        m
        for m in milestones
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
        m
        for m in nested_markets
        if (m.get("event_ticker") or "").upper().startswith("KXBTC15M")
        or (m.get("ticker") or "").upper().startswith("KXBTC15M")
    ]
    active_markets = [m for m in btc15m_markets if (m.get("status") or "").lower() == "active"]
    if not active_markets:
        print("No active BTC 15-minute markets found.")
        return None

    active_markets.sort(
        key=lambda m: (
            m.get("close_time") or "9999-12-31T23:59:59Z",
            m.get("ticker") or "",
        )
    )
    selected = active_markets[0]
    print(f"Found active market: {selected['ticker']}")
    print(f"Close time: {selected.get('close_time')}")
    return selected["ticker"]


def fetch_account_balance(client: KalshiClient, fallback: float | None = None) -> float:
    try:
        balance_data = client.get_balance()
        if isinstance(balance_data, dict):
            balance_cents = safe_float(balance_data.get("balance")) or 0.0
        else:
            balance_cents = safe_float(balance_data) or 0.0
        return balance_cents / 100.0
    except Exception as e:
        if fallback is not None:
            print(f"Could not fetch balance: {e}, using previous {fmt_dollars(fallback)}")
            return fallback
        raise


def ensure_csv_exists():
    CSV_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_LOG_PATH.exists():
        with open(CSV_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow(CSV_HEADERS)


def log_trade_row(session_number: int, session_start: str, session_end: str, market_ticker: str,
                  start_balance: float, end_balance: float, trade: dict | None):
    ensure_csv_exists()
    session_date = session_start.split()[0] if session_start else ""
    row = [
        session_number,
        session_date,
        session_start,
        session_end,
        market_ticker,
        round(start_balance, 4),
        round(end_balance, 4),
        round(end_balance - start_balance, 4),
        trade.get("side", "") if trade else "",
        trade.get("buy_time", "") if trade else "",
        trade.get("buy_price", "") if trade else "",
        trade.get("buy_quantity", "") if trade else "",
        trade.get("sell_time", "") if trade else "",
        trade.get("sell_price", "") if trade else "",
        trade.get("exit_type", "") if trade else "",
        trade.get("trade_pnl", "") if trade else "",
        trade.get("market_outcome", "") if trade else "",
    ]
    with open(CSV_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow(row)


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


def pick_lowest_side(yes_ask: float | None, no_ask: float | None) -> tuple[str, float] | None:
    if yes_ask is None and no_ask is None:
        return None
    if yes_ask is None:
        return "no", no_ask
    if no_ask is None:
        return "yes", yes_ask
    if yes_ask <= no_ask:
        return "yes", yes_ask
    return "no", no_ask


def held_side_bid(side: str, yes_bid: float | None, no_bid: float | None) -> float | None:
    return yes_bid if side == "yes" else no_bid


def held_side_ask(side: str, yes_ask: float | None, no_ask: float | None) -> float | None:
    return yes_ask if side == "yes" else no_ask


def format_api_error(exc: Exception) -> str:
    resp = getattr(exc, "response", None)
    if resp is None:
        return str(exc)
    try:
        return f"{exc} | {resp.json()}"
    except Exception:
        try:
            return f"{exc} | {resp.text}"
        except Exception:
            return str(exc)


def maker_buy_limit_price(bid: float, ask: float, tick_offset: int = 0) -> float | None:
    """Price at or below bid so a post_only buy rests below the ask."""
    price = bid - (MAKER_TICK_DOLLARS * tick_offset)
    price = max(0.01, price)
    if price >= ask:
        return None
    return price


def maker_sell_limit_price(bid: float, entry: float, tick_offset: int = 1) -> float:
    """Price one or more ticks above bid so a post_only sell rests as maker."""
    price = bid + (MAKER_TICK_DOLLARS * tick_offset)
    min_profit = entry + PROFIT_TARGET_DOLLARS
    return max(price, min_profit + MAKER_TICK_DOLLARS)


async def run_session(client: KalshiClient, market_ticker: str, initial_balance: float, session_number: int) -> float:
    async def _to_thread(func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _fetch_fills() -> list:
        response = await _to_thread(client.get_fills, ticker=market_ticker, limit=50)
        return response.get("fills", [])

    async def _apply_exit_fill(summary: dict, now_str: str) -> None:
        nonlocal position, current_balance, trade_record, outcome
        nonlocal pending_exit_order_id, pending_exit_time, exit_order_submitted

        avg_exit_price = summary["avg_fill_price"]
        total_filled = summary["quantity"]
        proceeds = summary["gross_total"] - summary["fee_total"]
        entry_cost = position.get(
            "entry_all_in_per_contract",
            position["entry_price"],
        ) * position["quantity"]
        current_balance += proceeds
        trade_pnl = proceeds - entry_cost
        trade_record = {
            "side": position["side"],
            "buy_time": position["entry_time"],
            "buy_price": round(position["entry_price"], 4),
            "buy_quantity": position["quantity"],
            "sell_time": now_str,
            "sell_price": round(avg_exit_price, 4),
            "exit_type": "TAKE_PROFIT",
            "trade_pnl": round(trade_pnl, 4),
            "market_outcome": "",
        }
        net_per_contract = proceeds / total_filled
        fee_note = (
            f" | fees {fmt_dollars(summary['fee_total'])}"
            if summary["fee_total"]
            else ""
        )
        print(
            f"[{now_str}] EXIT FILLED: {position['side'].upper()} "
            f"x {total_filled} @ {fmt_cents(avg_exit_price)} contract price"
            f"{fee_note} | net {fmt_cents(net_per_contract)} "
            f"| proceeds {fmt_dollars(proceeds)} | P&L {fmt_dollars(trade_pnl)}"
        )
        position = None
        pending_exit_order_id = None
        pending_exit_time = None
        exit_order_submitted = False
        outcome = "Take profit"

    async def _reconcile_exit_order(order_id: str, now_str: str) -> bool:
        if position is None:
            return False
        fills = await _fetch_fills()
        summary = summarize_order_fills(fills, order_id, position["side"])
        if summary and summary["quantity"] >= position["quantity"]:
            await _apply_exit_fill(summary, now_str)
            return True
        return False

    async def _sync_flat_position(now_str: str) -> bool:
        """Sync local position with Kalshi; clear state if flat or flipped to opposite side."""
        nonlocal position, exit_order_submitted, pending_exit_order_id, pending_exit_time
        if position is None:
            return False
        try:
            data = await _to_thread(client.get_positions, ticker=market_ticker)
            market_positions = data.get("market_positions", [])
            net_position = 0.0
            for mp in market_positions:
                if mp.get("ticker") != market_ticker:
                    continue
                net_position = float(mp.get("position_fp", mp.get("position", 0)))
                break

            if abs(net_position) < 0.01:
                print(
                    f"[{now_str}] Portfolio is flat for {market_ticker} — "
                    f"clearing local {position['side'].upper()} position (no duplicate sell)"
                )
                position = None
                pending_exit_order_id = None
                pending_exit_time = None
                exit_order_submitted = False
                return True

            if (net_position > 0) != (position["side"] == "yes"):
                print(
                    f"[{now_str}] WARNING: portfolio flipped to "
                    f"{'YES' if net_position > 0 else 'NO'} ({net_position:+.2f}) after exit — "
                    f"stopping further sells on this market"
                )
                position = None
                pending_exit_order_id = None
                pending_exit_time = None
                exit_order_submitted = False
                return True
        except Exception as e:
            print(f"[{now_str}] Warning: could not sync portfolio position: {e}")
        return False

    session_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session_start_balance = initial_balance
    current_balance = initial_balance

    market_data = await _to_thread(client.get_market, market_ticker)
    market_info = market_data.get("market", {})
    close_time_str = market_info.get("close_time")
    if not close_time_str:
        print(f"Warning: Could not get close time for {market_ticker}")
        return initial_balance

    close_time = dateutil_parser.parse(close_time_str)
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    print("\n" + "=" * 80)
    print("BTC 15M LOW-SIDE SCALP BOT")
    print(f"Market: {market_ticker}")
    print(f"Close time: {close_time_str}")
    print(f"Initial balance: {fmt_dollars(initial_balance)}")
    print(f"Entry rule: buy lower-ask side once per round (maker limit at bid, post_only)")
    print(f"Position size: {POSITION_SIZE_PCT * 100:.0f}% of balance")
    print(f"Take profit: sell when bid > entry + {PROFIT_TARGET_DOLLARS * 100:.0f}¢ (maker limit above bid, post_only)")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")

    position = None
    trade_record = None
    outcome = "No trades"
    round_entry_used = False  # Set once a buy order is accepted by Kalshi
    next_entry_attempt_time = datetime.min
    entry_bid_tick_offset = 0

    pending_entry_order_id = None
    pending_entry_side = None
    pending_entry_quantity = None
    pending_entry_limit_price = None
    pending_entry_time = None

    pending_exit_order_id = None
    pending_exit_time = None
    exit_order_submitted = False
    next_exit_attempt_time = datetime.min
    exit_sell_tick_offset = 1

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
                if retry_count > 0:
                    print(f"Reconnected to WebSocket (attempt {retry_count + 1})")
                else:
                    print("Connected to Kalshi WebSocket.")

                await subscribe_to_ticker(ws, market_ticker)
                print(f"Subscribed to ticker updates for {market_ticker}.\n")

                async for raw in ws:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    current_time = datetime.now(timezone.utc)
                    seconds_remaining = (close_time - current_time).total_seconds()

                    if seconds_remaining <= 0:
                        print(f"\n[{now}] Market {market_ticker} has CLOSED")
                        break

                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
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
                    if msg.get("market_ticker") != market_ticker:
                        continue

                    yes_bid = safe_float(msg.get("yes_bid_dollars"))
                    yes_ask = safe_float(msg.get("yes_ask_dollars"))
                    no_bid = (1.0 - yes_ask) if yes_ask is not None else None
                    no_ask = (1.0 - yes_bid) if yes_bid is not None else None

                    if (
                        position is None
                        and pending_entry_order_id is None
                        and not round_entry_used
                    ):
                        if datetime.now() < next_entry_attempt_time:
                            continue
                        choice = pick_lowest_side(yes_ask, no_ask)
                        if choice is not None:
                            side, ask_price = choice
                            bid_price = held_side_bid(side, yes_bid, no_bid)
                            if bid_price is None or bid_price <= 0 or ask_price is None:
                                continue
                            limit_price = maker_buy_limit_price(
                                bid_price, ask_price, entry_bid_tick_offset
                            )
                            if limit_price is None:
                                entry_bid_tick_offset = min(entry_bid_tick_offset + 1, 5)
                                continue

                            position_value = current_balance * POSITION_SIZE_PCT
                            quantity = int(position_value / limit_price)
                            max_affordable = int(current_balance / limit_price)
                            quantity = min(quantity, max_affordable)
                            if quantity < 1:
                                round_entry_used = True
                                print(f"[{now}] Insufficient balance for entry at {fmt_cents(limit_price)} — no retry this round")
                                continue
                            price_str = price_dollars_str(limit_price)
                            try:
                                print(
                                    f"[{now}] BUYING lower side {side.upper()} "
                                    f"maker limit {fmt_cents(limit_price)} x {quantity} "
                                    f"(bid {fmt_cents(bid_price)}, ask {fmt_cents(ask_price)}, "
                                    f"40% = {fmt_dollars(position_value)})"
                                )
                                if side == "yes":
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker,
                                        side="yes",
                                        action="buy",
                                        count=quantity,
                                        yes_price_dollars=price_str,
                                        post_only=True,
                                    )
                                else:
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker,
                                        side="no",
                                        action="buy",
                                        count=quantity,
                                        no_price_dollars=price_str,
                                        post_only=True,
                                    )
                                order = order_response.get("order", {})
                                round_entry_used = True
                                entry_bid_tick_offset = 0
                                pending_entry_order_id = order.get("order_id")
                                pending_entry_side = side
                                pending_entry_quantity = quantity
                                pending_entry_limit_price = limit_price
                                pending_entry_time = datetime.now()
                            except Exception as e:
                                err_text = format_api_error(e)
                                print(f"[{now}] ERROR placing maker entry order: {err_text}")
                                next_entry_attempt_time = datetime.now() + timedelta(
                                    seconds=ENTRY_RETRY_COOLDOWN_SECONDS
                                )
                                if "429" in err_text or "too many requests" in err_text.lower():
                                    next_entry_attempt_time = datetime.now() + timedelta(seconds=30)
                                    print(f"[{now}] Rate limited — will retry entry in 30s")
                                elif "post only" in err_text.lower() or "post_only" in err_text.lower():
                                    entry_bid_tick_offset = min(entry_bid_tick_offset + 1, 5)
                                    print(
                                        f"[{now}] post_only rejected — will retry in "
                                        f"{ENTRY_RETRY_COOLDOWN_SECONDS}s "
                                        f"with buy limit -{entry_bid_tick_offset} tick(s) below bid"
                                    )

                    if pending_entry_order_id and position is None:
                        fills_response = await _to_thread(client.get_fills, ticker=market_ticker, limit=50)
                        fills = fills_response.get("fills", [])
                        summary = summarize_order_fills(
                            fills, pending_entry_order_id, pending_entry_side
                        )

                        if summary:
                            avg_fill_price = summary["avg_fill_price"]
                            total_filled = summary["quantity"]
                            cost = summary["all_in_total"]
                            current_balance -= cost
                            position = {
                                "side": pending_entry_side,
                                "entry_price": avg_fill_price,
                                "entry_all_in_per_contract": summary["all_in_per_contract"],
                                "entry_fees": summary["fee_total"],
                                "quantity": total_filled,
                                "entry_time": now,
                                "entry_order_id": pending_entry_order_id,
                            }
                            pending_entry_order_id = None
                            pending_entry_side = None
                            pending_entry_quantity = None
                            pending_entry_limit_price = None
                            pending_entry_time = None
                            fee_note = (
                                f" | fees {fmt_dollars(summary['fee_total'])}"
                                if summary["fee_total"]
                                else ""
                            )
                            print(
                                f"\n[{now}] ENTRY FILLED: {position['side'].upper()} "
                                f"x {total_filled} @ {fmt_cents(avg_fill_price)} contract price"
                                f"{fee_note} | all-in {fmt_cents(summary['all_in_per_contract'])} "
                                f"| total {fmt_dollars(cost)}"
                            )
                        elif pending_entry_time and (datetime.now() - pending_entry_time).total_seconds() >= ENTRY_ORDER_TIMEOUT_SECONDS:
                            try:
                                await _to_thread(client.cancel_order, pending_entry_order_id)
                                print(f"[{now}] Entry order not filled in {ENTRY_ORDER_TIMEOUT_SECONDS}s - canceled")
                            except Exception as e:
                                print(f"[{now}] ERROR canceling entry order: {e}")
                            pending_entry_order_id = None
                            pending_entry_side = None
                            pending_entry_quantity = None
                            pending_entry_limit_price = None
                            pending_entry_time = None

                    if position and pending_exit_order_id is None and not exit_order_submitted:
                        bid = held_side_bid(position["side"], yes_bid, no_bid)
                        target_price = position["entry_price"] + PROFIT_TARGET_DOLLARS
                        if (
                            bid is not None
                            and bid > target_price
                            and datetime.now() >= next_exit_attempt_time
                        ):
                            sell_price = maker_sell_limit_price(
                                bid, position["entry_price"], exit_sell_tick_offset
                            )
                            sell_quantity = position["quantity"]
                            price_str = price_dollars_str(sell_price)
                            try:
                                print(
                                    f"\n[{now}] TAKE PROFIT: {position['side'].upper()} bid {fmt_cents(bid)} "
                                    f"> entry {fmt_cents(position['entry_price'])} + 1¢ "
                                    f"| maker sell @ {fmt_cents(sell_price)}"
                                )
                                if position["side"] == "yes":
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker,
                                        side="yes",
                                        action="sell",
                                        count=sell_quantity,
                                        yes_price_dollars=price_str,
                                        post_only=True,
                                    )
                                else:
                                    order_response = await _to_thread(
                                        client.create_order,
                                        ticker=market_ticker,
                                        side="no",
                                        action="sell",
                                        count=sell_quantity,
                                        no_price_dollars=price_str,
                                        post_only=True,
                                    )
                                order = order_response.get("order", {})
                                pending_exit_order_id = order.get("order_id")
                                pending_exit_time = datetime.now()
                                exit_order_submitted = True
                                exit_sell_tick_offset = 1
                            except Exception as e:
                                err_text = format_api_error(e)
                                print(f"[{now}] ERROR placing take-profit sell: {err_text}")
                                next_exit_attempt_time = datetime.now() + timedelta(
                                    seconds=EXIT_RETRY_COOLDOWN_SECONDS
                                )
                                if "429" in err_text or "too many requests" in err_text.lower():
                                    next_exit_attempt_time = datetime.now() + timedelta(seconds=30)
                                    print(f"[{now}] Rate limited — will retry exit in 30s")
                                elif "post only" in err_text.lower() or "post_only" in err_text.lower():
                                    exit_sell_tick_offset = min(exit_sell_tick_offset + 1, 5)
                                    print(
                                        f"[{now}] post_only rejected — will retry in "
                                        f"{EXIT_RETRY_COOLDOWN_SECONDS}s "
                                        f"with +{exit_sell_tick_offset} tick(s) above bid"
                                    )

                    if pending_exit_order_id and position is not None:
                        if await _reconcile_exit_order(pending_exit_order_id, now):
                            pass
                        elif pending_exit_time and (
                            datetime.now() - pending_exit_time
                        ).total_seconds() >= EXIT_ORDER_TIMEOUT_SECONDS:
                            if await _reconcile_exit_order(pending_exit_order_id, now):
                                pass
                            else:
                                canceled = False
                                try:
                                    await _to_thread(client.cancel_order, pending_exit_order_id)
                                    print(
                                        f"[{now}] Exit order not filled in "
                                        f"{EXIT_ORDER_TIMEOUT_SECONDS}s - canceled"
                                    )
                                    canceled = True
                                except Exception as e:
                                    err_text = format_api_error(e)
                                    print(f"[{now}] ERROR canceling exit order: {err_text}")
                                    if "404" in err_text or "not found" in err_text.lower():
                                        if await _reconcile_exit_order(pending_exit_order_id, now):
                                            pass
                                        else:
                                            await _sync_flat_position(now)
                                    else:
                                        await _reconcile_exit_order(pending_exit_order_id, now)

                                if position is not None:
                                    if canceled and not await _reconcile_exit_order(
                                        pending_exit_order_id, now
                                    ):
                                        pending_exit_order_id = None
                                        pending_exit_time = None
                                        exit_order_submitted = False
                                    elif not canceled:
                                        await _sync_flat_position(now)

            break

        except websockets.exceptions.ConnectionClosed as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 30)
                print(f"WebSocket closed ({e}). Reconnecting in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            raise
        except (socket.gaierror, OSError) as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 30)
                print(f"Network error ({e}). Reconnecting in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            raise

    session_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if position is not None:
        deadline = datetime.now(timezone.utc) + timedelta(seconds=120)
        market_result = None
        while datetime.now(timezone.utc) < deadline:
            try:
                market = await _to_thread(client.get_market, market_ticker)
                market_result = market.get("market", {}).get("result")
                if market_result in ("yes", "no"):
                    break
            except Exception:
                pass
            await asyncio.sleep(5)

        if market_result in ("yes", "no"):
            won = position["side"] == market_result
            settlement_price = 1.0 if won else 0.0
            proceeds = settlement_price * position["quantity"]
            current_balance += proceeds
            trade_pnl = proceeds - (position["entry_price"] * position["quantity"])
            trade_record = {
                "side": position["side"],
                "buy_time": position["entry_time"],
                "buy_price": round(position["entry_price"], 4),
                "buy_quantity": position["quantity"],
                "sell_time": session_end_time,
                "sell_price": round(settlement_price, 4),
                "exit_type": "SETTLEMENT",
                "trade_pnl": round(trade_pnl, 4),
                "market_outcome": market_result.upper(),
            }
            outcome = f"Held {position['side'].upper()} to settlement - {'WON' if won else 'LOST'}"
            print(f"Market settled: {market_result.upper()} | P&L {fmt_dollars(trade_pnl)}")
        else:
            outcome = f"Held {position['side'].upper()} at close (result unknown)"

    session_pnl = current_balance - session_start_balance
    print("\n" + "=" * 80)
    print(f"SESSION #{session_number} SUMMARY: {market_ticker}")
    print(f"Starting balance: {fmt_dollars(session_start_balance)}")
    print(f"Ending balance:   {fmt_dollars(current_balance)}")
    print(f"Session P&L:      {fmt_dollars(session_pnl)}")
    print(f"Outcome:          {outcome}")
    print("=" * 80 + "\n")

    log_trade_row(
        session_number,
        session_start_time,
        session_end_time,
        market_ticker,
        session_start_balance,
        current_balance,
        trade_record,
    )
    return current_balance


async def main_loop():
    print("Initializing Kalshi client...")
    client = KalshiClient(API_KEY_ID, PRIVATE_KEY_PATH, BASE_URL)
    print("Client initialized.\n")

    balance = fetch_account_balance(client, fallback=100.0)
    print(f"Account balance: {fmt_dollars(balance)}\n")

    session_count = 0
    while True:
        session_count += 1
        print(f"\n{'#' * 80}")
        print(f"STARTING SESSION #{session_count}")
        print(f"{'#' * 80}\n")

        if session_count > 1:
            balance = fetch_account_balance(client, fallback=balance)
            print(f"Updated balance: {fmt_dollars(balance)}\n")

        if balance <= MINIMUM_ACCOUNT_BALANCE:
            print(
                f"Balance {fmt_dollars(balance)} is below minimum "
                f"{fmt_dollars(MINIMUM_ACCOUNT_BALANCE)}. Waiting..."
            )
            await asyncio.sleep(MINIMUM_BALANCE_WAIT_SECONDS)
            continue

        ticker = find_latest_btc15m_market(client)
        if not ticker:
            print("No active BTC 15-minute market found. Retrying in 30 seconds...")
            await asyncio.sleep(30)
            continue

        balance = await run_session(client, ticker, balance, session_count)
        print(f"Rolling balance to next session: {fmt_dollars(balance)}")
        if SESSION_ROLLOVER_WAIT_SECONDS > 0:
            print(f"Waiting {SESSION_ROLLOVER_WAIT_SECONDS}s before next market...\n")
            await asyncio.sleep(SESSION_ROLLOVER_WAIT_SECONDS)
        else:
            print("Searching for next market immediately...\n")


def main():
    asyncio.run(main_loop())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
