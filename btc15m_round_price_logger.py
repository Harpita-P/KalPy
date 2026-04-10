import asyncio
import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

from phase6_live_trading import (
    BASE_URL,
    API_KEY_ID,
    PRIVATE_KEY_PATH,
    KalshiClient,
    base_url_to_ws_url,
    find_latest_btc15m_market,
    safe_float,
)


load_dotenv()


PRICE_LOG_PATH = Path("csv_trading_logs") / "btc15m_price_log.csv"
QUALIFIED_ROUNDS_PATH = Path("csv_trading_logs") / "btc15m_98_then_88_rounds.csv"


PRICE_LOG_HEADERS = [
    "timestamp",
    "market_ticker",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
]


QUALIFIED_HEADERS = [
    "market_ticker",
    "session_start_time",
    "market_close_time",
    "reached_time",
    "reached_side",
    "reached_price",
    "dropped_time",
    "dropped_side",
    "dropped_price",
]


ENTRY_LEVEL = 0.98
DROP_LEVEL = 0.88


def ensure_csv(path: Path, headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def append_row(path: Path, row: list) -> None:
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


async def subscribe_to_ticker(ws, market_ticker: str) -> None:
    msg = {
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["ticker"], "market_tickers": [market_ticker]},
    }
    await ws.send(json.dumps(msg))


@dataclass
class RoundState:
    market_ticker: str
    market_close_time: str
    session_start_time: str

    reached: bool = False
    reached_time: str = ""
    reached_side: str = ""
    reached_price: float | None = None

    dropped: bool = False
    dropped_time: str = ""
    dropped_side: str = ""
    dropped_price: float | None = None


def side_prices(yes_bid_f: float | None, yes_ask_f: float | None) -> tuple[float | None, float | None]:
    no_bid = (1.0 - yes_ask_f) if yes_ask_f is not None else None
    no_ask = (1.0 - yes_bid_f) if yes_bid_f is not None else None
    return no_bid, no_ask


def qualifies_transition(state: RoundState, yes_ask_f: float | None, no_ask: float | None, now_str: str) -> None:
    if not state.reached:
        if yes_ask_f is not None and yes_ask_f >= ENTRY_LEVEL:
            state.reached = True
            state.reached_time = now_str
            state.reached_side = "YES"
            state.reached_price = yes_ask_f
        elif no_ask is not None and no_ask >= ENTRY_LEVEL:
            state.reached = True
            state.reached_time = now_str
            state.reached_side = "NO"
            state.reached_price = no_ask

    if state.reached and not state.dropped:
        if yes_ask_f is not None and yes_ask_f <= DROP_LEVEL:
            state.dropped = True
            state.dropped_time = now_str
            state.dropped_side = "YES"
            state.dropped_price = yes_ask_f
        elif no_ask is not None and no_ask <= DROP_LEVEL:
            state.dropped = True
            state.dropped_time = now_str
            state.dropped_side = "NO"
            state.dropped_price = no_ask


async def monitor_single_market(client: KalshiClient, market_ticker: str) -> None:
    ensure_csv(PRICE_LOG_PATH, PRICE_LOG_HEADERS)
    ensure_csv(QUALIFIED_ROUNDS_PATH, QUALIFIED_HEADERS)

    market = client.get_market(market_ticker)
    market_info = market.get("market", {})
    close_time_str = market_info.get("close_time") or ""

    ws_url = base_url_to_ws_url(BASE_URL)
    headers = client.ws_auth_headers()

    state = RoundState(
        market_ticker=market_ticker,
        market_close_time=close_time_str,
        session_start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    last_logged_second = None

    async with websockets.connect(ws_url, additional_headers=headers, ping_interval=20, ping_timeout=20) as ws:
        await subscribe_to_ticker(ws, market_ticker)

        async for raw in ws:
            now = datetime.now(timezone.utc)
            now_str = now.astimezone().strftime("%Y-%m-%d %H:%M:%S")

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if data.get("type") != "ticker":
                continue

            msg = data.get("msg", {})
            if msg.get("market_ticker") != market_ticker:
                continue

            yes_bid_f = safe_float(msg.get("yes_bid_dollars"))
            yes_ask_f = safe_float(msg.get("yes_ask_dollars"))
            no_bid, no_ask = side_prices(yes_bid_f, yes_ask_f)

            qualifies_transition(state, yes_ask_f, no_ask, now_str)

            # Price log: once per second (to limit file size)
            if last_logged_second != now_str:
                append_row(
                    PRICE_LOG_PATH,
                    [
                        now_str,
                        market_ticker,
                        yes_bid_f if yes_bid_f is not None else "",
                        yes_ask_f if yes_ask_f is not None else "",
                        no_bid if no_bid is not None else "",
                        no_ask if no_ask is not None else "",
                    ],
                )
                last_logged_second = now_str

            if state.dropped:
                append_row(
                    QUALIFIED_ROUNDS_PATH,
                    [
                        state.market_ticker,
                        state.session_start_time,
                        state.market_close_time,
                        state.reached_time,
                        state.reached_side,
                        state.reached_price,
                        state.dropped_time,
                        state.dropped_side,
                        state.dropped_price,
                    ],
                )
                print(
                    f"QUALIFIED: {market_ticker} | {state.reached_side} reached {state.reached_price} @ {state.reached_time} "
                    f"then {state.dropped_side} dropped to {state.dropped_price} @ {state.dropped_time}"
                )
                return

            # Stop monitoring when market close time has passed
            # (close_time_str is UTC ISO; parse cheaply using fromisoformat after normalization)
            if close_time_str:
                try:
                    close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                    if now >= close_dt:
                        return
                except Exception:
                    pass


async def main() -> None:
    if not API_KEY_ID or not PRIVATE_KEY_PATH or not BASE_URL:
        raise ValueError("Missing environment variables. Check your .env file.")

    client = KalshiClient(API_KEY_ID, PRIVATE_KEY_PATH, BASE_URL)

    print("BTC 15m Round Logger")
    print(f"Looking for rounds where any side reaches >= {ENTRY_LEVEL:.2f} then drops <= {DROP_LEVEL:.2f}")
    print(f"Price log: {PRICE_LOG_PATH}")
    print(f"Qualified rounds: {QUALIFIED_ROUNDS_PATH}")

    last_market = None
    while True:
        market_ticker = find_latest_btc15m_market(client)
        if not market_ticker:
            await asyncio.sleep(30)
            continue

        if market_ticker == last_market:
            await asyncio.sleep(5)
            continue

        last_market = market_ticker
        print(f"Monitoring market: {market_ticker}")

        try:
            await monitor_single_market(client, market_ticker)
        except websockets.exceptions.ConnectionClosed:
            await asyncio.sleep(2)
        except Exception as e:
            print(f"ERROR monitoring {market_ticker}: {type(e).__name__} {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
