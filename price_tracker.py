import asyncio
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import websockets
from dotenv import load_dotenv
from dateutil import parser as dateutil_parser

load_dotenv()

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
BASE_URL = os.getenv("KALSHI_BASE_URL")

if not API_KEY_ID or not PRIVATE_KEY_PATH or not BASE_URL:
    raise ValueError("Missing environment variables. Check your .env file.")


def base_url_to_ws_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/trade-api/ws/v2"


WS_URL = base_url_to_ws_url(BASE_URL)

# Price tracking configuration
CRYPTO_CONFIGS = {
    "BTC": {
        "series": "KXBTC15M",
        "csv_path": Path("price_logs/btc_prices.csv")
    },
    "ETH": {
        "series": "KXETH15M",
        "csv_path": Path("price_logs/eth_prices.csv")
    },
    "XRP": {
        "series": "KXXRP15M",
        "csv_path": Path("price_logs/xrp_prices.csv")
    }
}

CSV_HEADERS = [
    "timestamp", "date", "time", "market_ticker", 
    "yes_bid", "yes_ask", "no_bid", "no_ask",
    "market_outcome"
]


def ensure_csv_exists(csv_path: Path):
    """Create CSV file with headers if it doesn't exist"""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists():
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)


def log_price_to_csv(csv_path: Path, timestamp: str, market_ticker: str, 
                     yes_bid: float, yes_ask: float, no_bid: float, no_ask: float,
                     market_outcome: str = ""):
    """Log a single price snapshot to CSV"""
    try:
        ensure_csv_exists(csv_path)
        
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")
        
        row = [
            timestamp,
            date_str,
            time_str,
            market_ticker,
            yes_bid if yes_bid is not None else "",
            yes_ask if yes_ask is not None else "",
            no_bid if no_bid is not None else "",
            no_ask if no_ask is not None else "",
            market_outcome
        ]
        
        with open(csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception as e:
        print(f"WARNING: Failed to log price to CSV: {e}")


def safe_float(val):
    """Safely convert to float, return None if invalid"""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def track_crypto_prices(crypto_name: str, series_ticker: str, csv_path: Path):
    """Track prices for a single cryptocurrency"""
    print(f"\n[{crypto_name}] Starting price tracker for {series_ticker}")
    
    current_market = None
    current_close_time = None
    last_log_minute = None
    latest_prices = {
        "yes_bid": None,
        "yes_ask": None,
        "no_bid": None,
        "no_ask": None
    }
    
    while True:
        try:
            # Find the latest active market
            import requests
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding
            import base64
            
            # Simple API client for getting markets
            with open(PRIVATE_KEY_PATH, "rb") as f:
                private_key = serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
            
            # Get events for this series
            path = f"/trade-api/v2/events?series_ticker={series_ticker}&status=open&limit=10"
            timestamp_ms = str(int(datetime.now().timestamp() * 1000))
            msg = timestamp_ms + "GET" + path
            
            signature = private_key.sign(
                msg.encode("utf-8"),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            sig_b64 = base64.b64encode(signature).decode("utf-8")
            
            headers = {
                "Content-Type": "application/json",
                "KALSHI-ACCESS-KEY": API_KEY_ID,
                "KALSHI-ACCESS-SIGNATURE": sig_b64,
                "KALSHI-ACCESS-TIMESTAMP": timestamp_ms
            }
            
            response = requests.get(BASE_URL + path, headers=headers)
            events_data = response.json()
            events = events_data.get("events", [])
            
            if not events:
                print(f"[{crypto_name}] No active events found, waiting 60s...")
                await asyncio.sleep(60)
                continue
            
            # Get the latest event
            latest_event = sorted(events, key=lambda e: e.get("event_ticker", ""), reverse=True)[0]
            event_ticker = latest_event.get("event_ticker")
            
            # Get markets for this event
            path = f"/trade-api/v2/events/{event_ticker}"
            timestamp_ms = str(int(datetime.now().timestamp() * 1000))
            msg = timestamp_ms + "GET" + path
            
            signature = private_key.sign(
                msg.encode("utf-8"),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            sig_b64 = base64.b64encode(signature).decode("utf-8")
            
            headers["KALSHI-ACCESS-SIGNATURE"] = sig_b64
            headers["KALSHI-ACCESS-TIMESTAMP"] = timestamp_ms
            
            response = requests.get(BASE_URL + path, headers=headers)
            event_data = response.json()
            markets = event_data.get("event", {}).get("markets", []) or event_data.get("markets", [])
            
            # Find active market
            active_markets = [m for m in markets if m.get("status", "").lower() == "active"]
            if not active_markets:
                print(f"[{crypto_name}] No active markets found, waiting 60s...")
                await asyncio.sleep(60)
                continue
            
            # Get the earliest closing market
            active_markets.sort(key=lambda m: (m.get("close_time") or "9999-12-31T23:59:59Z", m.get("ticker") or ""))
            market = active_markets[0]
            market_ticker = market["ticker"]
            close_time_str = market.get("close_time")
            close_time = dateutil_parser.parse(close_time_str)
            
            # If this is a new market, reset tracking
            if current_market != market_ticker:
                current_market = market_ticker
                current_close_time = close_time
                last_log_minute = None
                latest_prices = {"yes_bid": None, "yes_ask": None, "no_bid": None, "no_ask": None}
                print(f"[{crypto_name}] Tracking new market: {market_ticker} (closes at {close_time_str})")
            
            # Connect to WebSocket
            async with websockets.connect(WS_URL, extra_headers=headers) as ws:
                # Subscribe to ticker
                subscribe_msg = {
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["ticker"],
                        "market_ticker": market_ticker
                    }
                }
                await ws.send(json.dumps(subscribe_msg))
                
                print(f"[{crypto_name}] Connected to WebSocket for {market_ticker}")
                
                async for message in ws:
                    data = json.loads(message)
                    
                    if data.get("type") == "ticker":
                        msg_data = data.get("msg", {})
                        yes_bid = msg_data.get("yes_bid")
                        yes_ask = msg_data.get("yes_ask")
                        
                        yes_bid_f = safe_float(yes_bid)
                        yes_ask_f = safe_float(yes_ask)
                        
                        # Calculate NO prices
                        no_bid = (1.0 - yes_ask_f) if yes_ask_f is not None else None
                        no_ask = (1.0 - yes_bid_f) if yes_bid_f is not None else None
                        
                        # Update latest prices
                        latest_prices["yes_bid"] = yes_bid_f
                        latest_prices["yes_ask"] = yes_ask_f
                        latest_prices["no_bid"] = no_bid
                        latest_prices["no_ask"] = no_ask
                        
                        # Check if we should log (every minute)
                        now = datetime.now()
                        current_minute = now.strftime("%Y-%m-%d %H:%M")
                        
                        if current_minute != last_log_minute:
                            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                            log_price_to_csv(
                                csv_path,
                                timestamp,
                                market_ticker,
                                latest_prices["yes_bid"],
                                latest_prices["yes_ask"],
                                latest_prices["no_bid"],
                                latest_prices["no_ask"]
                            )
                            last_log_minute = current_minute
                            print(f"[{crypto_name}] Logged prices at {timestamp}")
                        
                        # Check if market has closed
                        if now >= current_close_time:
                            print(f"[{crypto_name}] Market {market_ticker} has closed, checking outcome...")
                            
                            # Wait a bit for settlement
                            await asyncio.sleep(5)
                            
                            # Get final market result
                            path = f"/trade-api/v2/markets/{market_ticker}"
                            timestamp_ms = str(int(datetime.now().timestamp() * 1000))
                            msg = timestamp_ms + "GET" + path
                            
                            signature = private_key.sign(
                                msg.encode("utf-8"),
                                padding.PSS(
                                    mgf=padding.MGF1(hashes.SHA256()),
                                    salt_length=padding.PSS.MAX_LENGTH
                                ),
                                hashes.SHA256()
                            )
                            sig_b64 = base64.b64encode(signature).decode("utf-8")
                            
                            headers["KALSHI-ACCESS-SIGNATURE"] = sig_b64
                            headers["KALSHI-ACCESS-TIMESTAMP"] = timestamp_ms
                            
                            response = requests.get(BASE_URL + path, headers=headers)
                            final_market = response.json()
                            result = final_market.get("market", {}).get("result")
                            
                            outcome = result.upper() if result else "UNKNOWN"
                            print(f"[{crypto_name}] Market outcome: {outcome}")
                            
                            # Log final entry with outcome
                            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                            log_price_to_csv(
                                csv_path,
                                timestamp,
                                market_ticker,
                                latest_prices["yes_bid"],
                                latest_prices["yes_ask"],
                                latest_prices["no_bid"],
                                latest_prices["no_ask"],
                                outcome
                            )
                            
                            # Break to find next market
                            break
        
        except Exception as e:
            print(f"[{crypto_name}] ERROR: {e}")
            await asyncio.sleep(10)


async def main():
    print("=" * 80)
    print("CRYPTO PRICE TRACKER")
    print("Tracking: BTC, ETH, XRP (15-minute markets)")
    print("Logging prices every minute to separate CSV files")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    # Create tasks for each cryptocurrency
    tasks = [
        track_crypto_prices(name, config["series"], config["csv_path"])
        for name, config in CRYPTO_CONFIGS.items()
    ]
    
    # Run all trackers concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nPrice tracker stopped by user")
