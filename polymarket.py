import json
import requests
from decimal import Decimal

SLUGS = [
    "btc-updown-5m-1780435500",
    "btc-updown-15m-1780435800",
]

TARGET_PRICE = Decimal("0.995")

def parse_json_field(value):
    if isinstance(value, str):
        return json.loads(value)
    return value

def get_market_by_slug(slug: str) -> dict:
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def get_tick_size(token_id: str) -> Decimal:
    url = "https://clob.polymarket.com/tick-size"
    r = requests.get(url, params={"token_id": token_id}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return Decimal(str(data["minimum_tick_size"]))

def price_allowed(price: Decimal, tick_size: Decimal) -> bool:
    return price % tick_size == 0

def check_market(slug: str):
    market = get_market_by_slug(slug)

    question = market.get("question")
    outcomes = parse_json_field(market.get("outcomes"))
    token_ids = parse_json_field(market.get("clobTokenIds"))

    print("\n" + "=" * 80)
    print(f"Slug: {slug}")
    print(f"Question: {question}")
    print(f"Outcomes: {outcomes}")
    print(f"Token IDs: {token_ids}")

    if not token_ids:
        print("No clobTokenIds found. This market may not be CLOB-enabled.")
        return

    for outcome, token_id in zip(outcomes, token_ids):
        tick = get_tick_size(str(token_id))
        allowed_995 = price_allowed(TARGET_PRICE, tick)

        print("\nOutcome:", outcome)
        print("Token ID:", token_id)
        print("Tick size:", tick)
        print("Can place 99.5c / 0.995 order?", allowed_995)

        if allowed_995:
            print("Use target price: 0.995")
        else:
            print("0.995 not valid. Use 0.99 or another valid tick price.")

def main():
    for slug in SLUGS:
        try:
            check_market(slug)
        except Exception as e:
            print(f"\nError checking {slug}: {e}")

if __name__ == "__main__":
    main()