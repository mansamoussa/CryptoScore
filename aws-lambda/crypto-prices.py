import boto3
import requests
from datetime import datetime
from decimal import Decimal

# === AWS Setup
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("crypto_market_prices")


# === Convert float to Decimal for DynamoDB
def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj


# === Fetch historical hourly data (past 24h)
def fetch_historical_prices(symbol, coin_id):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": 1
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    prices = data.get("prices", [])
    volumes = data.get("total_volumes", [])

    results = []
    for i in range(len(prices)):
        ts = datetime.utcfromtimestamp(prices[i][0] / 1000).isoformat() + "Z"
        results.append({
            "id": f"{symbol}_{ts}",
            "symbol": symbol,
            "timestamp": ts,
            "price": prices[i][1],
            "volume": volumes[i][1] if i < len(volumes) else None,
            "price_change_24h": None,  # could be added later
            "source": "coingecko",
            "historical": True
        })

    return results


# === Save to DynamoDB
def save_to_dynamodb(items):
    with table.batch_writer(overwrite_by_pkeys=["id"]) as batch:
        for item in items:
            batch.put_item(Item=to_decimal(item))
    print(f"✅ Saved {len(items)} items to DynamoDB.")


# === Lambda Handler
def lambda_handler(event, context):
    assets = event.get("assets", [])
    all_data = []

    for asset in assets:
        symbol = asset.get("symbol")
        coin_id = asset.get("id")

        try:
            print(f"📦 Fetching market data for {symbol} ({coin_id})")
            history = fetch_historical_prices(symbol, coin_id)
            all_data.extend(history)

        except Exception as e:
            print(f"❌ Error fetching market data for {symbol}: {e}")

    if all_data:
        save_to_dynamodb(all_data)

    return {
        "status": "done",
        "total_records": len(all_data)
    }


# === Local Test
if __name__ == "__main__":
    test_event = {
        "assets": [
            {"id": "bitcoin", "symbol": "BTCUSDT", "aliases": ["bitcoin", "btc"]},
            {"id": "ethereum", "symbol": "ETHUSDT", "aliases": ["ethereum", "eth"]},
            {"id": "solana", "symbol": "SOLUSDT", "aliases": ["solana", "sol"]}
        ]
    }
    lambda_handler(test_event, None)
