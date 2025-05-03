import boto3
import requests
import time
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("crypto_coins_gecko")


def fetch_coin_details(coin_id, max_retries=4, backoff_base=5):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                wait = backoff_base ** attempt
                print(f"⏳ Rate limited on {coin_id}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"⚠️ Error {response.status_code} fetching {coin_id}: {response.text}")
                break  # Non-retryable error

        except requests.RequestException as e:
            wait = backoff_base ** attempt
            print(f"❌ Network error fetching {coin_id}: {e} — retrying in {wait}s...")
            time.sleep(wait)

    print(f"💥 Failed to fetch {coin_id} after {max_retries} attempts.")
    return None


def convert_floats_to_decimal(obj):
    if isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj


def lambda_handler(event, context):
    saved = 0
    checked = 0

    for asset in event["assets"]:
        coin = asset.get("id")
        print(f"coin: {coin}")
        details = fetch_coin_details(coin)
        time.sleep(10)  # Respect API rate limit
        checked += 1

        if details:
            description = details.get("description", {}).get("en", "")
            now = datetime.utcnow().isoformat()

            # Snapshot metrics
            community_snapshot = convert_floats_to_decimal(
                {**details.get("community_data", {}), "timestamp": now}
            )
            developer_snapshot = convert_floats_to_decimal(
                {**details.get("developer_data", {}), "timestamp": now}
            )

            try:
                existing_item = table.get_item(Key={"id": coin}).get("Item", {})
            except Exception as e:
                print(f"⚠️ Error retrieving existing item for {coin}: {e}")
                existing_item = {}

            community_history = existing_item.get("community_data_history", [])
            developer_history = existing_item.get("developer_data_history", [])

            community_history.append(community_snapshot)
            developer_history.append(developer_snapshot)

            # Limit history arrays to 240 entries (60 days at 6-hour intervals)
            if len(community_history) > 240:
                community_history = community_history[-240:]
            if len(developer_history) > 240:
                developer_history = developer_history[-240:]

            item = {
                "id": coin,
                "symbol": details.get("symbol", ""),
                "name": details.get("name", ""),
                "listing_date": details.get("genesis_date"),
                "description": description,
                "categories": details.get("categories", []),
                "links": details.get("links", {}),
                "image": details.get("image", {}),
                "country_origin": details.get("country_origin", ""),
                'sentiment_votes_up_percentage': details.get("sentiment_votes_up_percentage", 0),
                'sentiment_votes_down_percentage': details.get("sentiment_votes_down_percentage", 0),
                'market_cap_rank': details.get("market_cap_rank", 0),
                "community_data_history": community_history,
                "developer_data_history": developer_history,
                "last_updated": details.get("last_updated"), 
                "source": "coingecko"
            }

            try:
                from copy import deepcopy
                item = convert_floats_to_decimal(deepcopy(item))  # Converts everything safely
                table.put_item(Item=item)

                saved += 1
                print(f"✅ Updated/Saved {coin} ({details.get('symbol')}) - listed on: {details.get('genesis_date')}")
            except Exception as e:
                print(f"❌ Failed to save {coin}: {e}")

        if saved >= 50:  # For testing limit
            break

    return {
        "status": "done",
        "total_saved": saved,
        "total_checked": checked
    }


if __name__ == "__main__":
    lambda_handler({
        "coins": ["bitcoin", "ethereum", "solana", "binancecoin", "ripple", "kaito", "bittensor", "berachain-bera", "hyperliquid", "sui"]
    })
