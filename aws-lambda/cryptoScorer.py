import boto3
import datetime
from decimal import Decimal
from statistics import mean
from boto3.dynamodb.conditions import Key

# === AWS Setup
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
market_table = dynamodb.Table("crypto_market_prices")
reddit_table = dynamodb.Table("reddit_posts")
coin_table = dynamodb.Table("crypto_coins_gecko")
scores_table = dynamodb.Table("crypto_scores")

def normalize(value, min_val, max_val):
    if value is None:
        return 0.0
    value = float(value)
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))

def compute_market_score(prices):
    if not prices:
        return 0.0

    latest = prices[-1]
    price = float(latest.get("price", 0))
    volume = float(latest.get("volume", 0))

    volume_score = normalize(volume, 1e5, 1e9)
    trend = price - float(prices[0].get("price", 0))
    pct_change = (trend / float(prices[0].get("price", 1)) * 100) if prices[0].get("price") else 0
    trend_score = normalize(pct_change, -10, 10)

    return round(0.5 * trend_score + 0.5 * volume_score, 3)

def compute_sentiment_score_from_coin_data(up, down):
    net_score = float(up or 0) - float(down or 0)
    return round(normalize(net_score, -100, 100), 3)

def compute_community_score(metrics):
    if not metrics:
        return 0.0
    for key, value in metrics.items():
        print(f"- {key}: {value}")
    score = 0.0
    score += normalize(metrics.get("twitter_followers", 0), 1000, 1_000_000)
    score += normalize(metrics.get("reddit_subscribers", 0), 500, 500_000)
    score += normalize(metrics.get("telegram_channel_user_count", 0), 100, 100_000)
    return round(score / 3, 3)

def compute_developer_score(metrics):
    if not metrics:
        return 0.0
    for key, value in metrics.items():
        print(f"- {key}: {value}")
    score = 0.0
    score += normalize(metrics.get("forks", 0), 10, 2000)
    score += normalize(metrics.get("stars", 0), 10, 3000)
    score += normalize(metrics.get("subscribers", 0), 10, 1000)
    score += normalize(metrics.get("total_issues", 0), 10, 2000)
    score += normalize(metrics.get("pull_requests_merged", 0), 5, 1000)
    return round(score / 5, 3)

def compute_energy_score(coin_id):
    energy_scores = {
        "bitcoin": 0.2,
        "ethereum": 0.4,
        "solana": 0.9,
        "binancecoin": 0.6,
        "ripple": 0.8,
        "kaito": 0.6,
        "bittensor": 0.7,
        "berachain-bera": 0.7,
        "hyperliquid": 0.5,
        "sui": 0.85
    }
    return energy_scores.get(coin_id, 0.5)

def get_latest_hour(symbol):
    data = market_table.query(
        IndexName="symbol-timestamp-index",
        KeyConditionExpression=Key("symbol").eq(symbol),
        ScanIndexForward=False,
        Limit=1
    ).get("Items", [])

    if not data:
        return None
    latest_ts = data[0]["timestamp"]
    return latest_ts[:13]  # e.g., '2025-04-21T14'


def lambda_handler(event, context):
    assets = event.get("assets", [])
    if not assets:
        return {"status": "error", "message": "No assets provided."}

    base_hour = get_latest_hour(assets[0]["symbol"])
    if not base_hour:
        return {"status": "error", "message": "No data found in market table."}

    timestamp = base_hour
    results = []

    for asset in assets:
        coin_id = asset.get("id")
        symbol = asset.get("symbol")

        market_data = market_table.query(
            IndexName="symbol-timestamp-index",
            KeyConditionExpression=Key("symbol").eq(symbol)
        ).get("Items", [])

        market_data = [d for d in market_data if d["timestamp"].startswith(timestamp)]

        reddit_posts = reddit_table.scan(
            FilterExpression=Key("Crypto").eq(coin_id)
        ).get("Items", [])
        reddit_posts = [p for p in reddit_posts if p["created_utc"].startswith(timestamp)]

        coin_data = coin_table.get_item(Key={"id": coin_id}).get("Item", {})
        community_data = (coin_data.get("community_data_history") or [])[-1] if coin_data else {}
        developer_data = (coin_data.get("developer_data_history") or [])[-1] if coin_data else {}

        market_score = compute_market_score(market_data)
        sentiment_score = compute_sentiment_score_from_coin_data(
            coin_data.get("sentiment_votes_up_percentage"),
            coin_data.get("sentiment_votes_down_percentage")
        )
        community_score = compute_community_score(community_data)
        developer_score = compute_developer_score(developer_data)
        energy_score = compute_energy_score(coin_id)

        overall_score = round((
            0.3 * market_score +
            0.25 * sentiment_score +
            0.15 * community_score +
            0.15 * developer_score +
            0.15 * energy_score
        ), 3)

        item = {
            "coin_id": coin_id,
            "timestamp": timestamp + ":00:00Z",
            "market_score": Decimal(str(market_score)),
            "sentiment_score": Decimal(str(sentiment_score)),
            "community_score": Decimal(str(community_score)),
            "developer_score": Decimal(str(developer_score)),
            "energy_score": Decimal(str(energy_score)),
            "overall_score": Decimal(str(overall_score))
        }

        print(f"✅ {coin_id}: {item}")
        scores_table.put_item(Item=item)
        results.append(item)

    return {
        "status": "ok",
        "records": len(results),
        "scores": results
    }


if __name__ == "__main__":
    lambda_handler({
        "assets": [
            {"id": "bitcoin", "symbol": "BTCUSDT"},
            {"id": "ethereum", "symbol": "ETHUSDT"},
            {"id": "solana", "symbol": "SOLUSDT"}
        ]
    }, None)
