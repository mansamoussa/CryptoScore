import boto3
import datetime
from decimal import Decimal
from statistics import mean
from boto3.dynamodb.conditions import Key, Attr
import time

# === AWS Setup
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
market_table = dynamodb.Table("crypto_market_prices")
reddit_table = dynamodb.Table("reddit_posts")
coin_table = dynamodb.Table("crypto_coins_gecko")
scores_table = dynamodb.Table("crypto_score")  # Changed from crypto_scores to crypto_score

def normalize(value, min_val, max_val):
    if value is None:
        return 0.0
    
    # Convert Decimal to float if needed
    if isinstance(value, Decimal):
        value = float(value)
    else:
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

def compute_coingecko_sentiment_score(up, down):
    # Convert to float to ensure we don't have decimal type issues
    up_value = float(up or 0)
    down_value = float(down or 0)
    net_score = up_value - down_value
    return round(normalize(net_score, -100, 100), 3)

def compute_reddit_sentiment_score(posts):
    if not posts:
        return 0.0
    
    # Calculate counts by sentiment category
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    total_karma = 0
    
    for post in posts:
        sentiment = post.get("sentiment", {})
        # Convert to float to avoid decimal issues
        compound_score = float(sentiment.get("compound", 0))
        user_karma = float(post.get("user_karma", 0))
        
        # Count posts by sentiment, weighted by user karma
        if compound_score >= 0.05:
            positive_count += 1
            total_karma += user_karma
        elif compound_score <= -0.05:
            negative_count += 1
            total_karma += user_karma
        else:
            neutral_count += 1
    
    total_count = positive_count + negative_count + neutral_count
    
    if total_count == 0:
        return 0.0
    
    # Calculate sentiment ratio (percentage of positive sentiment)
    sentiment_ratio = (positive_count + (neutral_count * 0.5)) / total_count
    
    # Calculate mention volume score (more mentions is better, up to a limit)
    mention_volume_score = normalize(total_count, 5, 100)
    
    # Calculate karma influence score (higher karma users increase score)
    karma_score = normalize(total_karma, 10000, 10000000) if total_karma > 0 else 0
    
    # Weighted combination of sentiment ratio, post volume, and karma influence
    reddit_score = (sentiment_ratio * 0.6) + (mention_volume_score * 0.3) + (karma_score * 0.1)
    
    return round(reddit_score, 3)

def compute_community_score(metrics):
    if not metrics:
        return 0.0
    
    # Debug output of metrics
    for key, value in metrics.items():
        if key not in ["timestamp", "user_id", "id"]:  # Skip non-metric fields
            print(f"- {key}: {value}")
    
    score = 0.0
    score += normalize(metrics.get("twitter_followers", 0), 1000, 1_000_000)
    score += normalize(metrics.get("reddit_subscribers", 0), 500, 500_000)
    score += normalize(metrics.get("telegram_channel_user_count", 0), 100, 100_000)
    
    # Add normalization for additional metrics if available
    if "facebook_likes" in metrics:
        score += normalize(metrics.get("facebook_likes", 0), 500, 500_000)
        return round(score / 4, 3)
    
    return round(score / 3, 3)

def compute_developer_score(metrics):
    if not metrics:
        return 0.0
    
    # Debug output of metrics
    for key, value in metrics.items():
        if key not in ["timestamp", "user_id", "id"]:  # Skip non-metric fields
            print(f"- {key}: {value}")
    
    score = 0.0
    score += normalize(metrics.get("forks", 0), 10, 2000)
    score += normalize(metrics.get("stars", 0), 10, 3000)
    score += normalize(metrics.get("subscribers", 0), 10, 1000)
    score += normalize(metrics.get("total_issues", 0), 10, 2000)
    score += normalize(metrics.get("pull_requests_merged", 0), 5, 1000)
    
    # Additional GitHub metrics if available
    if "closed_issues" in metrics and "total_issues" in metrics and float(metrics.get("total_issues", 0)) > 0:
        # Convert both to float to avoid decimal/float mixing
        closed = float(metrics.get("closed_issues", 0))
        total = float(metrics.get("total_issues", 1))
        issue_resolution_rate = closed / total
        score += issue_resolution_rate
        return round(score / 6, 3)
    
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

def get_all_market_hour_timestamps(symbol, limit=None):
    """Get all hourly timestamps for which we have market data for a symbol."""
    unique_timestamps = set()
    
    # Query the market data for this symbol with pagination
    response = market_table.query(
        IndexName="symbol-timestamp-index",
        KeyConditionExpression=Key("symbol").eq(symbol),
        ScanIndexForward=False  # Newest first
    )
    data = response.get("Items", [])
    
    # For very large datasets, implement pagination:
    last_evaluated_key = response.get('LastEvaluatedKey')
    while last_evaluated_key:
        response = market_table.query(
            IndexName="symbol-timestamp-index",
            KeyConditionExpression=Key("symbol").eq(symbol),
            ScanIndexForward=False,
            ExclusiveStartKey=last_evaluated_key
        )
        data.extend(response.get("Items", []))
        last_evaluated_key = response.get('LastEvaluatedKey')
    
    print(f"📈 Found {len(data)} total market data points for {symbol}")
    
    # Extract the hourly timestamp from each record
    for item in data:
        ts = item.get("timestamp", "")
        if ts:
            # Truncate to the hour
            hour_ts = ts[:13]  # e.g., '2025-04-21T14'
            unique_timestamps.add(hour_ts)
    
    all_timestamps = sorted(list(unique_timestamps), reverse=True)  # Newest first
    print(f"⏰ Found {len(all_timestamps)} unique hourly timestamps")
    
    # Apply limit if specified
    if limit and len(all_timestamps) > limit:
        print(f"ℹ️ Limiting to {limit} most recent timestamps")
        all_timestamps = all_timestamps[:limit]
        
    return all_timestamps

def check_if_score_exists(coin_id, timestamp_hour):
    """Check if a score already exists for this coin and hour."""
    timestamp_prefix = timestamp_hour + ":00:00Z"
    
    response = scores_table.query(
        KeyConditionExpression=
            Key("coin_id").eq(coin_id) & 
            Key("timestamp").begins_with(timestamp_prefix)
    )
    
    return len(response.get("Items", [])) > 0

def get_letter_grade(score):
    """Convert numerical score to letter grade."""
    if score >= 0.9:
        return "A"
    elif score >= 0.8:
        return "B"
    elif score >= 0.7:
        return "C"
    elif score >= 0.6:
        return "D"
    elif score >= 0.5:
        return "E"
    else:
        return "F"

def score_asset_for_timestamp(asset, timestamp_hour, max_reddit_age_days=7):
    """Score a single asset for a specific timestamp hour."""
    start_time = time.time()
    
    coin_id = asset.get("id")
    symbol = asset.get("symbol")
    timestamp_prefix = timestamp_hour + ":00:00Z"
    
    print(f"\n🔍 Scoring {coin_id} ({symbol}) for {timestamp_hour}")
    
    # Check if a score already exists for this coin and hour
    if check_if_score_exists(coin_id, timestamp_hour):
        print(f"⏭️ Score already exists for {coin_id} at {timestamp_hour}, skipping")
        return None
    
    # Get market data for this hour
    market_data = market_table.query(
        IndexName="symbol-timestamp-index",
        KeyConditionExpression=Key("symbol").eq(symbol)
    ).get("Items", [])
    
    # Filter for data from the specified timestamp
    market_data = [d for d in market_data if d["timestamp"].startswith(timestamp_hour)]
    
    if not market_data:
        print(f"⚠️ No market data found for {coin_id} at {timestamp_hour}, skipping")
        return None
    
    print(f"📊 Found {len(market_data)} market data points")
    
    # Get Reddit posts from the last X days
    current_time = datetime.datetime.fromisoformat(timestamp_hour)
    lookback_time = (current_time - datetime.timedelta(days=max_reddit_age_days)).isoformat()
    
    reddit_posts = reddit_table.scan(
        FilterExpression=Key("Crypto").eq(coin_id)
    ).get("Items", [])
    
    # Filter for posts within the lookback window
    reddit_posts = [p for p in reddit_posts if p["created_utc"] > lookback_time]
    print(f"💬 Found {len(reddit_posts)} Reddit posts within {max_reddit_age_days} days")
    
    # Fetch coin metadata
    coin_data = coin_table.get_item(Key={"id": coin_id}).get("Item", {})
    
    # Get community and developer data
    community_data = {}
    developer_data = {}
    
    if coin_data:
        community_history = coin_data.get("community_data_history", [])
        developer_history = coin_data.get("developer_data_history", [])
        
        if community_history:
            community_data = community_history[-1]  # Get the most recent snapshot
        
        if developer_history:
            developer_data = developer_history[-1]  # Get the most recent snapshot
    
    # Calculate component scores
    market_score = compute_market_score(market_data)
    print(f"📈 Market Score: {market_score}")
    
    coingecko_sentiment = compute_coingecko_sentiment_score(
        coin_data.get("sentiment_votes_up_percentage"),
        coin_data.get("sentiment_votes_down_percentage")
    )
    print(f"🦎 CoinGecko Sentiment Score: {coingecko_sentiment}")
    
    reddit_sentiment = compute_reddit_sentiment_score(reddit_posts)
    print(f"🔴 Reddit Sentiment Score: {reddit_sentiment}")
    
    # Combine CoinGecko and Reddit sentiment
    sentiment_score = (coingecko_sentiment * 0.4) + (reddit_sentiment * 0.6)
    print(f"😀 Combined Sentiment Score: {sentiment_score}")
    
    community_score = compute_community_score(community_data)
    print(f"👥 Community Score: {community_score}")
    
    developer_score = compute_developer_score(developer_data)
    print(f"👨‍💻 Developer Score: {developer_score}")
    
    energy_score = compute_energy_score(coin_id)
    print(f"⚡ Energy Score: {energy_score}")
    
    # Calculate overall score
    overall_score = round((
        0.25 * market_score +
        0.25 * sentiment_score +
        0.15 * community_score +
        0.20 * developer_score +
        0.15 * energy_score
    ), 3)
    
    # Get letter grade
    grade = get_letter_grade(overall_score)
    print(f"🏆 Overall Score: {overall_score} (Grade: {grade})")
    
    # Create the item to store in DynamoDB
    item = {
        "coin_id": coin_id,
        "timestamp": timestamp_prefix,
        "symbol": symbol,
        "name": coin_data.get("name", coin_id),
        "market_score": Decimal(str(market_score)),
        "sentiment_score": Decimal(str(sentiment_score)),
        "coingecko_sentiment": Decimal(str(coingecko_sentiment)),
        "reddit_sentiment": Decimal(str(reddit_sentiment)),
        "community_score": Decimal(str(community_score)),
        "developer_score": Decimal(str(developer_score)),
        "energy_score": Decimal(str(energy_score)),
        "overall_score": Decimal(str(overall_score)),
        "grade": grade,
        "market_cap_rank": coin_data.get("market_cap_rank", 0),
        "data_points": {
            "market_data_count": len(market_data),
            "reddit_posts_count": len(reddit_posts)
        },
        "processing_time": Decimal(str(round(time.time() - start_time, 2)))
    }
    
    # Store the score in DynamoDB
    scores_table.put_item(Item=item)
    print(f"✅ Saved score for {coin_id} at {timestamp_hour}")
    
    return item

def lambda_handler(event, context):
    start_time = time.time()
    assets = event.get("assets", [])
    if not assets:
        return {"status": "error", "message": "No assets provided."}
    
    # Get max hourly timestamps to process (optional parameter)
    max_hours = event.get("max_hours")
    
    print(f"🚀 Starting CryptoScorer for {len(assets)} assets (max hours: {max_hours if max_hours else 'unlimited'})")
    
    # Get all timestamps with market data for first asset
    all_timestamps = get_all_market_hour_timestamps(assets[0]["symbol"], limit=max_hours)
    
    if not all_timestamps:
        return {"status": "error", "message": "No market data found."}
    
    print(f"⏰ Found {len(all_timestamps)} unique hourly timestamps to process")
    
    all_results = []
    total_processed = 0
    total_skipped = 0
    
    # Track Lambda execution time to avoid timeouts
    lambda_start_time = time.time()
    max_execution_time = 840  # 14 minutes (leaving 1 minute buffer for a 15 minute Lambda)
    
    # Process each asset for each timestamp
    for asset in assets:
        coin_id = asset.get("id")
        
        for timestamp_hour in all_timestamps:
            # Check if we're approaching Lambda timeout
            current_execution_time = time.time() - lambda_start_time
            if current_execution_time > max_execution_time:
                print(f"⚠️ Approaching Lambda timeout after {int(current_execution_time)} seconds, stopping processing")
                break
                
            result = score_asset_for_timestamp(asset, timestamp_hour)
            
            if result:
                all_results.append(result)
                total_processed += 1
            else:
                total_skipped += 1
    
    # Create the final results list with all scores
    if all_results:
        # Get the most recent timestamp
        latest_timestamp = all_timestamps[0] if all_timestamps else None
        
        # Filter for just the latest scores to return
        latest_results = [r for r in all_results if r["timestamp"].startswith(latest_timestamp)] if latest_timestamp else []
        
        # Sort results by overall score (highest first)
        latest_results.sort(key=lambda x: float(x["overall_score"]), reverse=True)
        
        # Add ranking information
        for i, result in enumerate(latest_results):
            result["rank"] = i + 1
    else:
        latest_results = []
    
    total_time = time.time() - start_time
    print(f"✅ Completed scoring: processed {total_processed} records, skipped {total_skipped}")
    print(f"⏱️ Total execution time: {total_time:.2f} seconds")
    
    return {
        "status": "ok",
        "total_processed": total_processed,
        "total_skipped": total_skipped,
        "execution_time": round(total_time, 2),
        "latest_scores": latest_results
    }

if __name__ == "__main__":
    test_event = {
        "assets": [
            {"id": "bitcoin", "symbol": "BTC"},
            {"id": "ethereum", "symbol": "ETH"},
            {"id": "solana", "symbol": "SOL"}
        ],
        "max_hours": 24  # Process last 24 hours for testing, or omit for all available timestamps
    }
    result = lambda_handler(test_event, None)
    print(result)
