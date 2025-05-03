import boto3
import requests
import time
from datetime import datetime, timedelta
from decimal import Decimal

# === AWS Setup
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("crypto_market_prices")

# === Config
API_CALL_DELAY = 6  # seconds between API calls
MAX_RETRIES = 3     # number of retries for rate limit errors

# === Convert float to Decimal for DynamoDB
def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj

# === Fetch market data for the last 24 hours
def fetch_last_24h_data(symbol, coin_id, start_date=None, end_date=None):
    """Fetch market data for a coin using CoinGecko's range endpoint"""
    
    # Default: last 24 hours from now
    if not start_date and not end_date:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        start_date_str = start_date.isoformat() + "Z"
        end_date_str = end_date.isoformat() + "Z"
        print(f"📅 Using default: last 24 hours ({start_date_str} to {end_date_str})")
    else:
        start_date_str = start_date
        end_date_str = end_date
    
    # Convert to timestamps
    if isinstance(start_date, str):
        start_timestamp = int(datetime.fromisoformat(start_date.replace('Z', '')).timestamp())
    elif isinstance(start_date, datetime):
        start_timestamp = int(start_date.timestamp())
    else:
        start_timestamp = int(start_date)
        
    if isinstance(end_date, str):
        end_timestamp = int(datetime.fromisoformat(end_date.replace('Z', '')).timestamp())
    elif isinstance(end_date, datetime):
        end_timestamp = int(end_date.timestamp())
    else:
        end_timestamp = int(end_date)
    
    # CoinGecko endpoint
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_timestamp,
        "to": end_timestamp
    }
    
    print(f"📊 Fetching {coin_id} data from {start_date_str} to {end_date_str}")
    
    # Retry logic for rate limiting
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', API_CALL_DELAY * 2))
                print(f"⏳ Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            market_caps = data.get("market_caps", [])
            
            results = []
            for i in range(len(prices)):
                # Convert timestamp to ISO format with hour precision
                ts = datetime.utcfromtimestamp(prices[i][0] / 1000).isoformat() + "Z"
                hour_ts = ts[:13] + ":00:00Z"
                
                results.append({
                    "id": f"{symbol}_{hour_ts}",
                    "symbol": symbol,
                    "timestamp": hour_ts,
                    "price": prices[i][1],
                    "volume": volumes[i][1] if i < len(volumes) else None,
                    "market_cap": market_caps[i][1] if i < len(market_caps) else None,
                    "source": "coingecko"
                })
            
            # Calculate price changes
            if len(results) > 1:
                # Sort by timestamp
                sorted_results = sorted(results, key=lambda x: x["timestamp"])
                
                # Calculate price changes
                for i in range(1, len(sorted_results)):
                    current_price = sorted_results[i]["price"]
                    prev_price = sorted_results[i-1]["price"]
                    
                    if prev_price > 0:
                        pct_change = ((current_price - prev_price) / prev_price) * 100
                        sorted_results[i]["price_change"] = pct_change
                
                results = sorted_results
            
            print(f"✅ Retrieved {len(results)} data points for {symbol}")
            return results
            
        except Exception as e:
            print(f"❌ Error for {coin_id} (Attempt {attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                backoff = (2 ** attempt) * API_CALL_DELAY
                print(f"⏳ Retrying in {backoff}s...")
                time.sleep(backoff)
    
    print(f"💥 Failed to fetch data for {coin_id}")
    return []

# === Get existing timestamps for a symbol
def get_existing_timestamps(symbol):
    existing = set()
    response = table.query(
        IndexName="symbol-timestamp-index",
        KeyConditionExpression="symbol = :symbol",
        ExpressionAttributeValues={":symbol": symbol}
    )
    for item in response.get('Items', []):
        existing.add(item.get('timestamp'))
    return existing

# === Save to DynamoDB with deduplication
def save_to_dynamodb(items):
    # Group by symbol
    by_symbol = {}
    for item in items:
        symbol = item.get("symbol")
        if symbol not in by_symbol:
            by_symbol[symbol] = []
        by_symbol[symbol].append(item)
    
    total_new = 0
    total_skipped = 0
    
    # Process each symbol
    for symbol, symbol_items in by_symbol.items():
        existing = get_existing_timestamps(symbol)
        new_items = [item for item in symbol_items if item.get("timestamp") not in existing]
        
        duplicates = len(symbol_items) - len(new_items)
        total_skipped += duplicates
        
        if duplicates > 0:
            print(f"⏭️ Skipping {duplicates} existing items for {symbol}")
        
        if new_items:
            # Write in batches of 25
            for i in range(0, len(new_items), 25):
                batch = new_items[i:i+25]
                with table.batch_writer(overwrite_by_pkeys=["id"]) as batch_writer:
                    for item in batch:
                        batch_writer.put_item(Item=to_decimal(item))
                
                total_new += len(batch)
                print(f"✅ Saved batch of {len(batch)} items for {symbol}")
    
    print(f"✅ Saved {total_new} new items. Skipped {total_skipped} existing items.")
    return total_new

# === Lambda Handler
def lambda_handler(event, context):
    start_time = time.time()
    assets = event.get("assets", [])
    all_data = []
    
    # Get date parameters (or use last 24 hours)
    start_date = event.get("dateStartFrom")
    end_date = event.get("dateEndAt")
    
    # Process assets with delay for rate limits
    for i, asset in enumerate(assets):
        symbol = asset.get("symbol")
        coin_id = asset.get("id")
        
        try:
            history = fetch_last_24h_data(symbol, coin_id, start_date, end_date)
            if history:
                all_data.extend(history)
            
            # Rate limit delay (except for last asset)
            if i < len(assets) - 1:
                print(f"⏳ Waiting {API_CALL_DELAY}s before next request...")
                time.sleep(API_CALL_DELAY)
                
        except Exception as e:
            print(f"❌ Error processing {symbol}: {e}")
    
    # Save to DynamoDB
    new_records = 0
    if all_data:
        new_records = save_to_dynamodb(all_data)
    
    execution_time = time.time() - start_time
    
    return {
        "status": "done",
        "total_records": len(all_data),
        "new_records": new_records,
        "execution_time": round(execution_time, 2),
        "start_date": start_date,
        "end_date": end_date
    }

# === Local Test
if __name__ == "__main__":
    # Test with default (last 24 hours)
    test_event = {
        "assets": [
            {"id": "bitcoin", "symbol": "BTC"},
            {"id": "ethereum", "symbol": "ETH"}
        ]
    }
    
    # To test with specific date range
    # test_event = {
    #     "assets": [
    #         {"id": "bitcoin", "symbol": "BTC"},
    #         {"id": "ethereum", "symbol": "ETH"}
    #     ],
    #     "dateStartFrom": "2025-04-26T00:00:00Z",
    #     "dateEndAt": "2025-04-28T23:59:59Z"
    # }
    
    result = lambda_handler(test_event, None)
    print(result)
