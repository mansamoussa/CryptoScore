import boto3
import praw
import datetime
import os
import time
import concurrent.futures
from decimal import Decimal
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# === Reddit API credentials from env vars
REDDIT_CLIENT_ID = os.environ['REDDIT_CLIENT_ID']
REDDIT_CLIENT_SECRET = os.environ['REDDIT_CLIENT_SECRET']
REDDIT_USER_AGENT = os.environ['REDDIT_USER_AGENT']

# === Config with reduced limits for faster execution
SUBREDDIT = "all"
MIN_USER_KARMA = 1000  # Reduced from 5000 to find more posts
POST_LIMIT = 50  # Reduced from 100 to finish faster
MAX_PROCESSING_TIME = 240  # 4 minute maximum processing time
S3_BUCKET = 'mouhabucket123test'

# === Init Reddit client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# === AWS Setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('reddit_posts')

# === Sentiment Analyzer (initialized once)
analyzer = SentimentIntensityAnalyzer()

# === Sentiment Analysis
def analyze_sentiment(text):
    return analyzer.polarity_scores(text)

# === Convert float to Decimal
def convert_floats_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    else:
        return obj

# === Fetch Reddit Posts with timeout
def fetch_reddit_posts(crypto_name, aliases, timeout=60):
    collected = []
    query = " OR ".join(aliases)
    
    # Set timeout for this search
    start_time = time.time()
    
    try:
        # Use more focused search criteria
        for post in reddit.subreddit(SUBREDDIT).search(
            query, 
            sort="new", 
            time_filter="week",  # Only search the past week for faster results
            limit=POST_LIMIT
        ):
            # Check if we're approaching timeout
            if time.time() - start_time > timeout:
                print(f"⚠️ Search timeout for {crypto_name} after {len(collected)} posts")
                break
                
            user_karma = None
            try:
                if post.author:
                    user_karma = post.author.link_karma + post.author.comment_karma
            except Exception:
                # Author may be deleted or unavailable
                pass

            if user_karma is not None and user_karma >= MIN_USER_KARMA:
                # Analyze only title for faster processing
                sentiment = analyze_sentiment(post.title)
                collected.append({
                    "post_id": post.id,
                    "Crypto": crypto_name,
                    "title": post.title,
                    "text": "",  # Skip text for faster processing
                    "created_utc": str(datetime.datetime.fromtimestamp(post.created_utc)),
                    "author": str(post.author),
                    "user_karma": user_karma,
                    "sentiment": sentiment,
                    "url": post.url,
                    "score": post.score
                })
    except Exception as e:
        print(f"❌ Error searching for {crypto_name}: {e}")
        
    return collected

# === Save to DynamoDB with batching
def save_all_to_dynamodb(posts):
    # Process in smaller batches (max 25 items)
    batch_size = 25
    for i in range(0, len(posts), batch_size):
        batch = posts[i:i+batch_size]
        try:
            with table.batch_writer(overwrite_by_pkeys=['post_id']) as batch_writer:
                for post in batch:
                    safe_post = convert_floats_to_decimal(post)
                    batch_writer.put_item(Item=safe_post)
            print(f"✅ Saved batch of {len(batch)} posts to DynamoDB")
        except Exception as e:
            print(f"❌ Error saving batch to DynamoDB: {e}")

# === Process a single asset
def process_asset(asset):
    name = asset.get("id")
    aliases = asset.get("aliases", [name, asset.get("symbol", "")])
    
    print(f"🔍 Searching Reddit for: {name} using aliases {aliases}")
    posts = fetch_reddit_posts(name, aliases, timeout=60)
    print(f"✅ Found {len(posts)} posts for {name}")
    
    # Calculate sentiment metrics
    positive = sum(1 for post in posts if post["sentiment"]["compound"] >= 0.05)
    negative = sum(1 for post in posts if post["sentiment"]["compound"] <= -0.05)
    neutral = len(posts) - positive - negative
    
    # Return both posts and sentiment summary
    return {
        "posts": posts,
        "sentiment_summary": {
            "asset": name,
            "positive": positive,
            "negative": negative,
            "neutral": neutral,
            "total": len(posts)
        }
    }

# === Lambda Handler
def lambda_handler(event, context):
    start_time = time.time()
    assets = event.get("assets", [])
    all_posts = []
    sentiment_results = {}
    
    # Set maximum processing time to avoid timeouts
    max_end_time = start_time + MAX_PROCESSING_TIME
    
    # Process assets in smaller batches to avoid timeout
    max_assets_per_batch = 3
    for i in range(0, len(assets), max_assets_per_batch):
        # Check if we're approaching the timeout
        if time.time() > max_end_time:
            print(f"⚠️ Approaching Lambda timeout, processing only {i} of {len(assets)} assets")
            break
            
        batch = assets[i:i+max_assets_per_batch]
        
        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_asset = {executor.submit(process_asset, asset): asset for asset in batch}
            
            for future in concurrent.futures.as_completed(future_to_asset):
                asset = future_to_asset[future]
                try:
                    result = future.result()
                    all_posts.extend(result["posts"])
                    sentiment_results[result["sentiment_summary"]["asset"]] = result["sentiment_summary"]
                except Exception as e:
                    print(f"❌ Error processing {asset.get('id')}: {e}")
    
    # Save results to DynamoDB if we have any
    if all_posts:
        # Save only if we have time left
        if time.time() < max_end_time - 30:  # Allow 30 seconds for saving
            save_all_to_dynamodb(all_posts)
            print(f"✅ Saved {len(all_posts)} posts to DynamoDB.")
        else:
            print("⚠️ Not enough time left to save to DynamoDB")
    else:
        print("⚠️ No posts found.")
    
    # Calculate total execution time
    execution_time = time.time() - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")
    
    return {
        "statusCode": 200,
        "total_posts_saved": len(all_posts),
        "sentiment_results": sentiment_results,
        "execution_time": execution_time
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
