import boto3
import praw
import datetime
import os
from decimal import Decimal
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# === Reddit API credentials from env vars
REDDIT_CLIENT_ID = os.environ['REDDIT_CLIENT_ID']
REDDIT_CLIENT_SECRET = os.environ['REDDIT_CLIENT_SECRET']
REDDIT_USER_AGENT = os.environ['REDDIT_USER_AGENT']

# === Config
SUBREDDIT = "all"
MIN_USER_KARMA = 5000

# === Init Reddit client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# === AWS Setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('reddit_posts')


# === Sentiment Analysis
def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
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


# === Fetch Reddit Posts
def fetch_reddit_posts(crypto_name, aliases):
    collected = []
    query = " OR ".join(aliases)

    for post in reddit.subreddit(SUBREDDIT).search(query, sort="new", limit=100):
        if post.author and hasattr(post.author, 'comment_karma'):
            user_karma = post.author.link_karma + post.author.comment_karma
            if user_karma >= MIN_USER_KARMA:
                sentiment = analyze_sentiment(post.title + " " + post.selftext)
                collected.append({
                    "post_id": post.id,
                    "Crypto": crypto_name,
                    "title": post.title,
                    "text": post.selftext,
                    "created_utc": str(datetime.datetime.fromtimestamp(post.created_utc)),
                    "author": str(post.author),
                    "user_karma": user_karma,
                    "sentiment": sentiment,
                    "url": post.url,
                    "score": post.score
                })
    return collected


# === Save to DynamoDB
def save_all_to_dynamodb(posts):
    with table.batch_writer(overwrite_by_pkeys=['post_id']) as batch:
        for post in posts:
            safe_post = convert_floats_to_decimal(post)
            batch.put_item(Item=safe_post)


# === Lambda Handler
def lambda_handler(event, context):
    assets = event.get("assets", [])
    all_posts = []

    for asset in assets:
        name = asset.get("id")
        aliases = asset.get("aliases", [])

        print(f"🔍 Searching Reddit for: {name} using aliases {aliases}")
        posts = fetch_reddit_posts(name, aliases)
        print(f"✅ Found {len(posts)} posts for {name}")
        all_posts.extend(posts)

    if all_posts:
        save_all_to_dynamodb(all_posts)
        print(f"✅ Saved {len(all_posts)} posts to DynamoDB.")
    else:
        print("⚠️ No posts found.")

    return {
        "statusCode": 200,
        "total_posts_saved": len(all_posts)
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
