# CryptoScore

This is cryptocurrency scoring system project that evaluates digital assets using market data, sentiment analysis, community metrics, developer activity, and environmental impact.

## Overview

CryptoScorer provides comprehensive cryptocurrency ratings using a weighted scoring system across five key dimensions:
- Market Performance - Price trends and trading volume
- Sentiment Analysis - CoinGecko and Reddit community sentiment
- Community Metrics - Social media following and engagement
- Developer Activity - GitHub repository statistics and development momentum
- Energy Efficiency - Environmental impact and consensus mechanism efficiency

## Architecture

- 3 Lambda Functions - Data collection, scoring engine, and API endpoints
- 4 DynamoDB Tables - Market data, sentiment data, coin metadata, and scores storage
- 1 Step Function - Orchestrates the data pipeline and job scheduling
- CloudWatch - Monitoring and logging
- Glue - Data Processing for latter visualisation in Tableau

## Data Pipeline

- Collect market data from CoinGecko API
- Gather Reddit sentiment and GitHub developer metrics
- Calculate weighted scores using normalization algorithms
- Store results with historical tracking

## Project Structure

```
CryptoScore/
├── aws-lambda/          # Lambda functions
    └── crypto-coins.py
    └── crypto-prices.py
    └── cryptoScorer.py
    └── reddit-posts.py
└── aws-transfer/        # Database migration
    └── migrate_dynamodb.py
```

## Database Migration

The `aws-transfer/migrate_dynamodb.py` script enables seamless migration of DynamoDB tables between AWS accounts (e.g., from AWS Academy to Productive accounts).

### Usage:
1. Place your AWS credentials in text files:
   - `aws-academy-cred.txt` (source account)
   - `aws-personal-cred.txt` (destination account)

2. Run the migration script:
   ```bash
   cd aws-transfer
   python migrate_dynamodb.py
   ```

3. Choose migration option:
   - Export only (from Academy account)
   - Import only (to personal account)
   - Full migration (export then import)

### Tables Migrated:
- `crypto_coins_gecko` - Cryptocurrency metadata
- `crypto_market_prices` - Historical market data and price information
- `reddit_posts` - Reddit sentiment data and community metrics
