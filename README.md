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
AWS Stack:

3 Lambda Functions - Data collection, scoring engine, and API endpoints
4 DynamoDB Tables - Market data, sentiment data, coin metadata, and scores storage
1 Step Function - Orchestrates the data pipeline and job scheduling
CloudWatch - Monitoring and logging
Glue - Data Processing for latter visualisation in Tableau

## Data Pipeline:

Collect market data from CoinGecko API
Gather Reddit sentiment and GitHub developer metrics
Calculate weighted scores using normalization algorithms
Store results with historical tracking
