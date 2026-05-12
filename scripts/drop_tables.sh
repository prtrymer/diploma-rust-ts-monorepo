#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${SCYLLA_CONTAINER:-scylla_db}"

echo "🗑️ Dropping market_data tables..."
docker exec -i "${CONTAINER_NAME}" cqlsh <<'CQL'
USE market_data;
DROP TABLE IF EXISTS stock_ticks;
DROP TABLE IF EXISTS stock_1min;
DROP TABLE IF EXISTS stock_5min;
DROP TABLE IF EXISTS stock_15min;
DROP TABLE IF EXISTS stock_hourly;
DROP TABLE IF EXISTS stock_daily;
DROP TABLE IF EXISTS stock_latest_prices;
DROP TABLE IF EXISTS stock_metadata;
DROP TABLE IF EXISTS kafka_messages;
DROP TABLE IF EXISTS candles;
DROP TABLE IF EXISTS trading_signals;
DROP TABLE IF EXISTS trading_orders;
DROP TABLE IF EXISTS trading_fills;
DROP TABLE IF EXISTS portfolio_snapshots;
DROP TABLE IF EXISTS historical_candles;
CQL

echo "✅ Tables dropped"
