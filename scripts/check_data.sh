#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${SCYLLA_CONTAINER:-scylla_db}"

run_query() {
  local label="$1"
  local query="$2"
  echo "=== ${label} ==="
  docker exec -i "${CONTAINER_NAME}" cqlsh -e "USE market_data; ${query}"
  echo
}

echo "📊 Checking ScyllaDB data..."
run_query "Raw ticks count" "SELECT COUNT(*) FROM stock_ticks;"
run_query "1m candles count" "SELECT COUNT(*) FROM stock_1min;"
run_query "5m candles count" "SELECT COUNT(*) FROM stock_5min;"
run_query "Historical candles count" "SELECT COUNT(*) FROM historical_candles;"
run_query "Latest historical candles" "SELECT symbol, timeframe, bucket, tick_time, close, volume, source FROM historical_candles LIMIT 10;"
run_query "Kafka message count" "SELECT COUNT(*) FROM kafka_messages;"

echo "✅ Done"
