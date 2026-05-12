#!/usr/bin/env bash
set -euo pipefail

echo "🔄 Reinitializing ScyllaDB and Kafka stack..."
docker compose down -v
docker compose up -d scylla zookeeper kafka kafka-init

echo "⏳ Waiting for containers to warm up..."
sleep 8

./scripts/init_scylla.sh

echo "✅ Reinit complete"
echo "Run app: cargo run --bin db-con"
