#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${SCYLLA_CONTAINER:-scylla_db}"
MIGRATIONS_DIR="migrations/scylla"

echo "⏳ Waiting for ScyllaDB (${CONTAINER_NAME})..."
for i in {1..60}; do
  if docker exec "${CONTAINER_NAME}" cqlsh -e "DESCRIBE CLUSTER" >/dev/null 2>&1; then
    echo "✅ ScyllaDB is ready"
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "❌ ScyllaDB not ready after timeout"
    exit 1
  fi
done

echo "📦 Applying migrations from ${MIGRATIONS_DIR}..."
for file in $(ls "${MIGRATIONS_DIR}"/*.cql | sort); do
  echo "  -> $(basename "${file}")"
  docker exec -i "${CONTAINER_NAME}" cqlsh < "${file}"
done

echo "✅ All migrations applied"
