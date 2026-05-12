#!/usr/bin/env bash
set -euo pipefail

# ── ScyllaDB migrations ───────────────────────────────────────────────────────
echo "🔄 Applying ScyllaDB migrations..."
./scripts/init_scylla.sh
echo "✅ ScyllaDB migrations applied."

# ── PostgreSQL migrations ─────────────────────────────────────────────────────
# Uses docker exec so psql doesn't need to be installed on the host.
# The application also auto-runs these via sqlx::migrate! on startup.
PGCONTAINER="${POSTGRES_CONTAINER:-postgres_db}"
PGUSER="${POSTGRES_USER:-trading}"
PGDB="${POSTGRES_DB:-trading}"

echo "🔄 Applying PostgreSQL migrations via docker exec..."
for f in migrations/postgres/*.sql; do
    [[ "$f" == *"02_portfolio.sql" ]] && continue  # skip empty placeholder
    echo "   → $f"
    docker exec -i "$PGCONTAINER" psql -U "$PGUSER" -d "$PGDB" < "$f"
done
echo "✅ PostgreSQL migrations applied."
