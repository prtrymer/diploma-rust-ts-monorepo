USE market_data;

CREATE TABLE IF NOT EXISTS trading_signals (
    symbol text,
    bucket text,
    signal_time timestamp,
    signal_id text,
    direction text,
    strength text,
    strategy_name text,
    metadata text,
    PRIMARY KEY ((symbol, bucket), signal_time)
) WITH CLUSTERING ORDER BY (signal_time DESC)
AND default_time_to_live = 7776000;

CREATE TABLE IF NOT EXISTS trading_orders (
    symbol text,
    bucket text,
    order_time timestamp,
    order_id text,
    signal_id text,
    side text,
    quantity text,
    order_type text,
    limit_price text,
    stop_price text,
    PRIMARY KEY ((symbol, bucket), order_time)
) WITH CLUSTERING ORDER BY (order_time DESC)
AND default_time_to_live = 7776000;

CREATE TABLE IF NOT EXISTS trading_fills (
    symbol text,
    bucket text,
    fill_time timestamp,
    fill_id text,
    order_id text,
    side text,
    quantity text,
    fill_price text,
    commission text,
    slippage text,
    PRIMARY KEY ((symbol, bucket), fill_time)
) WITH CLUSTERING ORDER BY (fill_time DESC)
AND default_time_to_live = 7776000;

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id text,
    snapshot_time timestamp,
    cash text,
    total_value text,
    positions_json text,
    PRIMARY KEY (snapshot_id, snapshot_time)
) WITH CLUSTERING ORDER BY (snapshot_time DESC);
