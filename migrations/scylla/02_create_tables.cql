USE market_data;

-- Raw tick data (highest resolution) - 30 days retention
CREATE TABLE IF NOT EXISTS stock_ticks (
    symbol text,
    bucket text,
    tick_time timestamp,
    price text,
    volume bigint,
    bid text,
    ask text,
    source text,
    PRIMARY KEY ((symbol, bucket), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'DAYS'
}
AND default_time_to_live = 2592000;

-- 1-minute candles - 90 days retention
CREATE TABLE IF NOT EXISTS stock_1min (
    symbol text,
    bucket text,
    tick_time timestamp,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY ((symbol, bucket), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '7',
    'compaction_window_unit': 'DAYS'
}
AND default_time_to_live = 7776000;

-- 5-minute candles - 180 days retention
CREATE TABLE IF NOT EXISTS stock_5min (
    symbol text,
    date date,
    tick_time timestamp,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY ((symbol, date), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '14',
    'compaction_window_unit': 'DAYS'
}
AND default_time_to_live = 15552000;

-- 15-minute candles - no TTL
CREATE TABLE IF NOT EXISTS stock_15min (
    symbol text,
    date date,
    tick_time timestamp,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY ((symbol, date), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '30',
    'compaction_window_unit': 'DAYS'
};

-- Hourly candles - no TTL
CREATE TABLE IF NOT EXISTS stock_hourly (
    symbol text,
    date date,
    tick_time timestamp,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY ((symbol, date), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '30',
    'compaction_window_unit': 'DAYS'
};

-- Daily candles - no TTL
CREATE TABLE IF NOT EXISTS stock_daily (
    symbol text,
    date date,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY (symbol, date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Latest prices (for quick access)
CREATE TABLE IF NOT EXISTS stock_latest_prices (
    symbol text PRIMARY KEY,
    timeframe text,
    price text,
    tick_time timestamp,
    volume bigint
);

-- Stock metadata
CREATE TABLE IF NOT EXISTS stock_metadata (
    symbol text PRIMARY KEY,
    company_name text,
    exchange text,
    sector text,
    industry text,
    currency text,
    last_updated timestamp,
    last_price text,
    data_start_date date,
    data_end_date date
);

-- Kafka message tracking (for idempotency) - 7 days retention
CREATE TABLE IF NOT EXISTS kafka_messages (
    topic text,
    partition int,
    offset bigint,
    message_id text,
    symbol text,
    tick_time timestamp,
    processed_at timestamp,
    PRIMARY KEY ((topic, partition), offset)
) WITH CLUSTERING ORDER BY (offset DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'DAYS'
}
AND default_time_to_live = 604800;
