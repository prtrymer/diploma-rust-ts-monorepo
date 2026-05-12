USE market_data;

-- Unified candles table for all timeframes
CREATE TABLE IF NOT EXISTS candles (
    symbol text,
    timeframe text,        -- '1min', '5min', '15min', '1hour', 'daily'
    bucket text,           -- '2026-02-28' for partitioning
    tick_time timestamp,
    open text,
    high text,
    low text,
    close text,
    volume bigint,
    trades_count int,
    vwap text,
    PRIMARY KEY ((symbol, timeframe, bucket), tick_time)
) WITH CLUSTERING ORDER BY (tick_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '7',
    'compaction_window_unit': 'DAYS'
};

-- Index for querying by timeframe
CREATE INDEX IF NOT EXISTS candles_timeframe_idx ON candles (timeframe);
