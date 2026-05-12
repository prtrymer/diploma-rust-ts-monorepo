USE market_data;

-- Canonical historical dataset for backtests / RL training
CREATE TABLE IF NOT EXISTS historical_candles (
    symbol text,
    timeframe text,      -- e.g. '1m', '5m', '1d'
    bucket text,         -- UTC date partition: YYYY-MM-DD
    tick_time timestamp, -- candle open timestamp
    open text,
    high text,
    low text,
    close text,
    adj_close text,
    volume bigint,
    source text,
    PRIMARY KEY ((symbol, timeframe, bucket), tick_time)
) WITH CLUSTERING ORDER BY (tick_time ASC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '30',
    'compaction_window_unit': 'DAYS'
};
