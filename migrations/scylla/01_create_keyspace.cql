CREATE KEYSPACE IF NOT EXISTS market_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};