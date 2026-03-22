CREATE TABLE IF NOT EXISTS trades (
    exchange  String,
    symbol    String,
    side      String,
    price     Float64,
    amount    Float64,
    timestamp DateTime64(9)
) ENGINE = MergeTree()
ORDER BY (exchange, symbol, timestamp);
