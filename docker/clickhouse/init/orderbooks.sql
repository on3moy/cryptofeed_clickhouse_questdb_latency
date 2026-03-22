CREATE TABLE IF NOT EXISTS orderbooks (
    exchange  String,
    symbol    String,
    side      String,
    price     Float64,
    size      Float64,
    timestamp DateTime64(9)
) ENGINE = MergeTree()
ORDER BY (exchange, symbol, side, timestamp);
