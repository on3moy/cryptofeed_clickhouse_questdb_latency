from writers.questdb_writer import QuestDBWriter
from writers.clickhouse_writer import ClickHouseWriter

TRADE_WRITERS = [
    QuestDBWriter(),
    ClickHouseWriter(),
]

ORDERBOOK_WRITERS = [
    QuestDBWriter(),
    ClickHouseWriter(),
]
