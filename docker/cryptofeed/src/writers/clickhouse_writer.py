import asyncio
import time
import threading
from datetime import datetime, timezone
from clickhouse_driver import Client


class ClickHouseWriter:
    def __init__(self):
        self._local = threading.local()

    def _get_client(self):
        if not hasattr(self._local, "client"):
            self._local.client = Client(host="clickhouse", port=9000, user="default", password="")
        return self._local.client

    async def write_trade(self, data):
        start = time.perf_counter()
        await asyncio.to_thread(self._sync_write_trade, data)
        return time.perf_counter() - start

    def _sync_write_trade(self, data):
        ts = datetime.fromtimestamp(data.timestamp, tz=timezone.utc)
        self._get_client().execute(
            "INSERT INTO trades (exchange, symbol, side, price, amount, timestamp) VALUES",
            [[data.exchange, data.symbol, data.side, float(data.price), float(data.amount), ts]],
        )

    async def write_orderbook(self, data, receipt_time):
        start = time.perf_counter()
        await asyncio.to_thread(self._sync_write_orderbook, data, receipt_time)
        return time.perf_counter() - start

    def _sync_write_orderbook(self, data, receipt_time):
        ts = datetime.fromtimestamp(data.timestamp or receipt_time, tz=timezone.utc)
        rows = []
        bid_depth = len(data.book.bids)
        ask_depth = len(data.book.asks)

        for i in range(bid_depth):
            price, size = data.book.bids.index(i)
            rows.append([data.exchange, data.symbol, "bid", float(price), float(size), ts])
        for i in range(ask_depth):
            price, size = data.book.asks.index(i)
            rows.append([data.exchange, data.symbol, "ask", float(price), float(size), ts])

        self._get_client().execute(
            "INSERT INTO orderbooks (exchange, symbol, side, price, size, timestamp) VALUES",
            rows,
        )
