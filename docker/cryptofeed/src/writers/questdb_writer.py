import asyncio
import time
import threading
from questdb.ingress import Sender, Protocol, TimestampNanos


class QuestDBWriter:
    def __init__(self):
        self._local = threading.local()

    def _get_sender(self):
        if not hasattr(self._local, "sender"):
            s = Sender(Protocol.Tcp, "questdb", 9009)
            s.__enter__()
            self._local.sender = s
        return self._local.sender

    async def write_trade(self, data):
        start = time.perf_counter()
        await asyncio.to_thread(self._sync_write_trade, data)
        return time.perf_counter() - start

    def _sync_write_trade(self, data):
        ts = TimestampNanos(int(data.timestamp * 1_000_000_000))
        self._get_sender().row(
            "trades",
            symbols={
                "exchange": data.exchange,
                "symbol": data.symbol,
                "side": data.side,
            },
            columns={
                "price": float(data.price),
                "amount": float(data.amount),
            },
            at=ts,
        )
        self._get_sender().flush()

    async def write_orderbook(self, data, receipt_time):
        start = time.perf_counter()
        await asyncio.to_thread(self._sync_write_orderbook, data, receipt_time)
        return time.perf_counter() - start

    def _sync_write_orderbook(self, data, receipt_time):
        ts = TimestampNanos(int((data.timestamp or receipt_time) * 1_000_000_000))
        sender = self._get_sender()
        bid_depth = len(data.book.bids)
        ask_depth = len(data.book.asks)

        for i in range(bid_depth):
            price, size = data.book.bids.index(i)
            sender.row(
                "orderbooks",
                symbols={"exchange": data.exchange, "symbol": data.symbol, "side": "bid"},
                columns={"price": float(price), "size": float(size)},
                at=ts,
            )
        for i in range(ask_depth):
            price, size = data.book.asks.index(i)
            sender.row(
                "orderbooks",
                symbols={"exchange": data.exchange, "symbol": data.symbol, "side": "ask"},
                columns={"price": float(price), "size": float(size)},
                at=ts,
            )
        sender.flush()
