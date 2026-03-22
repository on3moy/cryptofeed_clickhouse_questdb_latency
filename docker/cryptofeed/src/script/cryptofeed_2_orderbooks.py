import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK
from coinbase import Coinbase
from cryptofeed.exchanges import Kraken, Bitstamp

import cryptofeed_tools as cft
from writers.registry import ORDERBOOK_WRITERS
from metrics_writer import METRICS


async def on_orderbook(data, receipt_time):
    latencies = await asyncio.gather(*[w.write_orderbook(data, receipt_time) for w in ORDERBOOK_WRITERS])
    for writer, latency in zip(ORDERBOOK_WRITERS, latencies):
        latency_ms = latency * 1000
        METRICS.record(writer.__class__.__name__.replace("Writer", ""), "orderbook", latency_ms)


ORDERBOOK_DEPTH = 20


def main():
    callbacks = {L2_BOOK: on_orderbook}
    test_symbols = ["BTC-USD"]

    f = FeedHandler()
    f.add_feed(Coinbase(max_depth=ORDERBOOK_DEPTH, channels=[L2_BOOK], symbols=test_symbols, callbacks=callbacks, snapshot_interval=100, snapshots_only=True))
    f.add_feed(Bitstamp(channels=[L2_BOOK], symbols=test_symbols, callbacks=callbacks, snapshot_interval=100, snapshots_only=True))
    f.add_feed(Kraken(channels=[L2_BOOK], symbols=test_symbols, callbacks=callbacks, snapshot_interval=100, snapshots_only=True))

    asyncio.set_event_loop(asyncio.new_event_loop())
    f.run()


if __name__ == "__main__":
    main()
