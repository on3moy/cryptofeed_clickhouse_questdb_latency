import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from coinbase import Coinbase
from cryptofeed.exchanges import Bitstamp, Kraken

import cryptofeed_tools as cft
from writers.registry import TRADE_WRITERS
from metrics_writer import METRICS


async def on_trade(data, receipt_time):
    latencies = await asyncio.gather(*[w.write_trade(data) for w in TRADE_WRITERS])
    for writer, latency in zip(TRADE_WRITERS, latencies):
        latency_ms = latency * 1000
        METRICS.record(writer.__class__.__name__.replace("Writer", ""), "trade", latency_ms)


def main():
    f = FeedHandler()
    f.add_feed(Coinbase(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: on_trade}))
    f.add_feed(Bitstamp(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: on_trade}))
    f.add_feed(Kraken(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: on_trade}))

    asyncio.set_event_loop(asyncio.new_event_loop())
    f.run()


if __name__ == "__main__":
    main()
