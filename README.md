# cryptofeed-clickhouse latency benchmark

Measures and compares write latency between **QuestDB** (ILP over TCP) and **ClickHouse** (native TCP) using real-time crypto market data from [cryptofeed](https://github.com/bmoscon/cryptofeed).

Live latency metrics are streamed into a **Deephaven** dashboard for side-by-side comparison.

## Architecture

```
Coinbase / Bitstamp / Kraken
          |
          | WebSocket
      cryptofeed
          |
          +---> QuestDB writer (ILP / TCP :9009)
          |
          +---> ClickHouse writer (native TCP :9000)
          |
          +---> latency_metrics --> QuestDB --> Deephaven dashboard
```

All three services run on the same Docker bridge network (`cryptofeed_net`) so network conditions are identical for both databases.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose v2
- [uv](https://github.com/astral-sh/uv) (to regenerate the lock file if dependencies change)

## Quickstart

```bash
git clone <repo-url>
cd cryptofeed-clickhouse/docker
docker compose up --build -d
```

First build takes a few minutes. Subsequent starts reuse cached layers.

## Services

| Service    | URL                        | Purpose                        |
|------------|----------------------------|--------------------------------|
| QuestDB    | http://localhost:9000      | Time-series DB — HTTP UI       |
| ClickHouse | http://localhost:8123/play | Column-store DB — HTTP UI      |
| Deephaven  | http://localhost:10000     | Live dashboard                 |

## Viewing the latency dashboard

1. Open **http://localhost:10000**
2. Click on panels, then dashboard to view.

The dashboard shows:
- **Stat cards** — Avg / P95 / P99 / Min / Max per database
- **Operation toggle** — switch between `trade` and `orderbook` writes
- **Window selector** — last 100 / 500 / 1000 samples
- **Live tables** — raw latency rows updating in real time

## Project structure

```
docker/
├── docker-compose.yml
├── cryptofeed/
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── src/
│       ├── coinbase.py            # Coinbase exchange adapter
│       ├── cryptofeed_tools.py    # Shared symbols config
│       ├── metrics_writer.py      # Writes latency to QuestDB
│       ├── writers/
│       │   ├── registry.py        # List of active writers
│       │   ├── questdb_writer.py  # QuestDB ILP writer
│       │   └── clickhouse_writer.py # ClickHouse native writer
│       └── script/
│           ├── main_runner.py     # Launches trades + orderbooks
│           ├── cryptofeed_1_trades.py
│           └── cryptofeed_2_orderbooks.py
├── clickhouse/
│   └── init/
│       ├── trades.sql             # Auto-runs on first start
│       └── orderbooks.sql
└── deephaven/
    ├── Dockerfile
    ├── config/deephaven.prop
    └── notebooks/
        ├── qdb.py                 # QuestDB connection helpers
        ├── qdb_backend.py         # Live table backend
        └── latency_dashboard.py   # Latency comparison dashboard
```

## How latency is measured

Each write callback fires concurrently for both databases using `asyncio.gather`:

```python
async def on_trade(data, receipt_time):
    latencies = await asyncio.gather(*[w.write_trade(data) for w in TRADE_WRITERS])
    for writer, latency in zip(TRADE_WRITERS, latencies):
        METRICS.record(writer.__class__.__name__, "trade", latency * 1000)
```

`time.perf_counter()` is taken immediately before and after each database call. Both writes start at the same moment so neither has a timing advantage.

## Author

🦖 Moy Patel

## License

MIT — see [LICENSE](LICENSE)

## Adding a new database

1. Create `docker/cryptofeed/src/writers/mydb_writer.py` with `write_trade` and `write_orderbook` methods matching the existing interface
2. Add the writer to `docker/cryptofeed/src/writers/registry.py`
3. Add the service to `docker/docker-compose.yml` on `cryptofeed_net`

## Updating dependencies

```bash
cd docker/cryptofeed
uv lock
cd ..
docker compose up --build
```
