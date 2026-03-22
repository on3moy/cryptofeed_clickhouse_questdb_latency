import threading
import time
from questdb.ingress import Sender, Protocol, TimestampNanos


class MetricsWriter:
    """Writes write-latency measurements to QuestDB latency_metrics table."""

    def __init__(self):
        self._local = threading.local()

    def _get_sender(self):
        if not hasattr(self._local, "sender"):
            s = Sender(Protocol.Tcp, "questdb", 9009)
            s.__enter__()
            self._local.sender = s
        return self._local.sender

    def record(self, writer: str, operation: str, latency_ms: float):
        ts = TimestampNanos(int(time.time() * 1_000_000_000))
        sender = self._get_sender()
        sender.row(
            "latency_metrics",
            symbols={"writer": writer, "operation": operation},
            columns={"latency_ms": latency_ms},
            at=ts,
        )
        sender.flush()


METRICS = MetricsWriter()
