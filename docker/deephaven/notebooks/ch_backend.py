"""
ClickHouse Backend for Deephaven TableDataService

Mirrors qdb_backend.py but targets ClickHouse via clickhouse-connect (HTTP/Arrow).

Features:
- Singleton pattern: one backend instance per table
- Live streaming with system.parts-based size polling
- LIMIT/OFFSET paging (ClickHouse supports it natively)
- Arrow-native column fetching via query_arrow()
- Thread-safe with proper cleanup

Usage:
    from ch_backend import create_live_table

    trades = create_live_table('trades')
    orderbooks = create_live_table('orderbooks')

    from ch_backend import stop_monitoring
    stop_monitoring()
"""

import re
import threading
import time
from abc import ABC

import pyarrow as pa
from deephaven.experimental.table_data_service import (
    TableDataService,
    TableDataServiceBackend,
    TableKey,
    TableLocationKey,
)
from deephaven.liveness_scope import LivenessScope

import ch


# =============================================================================
#  Configuration
# =============================================================================

DEFAULT_ORDER_BY_COL = "timestamp"
DEFAULT_PAGE_SIZE = 64_000
POLL_INTERVAL_SEC = 0.05


# =============================================================================
#  Global Registry (Singleton per table)
# =============================================================================

if "_CH_GLOBAL_BACKENDS" not in globals():
    _CH_GLOBAL_BACKENDS = {}
    _CH_GLOBAL_LOCK = threading.Lock()


# =============================================================================
#  TableKey & TableLocationKey
# =============================================================================


class ClickHouseTableKey(TableKey):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __hash__(self):
        return hash(self.table_name)

    def __eq__(self, other):
        return isinstance(other, ClickHouseTableKey) and self.table_name == other.table_name

    def __repr__(self):
        return f"ClickHouseTableKey({self.table_name!r})"


class ClickHouseTableLocationKey(TableLocationKey):
    def __init__(self, location_id: str = "main"):
        self.location_id = location_id

    def __hash__(self):
        return hash(self.location_id)

    def __eq__(self, other):
        return isinstance(other, ClickHouseTableLocationKey) and self.location_id == other.location_id

    def __repr__(self):
        return f"ClickHouseTableLocationKey({self.location_id!r})"


SINGLE_LOCATION_KEY = ClickHouseTableLocationKey("main")


# =============================================================================
#  ClickHouse type -> Arrow type mapping
# =============================================================================

_SIMPLE_TYPES = {
    "String": pa.string(),
    "Float32": pa.float32(),
    "Float64": pa.float64(),
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
    "Boolean": pa.bool_(),
    "DateTime": pa.timestamp("s", tz="UTC"),
}

_DATETIME64_UNITS = {9: "ns", 8: "ns", 7: "ns", 6: "us", 5: "us", 4: "us", 3: "ms", 2: "ms", 1: "ms", 0: "s"}


def _ch_type_to_arrow(ch_type: str) -> pa.DataType:
    if ch_type in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[ch_type]

    # Nullable(T) / LowCardinality(T) -> unwrap
    for wrapper in ("Nullable(", "LowCardinality("):
        if ch_type.startswith(wrapper):
            return _ch_type_to_arrow(ch_type[len(wrapper):-1])

    # DateTime64(precision) or DateTime64(precision, 'tz')
    m = re.match(r"DateTime64\((\d+)", ch_type)
    if m:
        precision = int(m.group(1))
        unit = _DATETIME64_UNITS.get(precision, "ns")
        return pa.timestamp(unit, tz="UTC")

    # FixedString(N)
    if ch_type.startswith("FixedString("):
        return pa.string()

    return pa.string()  # safe fallback


# =============================================================================
#  Backend Implementation
# =============================================================================


class ClickHouseBackend(TableDataServiceBackend, ABC):
    """
    ClickHouse-backed TableDataServiceBackend with live polling.

    - Schema from system.columns
    - Size from system.parts (fast, metadata-only)
    - Column data via query_arrow() with LIMIT/OFFSET
    """

    def __init__(self, order_by_col=DEFAULT_ORDER_BY_COL, table_name=None, verbose=False):
        super().__init__()
        self._order_by_col = order_by_col
        self._table_name = table_name
        self._verbose = verbose
        self._active_threads = {}
        self._lock = threading.Lock()
        self._schema_cache = {}
        print(f"[CH Backend] Created (id={id(self)}, table='{table_name}')")

    def __del__(self):
        self.cleanup()

    # -------------------------------------------------------------------------
    #  Lifecycle
    # -------------------------------------------------------------------------

    def cleanup(self):
        threads_to_stop = []
        with self._lock:
            for table_name, (stop_event, thread) in list(self._active_threads.items()):
                stop_event.set()
                threads_to_stop.append((table_name, thread))
            self._active_threads.clear()

        for table_name, thread in threads_to_stop:
            thread.join(timeout=2.0)
            if thread.is_alive():
                print(f"[CH Backend] Warning: thread for '{table_name}' didn't stop in 2s")

        if self._table_name:
            with _CH_GLOBAL_LOCK:
                _CH_GLOBAL_BACKENDS.pop(self._table_name, None)

    @classmethod
    def get_or_create(cls, table_name, order_by_col=DEFAULT_ORDER_BY_COL, verbose=False):
        with _CH_GLOBAL_LOCK:
            if table_name in _CH_GLOBAL_BACKENDS:
                existing = _CH_GLOBAL_BACKENDS[table_name]
                print(f"[CH Backend] Reusing existing backend for '{table_name}' (id={id(existing)})")
                return existing
            backend = cls(order_by_col=order_by_col, table_name=table_name, verbose=verbose)
            _CH_GLOBAL_BACKENDS[table_name] = backend
            return backend

    @classmethod
    def cleanup_all(cls):
        with _CH_GLOBAL_LOCK:
            backends = list(_CH_GLOBAL_BACKENDS.values())
            _CH_GLOBAL_BACKENDS.clear()

        for backend in backends:
            threads_to_stop = []
            with backend._lock:
                for table_name, (stop_event, thread) in list(backend._active_threads.items()):
                    stop_event.set()
                    threads_to_stop.append((table_name, thread))
                backend._active_threads.clear()
            for table_name, thread in threads_to_stop:
                thread.join(timeout=2.0)

    # -------------------------------------------------------------------------
    #  Schema
    # -------------------------------------------------------------------------

    def _get_schema(self, table_name: str) -> pa.Schema:
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        client = ch.get_client()
        result = client.query(
            "SELECT name, type FROM system.columns"
            " WHERE database = 'default' AND table = {table:String}"
            " ORDER BY position",
            parameters={"table": table_name},
        )
        if not result.result_rows:
            raise RuntimeError(f"Table {table_name!r} not found or has no columns")

        schema = pa.schema([pa.field(name, _ch_type_to_arrow(dtype)) for name, dtype in result.result_rows])
        self._schema_cache[table_name] = schema
        return schema

    def table_schema(self, table_key, schema_cb, failure_cb):
        try:
            schema = self._get_schema(table_key.table_name)
            schema_cb(schema, None)
        except Exception as e:
            failure_cb(e)

    # -------------------------------------------------------------------------
    #  Locations: single location, no partitioning
    # -------------------------------------------------------------------------

    def table_locations(self, table_key, location_cb, success_cb, failure_cb):
        try:
            location_cb(SINGLE_LOCATION_KEY, None)
            success_cb()
        except Exception as e:
            failure_cb(e)

    def subscribe_to_table_locations(self, table_key, location_cb, success_cb, failure_cb):
        try:
            location_cb(SINGLE_LOCATION_KEY, None)
            success_cb()
            return lambda: None
        except Exception as e:
            failure_cb(e)
            return lambda: None

    # -------------------------------------------------------------------------
    #  Size tracking: system.parts polling
    # -------------------------------------------------------------------------

    def _get_table_size(self, table_name: str) -> int:
        client = ch.get_client()
        result = client.query(
            "SELECT sum(rows) FROM system.parts"
            " WHERE database = 'default' AND table = {table:String} AND active = 1",
            parameters={"table": table_name},
        )
        count = result.result_rows[0][0] if result.result_rows else 0
        return int(count or 0)

    def _poll_loop(self, table_key, size_cb, success_cb, failure_cb, stop_event):
        try:
            table_name = table_key.table_name
            last_size = self._get_table_size(table_name)
            size_cb(last_size)
            success_cb()
            print(f"[CH Backend:{table_name}] Polling started, initial size: {last_size:,}")

            while not stop_event.is_set():
                time.sleep(POLL_INTERVAL_SEC)
                current_size = self._get_table_size(table_name)
                if current_size > last_size:
                    if self._verbose:
                        print(f"[CH Backend:{table_name}] Size: {last_size:,} -> {current_size:,}")
                    last_size = current_size
                    size_cb(current_size)
        except Exception as e:
            failure_cb(e)

    def table_location_size(self, table_key, table_location_key, size_cb, failure_cb):
        try:
            size_cb(self._get_table_size(table_key.table_name))
        except Exception as e:
            failure_cb(e)

    def subscribe_to_table_location_size(self, table_key, table_location_key, size_cb, success_cb, failure_cb):
        table_name = table_key.table_name

        old_thread = None
        with self._lock:
            if table_name in self._active_threads:
                print(f"[CH Backend] Thread already exists for '{table_name}', stopping old one...")
                old_stop, old_thread = self._active_threads[table_name]
                old_stop.set()

        if old_thread:
            old_thread.join(timeout=1.0)

        stop_event = threading.Event()
        t = threading.Thread(
            target=self._poll_loop,
            args=(table_key, size_cb, success_cb, failure_cb, stop_event),
            daemon=True,
            name=f"CH-Monitor-{table_name}-{id(self)}",
        )
        t.start()

        with self._lock:
            self._active_threads[table_name] = (stop_event, t)

        print(f"[CH Backend] Started monitoring thread for '{table_name}'")

        def unsubscribe():
            with self._lock:
                stop_event.set()
                self._active_threads.pop(table_name, None)

        return unsubscribe

    # -------------------------------------------------------------------------
    #  Column data: LIMIT/OFFSET + query_arrow()
    # -------------------------------------------------------------------------

    def column_values(self, table_key, table_location_key, col, offset, min_rows, max_rows, values_cb, failure_cb):
        try:
            table_name = table_key.table_name
            if self._verbose:
                print(f"[CH Backend:{table_name}] column_values: col={col}, offset={offset}, max_rows={max_rows}")

            client = ch.get_client()
            result = client.query_arrow(
                f"SELECT {col} FROM {table_name}"
                f" ORDER BY {self._order_by_col}"
                f" LIMIT {max_rows} OFFSET {offset}"
            )
            values_cb(result)
        except Exception as e:
            failure_cb(e)


# =============================================================================
#  Convenience Functions
# =============================================================================


def create_live_table(
    table_name: str,
    order_by_col: str = DEFAULT_ORDER_BY_COL,
    page_size: int = DEFAULT_PAGE_SIZE,
    refreshing: bool = True,
    verbose: bool = False,
    use_liveness_scope: bool = True,
):
    """
    Create a live Deephaven table backed by ClickHouse.

    Args:
        table_name: Name of the ClickHouse table (in the 'default' database)
        order_by_col: Column to order by (default: "timestamp")
        page_size: Page size for Deephaven (default: 64,000)
        refreshing: Whether to enable live updates (default: True)
        verbose: Enable verbose size-change logging (default: False)
        use_liveness_scope: Whether to wrap table creation in LivenessScope (default: True)

    Returns:
        Deephaven table object

    Example:
        trades = create_live_table('trades')
        orderbooks = create_live_table('orderbooks', verbose=True)
    """
    backend = ClickHouseBackend.get_or_create(
        table_name=table_name, order_by_col=order_by_col, verbose=verbose
    )
    tds = TableDataService(backend=backend, page_size=page_size)
    table_key = ClickHouseTableKey(table_name)

    if use_liveness_scope:
        scope = LivenessScope()
        with scope.open():
            t = tds.make_table(table_key, refreshing=refreshing)
        return t
    else:
        return tds.make_table(table_key, refreshing=refreshing)


def stop_monitoring():
    """
    Stop all ClickHouse monitoring threads.

    To restart, call create_live_table() again.
    """
    ClickHouseBackend.cleanup_all()
    print("All CH monitoring stopped.")
    print("Run create_live_table() again to restart monitoring.")
