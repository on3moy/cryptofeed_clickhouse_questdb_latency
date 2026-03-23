import threading

_local = threading.local()


def get_client():
    if not hasattr(_local, "client"):
        import clickhouse_connect
        _local.client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="default",
            password="",
        )
    return _local.client
