_POSTGRES_STRING = "postgresql://admin:quest@questdb:8812/qdb"

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(_POSTGRES_STRING, pool_pre_ping=True)
    return _engine
