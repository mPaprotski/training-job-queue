"""Raw owner connections and bounded, process-local runtime connection pools."""

from contextlib import contextmanager
from threading import Lock

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from training_job_queue.settings import Settings

OPTIONS = (r"-c default_transaction_isolation=read\ committed -c statement_timeout=5000 "
           "-c lock_timeout=3000 -c idle_in_transaction_session_timeout=5000")
_pools = {}
_pool_lock = Lock()


class RuntimePoolNotStarted(RuntimeError):
    """Pooling was enabled but its process lifecycle was not initialized."""


def connect(settings: Settings, *, autocommit: bool = False) -> psycopg.Connection:
    """Unpooled connection for migrations, fixtures and independent snapshots."""
    return psycopg.connect(settings.database_url, connect_timeout=3, autocommit=autocommit,
                           row_factory=dict_row, options=OPTIONS)


@contextmanager
def connection_pool(settings: Settings, *, size: int | None = None):
    """API owns up to four connections by default; each serial worker owns one."""
    if not settings.db_pool_enabled:
        yield None
        return
    capacity = size if size is not None else settings.db_pool_size
    pool = ConnectionPool(settings.database_url, min_size=capacity, max_size=capacity,
                          timeout=settings.db_pool_timeout, max_waiting=40, num_workers=1,
                          reconnect_timeout=5, open=False, name="runtime",
                          check=ConnectionPool.check_connection,
                          kwargs={"connect_timeout": 3, "row_factory": dict_row, "options": OPTIONS})
    try:
        pool.open(wait=True, timeout=5)
        with _pool_lock:
            if settings.database_url in _pools:
                raise RuntimeError("Runtime pool already open for this database")
            _pools[settings.database_url] = pool
        try:
            yield pool
        finally:
            with _pool_lock:
                del _pools[settings.database_url]
    finally:
        pool.close()


@contextmanager
def transaction(settings: Settings):
    with _pool_lock:
        pool = _pools.get(settings.database_url) if settings.db_pool_enabled else None
    if not settings.db_pool_enabled:
        with connect(settings) as connection:
            yield connection
    else:
        if pool is None:
            raise RuntimePoolNotStarted("Runtime pool is enabled but not started for this database")
        # psycopg_pool commits/rolls back before returning a connection to the
        # pool and discards broken connections. Both size and waiting are bounded.
        with pool.connection(timeout=settings.db_pool_timeout) as connection:
            yield connection


def ping(settings: Settings) -> None:
    with transaction(settings) as connection:
        connection.execute("SELECT 1")
