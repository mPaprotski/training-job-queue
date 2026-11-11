"""Real bounded pool: reuse, rollback, exhaustion, broken sockets and shutdown."""

from dataclasses import replace
import time

import psycopg
from psycopg_pool import PoolTimeout

from training_job_queue.db import connect, connection_pool, transaction
from support import DatabaseTestCase


class PoolTests(DatabaseTestCase):
    def setUp(self):
        self.pooled_settings = replace(self.runtime_settings, db_pool_enabled=True)

    def test_pool_reuses_connection_and_closes(self):
        settings = self.pooled_settings
        with connection_pool(settings, size=1) as pool:
            with transaction(settings) as connection:
                first = connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
            with transaction(settings) as connection:
                second = connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
            self.assertEqual(first, second)
        self.assertTrue(pool.closed)

    def test_error_rolls_back_before_connection_reuse(self):
        settings = self.pooled_settings
        with connection_pool(settings, size=1):
            with self.assertRaises(psycopg.errors.DivisionByZero):
                with transaction(settings) as connection:
                    connection.execute("INSERT INTO jobs (idempotency_key,payload,admission_slot) VALUES ('rolled-back','{}',1)")
                    connection.execute('SELECT 1/0')
            with transaction(settings) as connection:
                self.assertEqual(connection.execute("SELECT count(*) AS n FROM jobs WHERE idempotency_key='rolled-back'").fetchone()['n'], 0)
                self.assertEqual(connection.execute('SELECT 1 AS n').fetchone()['n'], 1)

    def test_exhaustion_wait_is_bounded_and_connection_remains_usable(self):
        settings = replace(self.pooled_settings, db_pool_timeout=0.05)
        with connection_pool(settings, size=1):
            with transaction(settings):
                started = time.monotonic()
                with self.assertRaises(PoolTimeout):
                    with transaction(settings):
                        self.fail('Pool exceeded its size limit')
                self.assertLess(time.monotonic() - started, 0.5)
            with transaction(settings) as connection:
                self.assertEqual(connection.execute('SELECT 1 AS n').fetchone()['n'], 1)

    def test_dead_idle_connection_is_replaced(self):
        settings = self.pooled_settings
        with connection_pool(settings, size=1):
            with transaction(settings) as connection:
                victim = connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
            with connect(settings) as killer:
                killer.execute('SELECT pg_terminate_backend(%s)', (victim,))
            with transaction(settings) as connection:
                self.assertNotEqual(connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid'], victim)

    def test_disabled_pool_uses_distinct_connections(self):
        settings = replace(self.runtime_settings, db_pool_enabled=False)
        with connection_pool(settings) as pool:
            self.assertIsNone(pool)
            with transaction(settings) as connection:
                first = connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
            with transaction(settings) as connection:
                second = connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
            self.assertNotEqual(first, second)
