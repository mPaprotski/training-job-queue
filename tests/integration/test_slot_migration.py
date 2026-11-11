"""Upgrade existing queued/running jobs without dropping data or reservations."""

from dataclasses import replace
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from training_job_queue.db import connect
from training_job_queue.migrate import migrate
from support import DatabaseTestCase


class SlotMigrationTests(DatabaseTestCase):
    def test_upgrade_backfills_only_live_jobs_and_enforces_slot_constraints(self):
        name = 'slot_upgrade_' + uuid4().hex
        with connect(self.admin_settings, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        settings = replace(self.settings, database_url=make_conninfo(self.settings.database_url, dbname=name))
        try:
            with TemporaryDirectory() as directory, patch.dict(os.environ, {'DATABASE_URL': settings.database_url}):
                old = Path(directory)
                for filename in ('0001_job_schema.sql', '0002_outstanding_index.sql'):
                    (old / filename).write_bytes((Path('migrations') / filename).read_bytes())
                migrate(old)
                with connect(settings) as connection:
                    queued = connection.execute("INSERT INTO jobs (idempotency_key,payload) VALUES ('queued','{}') RETURNING id").fetchone()['id']
                    running = connection.execute("INSERT INTO jobs (idempotency_key,payload,state,attempt_no,worker_id,started_at,lease_expires_at) VALUES ('running','{}','running',1,'owner',clock_timestamp(),clock_timestamp()+interval '30 seconds') RETURNING id").fetchone()['id']
                    connection.execute("INSERT INTO job_attempts (job_id,attempt_no,worker_id) VALUES (%s,1,'owner')", (running,))
                    cancelled = connection.execute("INSERT INTO jobs (idempotency_key,payload,state,finished_at) VALUES ('cancelled','{}','cancelled',clock_timestamp()) RETURNING id").fetchone()['id']
                migrate(Path('migrations'))
                migrate(Path('migrations'))
                with connect(settings) as connection:
                    rows = {r['id']: r for r in connection.execute('SELECT id,state,admission_slot FROM jobs').fetchall()}
                    self.assertEqual(set(rows), {queued, running, cancelled})
                    self.assertEqual(rows[queued]['state'], 'queued')
                    self.assertEqual(rows[running]['state'], 'running')
                    self.assertEqual({rows[queued]['admission_slot'], rows[running]['admission_slot']}, {1, 2})
                    self.assertIsNone(rows[cancelled]['admission_slot'])
                    for slot, error in [(None, psycopg.errors.CheckViolation),
                                        (0, psycopg.errors.CheckViolation),
                                        (1, psycopg.errors.UniqueViolation)]:
                        with self.subTest(slot=slot), self.assertRaises(error):
                            with connection.transaction():
                                connection.execute('INSERT INTO jobs (idempotency_key,payload,admission_slot) VALUES (%s,\'{}\',%s)', (uuid4().hex, slot))
        finally:
            with connect(self.admin_settings, autocommit=True) as admin:
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
