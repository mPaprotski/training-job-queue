"""Runtime privileges are sufficient for job processing but not administration."""

from dataclasses import replace
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo

from training_job_queue import jobs, processing
from training_job_queue.db import connect
from support import DatabaseTestCase


class RuntimePermissionTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute('TRUNCATE job_effects, job_attempts, jobs')

    def test_runtime_identity_has_no_administrative_attributes(self):
        with connect(self.runtime_settings) as connection:
            row = connection.execute('''SELECT current_user AS name, rolsuper, rolcreatedb,
                rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = current_user''').fetchone()
        self.assertEqual(row.pop('name'), 'queue_runtime')
        self.assertFalse(any(row.values()))

    def test_runtime_cannot_change_schema_roles_history_or_effects(self):
        statements = [
            'CREATE TABLE forbidden_table (id int)',
            'DROP TABLE job_effects',
            'ALTER TABLE jobs ADD COLUMN forbidden_column int',
            'TRUNCATE job_effects',
            'DELETE FROM jobs',
            "UPDATE job_effects SET effect = '{}'::jsonb",
            'SELECT * FROM schema_migrations',
            'CREATE TEMP TABLE forbidden_temp (id int)',
            'SET ROLE queue_tools',
            f'CREATE ROLE forbidden_{uuid4().hex}',
        ]
        for statement in statements:
            with self.subTest(statement=statement), connect(self.runtime_settings) as connection:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    with connection.transaction():
                        connection.execute(statement)

    def test_runtime_cannot_create_database(self):
        name = 'forbidden_' + uuid4().hex
        with connect(self.runtime_settings, autocommit=True) as connection:
            try:
                connection.execute(f'CREATE DATABASE {name}')
            except psycopg.errors.InsufficientPrivilege:
                return
            connection.execute(f'DROP DATABASE {name}')
            self.fail('Runtime unexpectedly had CREATEDB')

    def test_runtime_success_idempotency_and_cancellation(self):
        job, created = jobs.create_job(self.runtime_settings, 'success', {})
        self.assertTrue(created)
        self.assertEqual(jobs.create_job(self.runtime_settings, 'success', {})[0]['id'], job['id'])
        processing.claim_job(self.runtime_settings, 'runtime-worker')
        self.assertTrue(processing.renew_lease(self.runtime_settings, job['id'], 'runtime-worker', 1))
        self.assertTrue(processing.complete_job(self.runtime_settings, job['id'], 'runtime-worker', 1))
        other, _ = jobs.create_job(self.runtime_settings, 'cancel', {})
        processing.claim_job(self.runtime_settings, 'runtime-worker')
        self.assertEqual(jobs.cancel_job(self.runtime_settings, other['id'])['state'], 'cancelled')
        self.assertEqual(len(jobs.list_jobs(self.runtime_settings, 10, None)['items']), 2)

    def test_runtime_recovery_and_permanent_failure(self):
        job, _ = jobs.create_job(self.runtime_settings, 'retry', {})
        processing.claim_job(self.runtime_settings, 'old-worker')
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second'")
        self.assertEqual(processing.recover_expired(self.runtime_settings), 1)
        processing.claim_job(self.runtime_settings, 'new-worker')
        self.assertTrue(processing.fail_job(self.runtime_settings, job['id'], 'new-worker', 2,
                                            'permanent', permanent=True))
        self.assertEqual(jobs.get_job(self.runtime_settings, job['id'])['state'], 'failed')

    def test_migrator_and_tools_are_separate_non_superusers(self):
        with connect(self.settings) as connection:
            rows = connection.execute('''SELECT rolname, rolsuper, rolcreatedb, rolcreaterole
                FROM pg_roles WHERE rolname IN ('queue_tools', 'queue_migrator') ORDER BY rolname''').fetchall()
        self.assertEqual(rows, [
            {'rolname': 'queue_migrator', 'rolsuper': False, 'rolcreatedb': False, 'rolcreaterole': False},
            {'rolname': 'queue_tools', 'rolsuper': False, 'rolcreatedb': True, 'rolcreaterole': False},
        ])
        migrator = replace(self.settings, database_url=make_conninfo(
            self.settings.database_url, dbname='queue', user='queue_migrator', password='queue_migrator'))
        with connect(migrator) as connection:
            self.assertEqual(connection.execute('''SELECT tableowner FROM pg_tables
                WHERE schemaname='public' AND tablename='jobs' ''').fetchone()['tableowner'], 'queue_migrator')
