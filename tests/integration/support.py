"""Isolated PostgreSQL database fixture shared by integration suites."""

from dataclasses import replace
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

from psycopg import sql
from psycopg.conninfo import make_conninfo

from training_job_queue.db import connect
from training_job_queue.migrate import migrate
from training_job_queue.permissions import grant_runtime_permissions, runtime_settings
from training_job_queue.settings import Settings


class DatabaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_settings = Settings.from_env()
        cls.database_name = "schema_test_" + uuid4().hex
        with connect(cls.admin_settings, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database_name)))
        cls.addClassCleanup(cls.drop_database)
        cls.settings = replace(
            cls.admin_settings,
            database_url=make_conninfo(cls.admin_settings.database_url, dbname=cls.database_name),
            db_pool_enabled=False,  # Direct protocol fixtures explicitly use raw transactions.
        )
        with patch.dict(os.environ, {"DATABASE_URL": cls.settings.database_url}):
            migrate(Path("migrations"))
            migrate(Path("migrations"))
        grant_runtime_permissions(cls.settings)
        cls.runtime_settings = runtime_settings(cls.settings)

    @classmethod
    def drop_database(cls):
        with connect(cls.admin_settings, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(cls.database_name)))
