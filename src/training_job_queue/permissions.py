"""Grant runtime DML independently from schema ownership and test administration."""

from dataclasses import replace
import os

from psycopg import sql
from psycopg.conninfo import make_conninfo

from training_job_queue.db import connect


RUNTIME_ROLE = 'queue_runtime'


def runtime_settings(owner_settings):
    """Tools keep owner credentials locally; API/worker children use runtime only."""
    user = os.environ.get('RUNTIME_DATABASE_USER')
    password = os.environ.get('RUNTIME_DATABASE_PASSWORD')
    if user != RUNTIME_ROLE or not password:
        raise ValueError('Tools require RUNTIME_DATABASE_USER=queue_runtime and RUNTIME_DATABASE_PASSWORD')
    return replace(owner_settings, database_url=make_conninfo(
        owner_settings.database_url, user=user, password=password))


def grant_runtime_permissions(owner_settings):
    """Run as database/table owner after migrations, before starting runtime."""
    with connect(owner_settings) as connection:
        database = connection.execute('SELECT current_database() AS name').fetchone()['name']
        connection.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(database)))
        connection.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO queue_runtime').format(sql.Identifier(database)))
        connection.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
        connection.execute('GRANT USAGE ON SCHEMA public TO queue_runtime')
        connection.execute('REVOKE ALL ON ALL TABLES IN SCHEMA public FROM queue_runtime')
        connection.execute('GRANT SELECT, INSERT, UPDATE ON jobs, job_attempts TO queue_runtime')
        connection.execute('GRANT SELECT, INSERT ON job_effects TO queue_runtime')


if __name__ == '__main__':
    from training_job_queue.settings import Settings
    grant_runtime_permissions(Settings.from_env())
