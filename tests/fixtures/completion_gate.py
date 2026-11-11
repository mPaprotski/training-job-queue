"""Child-process crash probe; pauses around real SQL and real commit."""

from contextlib import contextmanager
import sys
from unittest.mock import patch
from uuid import UUID

from training_job_queue import processing
from training_job_queue.db import connect
from training_job_queue.settings import Settings

phase, job_id, worker_id = sys.argv[1:]


def gate():
    print("GATE", flush=True)
    if sys.stdin.readline() != "continue\n":
        raise RuntimeError("Gate abandoned")


class ConnectionProxy:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, parameters=None):
        effect = "INSERT INTO job_effects" in query
        if effect and phase == "before_effect":
            gate()
        cursor = self.connection.execute(query, parameters)
        if effect and phase == "after_effect":
            gate()
        return cursor


@contextmanager
def gated_connect(settings):
    with connect(settings) as connection:
        yield ConnectionProxy(connection)
    if phase == "after_commit":
        gate()


with patch.object(processing, "connect", gated_connect):
    processing.complete_job(Settings.from_env(), UUID(job_id), worker_id, 1)
