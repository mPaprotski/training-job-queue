"""Enabled runtime pooling must never silently fall back to raw connections."""

import unittest
from unittest.mock import patch

from training_job_queue.db import RuntimePoolNotStarted, transaction
from training_job_queue.settings import Settings


class PoolLifecycleTests(unittest.TestCase):
    def test_enabled_but_unopened_pool_fails_without_connecting(self):
        settings = Settings('postgresql://missing-runtime-pool', 15, 3, 5, 1)
        with patch('training_job_queue.db.connect') as raw:
            with self.assertRaises(RuntimePoolNotStarted):
                with transaction(settings):
                    self.fail('Missing pool must not create a raw transaction')
            raw.assert_not_called()
