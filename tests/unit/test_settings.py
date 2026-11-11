"""Reject invalid admission/pool budgets before serving requests."""

import unittest
from unittest.mock import patch

from training_job_queue.settings import Settings


class SettingsTests(unittest.TestCase):
    def test_default_budgets(self):
        with patch.dict('os.environ', {'DATABASE_URL': 'postgresql://example'}, clear=True):
            settings = Settings.from_env()
        self.assertEqual(settings.max_outstanding_jobs, 1000)
        self.assertTrue(settings.db_pool_enabled)
        self.assertEqual(settings.db_pool_size, 4)
        self.assertEqual(settings.db_pool_timeout, 1)

    def test_invalid_budgets_are_rejected(self):
        for name, value in [('MAX_OUTSTANDING_JOBS', '0'), ('MAX_OUTSTANDING_JOBS', '1000001'),
                            ('DB_POOL_ENABLED', 'false'), ('DB_POOL_SIZE', '0'), ('DB_POOL_SIZE', '17'),
                            ('DB_POOL_TIMEOUT', 'nan'), ('DB_POOL_TIMEOUT', '0'), ('DB_POOL_TIMEOUT', '6')]:
            with self.subTest(name=name, value=value), patch.dict(
                    'os.environ', {'DATABASE_URL': 'postgresql://example', name: value}, clear=True):
                with self.assertRaises(ValueError):
                    Settings.from_env()
