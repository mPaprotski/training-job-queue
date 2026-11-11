"""Compose-project selection and missing-container reporting."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from harness.docker_stats import main


class DockerStatsTests(unittest.TestCase):
    def sample(self, listing):
        with TemporaryDirectory() as directory:
            output = Path(directory) / 'samples.jsonl'
            with patch('sys.argv', ['docker_stats', '--project', 'custom-review',
                                    '--seconds', '0.1', '--output', str(output)]), \
                 patch('harness.docker_stats.time.monotonic', side_effect=[0, 0, 0.01, 1]), \
                 patch('harness.docker_stats.signal.signal'), \
                 patch('harness.docker_stats.Event') as event, \
                 patch('harness.docker_stats.subprocess.run') as run:
                event.return_value.is_set.return_value = False
                run.side_effect = [listing, SimpleNamespace(returncode=0, stdout='{"ID":"db123"}\n', stderr='')]
                main()
            return json.loads(output.read_text()), run.call_args_list

    def test_custom_project_filter_selects_only_database_and_tools(self):
        sample, calls = self.sample(SimpleNamespace(returncode=0, stdout='db123 db\napi456 api\n', stderr=''))
        self.assertIn('label=com.docker.compose.project=custom-review', calls[0].args[0])
        self.assertEqual(calls[1].args[0][-1], 'db123')
        self.assertNotIn('api456', calls[1].args[0])
        self.assertEqual(sample['project'], 'custom-review')
        self.assertEqual(sample['containers'], [{'ID': 'db123'}])
        self.assertEqual(sample['error'], '')

    def test_no_matching_containers_is_explicit(self):
        sample, calls = self.sample(SimpleNamespace(returncode=0, stdout='', stderr=''))
        self.assertIn('No matching', sample['error'])
        self.assertEqual(sample['containers'], [])
        self.assertEqual(len(calls), 1)

    def test_docker_ps_failure_is_explicit(self):
        sample, calls = self.sample(SimpleNamespace(returncode=1, stdout='', stderr='permission denied'))
        self.assertEqual(sample['error'], 'permission denied')
        self.assertEqual(len(calls), 1)
