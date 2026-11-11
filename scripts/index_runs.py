"""Build a single index of preserved chaos/load runs; never edit their raw files."""

from collections import Counter
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
runs = []
seen = set()
for directory in ('results', 'verification'):
    for path in sorted((root / directory).rglob('summary.json')):
        summary = json.loads(path.read_text())
        identifier = summary.get('run_id', '')
        if not identifier.startswith(('chaos-', 'load-')):
            continue
        if identifier in seen:
            raise RuntimeError(f'Duplicate run ID: {identifier}')
        seen.add(identifier)
        metadata_path = path.with_name('metadata.json')
        metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
        runs.append({'run_id': identifier, 'kind': identifier.split('-')[0],
                     'passed': summary['passed'], 'expected_jobs': summary.get('expected_jobs'),
                     'actual_jobs': summary.get('actual_jobs'), 'path': str(path.relative_to(root)),
                     'source_revision': metadata.get('source_revision', 'not recorded'),
                     'parameters': metadata.get('parameters', {}),
                     'source_files_sha256': metadata.get('source_files_sha256', {})})
runs.sort(key=lambda row: row['run_id'].split('-', 1)[1])
counts = Counter(row['kind'] for row in runs)
full = [r for r in runs if r['kind'] == 'chaos' and r['expected_jobs'] == 10000]
(root / 'RUNS.json').write_text(json.dumps({'counts': dict(counts), 'full_chaos_runs': len(full), 'runs': runs}, indent=2) + '\n')
lines = ['# All preserved chaos and load runs', '',
         'Generated with `python scripts/index_runs.py`. Counts include deliberate',
         'negative probes, calibration, packaging checks and diagnostic repeats.',
         'Historical versions/configurations differ; their successes are not all',
         'evidence for the current source. Raw files are never rewritten.', '',
         f"Total: **{counts['chaos']} chaos runs** ({len(full)} full 10,000-job runs), **{counts['load']} load runs**.", '',
         '| Run | Type | Result | Expected jobs | Source revision |',
         '| --- | --- | --- | ---: | --- |']
for row in runs:
    revision = row['source_revision'].replace('|', '\\|')
    lines.append(f"| [{row['run_id']}]({row['path']}) | {row['kind']} | {'PASS' if row['passed'] else 'FAIL'} | {row['expected_jobs']} | {revision} |")
lines += ['', 'Exact parameters and source hashes: [RUNS.json](RUNS.json).',
          'Current verdict: [VERDICT.md](VERDICT.md). Current raw capacity evidence:',
          '[verification/review-fixes-006/capacity-poll.json](verification/review-fixes-006/capacity-poll.json).']
(root / 'RUNS.md').write_text('\n'.join(lines) + '\n')
print(f'Indexed {len(runs)} preserved runs.')
