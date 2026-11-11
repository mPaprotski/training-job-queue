"""Recompute key claims from saved requests and exported database tables."""

from collections import Counter
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path

folder = Path(__file__).resolve().parent
checks = []
canonical = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'))


def percentile(values, percent):
    return sorted(values)[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else None


for run in sorted((folder / 'harness').iterdir()):
    if not (run / 'summary.json').exists():
        continue
    load = lambda name: [json.loads(line) for line in (run / name).read_text().splitlines()]
    summary = json.loads((run / 'summary.json').read_text())
    metadata = json.loads((run / 'metadata.json').read_text())
    manifest = json.loads((run / 'manifest.json').read_text())
    jobs, attempts, effects = load('jobs.jsonl'), load('job_attempts.jsonl'), load('job_effects.jsonl')
    assert len(jobs) == len({j['id'] for j in jobs}) == len({j['idempotency_key'] for j in jobs})
    by_key = {j['idempotency_key']: j for j in jobs}
    assert all(canonical(j['payload']) == canonical(manifest[j['idempotency_key']]) for j in jobs)
    assert len(effects) == len({e['job_id'] for e in effects})
    by_job = {e['job_id']: e for e in effects}
    by_attempt = {(a['job_id'], a['attempt_no']): a for a in attempts}
    assert len(by_attempt) == len(attempts)
    assert not any(a['outcome'] == 'running' for a in attempts)
    for job in jobs:
        assert job['state'] in ('succeeded', 'failed', 'cancelled')
        effect = by_job.get(job['id'])
        assert (effect is not None) == (job['state'] == 'succeeded')
        if effect:
            producing = by_attempt[job['id'], effect['attempt_no']]
            assert effect['attempt_no'] == job['attempt_no'] and producing['outcome'] == 'succeeded'
            assert effect['effect'] == job['result'] == {
                'job_id': job['id'], 'attempt_no': job['attempt_no'], 'worker_id': producing['worker_id']}
        assert all((job['id'], n) in by_attempt for n in range(1, job['attempt_no'] + 1))
    assert dict(Counter(j['state'] for j in jobs)) == summary['states']
    assert len(jobs) == summary['actual_jobs'] == summary['expected_jobs']
    assert len(effects) == summary['effects'] and not summary['violations'] and not summary['errors']
    assert not summary['database_retained']
    if run.name.startswith('load-'):
        assert all(key in by_key for key, payload in manifest.items() if payload.get('phase') == 'bootstrap')
        records = load('requests.jsonl')
        measured = [r for r in records if r['phase'] == 'measure']
        issued = [r for r in measured if not r['dropped']]
        metrics = summary['metrics']
        assert metrics['offered_requests'] == len(measured) == 6000
        assert metrics['issued_requests'] == len(issued)
        assert metrics['dropped_requests'] == len(measured) - len(issued)
        assert metrics['errors_including_drops'] == sum(not r['valid'] for r in measured)
        assert metrics['admission_rejections'] == sum(
            r.get('status') == 503 and r.get('error_code') == 'queue_full' for r in issued)
        for record in records:
            if record.get('kind') == 'create' and record.get('valid'):
                assert by_key[record['key']]['id'] == record['response_job_id']
        for field in ('scheduled_latency_seconds', 'response_seconds', 'dispatch_lag_seconds'):
            raw_field = 'scheduled_latency_seconds' if field == 'scheduled_latency_seconds' else field
            for p in (50, 95, 99):
                assert math.isclose(metrics[field][f'p{p}'], percentile([r[raw_field] for r in issued], p), abs_tol=1e-9)
        completion = [(datetime.fromisoformat(j['finished_at']) - datetime.fromisoformat(j['created_at'])).total_seconds()
                      for j in jobs if j['state'] == 'succeeded' and j['payload'].get('phase') == 'measure']
        assert len(completion) == metrics['job_latency_samples']
        for p in (50, 95, 99):
            assert math.isclose(metrics['job_completion_seconds'][f'p{p}'], percentile(completion, p), abs_tol=1e-9)
        samples = load('samples.jsonl')
        bound = metadata['deployment_settings']['max_outstanding_jobs']
        peak = max(s['queued'] + s['running'] for s in samples)
        assert peak <= bound
    else:
        assert len(jobs) == len(manifest) == 10000
        peak = None
    checks.append({'run_id': run.name, 'archive_passed': True, 'jobs': len(jobs), 'effects': len(effects),
                   'benchmark_passed': summary.get('benchmark_passed'), 'peak_observed_outstanding': peak})

baseline = json.loads((folder / 'historical-evidence-baseline.json').read_text())
assert all(hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest for name, digest in baseline.items())
(folder / 'archive-verification.json').write_text(json.dumps(checks, indent=2) + '\n')
print(json.dumps(checks, indent=2))
