from dataclasses import replace
import json,statistics,time
from training_job_queue.db import connection_pool,transaction
from training_job_queue.settings import Settings
base=Settings.from_env()
results=[]
for repetition in range(3):
    for enabled in ([False, True] if repetition%2==0 else [True,False]):
        settings=replace(base,db_pool_enabled=enabled,db_pool_size=1)
        with connection_pool(settings,size=1):
            # Exclude one warmup connection and pool startup from operation timing.
            with transaction(settings) as c:
                c.execute('SELECT 1').fetchone()
            ids=set()
            started=time.monotonic()
            for _ in range(500):
                with transaction(settings) as c:
                    ids.add(c.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid'])
            elapsed=time.monotonic()-started
        results.append({'repetition':repetition,'pool_enabled':enabled,'transactions':500,'elapsed_seconds':elapsed,'ms_per_transaction':elapsed*2,'unique_backends':len(ids)})
summary={'runs':results,'median_ms_per_transaction':{str(enabled):statistics.median(r['ms_per_transaction'] for r in results if r['pool_enabled']==enabled) for enabled in (False,True)},'scope':'SELECT transaction overhead, not HTTP or job throughput'}
print(json.dumps(summary,indent=2))
