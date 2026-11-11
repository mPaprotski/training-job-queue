import json,pathlib,subprocess,time,sys
out=pathlib.Path('verification/review-fixes-004')
network='soup-review-911_default'
entries=[]
with (out/'docker-stats-console.txt').open('x') as resource_log:
    monitor=subprocess.Popen([sys.executable,'harness/docker_stats.py','--project','soup-review-911',
                              '--seconds','1800','--output',str(out/'docker-stats.jsonl')],
                             stdout=resource_log,stderr=subprocess.STDOUT)
    try:
        for label,pool,workers in [('control','off',4),('pooled','on',4),('scaled','on',16)]:
            for profile in ('mixed','create'):
                name=f'{label}-{profile}'
                args=['docker','run','--rm','--init','--user','1000:1000','--network',network,
                      '--label','com.docker.compose.project=soup-review-911','--label','com.docker.compose.service=chaos',
                      '-e','DATABASE_URL=postgresql://queue_tools:queue_tools@db:5432/queue',
                      '-e','RUNTIME_DATABASE_USER=queue_runtime','-e','RUNTIME_DATABASE_PASSWORD=queue_runtime',
                      '-e','SOURCE_REVISION=8db3f12-plus-local-review-fixes-steps-1-10',
                      '-v',str((out/'harness').resolve())+':/out','training-job-queue:review-911',
                      'python','-m','harness.load','--profile',profile,'--pool',pool,'--workers',str(workers),
                      '--pool-size','4','--max-outstanding-jobs','1000','--rps','100','--warmup','10',
                      '--duration','60','--concurrency','128','--drain-timeout','120','--timeout','600',
                      '--seed','41','--output-dir','/out']
                print(json.dumps({'starting':name,'pool':pool,'workers':workers,'profile':profile}),flush=True)
                started=time.time()
                with (out/f'{name}-console.txt').open('x') as log:
                    result=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT)
                text=(out/f'{name}-console.txt').read_text()
                run_id=next((line.split('/out/',1)[1] for line in text.splitlines() if line.startswith('Run artifacts: /out/')),None)
                entry={'label':name,'pool':pool,'workers':workers,'profile':profile,'returncode':result.returncode,
                       'run_id':run_id,'start_unix':started,'end_unix':time.time(),'command':args}
                if run_id and (out/'harness'/run_id/'summary.json').exists():
                    entry['summary']=json.loads((out/'harness'/run_id/'summary.json').read_text())
                entries.append(entry)
                (out/'capacity-matrix.json').write_text(json.dumps(entries,indent=2)+'\n')
                print(json.dumps({'finished':name,'returncode':result.returncode,'run_id':run_id}),flush=True)
                if result.returncode not in (0,1) or not run_id or 'summary' not in entry:
                    raise RuntimeError('Infrastructure failure; stop matrix rather than silently lose evidence')
    finally:
        monitor.terminate()
        monitor.wait(timeout=20)
print('Capacity matrix complete',flush=True)
