import json,pathlib,subprocess,sys
out=pathlib.Path('verification/review-fixes-006');entries=[]


def run(label,profile,workers,seed):
    args=['docker','run','--rm','--init','--user','1000:1000','--network','soup-review-006_default',
          '--label','com.docker.compose.project=soup-review-006','--label','com.docker.compose.service=chaos',
          '-e','DATABASE_URL=postgresql://queue_tools:queue_tools@db:5432/queue',
          '-e','RUNTIME_DATABASE_USER=queue_runtime','-e','RUNTIME_DATABASE_PASSWORD=queue_runtime',
          '-e','SOURCE_REVISION=90cab34-plus-local-slot-fixes',
          '-v',str((out/'harness').resolve())+':/out','training-job-queue:review-006-poll',
          'python','-m','harness.load','--profile',profile,'--pool','on','--workers',str(workers),
          '--pool-size','4','--poll-seconds','0.1','--max-outstanding-jobs','1000','--rps','100','--warmup','10','--duration','60',
          '--concurrency','128','--drain-timeout','120','--timeout','600','--seed',str(seed),'--output-dir','/out']
    print(json.dumps({'starting':label}),flush=True)
    logpath=out/(label+'-console.txt')
    with logpath.open('x') as log:
        result=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT)
    identifier=next((line.split('/out/',1)[1] for line in logpath.read_text().splitlines() if line.startswith('Run artifacts: /out/')),None)
    if result.returncode not in (0,1) or not identifier:raise RuntimeError('Infrastructure failure; raw log preserved')
    summary=json.loads((out/'harness'/identifier/'summary.json').read_text())
    entry={'label':label,'profile':profile,'workers':workers,'seed':seed,'run_id':identifier,'returncode':result.returncode,'summary':summary}
    entries.append(entry);(out/'capacity-poll.json').write_text(json.dumps(entries,indent=2)+'\n')
    print(json.dumps({'finished':label,'passed':summary['passed'],'run_id':identifier}),flush=True)
    return summary['passed']


with (out/'docker-stats-poll-console.txt').open('x') as log:
    monitor=subprocess.Popen([sys.executable,'harness/docker_stats.py','--project','soup-review-006','--seconds','1800','--output',str(out/'docker-stats-poll.jsonl')],stdout=log,stderr=subprocess.STDOUT)
    try:
        results=[run(f'poll-create-16-repeat-{n}','create',16,70+n) for n in (1,2,3)]
        if all(results):run('poll-mixed-16-confirm','mixed',16,74)
    finally:
        monitor.terminate();monitor.wait(timeout=20)
print('Slot capacity verification complete',flush=True)
