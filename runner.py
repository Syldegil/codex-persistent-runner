#!/usr/bin/env python3
"""Single-worker, fail-closed Codex queue. Python standard library only."""
import shlex
import argparse, datetime, fcntl, hashlib, json, os, pathlib, re, selectors, shutil, signal, sqlite3, subprocess, sys, time, uuid
# Configuration is explicit and read before queue access. No production defaults.
CONFIG_PATH=os.environ.get('PERSISTENT_RUNNER_CONFIG')
CONFIG=json.loads(pathlib.Path(CONFIG_PATH).read_text()) if CONFIG_PATH else {}
BASE=pathlib.Path(CONFIG.get('state_dir', './state')).resolve()
WORK=pathlib.Path(CONFIG.get('work_dir', './work')).resolve()
CODEX=CONFIG.get('codex_bin', 'codex')
SERVICE=CONFIG.get('service_name', 'codex-persistent-runner.service')
SOURCE=pathlib.Path(__file__).resolve().parent
STATES=('PENDING','RUNNING','CHECKPOINTED','COMPLETED','RETRY_WAIT','FAILED','BLOCKED','CANCELLED')
STOP=False

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(path,data):
    path=pathlib.Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with open(tmp,'x') as f:
        os.chmod(tmp,0o600); f.write(json.dumps(data,ensure_ascii=False,indent=2)); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)
    fd=os.open(path.parent,os.O_DIRECTORY); os.fsync(fd); os.close(fd)
def clean(s):
    s=re.sub(r'(?i)(bearer\s+|(?:api[_-]?key|access_token|refresh_token|password)\s*[=:]\s*["\x27]?)[^\s,"\x27}]+',r'\1[REDACTED]',s)
    return re.sub(r'\bsk-[A-Za-z0-9_-]{12,}', '[REDACTED]',s)
def log(path,value):
    path=pathlib.Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.stat().st_size>2*1024*1024:
        for i in (2,1):
            p=path.with_name(path.name+'.'+str(i))
            if p.exists(): os.replace(p,path.with_name(path.name+'.'+str(i+1)))
        os.replace(path,path.with_name(path.name+'.1'))
    with open(path,'a') as f: f.write(clean(value)+'\n')
def event(kind,**kw):
    v=dict(time=now(),event=kind,**kw); log(BASE/'orchestrator.jsonl',json.dumps(v)); print(json.dumps(v),flush=True)
def db():
    BASE.mkdir(parents=True,exist_ok=True); os.chmod(BASE,0o700)
    c=sqlite3.connect(BASE/'queue.sqlite',timeout=15); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=FULL')
    c.execute('CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, data TEXT NOT NULL)')
    c.execute('CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT)')
    return c
def save(c,t):
    c.execute('INSERT OR REPLACE INTO tasks VALUES(?,?)',(t['id'],json.dumps(t))); c.commit()
def tasks(c): return [json.loads(r['data']) for r in c.execute('SELECT data FROM tasks ORDER BY rowid')]
def get(c,id):
    r=c.execute('SELECT data FROM tasks WHERE id=?',(id,)).fetchone()
    if not r: raise ValueError('unknown task')
    return json.loads(r['data'])
def setting(c,k,default='0'):
    r=c.execute('SELECT value FROM settings WHERE key=?',(k,)).fetchone(); return r[0] if r else default
def state(c,t,s,error=None):
    assert s in STATES; t.update(state=s,updated=now(),error=error); save(c,t); event('transition',task=t['id'],state=s,error=error)
def resources(path=WORK):
    m={k:int(v.split()[0])*1024 for k,v in (l.split(':',1) for l in pathlib.Path('/proc/meminfo').read_text().splitlines())}
    d=shutil.disk_usage(path); st=os.statvfs(path)
    temps={p.parent.name:p.read_text().strip() for p in pathlib.Path('/sys/class/thermal').glob('thermal_zone*/temp')}
    return dict(time=now(),cpu_count=os.cpu_count(),load=os.getloadavg(),mem_available=m['MemAvailable'],swap_free=m['SwapFree'],disk_free=d.free,inodes_free=st.f_favail,temperatures=temps,pressure={p.name:p.read_text() for p in pathlib.Path('/proc/pressure').glob('*')},diskstats=pathlib.Path('/proc/diskstats').read_text()[-12000:])
def process_snapshot(pid):
    pending=[pid]; found=[]
    while pending and len(found)<256:
        current=pending.pop(); root=pathlib.Path('/proc')/str(current)
        try:
            stat=(root/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
            io={k:int(v) for k,v in (line.split(':',1) for line in (root/'io').read_text().splitlines())}
            found.append(dict(pid=current,state=fields[0],cpu_ticks=int(fields[11])+int(fields[12]),read_bytes=io.get('read_bytes',0),write_bytes=io.get('write_bytes',0)))
            pending.extend(int(n) for n in (root/'task'/str(current)/'children').read_text().split())
        except (OSError,ValueError,IndexError): continue
    return found

def gate(t):
    p=pathlib.Path(t['cwd']).resolve()
    if not p.is_relative_to(WORK.resolve()) or p==WORK.resolve(): return 'workdir outside authorized project roots'
    if not p.is_dir(): return 'workdir unavailable'
    for mount in t.get('mounts',[]):
        r=subprocess.run(['findmnt','-rn','-M',mount['target'],'-o','SOURCE'],capture_output=True,text=True)
        if r.returncode or r.stdout.strip()!=mount['source']: return 'required mount identity unavailable'
    r=resources(p)
    if r['mem_available']<max(CONFIG.get('min_ram_gib',4)*1024**3,t['ram_gib']*1024**3): return 'insufficient available RAM'
    if r['disk_free']<(CONFIG.get('disk_reserve_gib',20)+t['disk_gib'])*1024**3: return 'insufficient disk reserve'
    if r['inodes_free']<CONFIG.get('min_free_inodes',10000): return 'insufficient inodes'
    if r['load'][0]>CONFIG.get('max_load',4): return 'CPU load above admission limit'
    if t['heavy']: return 'heavy build gate: dedicated storage and host capacity validation required'
    if not shutil.which(CODEX): return 'Codex unavailable'
    return None

def checkpoint(c,t,semantic=None):
    v=dict(task=t['id'],time=now(),state=t['state'],attempt=t['attempts'],session=t.get('session'),pid=t.get('pid'),last_activity=t.get('last_activity'),operations_succeeded=[],operations_failed=[],artifacts=[],modified_files=[],tests=[],verified_results=[],next_action='Inspect artifacts, logs and project state before continuing',resume_command='PERSISTENT_RUNNER_CONFIG='+shlex.quote(str(CONFIG_PATH or 'CONFIG.json'))+' '+shlex.join([sys.executable,str(SOURCE/'runner.py'),'resume',t['id']]),risks=['Interrupted operations require reconciliation; no exactly-once side-effect guarantee'],kind='supervisor-observation')
    if semantic is not None:
        for k in ('operations_succeeded','operations_failed','artifacts','modified_files','tests','verified_results','next_action','risks'):
            if k not in semantic: raise ValueError('missing checkpoint field '+k)
        v.update(semantic); v['task']=t['id']; v['kind']='project-checkpoint'
    v['project_state']={}
    for name in ('STATE.md','CHECKPOINT.md'):
        src=pathlib.Path(t['cwd'])/name
        if src.is_file() and src.resolve().is_relative_to(pathlib.Path(t['cwd']).resolve()) and src.stat().st_size<=262144:
            content=src.read_text(errors='replace'); v['project_state'][name]=dict(sha256=hashlib.sha256(content.encode()).hexdigest(),content=clean(content))
    p=BASE/'tasks'/t['id']/'checkpoint.json'; atomic(p,v)
    t['checkpoint']=str(p); save(c,t); log(p.parent/'checkpoints.jsonl',json.dumps(v)); return v

def classify(rc,error):
    s=error.lower()
    if any(x in s for x in ('auth','401','403','quota','usage limit','rate limit','permission','sandbox','no space','corrupt','insufficient')): return 'BLOCKED'
    if any(x in s for x in ('network','connection','timeout','timed out','temporarily','502','503','504','stream disconnected')): return 'RETRY_WAIT'
    return 'FAILED'

def reconcile(c):
    for t in tasks(c):
        if t['state'] not in ('RUNNING','CHECKPOINTED'): continue
        # Never infer success from exit=0 alone. Durable result is written before queue commit.
        receipt=BASE/'tasks'/t['id']/f"attempt-{t['attempts']}"/'receipt.json'
        if receipt.exists():
            v=json.loads(receipt.read_text())
            if v.get('state')=='COMPLETED':
                t['result']=v.get('result'); state(c,t,'COMPLETED'); continue
        pid=t.get('pid')
        if pid and pathlib.Path('/proc',str(pid)).exists() and t.get('boot')==pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip():
            # Even PID reuse blocks safely; never kill an unidentified process.
            state(c,t,'BLOCKED','recorded process still exists; verify ownership before resume'); continue
        if t['resume_safe'] and t['attempts']<t['max_attempts']:
            t['retry_at']=time.time()+5; state(c,t,'RETRY_WAIT','interrupted; reconcile artifacts before resume')
        else: state(c,t,'BLOCKED','interrupted task is not authorized for automatic replay')

def run_task(c,t):
    err=gate(t)
    if err:
        if err.startswith(('insufficient','required mount','Codex unavailable','CPU load')):
            t['admission_failures']=t.get('admission_failures',0)+1; t['retry_at']=time.time()+60
            state(c,t,'RETRY_WAIT' if t['admission_failures']<8 else 'BLOCKED',err)
        else: state(c,t,'BLOCKED',err)
        return
    previous_checkpoint=pathlib.Path(t['checkpoint']).read_text() if t.get('checkpoint') and pathlib.Path(t['checkpoint']).exists() else '{}'
    t.update(attempts=t['attempts']+1,started=now(),last_activity=now(),pid=None,boot=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    state(c,t,'RUNNING'); checkpoint(c,t)
    run=BASE/'tasks'/t['id']/f"attempt-{t['attempts']}"; run.mkdir(parents=True)
    t['logs']=str(run); save(c,t)
    for name in ('codex-events.jsonl','codex-stdout.log','codex-stderr.log','builds.log','tests.log'): (run/name).touch(mode=0o600)
    prompt=pathlib.Path(t['prompt']).read_text()
    if hashlib.sha256(prompt.encode()).hexdigest()!=t['prompt_sha256']:
        state(c,t,'BLOCKED','queued prompt integrity mismatch'); return
    env={k:v for k,v in os.environ.items() if k in ('HOME','PATH','LANG','TZ','SSL_CERT_FILE','SSL_CERT_DIR')}
    env['CODEX_HOME']=str(BASE/'codex-home')
    command=[CODEX,'-a','never','exec','--ignore-user-config','--json','--color','never','-s','workspace-write','-C',t['cwd'],'--skip-git-repo-check','-m',t['model'],'-c','sandbox_workspace_write.network_access=false','--output-schema',str(SOURCE/'result-schema.json'),'-o',str(run/'last-message.json')]
    if t.get('session'):
        # exec options precede the resume subcommand; keep the same sandbox on resume.
        command+=['resume',t['session'],'-']
    else: command+=['-']
    prefix='You run unattended. No privilege escalation, phone access, destructive operations or global service changes. Read project AGENTS.md, STATE.md and CHECKPOINT.md. Reconcile existing artifacts and tests before acting; never blindly replay interrupted work. Report blocked when required permissions/resources are unavailable. Write semantic checkpoints atomically after meaningful steps. Final JSON must accurately distinguish completed, checkpointed, blocked and failed. Only mark completed if the bounded queued task is verified.\n'
    if t['attempts']>1: prefix+='This is a RECOVERY attempt. Previous supervisor checkpoint: '+previous_checkpoint+'\n'
    semantic=BASE/'tasks'/t['id']/'semantic-checkpoint.json'
    if semantic.exists(): prefix+='Persisted semantic checkpoint (verify against files): '+semantic.read_text()[:262144]+'\n'
    if t['kind']!='codex': command=[sys.executable,str(SOURCE/'selftest_worker.py'),t['kind'],str(run),t['cwd']]
    completed_event=False; errors=[]; buffers={}; sel=selectors.DefaultSelector()
    try:
        p=subprocess.Popen(command,cwd=t['cwd'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        t['pid']=p.pid; save(c,t)
        p.stdin.write((prefix+prompt).encode()); p.stdin.close()
        for stream,name in ((p.stdout,'stdout'),(p.stderr,'stderr')):
            os.set_blocking(stream.fileno(),False); sel.register(stream,selectors.EVENT_READ,name); buffers[name]=b''
        beat=time.monotonic(); terminating=None; total=0
        while sel.get_map() or p.poll() is None:
            if STOP and terminating is None:
                os.killpg(p.pid,signal.SIGTERM); terminating=time.monotonic()
            if terminating and time.monotonic()-terminating>20 and p.poll() is None: os.killpg(p.pid,signal.SIGKILL)
            for key,_ in sel.select(1):
                chunk=os.read(key.fileobj.fileno(),65536); name=key.data
                if not chunk: sel.unregister(key.fileobj); continue
                total+=len(chunk); t['last_activity']=now()
                log(run/('codex-'+name+'.log'),chunk.decode(errors='replace'))
                buffers[name]=(buffers[name]+chunk)[-262144:]
                while b'\n' in buffers[name]:
                    line,buffers[name]=buffers[name].split(b'\n',1)
                    if name=='stdout':
                        try: v=json.loads(line)
                        except ValueError: continue
                        log(run/'codex-events.jsonl',json.dumps(v))
                        item=v.get('item',{})
                        if item.get('type')=='command_execution':
                            cmd=item.get('command','')
                            if re.search(r'\b(make|ninja|gradle|mka|soong_ui)\b',cmd): log(run/'builds.log',json.dumps(v))
                            if re.search(r'\b(pytest|unittest|ctest|cargo test|test)\b',cmd): log(run/'tests.log',json.dumps(v))
                        if v.get('type')=='thread.started': t['session']=v.get('thread_id'); save(c,t)
                        if v.get('type')=='turn.completed': completed_event=True
                        if v.get('type') in ('error','turn.failed'): errors.append(clean(json.dumps(v))[:4000]); errors=errors[-10:]
                    elif t['kind']!='codex': errors.append(line.decode(errors='replace'))
            if total>64*1024**2 and terminating is None:
                errors.append('output limit exceeded'); os.killpg(p.pid,signal.SIGTERM); terminating=time.monotonic()
            if time.monotonic()-beat>15:
                checkpoint(c,t); atomic(BASE/'heartbeat.json',dict(time=now(),task=t['id'],pid=p.pid,resources=resources(t['cwd']),processes=process_snapshot(p.pid),observation='Process CPU/I/O snapshots; silence alone never triggers termination'))
                beat=time.monotonic()
        rc=p.wait(); t['exit_code']=rc; t['pid']=None
        last=run/'last-message.json'; result=None
        if last.exists() and last.stat().st_size<1024*1024:
            try: result=json.loads(last.read_text())
            except ValueError: pass
        t['result']=result
        error=' '.join(errors)[-8000:]
        if STOP: target='CHECKPOINTED'; error='service stop requested'
        elif rc==0 and completed_event and isinstance(result,dict) and result.get('status')=='completed': target='COMPLETED'
        elif rc<0 and t['resume_safe']: target='RETRY_WAIT'; error='child process interrupted by signal '+str(-rc)
        elif rc==0 and result and result.get('status')=='checkpointed': target='RETRY_WAIT'; error='bounded step checkpointed'
        elif rc==0 and result and result.get('status')=='blocked': target='BLOCKED'; error=result.get('summary','blocked')
        else: target=classify(rc,error); error=error or 'no validated completion result; inspect private attempt logs'
        if target=='RETRY_WAIT':
            if not t['resume_safe']: target='BLOCKED'
            elif t['attempts']>=t['max_attempts']: target='FAILED'; error='retry budget exhausted: '+error
            else: t['retry_at']=time.time()+min(900,5*2**(t['attempts']-1))
        atomic(run/'receipt.json',dict(time=now(),state=target,exit_code=rc,result=result,error=error))
        state(c,t,target,error if target!='COMPLETED' else None); checkpoint(c,t)
    except Exception as exc:
        if 'p' in locals() and p.poll() is None:
            os.killpg(p.pid,signal.SIGTERM)
            try:p.wait(timeout=20)
            except subprocess.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL); p.wait()
        t['pid']=None; state(c,t,'BLOCKED',clean(str(exc))); checkpoint(c,t)
    finally:
        sel.close()
        if 'p' in locals():
            for stream in (p.stdin,p.stdout,p.stderr):
                if stream: stream.close()

def worker():
    global STOP
    if SERVICE not in pathlib.Path('/proc/self/cgroup').read_text():
        raise SystemExit('worker must run inside the configured systemd service')
    lock=open(BASE/'worker.lock','a')
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: raise SystemExit('another worker holds lock')
    signal.signal(signal.SIGTERM,lambda *_:globals().__setitem__('STOP',True))
    signal.signal(signal.SIGINT,lambda *_:globals().__setitem__('STOP',True))
    c=db(); reconcile(c); event('worker-start')
    while not STOP:
        atomic(BASE/'heartbeat.json',dict(time=now(),paused=setting(c,'paused'),resources=resources()))
        if setting(c,'paused')!='1':
            all_tasks=tasks(c); done={t['id'] for t in all_tasks if t['state']=='COMPLETED'}
            t=next((t for t in all_tasks if t['enabled'] and t['state'] in ('PENDING','RETRY_WAIT') and t.get('retry_at',0)<=time.time() and set(t['dependencies'])<=done),None)
            if t: run_task(c,t); continue
        time.sleep(2)
    event('worker-stop')

def cli():
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest='cmd',required=True)
    for name in ('worker','status','list','start','stop','pause','report'): sub.add_parser(name)
    r=sub.add_parser('resume'); r.add_argument('id',nargs='?')
    r=sub.add_parser('cancel'); r.add_argument('id')
    r=sub.add_parser('logs'); r.add_argument('id',nargs='?')
    r=sub.add_parser('checkpoint'); r.add_argument('id'); r.add_argument('--file',required=True)
    r=sub.add_parser('enqueue'); r.add_argument('--project',required=True); r.add_argument('--cwd',required=True); r.add_argument('--prompt',required=True); r.add_argument('--resume-safe',action='store_true'); r.add_argument('--held',action='store_true'); r.add_argument('--heavy',action='store_true'); r.add_argument('--ram-gib',type=int,default=4); r.add_argument('--disk-gib',type=int,default=1); r.add_argument('--depends',action='append',default=[]); r.add_argument('--model',default=CONFIG.get('model')); r.add_argument('--test-kind',choices=('success','network-failure','slow','auth-failure','isolation')); r.add_argument('--max-attempts',type=int,default=3)
    a=p.parse_args(); os.umask(0o077)
    if not CONFIG_PATH: raise ValueError('set PERSISTENT_RUNNER_CONFIG to an explicit JSON configuration')
    for key in ('state_dir','work_dir','codex_bin','service_name'):
        if not CONFIG.get(key): raise ValueError('missing configuration: '+key)
    if not pathlib.Path(CONFIG['state_dir']).is_absolute() or not pathlib.Path(CONFIG['work_dir']).is_absolute(): raise ValueError('absolute directories required')
    if '/' in CODEX and not pathlib.Path(CODEX).is_absolute(): raise ValueError('codex_bin must be absolute or a PATH command name')
    if a.cmd=='enqueue' and not a.model: raise ValueError('configure model or pass --model explicitly')
    c=db()
    if a.cmd=='worker': return worker()
    if a.cmd in ('start','stop'): return subprocess.call(['systemctl','--user',a.cmd,SERVICE])
    if a.cmd=='enqueue':
        cwd=pathlib.Path(a.cwd).resolve(strict=True)
        if not cwd.is_relative_to(WORK.resolve()) or cwd==WORK.resolve(): raise ValueError('cwd must be a dedicated project below '+str(WORK))
        if not 1<=a.max_attempts<=8: raise ValueError('attempt budget 1..8 required')
        for dep in a.depends: get(c,dep)
        ident=uuid.uuid4().hex[:16]; dest=BASE/'tasks'/ident; dest.mkdir(parents=True)
        data=pathlib.Path(a.prompt).read_bytes()
        if len(data)>1024*1024: raise ValueError('prompt exceeds 1MiB')
        (dest/'PROMPT.md').write_bytes(data)
        t=dict(id=ident,project=a.project,cwd=str(cwd),prompt=str(dest/'PROMPT.md'),prompt_source=str(pathlib.Path(a.prompt).resolve()),prompt_sha256=hashlib.sha256(data).hexdigest(),created=now(),state='PENDING',attempts=0,max_attempts=a.max_attempts,resume_safe=a.resume_safe,enabled=not a.held,dependencies=a.depends,heavy=a.heavy,ram_gib=a.ram_gib,disk_gib=a.disk_gib,model=a.model,kind=a.test_kind or 'codex',mounts=CONFIG.get('mounts',[]),session=None,checkpoint=None,exit_code=None,error=None)
        save(c,t); print(ident); return
    if a.cmd=='pause' or (a.cmd=='resume' and not a.id):
        c.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('paused','1' if a.cmd=='pause' else '0')); c.commit(); return
    if a.cmd in ('resume','cancel','checkpoint'):
        # Serialize control mutations against a running worker by refusing active task mutation.
        t=get(c,a.id)
        if a.cmd=='checkpoint':
            v=json.loads(pathlib.Path(a.file).read_text())
            # Semantic sidecar is independent; worker remains sole owner of active task row.
            for k in ('operations_succeeded','operations_failed','artifacts','modified_files','tests','verified_results','next_action','risks'):
                if k not in v: raise ValueError('missing '+k)
            atomic(BASE/'tasks'/a.id/'semantic-checkpoint.json',v); return
        lock=open(BASE/'worker.lock','a')
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise ValueError('stop service before changing task state; pause only prevents new starts')
        if t['state']=='COMPLETED': raise ValueError('completed tasks cannot be replayed')
        if a.cmd=='cancel': state(c,t,'CANCELLED','operator cancelled'); return
        if t['state'] in ('RUNNING','CHECKPOINTED') and t.get('pid') and pathlib.Path('/proc',str(t['pid'])).exists(): raise ValueError('process still exists')
        t['enabled']=True
        if t['attempts']>=t['max_attempts']: raise ValueError('attempt budget exhausted; review and enqueue a new bounded task')
        t['retry_at']=0; state(c,t,'PENDING'); return
    if a.cmd=='logs':
        path=BASE/'orchestrator.jsonl' if not a.id else BASE/'tasks'/get(c,a.id)['id']/'checkpoints.jsonl'
        if path.exists(): print(path.read_text()[-16000:])
        return
    output=dict(time=now(),paused=setting(c,'paused'),tasks=tasks(c))
    if a.cmd in ('status','report'):
        output['resources']=resources(); output['systemd']=subprocess.run(['systemctl','--user','show',SERVICE,'-p','ActiveState','-p','SubState','-p','MainPID','-p','MemoryCurrent','-p','CPUUsageNSec'],capture_output=True,text=True).stdout
        h=BASE/'heartbeat.json'; output['heartbeat']=json.loads(h.read_text()) if h.exists() else None
        output['critical_services']=subprocess.run(['systemctl','--failed','--no-pager'],capture_output=True,text=True).stdout
        output['log_bytes']=sum(p.stat().st_size for p in BASE.rglob('*.log*') if p.is_file())
    print(json.dumps(output,ensure_ascii=False,indent=2))
if __name__=='__main__':
    try: sys.exit(cli() or 0)
    except (ValueError,OSError,sqlite3.Error) as e: print(clean(str(e)),file=sys.stderr); sys.exit(2)
