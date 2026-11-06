import json,pathlib,sys,time
kind,run,cwd=sys.argv[1:];run=pathlib.Path(run);cwd=pathlib.Path(cwd)
sys.stdin.read()
print(json.dumps({'type':'thread.started','thread_id':'synthetic-session'}),flush=True)
if kind in ('network-failure','auth-failure'):
 print(json.dumps({'type':'error','message':'network timeout (simulated)' if kind=='network-failure' else 'authentication expired (simulated)'}),flush=True);sys.exit(75)
if kind=='isolation':
 print(json.dumps({'type':'error','message':'isolation fixture requires a separately qualified sandbox'}),flush=True);sys.exit(1)
if kind=='slow':
 marker=cwd/'durable-step.txt'
 if not marker.exists():marker.write_text('synthetic durable step')
 time.sleep(2)
(cwd/'test-result.txt').write_text('synthetic result')
(run/'last-message.json').write_text(json.dumps({'status':'completed','summary':'fixture passed','evidence':['test-result.txt'],'next_action':'none'}))
print(json.dumps({'type':'turn.completed'}),flush=True)
