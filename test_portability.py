import json, os, pathlib, subprocess, sys, tempfile, unittest
from unittest.mock import patch
import runner as r

class Portability(unittest.TestCase):
 def test_configuration_required_before_queue_creation(self):
  with tempfile.TemporaryDirectory() as d:
   env=dict(os.environ);env.pop('PERSISTENT_RUNNER_CONFIG',None)
   p=subprocess.run([sys.executable,str(pathlib.Path(r.__file__)),'list'],cwd=d,env=env,capture_output=True,text=True)
   self.assertNotEqual(p.returncode,0);self.assertEqual(list(pathlib.Path(d).iterdir()),[])
 def test_configured_held_queue(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);work=root/'work';project=work/'demo';project.mkdir(parents=True)
   config=root/'config.json';config.write_text(json.dumps(dict(state_dir=str(root/'state'),work_dir=str(work),codex_bin='/bin/true',service_name='synthetic.service',model='synthetic-model',mounts=[])))
   prompt=root/'prompt.txt';prompt.write_text('Synthetic fixture; do not run a model.')
   env=dict(os.environ,PERSISTENT_RUNNER_CONFIG=str(config))
   p=subprocess.run([sys.executable,r.__file__,'enqueue','--project','fixture','--cwd',str(project),'--prompt',str(prompt),'--held'],env=env,capture_output=True,text=True)
   self.assertEqual(p.returncode,0,p.stderr)
   p=subprocess.run([sys.executable,r.__file__,'list'],env=env,capture_output=True,text=True);task=json.loads(p.stdout)['tasks'][0]
   self.assertFalse(task['enabled']);self.assertEqual(task['mounts'],[]);self.assertEqual(task['model'],'synthetic-model')
 def test_fixture_receipt_and_retry_budget(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);work=root/'work';work.mkdir();prompt=root/'prompt';prompt.write_text('synthetic')
   with patch.object(r,'BASE',root/'state'),patch.object(r,'WORK',root),patch.object(r,'gate',return_value=None):
    c=r.db()
    try:
     task=dict(id='fixture',cwd=str(work),prompt=str(prompt),prompt_sha256=r.hashlib.sha256(prompt.read_bytes()).hexdigest(),state='PENDING',attempts=0,max_attempts=1,resume_safe=True,model='synthetic',kind='network-failure',checkpoint=None,session=None)
     r.save(c,task);r.run_task(c,task);self.assertEqual(r.get(c,'fixture')['state'],'FAILED')
     task.update(id='success',state='PENDING',attempts=0,kind='success',checkpoint=None,session=None);r.save(c,task);r.run_task(c,task)
     self.assertEqual(r.get(c,'success')['state'],'COMPLETED');self.assertTrue((r.BASE/'tasks/success/attempt-1/receipt.json').exists())
    finally:c.close()
if __name__=='__main__':unittest.main()
