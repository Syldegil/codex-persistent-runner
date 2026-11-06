import importlib.util,json,pathlib,tempfile,unittest
spec=importlib.util.spec_from_file_location('runner',pathlib.Path(__file__).with_name('runner.py')); r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();r.BASE=pathlib.Path(self.tmp.name);self.c=r.db()
 def tearDown(self):self.c.close();self.tmp.cleanup()
 def test_atomic_preserves_valid_checkpoint_on_failure(self):
  p=r.BASE/'c.json';r.atomic(p,{'valid':1})
  with self.assertRaises(TypeError):r.atomic(p,{'bad':object()})
  self.assertEqual(json.loads(p.read_text()),{'valid':1})
 def test_auth_and_quota_do_not_retry(self):
  for s in ['401 authentication expired','quota exceeded','permission denied','sandbox failed']:self.assertEqual(r.classify(1,s),'BLOCKED')
 def test_network_is_recoverable_and_unknown_is_not(self):
  self.assertEqual(r.classify(1,'connection timeout'),'RETRY_WAIT');self.assertEqual(r.classify(1,'unknown'),'FAILED')
 def task(self,safe=True):
  return dict(id='test',state='RUNNING',attempts=1,max_attempts=3,resume_safe=safe,pid=None)
 def test_reconcile_safe_interruption(self):
  t=self.task();r.save(self.c,t);r.reconcile(self.c);self.assertEqual(r.get(self.c,'test')['state'],'RETRY_WAIT')
 def test_reconcile_unsafe_interruption(self):
  t=self.task(False);r.save(self.c,t);r.reconcile(self.c);self.assertEqual(r.get(self.c,'test')['state'],'BLOCKED')
 def test_completion_receipt_prevents_replay_after_crash(self):
  t=self.task();r.save(self.c,t);r.atomic(r.BASE/'tasks/test/attempt-1/receipt.json',{'state':'COMPLETED','result':{'summary':'verified'}})
  r.reconcile(self.c);self.assertEqual(r.get(self.c,'test')['state'],'COMPLETED')
 def test_exhausted_recovery_budget(self):
  t=self.task();t['attempts']=3;r.save(self.c,t);r.reconcile(self.c);self.assertEqual(r.get(self.c,'test')['state'],'BLOCKED')
 def test_path_escape_blocked(self):self.assertIn('outside',r.gate({'cwd':'/'}))
 def test_secret_redaction(self):self.assertNotIn('secret123',r.clean('Bearer secret123'))
if __name__=='__main__':unittest.main()
