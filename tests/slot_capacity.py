"""Exercise real Linux locks with temporary roots; no production jobs touched."""
import json,pathlib,subprocess,sys,tempfile,time,unittest
SOURCE=pathlib.Path(sys.argv.pop(1)).read_text()
class CapacityContracts(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='agentos-slots-')
        self.root=pathlib.Path(self.temp.name)
        self.script=self.root/'runner.py'
        self.script.write_text(SOURCE.replace("root=pathlib.Path('/var/lib/agentos-bridge')",'root=pathlib.Path('+repr(str(self.root))+')'))
        self.children=[]
    def tearDown(self):
        for p in self.children:
            if p.poll() is None:p.terminate()
        for p in self.children:
            p.wait(timeout=5)
            p.stdout.close();p.stderr.close()
        self.temp.cleanup()
    def start(self,key):
        ready=self.root/(key+'.ready')
        p=subprocess.Popen([sys.executable,str(self.script),'test/'+key,'1',sys.executable,'-c',"import pathlib,time;pathlib.Path("+repr(str(ready))+").touch();time.sleep(60)"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.children.append(p)
        for _ in range(100):
            if ready.exists() or p.poll() is not None:break
            time.sleep(.02)
        return p,ready
    def test_hot_increase_preserves_old_slots_and_rejects_ninth(self):
        a,_=self.start('first');b,_=self.start('second');c,_=self.start('third')
        self.assertEqual(c.wait(timeout=5),75)
        (self.root/'runtime-capacity.json').write_text(json.dumps({'maxRuns':8}))
        for i in range(6):
            p,ready=self.start('new'+str(i));self.assertTrue(ready.exists());self.assertIsNone(p.poll())
        p,_=self.start('ninth');self.assertEqual(p.wait(timeout=5),75)
        self.assertIsNone(a.poll());self.assertIsNone(b.poll())
    def test_workspace_exclusive_and_invalid_configuration(self):
        (self.root/'runtime-capacity.json').write_text('{"maxRuns":8}')
        self.start('same');p,_=self.start('same');self.assertEqual(p.wait(timeout=5),75)
        (self.root/'runtime-capacity.json').write_text('{"maxRuns":100}')
        p,_=self.start('invalid');self.assertNotEqual(p.wait(timeout=5),0)
unittest.main()
