import importlib.util,pathlib,tempfile,unittest,types,os,multiprocessing
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('manager',ROOT/'bridge/workspace-manager.py')
manager=importlib.util.module_from_spec(spec);spec.loader.exec_module(manager)
storage=manager.storage_capacity

def hold_and_exit(root,signal):
    with patch.object(storage.shutil,'disk_usage',return_value=types.SimpleNamespace(free=4*1024**3)):
        with storage.reserve(pathlib.Path(root),pathlib.Path(root)/'control',1024**3):
            signal.send('held');signal.recv();os._exit(0)

class StorageTests(unittest.TestCase):
 def test_concurrent_reservations_cannot_spend_same_bytes_and_crash_releases_ownership(self):
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);parent,child=multiprocessing.Pipe();p=multiprocessing.Process(target=hold_and_exit,args=(temp,child));p.start()
   try:
    self.assertEqual(parent.recv(),'held')
    with patch.object(storage.shutil,'disk_usage',return_value=types.SimpleNamespace(free=4*1024**3)):
     self.assertEqual(storage.capacity(root,root/'control')['reservedBytes'],1024**3)
     with self.assertRaisesRegex(ValueError,'DISK_CAPACITY'):
      with storage.reserve(root,root/'control',2*1024**3):pass
     parent.send('exit');p.join(5);self.assertFalse(p.is_alive())
     self.assertEqual(storage.capacity(root,root/'control')['reservedBytes'],0)
     with storage.reserve(root,root/'control',2*1024**3):pass
   finally:
    if p.is_alive():p.terminate();p.join()
 def test_excluded_dependencies_do_not_inflate_the_copy_estimate(self):
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);(root/'source.mjs').write_text('source')
   base=manager.copy_reservation(root)
   (root/'node_modules').mkdir();(root/'node_modules'/'huge').write_bytes(b'x'*1024*1024)
   (root/'evidence').mkdir();(root/'evidence'/'old').write_bytes(b'x'*1024*1024)
   self.assertEqual(manager.copy_reservation(root),base)
   (root/'assets').mkdir();(root/'assets'/'a').write_bytes(b'x'*8192)
   self.assertEqual(manager.copy_reservation(root),base+8192)
 def test_failed_preparation_removes_only_its_own_unpublished_staging(self):
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);work=root/'work';data=root/'control';(work/'project'/'source').mkdir(parents=True)
   (work/'project'/'source'/'code.mjs').write_text('preserve')
   unrelated=work/'project'/'.prepare-unrelated';unrelated.mkdir();(unrelated/'keep').write_text('keep')
   def fail(source,target,**kwargs):target.mkdir();(target/'partial').write_text('partial');raise OSError('copy failed')
   with patch.object(manager,'ROOT',work),patch.object(manager,'DATA',data),patch.object(manager.shutil,'copytree',side_effect=fail):
    with self.assertRaisesRegex(OSError,'copy failed'):manager.prepare({'workspace':'project/o-target','sourceWorkspace':'project/source'})
   self.assertEqual([x.name for x in (work/'project').glob('.prepare-*')],['.prepare-unrelated'])
   self.assertEqual((work/'project'/'source'/'code.mjs').read_text(),'preserve')
   self.assertEqual(storage.capacity(work,data)['reservedBytes'],0)
 def test_prepared_marker_must_name_the_exact_target(self):
  import json
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);work=root/'work';target=work/'project'/'o-target';target.mkdir(parents=True)
   (target/'.agentos-snapshot.json').write_text(json.dumps({'workspace':'project/o-other','sourceWorkspace':'project/source'}))
   with patch.object(manager,'ROOT',work),patch.object(manager,'DATA',root/'data'):
    with self.assertRaisesRegex(ValueError,'SNAPSHOT_CONFLICT'):manager.prepare({'workspace':'project/o-target','sourceWorkspace':'project/source'})

if __name__=='__main__':unittest.main()
