"""Run on Linux; temporary files only, no production paths or model calls."""
import fcntl, importlib.util, os, pathlib, subprocess, sys, tempfile, unittest
spec=importlib.util.spec_from_file_location('manager',sys.argv.pop(1))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class WorkspaceContracts(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='agentos-contract-')
        m.ROOT=pathlib.Path(self.temp.name)/'workspaces';m.DATA=pathlib.Path(self.temp.name)/'data'
        self.base=m.ROOT/'test'/'base';self.base.mkdir(parents=True)
        (self.base/'main.js').write_text('old line\n')
        (self.base/'assets').mkdir();(self.base/'assets'/'art.bin').write_bytes(b'original asset')
        (self.base/'.env').write_text('fixture-only')
    def tearDown(self): self.temp.cleanup()
    def clone(self,name): return m.prepare({'sourceWorkspace':'test/base','workspace':'test/'+name})
    def test_independent_copies_and_idempotency(self):
        a=self.clone('one');b=self.clone('other')
        self.assertEqual(a,self.clone('one'));self.assertEqual(a['baseDigest'],b['baseDigest'])
        (m.ROOT/'test/one/main.js').write_text('first changed\n')
        self.assertEqual((m.ROOT/'test/other/main.js').read_text(),'old line\n')
        self.assertFalse((m.ROOT/'test/one/.env').exists())
        self.assertNotEqual((m.ROOT/'test/one/assets/art.bin').stat().st_ino,(self.base/'assets/art.bin').stat().st_ino)
    def test_patch_reconstructs_candidate_without_newline(self):
        self.clone('one');self.clone('other');one=m.ROOT/'test/one';other=m.ROOT/'test/other'
        (one/'main.js').write_text('new line without newline');(one/'extra.js').write_text('extra\n')
        evidence=m.inspect({'workspace':'test/one'})
        subprocess.run(['git','apply','-'],cwd=other,input=evidence['patch'].encode(),check=True)
        self.assertEqual(m.inspect({'workspace':'test/other'})['candidateDigest'],evidence['candidateDigest'])
        (one/'evidence/report.json').write_text('{}')
        self.assertEqual(m.inspect({'workspace':'test/one'})['candidateDigest'],evidence['candidateDigest'])
    def test_generated_test_data_symlinks_do_not_block_clone_or_inspect(self):
        gen=self.base/".local-test/artifacts";gen.mkdir(parents=True)
        (gen/"link").symlink_to("/etc/hostname")
        (self.base/"test-results").mkdir();(self.base/"test-results/report.html").write_text("x")
        self.clone("one")
        self.assertFalse((m.ROOT/"test/one/.local-test").exists())
        self.assertFalse((m.ROOT/"test/one/test-results").exists())
        before=m.inspect({"workspace":"test/one"})["candidateDigest"]
        gen2=m.ROOT/"test/one/.local-test/artifacts";gen2.mkdir(parents=True)
        (gen2/"link").symlink_to("/etc/hostname")
        self.assertEqual(m.inspect({"workspace":"test/one"})["candidateDigest"],before)
    def test_runtime_bytecode_cache_does_not_block_clone_or_inspect(self):
        cache=self.base/"bridge/__pycache__";cache.mkdir(parents=True)
        (cache/"mod.cpython-312.pyc").write_bytes(b'\x42\x0d\x0d\x0a\x00\xff')
        self.clone("one")
        self.assertFalse((m.ROOT/"test/one/bridge/__pycache__").exists())
        before=m.inspect({"workspace":"test/one"})["candidateDigest"]
        cache2=m.ROOT/"test/one/bridge/__pycache__";cache2.mkdir(parents=True)
        (cache2/"mod.cpython-312.pyc").write_bytes(b'\x42\x0d\x0d\x0a\x00\xff')
        (m.ROOT/"test/one/loose.pyc").write_bytes(b'\x00\xff')
        self.assertEqual(m.inspect({"workspace":"test/one"})["candidateDigest"],before)
        (m.ROOT/"test/one/real.bin").write_bytes(b'\x00\xff')
        with self.assertRaisesRegex(ValueError,'PATCH_BINARY_SOURCE'):m.inspect({"workspace":"test/one"})
    def test_source_symlinks_are_rejected(self):
        (self.base/'escape').symlink_to('/etc/hostname')
        with self.assertRaisesRegex(ValueError,'SOURCE_SYMLINK'):self.clone('one')
    def test_shared_readers_do_not_serialize_independent_snapshots(self):
        with m.locks(['test/base'],shared=['test/base']):
            self.clone('one');self.clone('other')
    def test_legacy_active_worker_prevents_copy(self):
        m.DATA.mkdir(parents=True)
        with (m.DATA/'worker.lock').open('a') as h:
            fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError,'SOURCE_BUSY'):self.clone('one')
    def test_special_file_does_not_block_inspection(self):
        self.clone('one');os.mkfifo(m.ROOT/'test/one/pipe')
        with self.assertRaisesRegex(ValueError,'SOURCE_NOT_REGULAR'):m.inspect({'workspace':'test/one'})
unittest.main()
