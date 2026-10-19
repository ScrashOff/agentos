import base64, hashlib, importlib.util, json, os, pathlib, stat, tempfile, types, unittest, uuid
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('evidence',ROOT/'scripts/agentos-candidate-validation-evidence.py')
TARGET=importlib.util.module_from_spec(spec);spec.loader.exec_module(TARGET)

class EvidenceTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
  self.state=self.root/'states';self.state.mkdir(mode=0o700)
  self.req={name:str(uuid.uuid4()) for name in ['actionId','managerRunId','goalId','taskId','runId','runFence','contextVersion']}
  raw=json.dumps({'candidateDigest':'a'*64}).encode()
  self.req.update(expectedAttempt=1,project='agentos',workspace='agentos/otest',candidateDigest='a'*64,candidateArtifactDigest=TARGET.sha(raw),contractDigest='b'*64,candidateBase64=base64.b64encode(raw).decode(),candidateSize=len(raw),reason='Publicação correta 🚀')
  self.req['frameworkAuthorization']={'kind':'registered_agentos_source','project':'agentos','goalId':self.req['goalId']}
  self.req['decisionProof']={'coordinationArtifactId':str(uuid.uuid4()),'coordinationArtifactDigest':'c'*64,'coordinationSize':100,'actionIndex':0,'actionDigest':'d'*64}
  self.dir=self.state/self.req['actionId'];self.dir.mkdir(mode=0o700)
  self.log='First real build line\n'+('A real canonical test passed\n'*900)+'Last real suite line 🚀\npostgresql://user:privatepass@db/test\nAuthorization: Bearer abcdefghijklmnopqrst\nCookie: session=privatecookie\nDATABASE_URL=privatepass\n{"password":"jsonprivatevalue","headers":{"authorization":"jsonprivateheader"}}'
  output={'ok':True,'output':{'passed':True,'digest':'e'*64,'base':'/opt/agentos/releases/managed-base'},'artifacts':[{'name':'validation.log','mime':'text/plain','content':self.log}]}
  self.write('request.json',TARGET.canonical(self.req));self.write('result.json',TARGET.canonical(output))
  receipt={'state':'completed','passed':True,'code':'VALIDATION_PASSED','actionId':self.req['actionId'],'candidateDigest':'a'*64,'validatedCandidateDigest':'a'*64,'requestDigest':TARGET.sha(TARGET.canonical(self.req)),
   'frozenWorkspace':'agentos/ocv'+self.req['actionId'].replace('-',''),'registeredActionSha':TARGET.broker.REGISTERED_HASH,
   'resultDigest':TARGET.sha(TARGET.canonical(output)),'resultRef':str(self.dir/'result.json'),'publishedBaseline':'/opt/agentos/releases/managed-base','validatedReleaseDigest':'e'*64,
   **{k:'f'*64 for k in ['publishedBaselineDigest','publicationProofDigest','validationProofDigest','registeredReceiptSha']}}
  self.write('receipt.json',TARGET.canonical(receipt))
  self.original_lstat=pathlib.Path.lstat;self.original_fstat=os.fstat
 def tearDown(self):self.temp.cleanup()
 def write(self,name,bytes):
  file=self.dir/name;file.write_bytes(bytes);file.chmod(0o600)
 def as_root(self,info,outside=False):
  return types.SimpleNamespace(st_uid=0,st_mode=info.st_mode & ~0o022 if outside else info.st_mode,st_size=info.st_size,st_dev=info.st_dev,st_ino=info.st_ino)
 def export(self):
  root=self.root
  def lstat(path,*args,**kwargs):return self.as_root(self.original_lstat(path,*args,**kwargs),not pathlib.Path(path).is_relative_to(root))
  with patch.object(TARGET,'STATE',self.state),patch.object(TARGET.os,'geteuid',return_value=0),patch.object(pathlib.Path,'lstat',lstat),patch.object(TARGET.os,'fstat',side_effect=lambda fd:self.as_root(self.original_fstat(fd))),patch.object(TARGET.broker,'registered_profile',return_value={'command':'/usr/bin/python3','args':['/opt/agentos-control/agentos-release-action.py','validate']}):
   return TARGET.verified_export(self.req)
 def test_full_real_log_is_preserved_after_scrubbing_with_raw_and_exported_hashes(self):
  value=self.export();clean=base64.b64decode(value['inputs'][1]['base64']).decode()
  self.assertIn('First real build line',clean);self.assertIn('Last real suite line 🚀',clean)
  self.assertGreater(len(clean),6000);self.assertEqual(clean.count('A real canonical test passed'),900)
  self.assertNotIn('privatepass',clean);self.assertNotIn('privatecookie',clean);self.assertNotIn('abcdefghijklmnopqrst',clean)
  self.assertNotIn('jsonprivatevalue',clean);self.assertNotIn('jsonprivateheader',clean)
  self.assertEqual(value['authority']['log']['rawDigest'],TARGET.sha(self.log.encode()))
  self.assertEqual(value['authority']['log']['digest'],TARGET.sha(clean.encode()))
  self.assertEqual(value['authority']['producer']['runId'],self.req['runId']);self.assertFalse(value['authority']['acceptance'])
 def test_repeat_export_is_read_only_and_has_identical_bytes(self):
  before={p.name:p.read_bytes() for p in self.dir.iterdir()}
  one=self.export();two=self.export();self.assertEqual(one,two)
  self.assertEqual(before,{p.name:p.read_bytes() for p in self.dir.iterdir()})
 def test_tampered_result_refused(self):
  self.write('result.json',b'{}')
  with self.assertRaisesRegex(ValueError,'RESULT_CHANGED'):self.export()
 def test_changed_request_unicode_bytes_refused(self):
  self.req['reason']='Outra publicação 🚀'
  with self.assertRaisesRegex(ValueError,'REQUEST_CHANGED'):self.export()
 def test_receipt_source_identity_refused(self):
  receipt=json.loads((self.dir/'receipt.json').read_bytes());receipt['candidateDigest']='0'*64;self.write('receipt.json',TARGET.canonical(receipt))
  with self.assertRaisesRegex(ValueError,'RECEIPT_UNBOUND'):self.export()
 def test_live_or_incomplete_receipt_refused(self):
  receipt=json.loads((self.dir/'receipt.json').read_bytes());receipt['state']='running';self.write('receipt.json',TARGET.canonical(receipt))
  with self.assertRaisesRegex(ValueError,'RECEIPT_UNBOUND'):self.export()
 def test_unproven_published_baseline_refused(self):
  receipt=json.loads((self.dir/'receipt.json').read_bytes());receipt.pop('registeredReceiptSha');self.write('receipt.json',TARGET.canonical(receipt))
  with self.assertRaisesRegex(ValueError,'BASELINE_UNPROVEN'):self.export()
 def test_arbitrary_result_path_refused(self):
  receipt=json.loads((self.dir/'receipt.json').read_bytes());receipt['resultRef']='/etc/passwd';self.write('receipt.json',TARGET.canonical(receipt))
  with self.assertRaisesRegex(ValueError,'RESULT_REF_INVALID'):self.export()
 def test_symlink_or_public_private_store_refused(self):
  (self.dir/'result.json').unlink();(self.dir/'result.json').symlink_to(self.dir/'receipt.json')
  with self.assertRaisesRegex(ValueError,'UNPROTECTED'):self.export()
  (self.dir/'result.json').unlink();self.dir.chmod(0o755)
  with self.assertRaisesRegex(ValueError,'UNPROTECTED'):self.export()
 def test_non_root_cannot_export_private_evidence(self):
  with patch.object(TARGET.os,'geteuid',return_value=1234):
   with self.assertRaisesRegex(ValueError,'PRIVILEGE_REQUIRED'):TARGET.verified_export(self.req)
 def test_no_model_log_or_boolean_is_promoted(self):
  output=json.loads((self.dir/'result.json').read_bytes());output['output']['passed']=False;self.write('result.json',TARGET.canonical(output))
  receipt=json.loads((self.dir/'receipt.json').read_bytes());receipt['resultDigest']=TARGET.sha(TARGET.canonical(output));self.write('receipt.json',TARGET.canonical(receipt))
  with self.assertRaisesRegex(ValueError,'OUTCOME_CHANGED'):self.export()

if __name__=='__main__':unittest.main()
