import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from collections import namedtuple

spec=importlib.util.spec_from_file_location('backup_preflight',Path(__file__).resolve().parents[2]/'scripts/backup-preflight.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class BackupCapacity(unittest.TestCase):
    def test_reserves_operating_space_and_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'snapshot';folder.mkdir()
            with (folder/'database.dump').open('wb') as file:file.truncate(25*module.GIB)
            with (folder/'artifacts.tar.gz').open('wb') as file:file.truncate(module.GIB)
            self.assertGreater(module.required_bytes(root),38*module.GIB)

    def test_refuses_current_low_disk_without_creating_a_backup(self):
        Usage=namedtuple('Usage','total used free')
        with patch.object(module,'required_bytes',return_value=40*module.GIB),patch.object(module.shutil,'disk_usage',return_value=Usage(1000,980,20*module.GIB)):
            self.assertEqual(module.main(),1)

if __name__=='__main__':unittest.main()
