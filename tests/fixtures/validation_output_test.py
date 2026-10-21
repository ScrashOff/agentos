from pathlib import Path
import importlib.util
import unittest
spec=importlib.util.spec_from_file_location('validation_output',Path(__file__).resolve().parents[2]/'scripts/agentos-release-action.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class ValidationOutput(unittest.TestCase):
    complete='vite\n✓ built in 1.21s\n Test Files 3 passed (3)\n Tests 10 passed | 2 skipped (12)\n Duration 9.12s (tests 5s)\n'
    def test_accepts_complete_ansi_report(self):
        self.assertTrue(module.validation_complete('\x1b[32m'+self.complete+'\x1b[0m'))
    def test_rejects_interrupted_zero_exit_without_summary(self):
        self.assertFalse(module.validation_complete('✓ built in 1.21s\n ✓ some.test.ts (10 tests)\n'))
    def test_rejects_missing_test_or_build_result(self):
        self.assertFalse(module.validation_complete(self.complete.replace(' Test Files 3 passed (3)\n','')))
        self.assertFalse(module.validation_complete(self.complete.replace('✓ built in 1.21s','')))
    def test_rejects_failures_or_wrong_denominator(self):
        self.assertFalse(module.validation_complete(self.complete.replace('3 passed (3)','2 passed | 1 failed (3)')))
        self.assertFalse(module.validation_complete(self.complete.replace('10 passed | 2 skipped (12)','10 passed (12)')))
    def test_rejects_activity_after_a_nested_success_footer(self):
        self.assertFalse(module.validation_complete(self.complete+'another.test.ts running\n'))
if __name__=='__main__':unittest.main()
