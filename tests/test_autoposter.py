"""Auto-poster (engine, file posts, channel import, handlers, wiring) - runs tests/ap_harness.py in a subprocess with stub telegram / motor."""
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class AutoposterTests(unittest.TestCase):
    def test_harness(self):
        r = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "ap_harness.py"), ROOT], capture_output=True, text=True, timeout=300)
        self.assertEqual(r.returncode, 0, r.stdout[-3000:] + r.stderr[-6000:])
        self.assertIn("OK ", r.stdout)


if __name__ == "__main__":
    unittest.main()
