"""The REAL handlers.py and main.py wired to the multi-bot router (stubbed Telegram/FastAPI, see routing_*_harness.py)."""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(harness: str):
    r = subprocess.run([sys.executable, "-I", str(ROOT / "tests" / harness), str(ROOT)], env={"PATH": os.environ.get("PATH", "")},
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-3000:]
    return json.loads(r.stdout.strip().splitlines()[-1])


class HandlerRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.o = run("routing_handlers_harness.py")

    def test_overloaded_bot_sends_the_user_to_the_other_bot_and_creates_no_session(self):
        m = self.o["move"]
        self.assertEqual(m["sessions"], 0)
        self.assertEqual(m["url"], "https://t.me/otherbot?start=tok12345")      # same token, so the file link keeps working
        self.assertTrue(any("very busy" in t for t in m["text"]))

    def test_normal_arrival_is_served_here(self):
        self.assertEqual(self.o["serve"], {"sessions": 1, "arrivals": [1002]})

    def test_router_failure_or_slowness_never_blocks_a_delivery(self):
        self.assertEqual(self.o["boom"]["sessions"], 1)                          # database error inside the router
        self.assertEqual(self.o["hang"]["sessions"], 1)                          # router slower than the timeout

    def test_works_without_a_router(self):
        self.assertEqual(self.o["no_router"]["sessions"], 1)
        self.assertEqual(self.o["no_attr"]["sessions"], 1)

    def test_plain_start_without_token_does_not_touch_routing(self):
        self.assertEqual(self.o["plain_start_arrivals"], [])

    def test_menu_taps_and_text_slide_the_sticky_window(self):
        self.assertEqual(self.o["touches"], [1007, 1008])


class GatewayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.o = run("routing_main_harness.py")

    def test_first_click_gets_a_bot_and_a_hardened_signed_cookie_without_user_data(self):
        f = self.o["first"]
        self.assertEqual(f["status"], 200)
        self.assertIn(f["bot"], ("bot1", "bot2"))
        self.assertEqual(f["cookie"], {"key": "sb", "max_age": 900, "httponly": True, "secure": True, "samesite": "lax", "path": "/"})
        self.assertTrue(f["cookie_has_no_user_data"])

    def test_same_browser_stays_on_its_bot_across_many_clicks(self):
        self.assertEqual(self.o["sticky_bots"], [self.o["sticky_expected"]])

    def test_forged_cookies_are_ignored_not_errors(self):
        self.assertEqual(self.o["forged"], [[200, True]] * 3)

    def test_invalid_token_is_refused_before_routing(self):
        self.assertEqual(self.o["bad_token"], 404)

    def test_routing_off_keeps_the_old_behaviour(self):
        self.assertEqual(self.o["router_off"], {"bot": "legacybot", "cookies": 0})


if __name__ == "__main__":
    unittest.main()
