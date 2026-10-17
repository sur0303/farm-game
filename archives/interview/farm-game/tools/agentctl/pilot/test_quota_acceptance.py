"""Acceptance test for the synthetic quota pilot bug.

Not discovered by `unittest discover -s tools/agentctl/tests`.
Pilot Manifests run:

    python3 -m unittest tools.agentctl.pilot.test_quota_acceptance -v

from the repo root, or the absolute-module form used in the pilot Manifest.
"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

PILOT_DIR = Path(__file__).resolve().parent
if str(PILOT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(PILOT_DIR.parent))

from pilot.quota import remaining  # noqa: E402


class PilotQuotaAcceptanceTest(unittest.TestCase):
    def test_remaining_exact_and_zero_when_exhausted(self):
        self.assertEqual(remaining(5, 2), 3)
        self.assertEqual(remaining(3, 3), 0)
        self.assertEqual(remaining(1, 0), 1)


if __name__ == "__main__":
    unittest.main()
