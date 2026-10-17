from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("render-route-intent.py")
NONCE = "a1b2c3d4e5f60718293a4b5c"


class RouteIntentTest(unittest.TestCase):
    def render(self, profile: str, shards: int = 1024) -> tuple[dict, dict]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        route = Path(temporary.name) / "route.json"
        receipt = Path(temporary.name) / "receipt.json"
        subprocess.run(
            [
                "python3", str(SCRIPT), "--profile", profile,
                "--generation", "7", "--nonce", NONCE,
                "--shard-count", str(shards), "--output", str(route),
                "--receipt", str(receipt),
            ],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        return json.loads(route.read_text()), json.loads(receipt.read_text())

    def test_functional_splits_shards_and_uses_generation_boots(self) -> None:
        route, receipt = self.render("functional")
        self.assertEqual(route["player"]["shard_count"], 1024)
        self.assertEqual(route["farm"]["shard_count"], 1024)
        self.assertEqual({item["holder_index"] for item in route["player"]["routes"]}, {0, 1})
        self.assertEqual({item["holder_index"] for item in route["farm"]["routes"]}, {2, 3})
        self.assertEqual(route["holders"][0]["boot_id"], f"player-svr-0-functional-g7-{NONCE}")
        self.assertIn(".farm-l2-r2.svc.cluster.local:", route["holders"][0]["endpoint"])
        self.assertEqual(receipt["status"], "PLACEMENT_INTENT_NOT_AUTHORITY_INITIALIZED")
        self.assertFalse(receipt["activatable"])
        self.assertTrue(all(item["lease_expire_at_ms"] == 0 for item in route["player"]["routes"]))

    def test_single_target_profiles_keep_other_domain_split(self) -> None:
        player, _ = self.render("l2-player", 4)
        self.assertEqual({item["holder_index"] for item in player["player"]["routes"]}, {0})
        self.assertEqual({item["holder_index"] for item in player["farm"]["routes"]}, {2, 3})
        farm, _ = self.render("l2-farm", 4)
        self.assertEqual({item["holder_index"] for item in farm["farm"]["routes"]}, {2})
        self.assertEqual({item["holder_index"] for item in farm["player"]["routes"]}, {0, 1})

    def test_rejects_unreviewed_shard_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            process = subprocess.run(
                ["python3", str(SCRIPT), "--profile", "functional", "--generation", "1",
                 "--nonce", NONCE, "--shard-count", "16", "--output", str(Path(temporary) / "route.json")],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("only render-only 4 or frozen local 1024", process.stderr)


if __name__ == "__main__":
    unittest.main()
