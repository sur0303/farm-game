#!/usr/bin/env python3
import importlib.machinery
import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("patch-dynamic-opportunity-mode")
LOADER = importlib.machinery.SourceFileLoader(
    "patch_dynamic_opportunity_mode",
    str(SCRIPT),
)
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
MODE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODE
LOADER.exec_module(MODE)


class DynamicOpportunityModeContractTest(unittest.TestCase):
    def test_activation_receipts_do_not_claim_traffic_verification(self) -> None:
        for stage in MODE.STAGES.values():
            self.assertIn("ACTIVATED_NOT_TRAFFIC_VERIFIED", stage.status)

    def test_final_stage_is_bound_to_complete_production_mode_contract(self) -> None:
        self.assertEqual(MODE.EXPECTED_FINAL_PRODUCTION_MODES, {
            "hint_transport": "direct",
            "header_read_mode": "narrow",
            "schedule_due_index": "dedicated_only",
            "friend_navigation": "navigator-v3",
            "candidate_window": "top-k",
            "wakeup": "active",
            "wakeup_poll_interval": "1s",
            "worker_assignment": "route",
            "actor_persistence": "authority",
            "authority_membership": "composed",
            "friendship_deployment": "remote_player",
            "gate_transport": "websocket",
            "presence_cleanup": "enabled",
            "redis_logical_domains": ["authority", "runtime", "projection"],
            "theft_prf_mode": "production",
        })

    def test_live_platform_config_requires_complete_production_modes(self) -> None:
        class Kubectl:
            def __init__(self, data):
                self.data = data

            def platform_config_map(self):
                return {"metadata": {"uid": "platform-uid"}, "data": self.data}

        complete = dict(MODE.EXPECTED_PLATFORM_MODES)
        value = MODE.verify_platform_modes(Kubectl(complete))
        self.assertEqual(value["metadata"]["uid"], "platform-uid")
        complete["FRIEND_NAVIGATION_WINDOW_MODE"] = "full-tail"
        with self.assertRaisesRegex(ValueError, "platform modes mismatch"):
            MODE.verify_platform_modes(Kubectl(complete))

    def test_traffic_receipt_requires_account_product_e2e(self) -> None:
        source = SCRIPT.read_text()
        self.assertIn("DYNAMIC_H5_PRODUCT_E2E_VERIFIED", source)
        self.assertIn("h5_runtime_account_auth_verified", source)
        self.assertIn("h5_runtime_packaged_image_loaded", source)
        self.assertNotIn('"h5_runtime_product_e2e": "NOT_RUN"', source)

    def test_mode_ladder_only_moves_forward_one_stage(self) -> None:
        MODE.validate_mode_transition(
            MODE.STAGES["compatibility"],
            "compatibility",
            "full",
            "pubsub",
        )
        MODE.validate_mode_transition(
            MODE.STAGES["shadow"],
            "compatibility",
            "full",
            "pubsub",
        )
        MODE.validate_mode_transition(
            MODE.STAGES["final"],
            "shadow",
            "narrow-shadow",
            "shadow-direct",
        )
        MODE.validate_mode_transition(
            MODE.STAGES["shadow"],
            "shadow",
            "narrow-shadow",
            "shadow-direct",
        )
        invalid = (
            ("final", "compatibility", "full", "pubsub"),
            ("compatibility", "final", "narrow", "direct"),
            ("shadow", "compatibility", "full", "direct"),
        )
        for requested, current, header, hint in invalid:
            with self.subTest(requested=requested, current=current):
                with self.assertRaises(ValueError):
                    MODE.validate_mode_transition(
                        MODE.STAGES[requested],
                        current,
                        header,
                        hint,
                    )

    def test_metric_value_requires_exact_labels(self) -> None:
        metrics = "\n".join((
            '# HELP mode Current mode.',
            'mode{mode="full"} 0',
            'mode{mode="narrow-shadow"} 1',
            "counter_total 0",
        ))
        self.assertEqual(
            MODE.metric_value(
                metrics,
                "mode",
                {"mode": "narrow-shadow"},
            ),
            1,
        )
        self.assertEqual(MODE.metric_value(metrics, "counter_total"), 0)
        with self.assertRaises(ValueError):
            MODE.metric_value(metrics, "mode", {"mode": "direct"})

    def test_behavior_deltas_require_stage_specific_product_traffic(self) -> None:
        baseline = {
            "farm-svr-0": {
                "header_read_mode": "narrow-shadow",
                "mode_calls_total": 2,
                "shadow_mismatch_total": 0,
            },
            "farm-svr-1": {
                "header_read_mode": "narrow-shadow",
                "mode_calls_total": 3,
                "shadow_mismatch_total": 0,
            },
            "farm-worker-a-0": {
                "hint_transport": "shadow-direct",
                "pubsub_published_total": 4,
                "pubsub_errors_total": 0,
                "direct_suppressed_total": 4,
                "direct_accepted_total": 0,
                "direct_unavailable_total": 0,
                "direct_connection_not_found_total": 0,
                "direct_queue_full_total": 0,
            },
            "farm-worker-b-0": {
                "hint_transport": "shadow-direct",
                "pubsub_published_total": 5,
                "pubsub_errors_total": 0,
                "direct_suppressed_total": 5,
                "direct_accepted_total": 0,
                "direct_unavailable_total": 0,
                "direct_connection_not_found_total": 0,
                "direct_queue_full_total": 0,
            },
        }
        current = {
            name: dict(value)
            for name, value in baseline.items()
        }
        with self.assertRaises(MODE.TrafficNotReady):
            MODE.behavior_deltas(
                MODE.STAGES["shadow"],
                baseline,
                current,
            )
        current["farm-svr-0"]["mode_calls_total"] = 3
        current["farm-worker-a-0"]["pubsub_published_total"] = 5
        current["farm-worker-a-0"]["direct_suppressed_total"] = 5
        deltas = MODE.behavior_deltas(
            MODE.STAGES["shadow"],
            baseline,
            current,
        )
        self.assertEqual(deltas["header_mode_calls"], 1)
        self.assertEqual(deltas["pubsub_published"], 1)
        self.assertEqual(deltas["direct_suppressed"], 1)

        current["farm-worker-b-0"]["direct_unavailable_total"] = 1
        with self.assertRaisesRegex(MODE.TrafficViolation, "negative delivery"):
            MODE.behavior_deltas(
                MODE.STAGES["shadow"],
                baseline,
                current,
            )

    def test_pod_summary_requires_exact_ready_zero_restart_topology(self) -> None:
        items = []
        for app, count in MODE.EXPECTED_APPS.items():
            for index in range(count):
                name = f"{app}-{index}"
                items.append({
                    "metadata": {
                        "name": name,
                        "uid": f"uid-{name}",
                        "labels": {"app.kubernetes.io/name": app},
                    },
                    "status": {
                        "containerStatuses": [{
                            "ready": True,
                            "restartCount": 0,
                        }],
                        "initContainerStatuses": [{
                            "restartCount": 0,
                        }],
                    },
                })
        summary = MODE.summarize_pods({"items": items})
        self.assertEqual(len(summary), 9)
        items[0]["status"]["containerStatuses"][0]["restartCount"] = 1
        with self.assertRaisesRegex(ValueError, "restart"):
            MODE.summarize_pods({"items": items})


if __name__ == "__main__":
    unittest.main()
