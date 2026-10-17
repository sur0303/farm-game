#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts/local-k8s/materialize-dynamic-route-profile.py"
WORKER_EXTRACTOR = REPO / "scripts/local-k8s/extract_dynamic_worker_route_holders.py"
PRODUCT_ENV_DEFAULTS = {
    "SCALEPLAN_MODE": "off",
    "SCALEPLAN_TOKENREVIEW_RBAC": "false",
    "SCALEPLAN_ROLLOUT_ENABLED": "false",
    "FARM_SCHEDULE_DUE_INDEX_MODE": "dedicated_only",
    "FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY": "true",
    "FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED": "true",
    "FARM_OPPORTUNITY_HEADER_READ_MODE": "full",
    "FARM_OPPORTUNITY_WAKEUP_ENABLED": "true",
    "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE": "route",
    "FARM_OPPORTUNITY_HINT_TRANSPORT": "pubsub",
}


def materialize(root: str, run_id: str, redis_env: str, overrides: dict[str, str] | None = None):
    """Materializes one profile and returns its manifests indexed by kind/name
    together with the profile receipt."""
    lab = Path(root)
    receipts = lab / "receipts"
    receipts.mkdir(exist_ok=True)
    head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
    (receipts / f"route-runtime-image-{run_id}.json").write_text(json.dumps({
        "status": "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED",
        "run_id": run_id,
        "source_head": head,
        "registry_ref": "127.0.0.1:5001/farm/route-runtime@sha256:" + "1" * 64,
    }))
    (receipts / f"dynamic-h5-image-{run_id}.json").write_text(json.dumps({
        "status": "PUBLISHED_LOCAL_DYNAMIC_H5_NOT_DEPLOYED",
        "run_id": run_id,
        "source_commit": "fc7be57174e9ff4f18e6dfeb5fc1610d681172cf",
        "source_tree": "cfeb47072287a2f8533dc7366481c006c4010012",
        "build_mode": "web",
        "registry_ref": "127.0.0.1:5001/farm/h5@sha256:" + "2" * 64,
        "cluster_applied": False,
        "company_redis_contacted": False,
        "capacity_eligible": False,
    }))
    env = os.environ.copy()
    env.update({
        "FARM_L2_LAB_ROOT": str(lab),
        "ROUTE_RUN_ID": run_id,
        "REDIS_NAMESPACE_ENVIRONMENT": redis_env,
        "REDIS_NAMESPACE_ZONE": "z1",
        **PRODUCT_ENV_DEFAULTS,
        **(overrides or {}),
    })
    subprocess.run([str(SCRIPT)], cwd=REPO, env=env, check=True, stdout=subprocess.DEVNULL)
    profile = json.loads((lab / f"runtime/dynamic-route-{run_id}/profile.json").read_text())
    by_kind_name = {(item["kind"], item["metadata"]["name"]): item for item in profile["items"]}
    receipt = json.loads((receipts / f"dynamic-route-profile-{run_id}.json").read_text())
    return by_kind_name, receipt


EXPECTED_SCALEPLAN_ALLOWLIST = [
    {
        "realm": "classic-farm-dynamic-local",
        "domain": "PLAYER",
        "namespace": "farm-route-dynamic",
        "kind": "StatefulSet",
        "name": "player-svr",
    },
    {
        "realm": "classic-farm-dynamic-local",
        "domain": "FARM",
        "namespace": "farm-route-dynamic",
        "kind": "StatefulSet",
        "name": "farm-svr",
    },
]


def expected_scaleplan_config(
    mode: str, tokenreview_rbac: bool, holder_discovery_mode: str
) -> dict:
    return {
        "schema_version": "scaleplan-render-config.v1",
        "mode": mode,
        "realm": "classic-farm-dynamic-local",
        "route_domains": ["PLAYER", "FARM"],
        "public_domain": "farm.dynamic.local",
        "workload_allowlist": EXPECTED_SCALEPLAN_ALLOWLIST,
        "tokenreview_rbac": tokenreview_rbac,
        "holder_discovery_mode": holder_discovery_mode,
        "holder_identity_transport": "K8S_INFORMER_HOLDERCONTROL_TOKENREVIEW_V1",
        "auto_reconcile_interval": "5s",
        "route_failover_reconcile_interval": "1s",
        "route_failover_max_plans_per_pass": 32,
        "route_failover_max_authority_reads_per_pass": 128,
        "safe_desired_annotation": "scaleplan.farm/safe-desired-replicas",
        "rollout_enabled": False,
        "rollout_annotation": "scaleplan.farm/managed-rollout-state",
        "rollout_intent_annotation": "scaleplan.farm/rollout-intent",
    }


def expected_scaleplan_hash(
    mode: str, tokenreview_rbac: bool, holder_discovery_mode: str
) -> str:
    return hashlib.sha256(
        json.dumps(
            expected_scaleplan_config(mode, tokenreview_rbac, holder_discovery_mode),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class DynamicRouteProfileTest(unittest.TestCase):
    def test_materialized_profile_is_remote_watch_four_by_four(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            lab = Path(root)
            receipts = lab / "receipts"
            receipts.mkdir()
            run_id = "dynamic-profile-test"
            head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
            runtime_ref = "127.0.0.1:5001/farm/route-runtime@sha256:" + "1" * 64
            h5_ref = "127.0.0.1:5001/farm/h5@sha256:" + "2" * 64
            (receipts / f"route-runtime-image-{run_id}.json").write_text(json.dumps({
                "status": "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED", "run_id": run_id,
                "source_head": head, "registry_ref": runtime_ref,
            }))
            (receipts / f"dynamic-h5-image-{run_id}.json").write_text(json.dumps({
                "status": "PUBLISHED_LOCAL_DYNAMIC_H5_NOT_DEPLOYED",
                "run_id": run_id,
                "source_commit": "fc7be57174e9ff4f18e6dfeb5fc1610d681172cf",
                "source_tree": "cfeb47072287a2f8533dc7366481c006c4010012",
                "attestation_commit": None,
                "build_mode": "web",
                "registry_ref": h5_ref,
                "cluster_applied": False,
                "company_redis_contacted": False,
                "capacity_eligible": False,
            }))
            env = os.environ.copy()
            env.update({
                "FARM_L2_LAB_ROOT": str(lab), "ROUTE_RUN_ID": run_id,
                "REDIS_NAMESPACE_ENVIRONMENT": "dynamicprofiletest", "REDIS_NAMESPACE_ZONE": "z1",
                **PRODUCT_ENV_DEFAULTS,
            })
            subprocess.run([str(SCRIPT)], cwd=REPO, env=env, check=True, stdout=subprocess.DEVNULL)
            output_dir = lab / f"runtime/dynamic-route-{run_id}"
            profile_path = output_dir / "profile.json"
            profile = json.loads(profile_path.read_text())
            items = profile["items"]
            by_kind_name = {(item["kind"], item["metadata"]["name"]): item for item in items}
            controller = by_kind_name[("Deployment", "route-controller")]
            controller_service = by_kind_name[("Service", "route-controller")]
            self.assertEqual(controller["spec"]["replicas"], 1)
            self.assertEqual(controller["spec"]["strategy"], {"type": "Recreate"})
            self.assertTrue(controller_service["spec"]["publishNotReadyAddresses"])
            controller_env = {e["name"]: e for e in controller["spec"]["template"]["spec"]["containers"][0]["env"]}
            self.assertEqual(controller_env["ROUTE_CONTROLLER_LEASE_PROJECTION_MODE"]["value"], "disabled")
            self.assertEqual(controller_env["SCALEPLAN_MODE"]["value"], "off")
            self.assertEqual(controller["spec"]["template"]["spec"]["automountServiceAccountToken"], False)
            self.assertNotIn("serviceAccountName", controller["spec"]["template"]["spec"])
            self.assertEqual(controller_env["ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES"]["value"], "route-client.farm-route-dynamic.local")
            self.assertEqual(controller_env["ROUTE_CONTROLLER_PLAN_READ_IDENTITIES"]["value"], "route-operator.farm-route-dynamic.local")
            self.assertEqual(controller_env["ROUTE_CONTROLLER_OPERATOR_IDENTITIES"]["value"], "route-operator.farm-route-dynamic.local")
            platform = by_kind_name[("ConfigMap", "farm-staging-platform")]
            self.assertEqual(platform["data"]["ACTOR_ROUTE_STARTUP_TIMEOUT"], "120s")
            expected_redis_keys = {
                "FARM_REDIS_URL": "redis-url",
                "FARM_AUTHORITY_REDIS_URL": "authority-redis-url",
                "FARM_RUNTIME_REDIS_URL": "runtime-redis-url",
                "FARM_PROJECTION_REDIS_URL": "projection-redis-url",
            }
            for env_name, secret_key in expected_redis_keys.items():
                self.assertEqual(
                    controller_env[env_name]["valueFrom"]["secretKeyRef"],
                    {"name": "farm-dynamic-secrets", "key": secret_key},
                )
            self.assertEqual(controller_env["FARM_REDIS_CLUSTER"]["value"], "1")
            self.assertEqual(
                controller_env["FARM_REDIS_CLUSTER_URLS"]["value"],
                ",".join(
                    f"redis://farm-local-redis-{index}.farm-local-redis-headless.farm-l2-r2-deps.svc.cluster.local:6379/0"
                    for index in range(3)
                ),
            )
            controller_tls = next(v for v in controller["spec"]["template"]["spec"]["volumes"] if v["name"] == "route-control-tls")
            self.assertEqual({x["key"] for x in controller_tls["secret"]["items"]}, {"ca.crt", "route-controller.crt", "route-controller.key"})
            self.assertNotIn("NetworkPolicy", {item["kind"] for item in items})
            for name, replicas in (("player-svr", 2), ("farm-svr", 2)):
                holder = by_kind_name[("StatefulSet", name)]
                self.assertEqual(holder["spec"]["replicas"], replicas)
                pod = holder["spec"]["template"]["spec"]
                container = pod["containers"][0]
                env_by_name = {e["name"]: e for e in container["env"]}
                self.assertEqual(env_by_name["HOLDER_PROCESS_BOOT_MODE"]["value"], "pod-uid-then-random")
                self.assertEqual(env_by_name["ACTOR_HOLDER_IDENTITY_MODE"]["value"], "kubernetes-process-v1")
                self.assertEqual(
                    env_by_name["ACTOR_HOLDER_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"],
                    "metadata.uid",
                )
                self.assertEqual(env_by_name["ACTOR_HOLDER_WORKLOAD_UID"]["value"], "PENDING_STAGED_APPLY")
                self.assertEqual(
                    env_by_name["ACTOR_HOLDER_ORDINAL"]["valueFrom"]["fieldRef"]["fieldPath"],
                    "metadata.labels['apps.kubernetes.io/pod-index']",
                )
                self.assertNotIn("ACTOR_HOLDER_BOOT_ID", env_by_name)
                self.assertNotIn("HOLDER_ALLOW_INITIAL_SERVING", env_by_name)
                self.assertNotIn("farm-static-route", {v["name"] for v in pod.get("volumes", [])})
                for env_name, secret_key in expected_redis_keys.items():
                    self.assertEqual(
                        env_by_name[env_name]["valueFrom"]["secretKeyRef"],
                        {"name": "farm-dynamic-secrets", "key": secret_key},
                    )
                self.assertEqual(env_by_name["FARM_REDIS_CLUSTER"]["value"], "1")
                self.assertTrue(env_by_name["FARM_REDIS_CLUSTER_URLS"]["value"].startswith("redis://farm-local-redis-0."))
                if name == "farm-svr":
                    self.assertEqual(env_by_name["FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED"]["value"], "true")
                    self.assertEqual(env_by_name["FARM_OPPORTUNITY_HEADER_READ_MODE"]["value"], "full")
                    self.assertEqual(
                        env_by_name["FARM_OPPORTUNITY_HEADER_MODE_FILE"]["value"],
                        "/etc/farm/opportunity-mode/header-read-mode",
                    )
                    self.assertEqual(
                        env_by_name["FARM_OPPORTUNITY_HEADER_MODE_POLL_INTERVAL"]["value"],
                        "1s",
                    )
                    self.assertIn(
                        "farm-opportunity-mode",
                        {v["name"] for v in pod["volumes"]},
                    )
                tls = next(v for v in pod["volumes"] if v["name"] == "route-control-tls")
                self.assertEqual({x["key"] for x in tls["secret"]["items"]}, {"ca.crt", "route-client.crt", "route-client.key", "holder-server.crt", "holder-server.key"})
                self.assertEqual([x["name"] for x in pod["initContainers"]], ["dynamic-route-controller-barrier"])
                barrier = " ".join(pod["initContainers"][0]["args"])
                subprocess.run(["/bin/sh", "-n", "-c", barrier], check=True)
                self.assertIn("route-controller", barrier)
                self.assertNotIn("/readyz", barrier)
                self.assertIn("/healthz", barrier)
                self.assertIn("wget", barrier)
                self.assertIn("player-svr-0.player-svr-headless", barrier)
                self.assertIn("farm-svr-1.farm-svr-headless", barrier)
            for service_name in ("player-svr-headless", "farm-svr-headless"):
                self.assertTrue(by_kind_name[("Service", service_name)]["spec"]["publishNotReadyAddresses"])
            self.assertIn({"name": "health", "port": 9101, "targetPort": "health", "protocol": "TCP"}, by_kind_name[("Service", "player-svr")]["spec"]["ports"])
            self.assertIn({"name": "health", "port": 9091, "targetPort": "health", "protocol": "TCP"}, by_kind_name[("Service", "farm-svr")]["spec"]["ports"])
            self.assertEqual(by_kind_name[("Deployment", "farm-gate")]["spec"]["replicas"], 1)
            gate_container = by_kind_name[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["containers"][0]
            gate_env = {e["name"]: e for e in gate_container["env"]}
            worker_envs = {}
            for worker_name in ("farm-worker-a", "farm-worker-b"):
                worker = by_kind_name[("Deployment", worker_name)]
                self.assertEqual(worker["spec"]["replicas"], 1)
                self.assertEqual(worker["spec"]["strategy"], {"type": "Recreate"})
                self.assertEqual(
                    worker["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"],
                    worker_name,
                )
                worker_envs[worker_name] = {
                    e["name"]: e
                    for e in worker["spec"]["template"]["spec"]["containers"][0]["env"]
                }
                self.assertEqual(worker_envs[worker_name]["WORKER_ID"]["value"], worker_name)
            for env_by_name in (*worker_envs.values(), gate_env):
                for env_name, secret_key in expected_redis_keys.items():
                    self.assertEqual(
                        env_by_name[env_name]["valueFrom"]["secretKeyRef"],
                        {"name": "farm-dynamic-secrets", "key": secret_key},
                    )
                self.assertEqual(env_by_name["FARM_REDIS_CLUSTER"]["value"], "1")
                self.assertIn("farm-local-redis-0.farm-local-redis-headless.farm-l2-r2-deps", env_by_name["FARM_REDIS_CLUSTER_URLS"]["value"])
                self.assertIn("farm-local-redis-2.farm-local-redis-headless.farm-l2-r2-deps", env_by_name["FARM_REDIS_CLUSTER_URLS"]["value"])
            for worker_name, worker_env in worker_envs.items():
                self.assertEqual(
                    worker_env["PERMIT_SECRET"]["valueFrom"]["secretKeyRef"],
                    {"name": "farm-dynamic-secrets", "key": "permit-secret"},
                )
                self.assertEqual(worker_env["FARM_OPPORTUNITY_WAKEUP_ENABLED"]["value"], "true")
                self.assertEqual(worker_env["FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE"]["value"], "route")
                self.assertEqual(worker_env["FARM_OPPORTUNITY_HINT_TRANSPORT"]["value"], "pubsub")
                self.assertEqual(
                    worker_env["FARM_OPPORTUNITY_HINT_MODE_FILE"]["value"],
                    "/etc/farm/opportunity-mode/hint-transport",
                )
                self.assertEqual(
                    worker_env["FARM_OPPORTUNITY_HINT_MODE_POLL_INTERVAL"]["value"],
                    "1s",
                )
                self.assertEqual(worker_env["FARM_WORKER_AUTHORITY_RENEW_TTL"]["value"], "60s")
                self.assertEqual(worker_env["FARM_WORKER_AUTHORITY_RENEW_INTERVAL"]["value"], "10s")
                self.assertEqual(worker_env["FARM_WORKER_AUTHORITY_RENEW_TIMEOUT"]["value"], "2s")
                self.assertEqual(worker_env["FARM_WORKER_ROUTE_HOLDER_ONLY"]["value"], "true")
                self.assertEqual(
                    worker_env["FARM_WORKER_LOGIC_ENDPOINT"]["value"],
                    f"{worker_name}.farm-route-dynamic.svc.cluster.local:9201",
                )
                self.assertEqual(
                    worker_env["FARM_WORKER_HOLDER_CONTROL_ENDPOINT"]["value"],
                    f"{worker_name}.farm-route-dynamic.svc.cluster.local:9201",
                )
                self.assertEqual(
                    worker_env["FARM_WORKER_HOLDER_TLS_DNS_IDENTITY"]["value"],
                    "holder-control.farm-route-dynamic.svc.cluster.local",
                )
            self.assertEqual(gate_env["GATE_DOWNLINK_WRITER_MAX_ITEMS"]["value"], "256")
            self.assertEqual(gate_env["GATE_DOWNLINK_WRITER_MAX_BYTES"]["value"], "1048576")
            self.assertEqual(gate_env["GATE_DOWNLINK_HINT_MAX_ITEMS"]["value"], "32")
            self.assertEqual(gate_env["GATE_DOWNLINK_HINT_MAX_BYTES"]["value"], "65536")
            self.assertEqual(gate_env["GATE_DOWNLINK_DRAIN_QUANTUM"]["value"], "64")
            self.assertEqual(
                by_kind_name[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["containers"][0]["resources"],
                {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "4", "memory": "4Gi"}},
            )
            self.assertEqual(
                by_kind_name[("Deployment", "farm-worker-a")]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"],
                {"cpu": "1", "memory": "768Mi"},
            )
            gate_tls = next(v for v in by_kind_name[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["volumes"] if v["name"] == "route-control-tls")
            for worker_name in worker_envs:
                worker_pod = by_kind_name[("Deployment", worker_name)]["spec"]["template"]["spec"]
                worker_container = worker_pod["containers"][0]
                worker_tls = next(v for v in worker_pod["volumes"] if v["name"] == "route-control-tls")
                self.assertEqual({x["key"] for x in worker_tls["secret"]["items"]}, {"ca.crt", "route-client.crt", "route-client.key"})
                mode_volume = next(
                    v for v in worker_pod["volumes"]
                    if v["name"] == "farm-opportunity-mode"
                )
                self.assertEqual(
                    mode_volume["configMap"]["name"],
                    "farm-opportunity-mode",
                )
                mode_mount = next(
                    mount for mount in worker_container["volumeMounts"]
                    if mount["name"] == "farm-opportunity-mode"
                )
                self.assertEqual(
                    mode_mount,
                    {
                        "name": "farm-opportunity-mode",
                        "mountPath": "/etc/farm/opportunity-mode",
                        "readOnly": True,
                    },
                )
                self.assertEqual(worker_container["readinessProbe"]["httpGet"], {"path": "/readyz", "port": 9201})
                self.assertEqual(worker_container["livenessProbe"]["httpGet"], {"path": "/healthz", "port": 9201})
                self.assertEqual(
                    by_kind_name[("Service", worker_name)]["spec"]["selector"],
                    {"app.kubernetes.io/name": worker_name},
                )
            self.assertEqual({x["key"] for x in gate_tls["secret"]["items"]}, {"ca.crt", "route-client.crt", "route-client.key", "gate-verifier.crt", "gate-verifier.key"})
            self.assertEqual(by_kind_name[("Deployment", "farm-h5")]["spec"]["replicas"], 1)
            platform = by_kind_name[("ConfigMap", "farm-staging-platform")]["data"]
            self.assertEqual(
                by_kind_name[("ConfigMap", "farm-opportunity-mode")]["data"],
                {
                    "header-read-mode": "full",
                    "hint-transport": "pubsub",
                    "stage": "compatibility",
                },
            )
            self.assertEqual(platform["ACTOR_ASSIGNMENT_MODE"], "remote-watch")
            self.assertEqual(platform["AUTHORITY_MEMBERSHIP_MODE"], "composed")
            self.assertEqual(platform["FARM_SCHEDULE_DUE_INDEX_MODE"], "dedicated_only")
            self.assertEqual(platform["FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY"], "true")
            self.assertEqual(
                {
                    name: platform[name]
                    for name in (
                        "FARM_FRIEND_NAVIGATION_V3_ENABLED",
                        "FRIEND_NAVIGATION_WINDOW_MODE",
                        "FRIEND_NAVIGATION_WINDOW_SIZE",
                        "FARM_OPPORTUNITY_WAKEUP_ENABLED",
                        "FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL",
                        "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE",
                        "GATE_TRANSPORT",
                        "GATE_BOOT_REAPER_MODE",
                        "THEFT_PRF_MODE",
                    )
                },
                {
                    "FARM_FRIEND_NAVIGATION_V3_ENABLED": "1",
                    "FRIEND_NAVIGATION_WINDOW_MODE": "top-k",
                    "FRIEND_NAVIGATION_WINDOW_SIZE": "32",
                    "FARM_OPPORTUNITY_WAKEUP_ENABLED": "true",
                    "FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL": "1s",
                    "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE": "route",
                    "GATE_TRANSPORT": "websocket",
                    "GATE_BOOT_REAPER_MODE": "enabled",
                    "THEFT_PRF_MODE": "production",
                },
            )
            expected_prefixes = {
                "WORKFLOW_STORE_PREFIX": "workflow", "PLAYER_STORE_PREFIX": "player", "FARM_STORE_PREFIX": "farm",
                "SHARE_STORE_PREFIX": "share", "WEB_ACCOUNT_STORE_PREFIX": "web-account",
                "THEFT_GUARD_SNAPSHOT_PREFIX": "theft-guard", "THEFT_TARGET_BINDING_PREFIX": "theft-target",
            }
            for name, suffix in expected_prefixes.items():
                self.assertEqual(platform[name], f"farm:dynamicprofiletest:z1:dynamic:{suffix}:v1")
            self.assertEqual(platform["GATE_WS_ALLOWED_ORIGINS"], "farm.dynamic.local")
            self.assertEqual(platform["GATE_WS_RATE_PER_SEC"], "5000")
            self.assertEqual(platform["GATE_WS_RATE_BURST"], "20000")
            self.assertEqual(platform["GATE_PREAUTH_MAX_CONNECTIONS_PER_IP"], "16384")
            self.assertEqual(platform["GATE_PREAUTH_MAX_BYTES_PER_IP"], "1073741824")
            self.assertEqual(platform["GATE_PREAUTH_MAX_CONNECTIONS"], "4096")
            self.assertEqual(platform["GATE_PREAUTH_MAX_BYTES_POD"], "67108864")
            self.assertEqual(platform["GATE_WS_TRUSTED_PROXIES"], "")
            self.assertEqual(platform["GATE_HTTP_TRUSTED_PROXIES"], "")
            self.assertEqual(platform["WSS_ENDPOINT"], "wss://farm.dynamic.local/ws")
            self.assertEqual(platform["H5_PUBLIC_ORIGIN"], "https://farm.dynamic.local")
            self.assertEqual(platform["H5_PUBLIC_WSS"], "wss://farm.dynamic.local/ws")
            h5 = by_kind_name[("Deployment", "farm-h5")]
            h5_pod = h5["spec"]["template"]["spec"]
            self.assertFalse(h5_pod["automountServiceAccountToken"])
            self.assertTrue(h5_pod["securityContext"]["runAsNonRoot"])
            self.assertEqual(h5_pod["securityContext"]["runAsUser"], 101)
            self.assertEqual(h5_pod["securityContext"]["runAsGroup"], 101)
            self.assertEqual(h5_pod["securityContext"]["fsGroup"], 101)
            self.assertEqual(h5_pod["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
            h5_container = h5_pod["containers"][0]
            self.assertEqual([x["name"] for x in h5_pod["initContainers"]], ["dynamic-gate-ready-barrier"])
            self.assertEqual(h5_pod["initContainers"][0]["image"], h5_ref)
            self.assertEqual(h5_container["image"], h5_ref)
            h5_barrier = " ".join(h5_pod["initContainers"][0]["args"])
            subprocess.run(["/bin/sh", "-n", "-c", h5_barrier], check=True)
            self.assertIn("farm-gate.farm-route-dynamic.svc.cluster.local:8080/readyz", h5_barrier)
            self.assertTrue(h5_container["securityContext"]["readOnlyRootFilesystem"])
            self.assertEqual(h5_container["securityContext"]["capabilities"], {"drop": ["ALL"]})
            mounts = {m["mountPath"] for m in h5_container["volumeMounts"]}
            self.assertTrue({"/run", "/var/cache/nginx", "/tmp"}.issubset(mounts))

            holders = json.loads((output_dir / "holders.json").read_text())["items"]
            control_plane = json.loads((output_dir / "control-plane.json").read_text())["items"]
            final_control_plane = json.loads((output_dir / "final-control-plane.json").read_text())["items"]
            business = json.loads((output_dir / "business.json").read_text())["items"]
            self.assertEqual({(x["kind"], x["metadata"]["name"]) for x in control_plane}, {
                ("Service", "route-controller"), ("Deployment", "route-controller")})
            self.assertEqual({(x["kind"], x["metadata"]["name"]) for x in final_control_plane}, {
                ("Service", "route-controller"), ("Deployment", "route-controller")})
            final_controller = next(x for x in final_control_plane if x["kind"] == "Deployment")
            final_env = {e["name"]: e["value"] for e in final_controller["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in e}
            self.assertEqual(final_env["ROUTE_CONTROLLER_BOOT"], receipt_boot := json.loads((receipts / f"dynamic-route-profile-{run_id}.json").read_text())["controller_boot_generations"]["final_dynamic_runtime"])
            self.assertNotIn("route-controller", {x["metadata"]["name"] for x in holders})
            self.assertEqual({x["metadata"]["name"] for x in business if x["kind"] == "Deployment"}, {
                "farm-worker-a", "farm-worker-b", "farm-gate", "farm-h5"})
            self.assertFalse({"farm-worker-a", "farm-worker-b", "farm-gate", "farm-h5"} & {x["metadata"]["name"] for x in holders + control_plane})
            extracted_workers = json.loads(subprocess.check_output(
                ["python3", str(WORKER_EXTRACTOR), str(output_dir / "business.json")],
                text=True,
            ))
            self.assertEqual(extracted_workers["kind"], "List")
            self.assertEqual(
                {
                    (item["kind"], item["metadata"]["name"])
                    for item in extracted_workers["items"]
                },
                {
                    ("Deployment", "farm-worker-a"),
                    ("Deployment", "farm-worker-b"),
                },
            )

            receipt = json.loads((receipts / f"dynamic-route-profile-{run_id}.json").read_text())
            self.assertEqual(receipt["gate_limits"], {"cpu": "4", "memory": "4Gi"})
            self.assertEqual(receipt["source_ip_admission"], {
                "GATE_WS_RATE_PER_SEC": "5000",
                "GATE_WS_RATE_BURST": "20000",
                "GATE_PREAUTH_MAX_CONNECTIONS_PER_IP": "16384",
                "GATE_PREAUTH_MAX_BYTES_PER_IP": "1073741824",
                "pod_wide_preauth_caps": "product-default",
            })
            self.assertEqual(receipt["shards"], {"PLAYER": 4, "FARM": 4, "FARM_WORKER": 4})
            self.assertEqual(
                receipt["consumer_replicas"]["FARM_WORKER"],
                {"farm-worker-a": 1, "farm-worker-b": 1},
            )
            self.assertEqual(
                receipt["farm_worker_assignments"]["farm-worker-a"]["shards"],
                [0, 2],
            )
            self.assertEqual(
                receipt["farm_worker_assignments"]["farm-worker-b"]["shards"],
                [1, 3],
            )
            self.assertEqual(receipt["lease_projection_mode"], "disabled")
            self.assertEqual(receipt["farm_worker_authority_renewal"], {
                "ttl": "60s",
                "interval": "10s",
                "operation_timeout": "2s",
                "pre_verification_holder_only": True,
            })
            for worker_name, worker_env in worker_envs.items():
                self.assertEqual(
                    worker_env["WORKER_BOOT_ID"]["value"],
                    receipt["farm_worker_assignments"][worker_name]["boot_id"],
                )
            self.assertEqual(receipt["h5_image_ref"], h5_ref)
            self.assertEqual(receipt["h5_source_commit"], "fc7be57174e9ff4f18e6dfeb5fc1610d681172cf")
            self.assertEqual(receipt["h5_source_tree"], "cfeb47072287a2f8533dc7366481c006c4010012")
            self.assertIsNone(receipt["h5_attestation_commit"])
            self.assertEqual(
                receipt["h5_image_receipt_sha256"],
                hashlib.sha256((receipts / f"dynamic-h5-image-{run_id}.json").read_bytes()).hexdigest(),
            )
            self.assertEqual(receipt["schedule_due_index"], {
                "mode": "dedicated_only",
                "dedicated_ready": True,
            })
            self.assertEqual(receipt["friend_opportunity"], {
                "wakeup_attach_enabled": True,
                "header_read_mode": "full",
                "wakeup_enabled": True,
                "wakeup_poll_interval": "1s",
                "wakeup_assignment_mode": "route",
                "hint_transport": "pubsub",
                "navigation": "navigator-v3",
                "candidate_window_mode": "top-k",
                "candidate_window_size": 32,
                "dynamic_mode_config_map": "farm-opportunity-mode",
                "initial_stage": "compatibility",
                "ladder_walked_from_first_rung": True,
                "mode_ladder": [
                    {
                        "stage": "compatibility",
                        "header_read_mode": "full",
                        "hint_transport": "pubsub",
                    },
                    {
                        "stage": "shadow",
                        "header_read_mode": "narrow-shadow",
                        "hint_transport": "shadow-direct",
                    },
                    {
                        "stage": "final",
                        "header_read_mode": "narrow",
                        "hint_transport": "direct",
                    },
                ],
            })
            self.assertEqual(receipt["final_production_modes"], {
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
            self.assertFalse(receipt["redis_domain_clients"]["physical_failure_domain_isolation"])
            self.assertEqual(receipt["gate_downlink_capacity"], {
                "writer_max_items": 256,
                "writer_max_bytes": 1048576,
                "hint_max_items": 32,
                "hint_max_bytes": 65536,
                "drain_quantum": 64,
            })
            self.assertEqual(
                receipt["gate_downlink_transport_security"],
                "PLAINTEXT_LOCAL_LAB_ONLY",
            )
            boots = receipt["controller_boot_generations"]
            self.assertEqual(len(set(boots.values())), 3)
            self.assertEqual(controller_env["ROUTE_CONTROLLER_BOOT"]["value"], boots["initial_zero_shard_runtime"])
            self.assertEqual(receipt["phase_order"], [
                "holders", "zero-shard-bootstrap", "initial-controller", "ready-zero-shard",
                "farm-worker-zero-shard-route-holders", "stopped-external-assignment",
                "final-controller", "final-route-verification", "business",
            ])
            self.assertEqual(receipt["public_web_tls"], "NOT_CLAIMED")
            self.assertEqual(receipt["network_policy_enforcement"], "NOT_CLAIMED")
            self.assertFalse(receipt["company_redis_contacted"])
            for field, name in (
                ("holders_sha256", "holders.json"), ("control_plane_sha256", "control-plane.json"),
                ("final_control_plane_sha256", "final-control-plane.json"), ("business_sha256", "business.json"),
            ):
                self.assertEqual(receipt[field], hashlib.sha256((output_dir / name).read_bytes()).hexdigest())

    def test_scaleplan_off_is_zero_rbac_and_holder_tokens_are_isolated(self) -> None:
        items, receipt = materialize(
            tempfile.mkdtemp(),
            "profile-scaleplan-off",
            "profilescaleplanoff",
        )
        self.assertEqual(receipt["scaleplan"]["mode"], "off")
        self.assertEqual(receipt["scaleplan"]["workload_allowlist"], EXPECTED_SCALEPLAN_ALLOWLIST)
        self.assertEqual(
            receipt["scaleplan"]["config_hash"],
            expected_scaleplan_hash("off", False, "legacy"),
        )
        self.assertEqual(
            {
                kind
                for kind in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding")
                if any(item["kind"] == kind for item in items.values())
            },
            set(),
        )
        self.assertEqual(
            {name for kind, name in items if kind == "ServiceAccount"},
            {"player-holder", "farm-holder"},
        )
        for holder_name, service_account in (("player-svr", "player-holder"), ("farm-svr", "farm-holder")):
            pod = items[("StatefulSet", holder_name)]["spec"]["template"]["spec"]
            self.assertEqual(pod["serviceAccountName"], service_account)
            self.assertFalse(pod["automountServiceAccountToken"])
            sa = items[("ServiceAccount", service_account)]
            self.assertFalse(sa["automountServiceAccountToken"])
            token_volume = next(v for v in pod["volumes"] if v["name"] == "holder-control-token")
            self.assertEqual(token_volume["projected"]["defaultMode"], 256)
            token_source = token_volume["projected"]["sources"]
            self.assertEqual(len(token_source), 1)
            self.assertEqual(
                token_source[0]["serviceAccountToken"],
                {
                    "audience": "classic-farm-holder-control",
                    "expirationSeconds": 600,
                    "path": "token",
                },
            )
            container = pod["containers"][0]
            token_mount = next(
                mount for mount in container["volumeMounts"]
                if mount["name"] == "holder-control-token"
            )
            self.assertEqual(token_mount["mountPath"], "/var/run/secrets/farm/holder-control")
            self.assertTrue(token_mount["readOnly"])
            env = {entry["name"]: entry for entry in container["env"]}
            self.assertEqual(
                env["HOLDER_WORKLOAD_IDENTITY_TOKEN_FILE"]["value"],
                "/var/run/secrets/farm/holder-control/token",
            )
        config = items[("ConfigMap", "scaleplan-config")]["data"]
        self.assertEqual(config["mode"], "off")
        self.assertEqual(config["realm"], "classic-farm-dynamic-local")
        self.assertEqual(json.loads(config["route-domains"]), ["PLAYER", "FARM"])
        self.assertEqual(json.loads(config["workload-allowlist"]), EXPECTED_SCALEPLAN_ALLOWLIST)
        self.assertEqual(config["config-hash"], expected_scaleplan_hash("off", False, "legacy"))
        self.assertEqual(config["tokenreview-rbac"], "false")
        self.assertEqual(
            config["holder-identity-transport"],
            "K8S_INFORMER_HOLDERCONTROL_TOKENREVIEW_V1",
        )
        self.assertEqual(config["holder-discovery-mode"], "legacy")

    def test_scaleplan_manual_uses_minimal_namespaced_rbac_and_isolated_controller_sa(self) -> None:
        items, receipt = materialize(
            tempfile.mkdtemp(),
            "profile-scaleplan-manual",
            "profilescaleplanmanual",
            {
                "SCALEPLAN_MODE": "manual",
                "HOLDER_DISCOVERY_MODE": "k8s-canary",
                "SCALEPLAN_TOKENREVIEW_RBAC": "true",
            },
        )
        self.assertEqual(receipt["scaleplan"]["mode"], "manual")
        self.assertEqual(receipt["scaleplan"]["controller_service_account"], "scaleplan-controller")
        self.assertEqual(
            receipt["scaleplan"]["config_hash"],
            expected_scaleplan_hash("manual", True, "k8s-canary"),
        )
        self.assertEqual(receipt["scaleplan"]["holder_discovery_mode"], "k8s-canary")
        controller_pod = items[("Deployment", "route-controller")]["spec"]["template"]["spec"]
        self.assertEqual(controller_pod["serviceAccountName"], "scaleplan-controller")
        self.assertTrue(controller_pod["automountServiceAccountToken"])
        self.assertEqual(
            {name for kind, name in items if kind == "ServiceAccount"},
            {"player-holder", "farm-holder", "scaleplan-controller"},
        )
        role = items[("Role", "scaleplan-controller")]
        self.assertEqual(role["rules"], [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]},
            {"apiGroups": ["apps"], "resources": ["statefulsets"], "verbs": ["get", "list", "watch", "patch"]},
            {"apiGroups": ["apps"], "resources": ["statefulsets/scale"], "verbs": ["get", "update"]},
        ])
        binding = items[("RoleBinding", "scaleplan-controller")]
        self.assertEqual(binding["roleRef"], {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "scaleplan-controller",
        })
        self.assertEqual(binding["subjects"], [{
            "kind": "ServiceAccount",
            "name": "scaleplan-controller",
            "namespace": "farm-route-dynamic",
        }])
        self.assertIn(("ClusterRole", "scaleplan-controller-tokenreview"), items)
        self.assertIn(("ClusterRoleBinding", "scaleplan-controller-tokenreview"), items)
        for item in items.values():
            if item["kind"] in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"}:
                self.assertNotIn("cluster-admin", item["metadata"]["name"])
                for rule in item.get("rules", []):
                    self.assertNotIn("*", rule.get("apiGroups", []))
                    self.assertNotIn("*", rule.get("resources", []))
                    self.assertNotIn("*", rule.get("verbs", []))
        for holder_name in ("player-holder", "farm-holder"):
            self.assertFalse(items[("ServiceAccount", holder_name)]["automountServiceAccountToken"])
        config = items[("ConfigMap", "scaleplan-config")]["data"]
        self.assertEqual(
            config["config-hash"],
            expected_scaleplan_hash("manual", True, "k8s-canary"),
        )
        controller_env = {
            entry["name"]: entry
            for entry in controller_pod["containers"][0]["env"]
        }
        self.assertEqual(controller_env["HOLDER_DISCOVERY_MODE"]["value"], "k8s-canary")
        self.assertEqual(
            controller_env["HOLDER_DISCOVERY_AUDIENCE"]["value"],
            "classic-farm-holder-control",
        )

    def test_scaleplan_manual_tokenreview_is_separate_and_single_verb(self) -> None:
        items, receipt = materialize(
            tempfile.mkdtemp(),
            "profile-scaleplan-tokenreview",
            "profilescaleplantokenreview",
            {
                "SCALEPLAN_MODE": "manual",
                "HOLDER_DISCOVERY_MODE": "shadow",
                "SCALEPLAN_TOKENREVIEW_RBAC": "true",
            },
        )
        self.assertTrue(receipt["scaleplan"]["tokenreview_rbac"])
        self.assertEqual(
            items[("ClusterRole", "scaleplan-controller-tokenreview")]["rules"],
            [{
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["tokenreviews"],
                "verbs": ["create"],
            }],
        )
        binding = items[("ClusterRoleBinding", "scaleplan-controller-tokenreview")]
        self.assertEqual(binding["roleRef"]["kind"], "ClusterRole")
        self.assertEqual(binding["roleRef"]["name"], "scaleplan-controller-tokenreview")
        self.assertEqual(binding["subjects"], [{
            "kind": "ServiceAccount",
            "name": "scaleplan-controller",
            "namespace": "farm-route-dynamic",
        }])
        self.assertEqual(
            items[("ConfigMap", "scaleplan-config")]["data"]["config-hash"],
            expected_scaleplan_hash("manual", True, "shadow"),
        )

    def test_scaleplan_auto_renders_discovery_scale_rbac_and_reconcile_contract(self) -> None:
        items, receipt = materialize(
            tempfile.mkdtemp(),
            "profile-scaleplan-auto",
            "profilescaleplanauto",
            {
                "SCALEPLAN_MODE": "auto",
                "HOLDER_DISCOVERY_MODE": "k8s",
                "SCALEPLAN_TOKENREVIEW_RBAC": "true",
            },
        )
        self.assertEqual(receipt["scaleplan"]["mode"], "auto")
        self.assertEqual(receipt["scaleplan"]["controller_service_account"], "scaleplan-controller")
        self.assertEqual(receipt["scaleplan"]["auto_reconcile_interval"], "5s")
        self.assertEqual(receipt["scaleplan"]["route_failover_reconcile_interval"], "1s")
        self.assertEqual(receipt["scaleplan"]["route_failover_max_plans_per_pass"], 32)
        self.assertEqual(
            receipt["scaleplan"]["route_failover_max_authority_reads_per_pass"],
            128,
        )
        self.assertEqual(
            receipt["scaleplan"]["safe_desired_annotation"],
            "scaleplan.farm/safe-desired-replicas",
        )
        controller_pod = items[("Deployment", "route-controller")]["spec"]["template"]["spec"]
        self.assertTrue(controller_pod["automountServiceAccountToken"])
        self.assertEqual(controller_pod["serviceAccountName"], "scaleplan-controller")
        env = {
            entry["name"]: entry
            for entry in controller_pod["containers"][0]["env"]
        }
        self.assertEqual(env["SCALEPLAN_MODE"]["value"], "auto")
        self.assertEqual(env["SCALEPLAN_AUTO_RECONCILE_INTERVAL"]["value"], "5s")
        self.assertEqual(env["ROUTE_FAILOVER_RECONCILE_INTERVAL"]["value"], "1s")
        self.assertEqual(env["ROUTE_FAILOVER_MAX_PLANS_PER_PASS"]["value"], "32")
        self.assertEqual(
            env["ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS"]["value"],
            "128",
        )
        self.assertEqual(
            env["SCALEPLAN_SAFE_DESIRED_ANNOTATION"]["value"],
            "scaleplan.farm/safe-desired-replicas",
        )
        self.assertIn(("Role", "scaleplan-controller"), items)
        self.assertIn(("ClusterRole", "scaleplan-controller-tokenreview"), items)
        scaleplan_config = items[("ConfigMap", "scaleplan-config")]["data"]
        self.assertEqual(scaleplan_config["route-failover-reconcile-interval"], "1s")
        self.assertEqual(scaleplan_config["route-failover-max-plans-per-pass"], "32")
        self.assertEqual(
            scaleplan_config["route-failover-max-authority-reads-per-pass"],
            "128",
        )

    def test_scaleplan_auto_rollout_uses_ondelete_and_minimal_delete_rbac(self) -> None:
        items, receipt = materialize(
            tempfile.mkdtemp(),
            "profile-scaleplan-rollout",
            "profilescaleplanrollout",
            {
                "SCALEPLAN_MODE": "auto",
                "HOLDER_DISCOVERY_MODE": "k8s",
                "SCALEPLAN_TOKENREVIEW_RBAC": "true",
                "SCALEPLAN_ROLLOUT_ENABLED": "true",
            },
        )
        self.assertTrue(receipt["scaleplan"]["rollout_enabled"])
        self.assertEqual(
            receipt["scaleplan"]["rollout_annotation"],
            "scaleplan.farm/managed-rollout-state",
        )
        self.assertEqual(
            receipt["scaleplan"]["rollout_intent_annotation"],
            "scaleplan.farm/rollout-intent",
        )
        for name in ("player-svr", "farm-svr"):
            self.assertEqual(
                items[("StatefulSet", name)]["spec"]["updateStrategy"],
                {"type": "OnDelete"},
            )
        role = items[("Role", "scaleplan-controller")]
        self.assertEqual(role["rules"], [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "delete"]},
            {"apiGroups": ["apps"], "resources": ["statefulsets"], "verbs": ["get", "list", "watch", "patch", "update"]},
            {"apiGroups": ["apps"], "resources": ["statefulsets/scale"], "verbs": ["get", "update"]},
        ])
        controller = items[("Deployment", "route-controller")]["spec"]["template"]["spec"]["containers"][0]
        env = {entry["name"]: entry for entry in controller["env"]}
        self.assertEqual(env["SCALEPLAN_ROLLOUT_ENABLED"]["value"], "true")
        self.assertEqual(
            env["SCALEPLAN_ROLLOUT_ANNOTATION"]["value"],
            "scaleplan.farm/managed-rollout-state",
        )
        self.assertEqual(
            env["SCALEPLAN_ROLLOUT_INTENT_ANNOTATION"]["value"],
            "scaleplan.farm/rollout-intent",
        )

    def test_the_product_profile_carries_no_login_bypass(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(root, "profile-showcase", "profileshowcase")
            gate = items[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["containers"][0]
            self.assertNotIn(
                "LOAD_TEST_TOKEN_SECRET",
                {entry["name"] for entry in gate["env"]},
            )
            self.assertEqual(
                items[("ConfigMap", "farm-staging-platform")]["data"]["FARM_APP_PROFILE"],
                "showcase",
            )
            self.assertFalse(receipt["load_test_login_enabled"])
            self.assertTrue(receipt["eligible_for_product_acceptance"])

    def test_a_capacity_profile_signs_the_driver_in_and_forfeits_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-capacity",
                "profilecapacity",
                {"FARM_APP_PROFILE": "test"},
            )
            gate = items[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["containers"][0]
            gate_env = {entry["name"]: entry for entry in gate["env"]}
            self.assertEqual(
                gate_env["LOAD_TEST_TOKEN_SECRET"]["valueFrom"]["secretKeyRef"],
                {"name": "farm-dynamic-secrets", "key": "load-test-token-secret"},
            )
            self.assertEqual(
                items[("ConfigMap", "farm-staging-platform")]["data"]["FARM_APP_PROFILE"],
                "test",
            )
            self.assertTrue(receipt["load_test_login_enabled"])
            self.assertFalse(receipt["eligible_for_product_acceptance"])

    def test_capacity_gate_limits_can_be_raised_without_changing_workers(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-gate-limits",
                "profilegatelimits",
                {
                    "FARM_APP_PROFILE": "test",
                    "FARM_GATE_CPU_LIMIT": "2",
                    "FARM_GATE_MEMORY_LIMIT": "8Gi",
                },
            )
            self.assertEqual(
                items[("Deployment", "farm-gate")]["spec"]["template"]["spec"]["containers"][0]["resources"],
                {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "8Gi"}},
            )
            self.assertEqual(
                items[("Deployment", "farm-worker-a")]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"],
                {"cpu": "1", "memory": "768Mi"},
            )
            self.assertEqual(receipt["gate_limits"], {"cpu": "2", "memory": "8Gi"})

    def test_farm_svr_limits_can_be_raised_without_changing_player_svr(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-farm-svr-limits",
                "profilefarmsvrlimits",
                {
                    "FARM_APP_PROFILE": "test",
                    "FARM_HOLDER_CPU_LIMIT": "2",
                    "FARM_HOLDER_MEMORY_LIMIT": "4Gi",
                    "FARM_SVR_CPU_LIMIT": "4",
                    "FARM_SVR_MEMORY_LIMIT": "4Gi",
                },
            )
            self.assertEqual(
                items[("StatefulSet", "player-svr")]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"],
                {"cpu": "2", "memory": "4Gi"},
            )
            self.assertEqual(
                items[("StatefulSet", "farm-svr")]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"],
                {"cpu": "4", "memory": "4Gi"},
            )
            self.assertEqual(
                receipt["holder_limits"],
                {
                    "cpu": "2",
                    "memory": "4Gi",
                    "player": {"cpu": "2", "memory": "4Gi"},
                    "farm": {"cpu": "4", "memory": "4Gi"},
                },
            )

    def test_source_ip_admission_defaults_do_not_measure_the_abuse_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-source-ip",
                "profilesourceip",
                {"FARM_APP_PROFILE": "test"},
            )
            platform = items[("ConfigMap", "farm-staging-platform")]["data"]
            self.assertEqual(platform["GATE_WS_RATE_PER_SEC"], "5000")
            self.assertEqual(platform["GATE_WS_RATE_BURST"], "20000")
            self.assertEqual(platform["GATE_PREAUTH_MAX_CONNECTIONS_PER_IP"], "16384")
            self.assertEqual(platform["GATE_PREAUTH_MAX_BYTES_PER_IP"], "1073741824")
            self.assertEqual(platform["GATE_PREAUTH_MAX_CONNECTIONS"], "4096")
            self.assertEqual(receipt["source_ip_admission"]["GATE_WS_RATE_PER_SEC"], "5000")
            self.assertNotIn("ACTIVATION_RATE_PER_SEC", platform)
            self.assertEqual(receipt["activation_storm_gate"]["ACTIVATION_RATE_PER_SEC"], "binary-default-1000")
            self.assertFalse(receipt["activation_storm_gate"]["opened_for_pod_fill"])

    def test_activation_storm_gate_opens_only_when_capacity_fill_asks(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-fill-gate",
                "profilefillgate",
                {
                    "FARM_APP_PROFILE": "test",
                    "ACTIVATION_RATE_PER_SEC": "0",
                    "MAX_CONCURRENT_ACTOR_LOADS": "1024",
                    "MAX_PENDING_ACTOR_LOADS": "4096",
                },
            )
            platform = items[("ConfigMap", "farm-staging-platform")]["data"]
            self.assertEqual(platform["ACTIVATION_RATE_PER_SEC"], "0")
            self.assertEqual(platform["MAX_CONCURRENT_ACTOR_LOADS"], "1024")
            self.assertEqual(platform["MAX_PENDING_ACTOR_LOADS"], "4096")
            self.assertEqual(receipt["activation_storm_gate"], {
                "ACTIVATION_RATE_PER_SEC": "0",
                "MAX_CONCURRENT_ACTOR_LOADS": "1024",
                "MAX_PENDING_ACTOR_LOADS": "4096",
                "opened_for_pod_fill": True,
            })

    def test_a_product_run_is_born_on_the_first_rung(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(root, "profile-rung", "profilerung")
            self.assertEqual(
                items[("ConfigMap", "farm-opportunity-mode")]["data"],
                {
                    "header-read-mode": "full",
                    "hint-transport": "pubsub",
                    "stage": "compatibility",
                },
            )
            self.assertTrue(receipt["friend_opportunity"]["ladder_walked_from_first_rung"])
            self.assertEqual(receipt["friend_opportunity"]["initial_stage"], "compatibility")
            self.assertTrue(receipt["eligible_for_product_acceptance"])

    def test_a_capacity_run_may_be_born_on_the_rung_the_product_ends_on(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            items, receipt = materialize(
                root,
                "profile-final-rung",
                "profilefinalrung",
                {
                    "FARM_APP_PROFILE": "test",
                    "FARM_OPPORTUNITY_HEADER_READ_MODE": "narrow",
                    "FARM_OPPORTUNITY_HINT_TRANSPORT": "direct",
                },
            )
            self.assertEqual(
                items[("ConfigMap", "farm-opportunity-mode")]["data"],
                {
                    "header-read-mode": "narrow",
                    "hint-transport": "direct",
                    "stage": "final",
                },
            )
            self.assertFalse(receipt["friend_opportunity"]["ladder_walked_from_first_rung"])
            self.assertEqual(receipt["friend_opportunity"]["initial_stage"], "final")
            self.assertFalse(receipt["eligible_for_product_acceptance"])

    def test_worker_extractor_rejects_incomplete_kubernetes_list(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            manifest = Path(root) / "business.json"
            manifest.write_text(json.dumps({
                "apiVersion": "v1",
                "kind": "List",
                "items": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"name": "farm-worker-a"},
                }],
            }))
            result = subprocess.run(
                ["python3", str(WORKER_EXTRACTOR), str(manifest)],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(
                "must contain exactly both FARM_WORKER deployments",
                result.stderr,
            )

    def test_materializer_rejects_frozen_h5_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            lab = Path(root)
            receipts = lab / "receipts"
            receipts.mkdir()
            run_id = "dynamic-profile-no-h5-fallback"
            head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
            (receipts / f"route-runtime-image-{run_id}.json").write_text(json.dumps({
                "status": "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED",
                "run_id": run_id,
                "source_head": head,
                "registry_ref": "127.0.0.1:5001/farm/route-runtime@sha256:" + "1" * 64,
            }))
            (receipts / "frozen-images-local-registry.json").write_text(json.dumps({
                "status": "PUBLISHED_NOT_ACTIVATED",
                "h5_registry_ref": "127.0.0.1:5001/farm/h5@sha256:" + "2" * 64,
            }))
            env = os.environ.copy()
            env.update({
                "FARM_L2_LAB_ROOT": str(lab),
                "ROUTE_RUN_ID": run_id,
                "REDIS_NAMESPACE_ENVIRONMENT": "dynamicprofilenoh5",
                "REDIS_NAMESPACE_ZONE": "z1",
                **PRODUCT_ENV_DEFAULTS,
            })
            result = subprocess.run(
                [str(SCRIPT)],
                cwd=REPO,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("exact H5 image receipt missing", result.stderr)
            self.assertNotIn("frozen-images-local-registry", result.stderr)

    def test_materializer_rejects_invalid_product_modes(self) -> None:
        cases = (
            ("unknown schedule mode", {
                "FARM_SCHEDULE_DUE_INDEX_MODE": "typo",
            }, "FARM_SCHEDULE_DUE_INDEX_MODE must be one of"),
            ("dedicated without readiness", {
                "FARM_SCHEDULE_DUE_INDEX_MODE": "dedicated_only",
                "FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY": "false",
            }, "dedicated_only requires dedicated readiness"),
            ("legacy schedule mode", {
                "FARM_SCHEDULE_DUE_INDEX_MODE": "dual_shadow",
                "FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY": "true",
            }, "legacy index modes are migration/rollback only"),
            ("wakeup disabled", {
                "FARM_OPPORTUNITY_WAKEUP_ENABLED": "false",
            }, "requires active route-assigned wakeup"),
            ("static worker assignment", {
                "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE": "static",
            }, "requires active route-assigned wakeup"),
            ("unknown header mode", {
                "FARM_OPPORTUNITY_HEADER_READ_MODE": "wide",
            }, "FARM_OPPORTUNITY_HEADER_READ_MODE must be one of"),
            ("unknown hint transport", {
                "FARM_OPPORTUNITY_HINT_TRANSPORT": "broadcast",
            }, "FARM_OPPORTUNITY_HINT_TRANSPORT must be one of"),
            ("unknown app profile", {
                "FARM_APP_PROFILE": "production",
            }, "FARM_APP_PROFILE must be one of"),
            ("non-baseline initial product pair", {
                "FARM_OPPORTUNITY_HEADER_READ_MODE": "narrow-shadow",
                "FARM_OPPORTUNITY_HINT_TRANSPORT": "shadow-direct",
            }, "a new product run must start in full/pubsub"),
            ("product run born on the final rung", {
                "FARM_OPPORTUNITY_HEADER_READ_MODE": "narrow",
                "FARM_OPPORTUNITY_HINT_TRANSPORT": "direct",
            }, "a new product run must start in full/pubsub"),
            ("modes that name no rung", {
                "FARM_APP_PROFILE": "test",
                "FARM_OPPORTUNITY_HEADER_READ_MODE": "narrow",
                "FARM_OPPORTUNITY_HINT_TRANSPORT": "pubsub",
            }, "opportunity modes must name a ladder rung"),
        )
        for index, (name, overrides, expected_error) in enumerate(cases):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                lab = Path(root)
                receipts = lab / "receipts"
                receipts.mkdir()
                run_id = f"invalid-product-mode-{index}"
                head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
                (receipts / f"route-runtime-image-{run_id}.json").write_text(json.dumps({
                    "status": "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED",
                    "run_id": run_id,
                    "source_head": head,
                    "registry_ref": "127.0.0.1:5001/farm/route-runtime@sha256:" + "1" * 64,
                }))
                (receipts / f"dynamic-h5-image-{run_id}.json").write_text(json.dumps({
                    "status": "PUBLISHED_LOCAL_DYNAMIC_H5_NOT_DEPLOYED",
                    "run_id": run_id,
                    "source_commit": "fc7be57174e9ff4f18e6dfeb5fc1610d681172cf",
                    "source_tree": "cfeb47072287a2f8533dc7366481c006c4010012",
                    "build_mode": "web",
                    "registry_ref": "127.0.0.1:5001/farm/h5@sha256:" + "2" * 64,
                    "cluster_applied": False,
                    "company_redis_contacted": False,
                    "capacity_eligible": False,
                }))
                env = os.environ.copy()
                env.update({
                    "FARM_L2_LAB_ROOT": str(lab),
                    "ROUTE_RUN_ID": run_id,
                    "REDIS_NAMESPACE_ENVIRONMENT": f"invalidproductmode{index}",
                    "REDIS_NAMESPACE_ZONE": "z1",
                    **PRODUCT_ENV_DEFAULTS,
                    **overrides,
                })
                result = subprocess.run(
                    [str(SCRIPT)],
                    cwd=REPO,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

    def test_apply_contract_enforces_phase_order_and_exact_verification(self) -> None:
        apply = (REPO / "scripts/local-k8s/apply-dynamic-route-profile").read_text()
        ordered = [
            'kubectl apply -f "$HOLDERS_ZERO"',
            'set env statefulset/player-svr',
            'scale statefulset/player-svr statefulset/farm-svr --replicas=2',
            '"$SCRIPT_DIR/bootstrap-dynamic-route" --bootstrap-only',
            'kubectl apply -f "$CONTROL_PLANE"',
            '"$SCRIPT_DIR/verify-dynamic-route-state" --ready-zero-shard',
            'stage="farm_worker_zero_shard_route_holder_startup"',
            '"$SCRIPT_DIR/extract_dynamic_worker_route_holders.py" "$BUSINESS"',
            'scale deployment/route-controller --replicas=0',
            '"$SCRIPT_DIR/bootstrap-dynamic-route" --assign',
            'kubectl apply -f "$FINAL_CONTROL_PLANE"',
            '"$SCRIPT_DIR/verify-dynamic-route-state" --final',
            'kubectl apply -f "$BUSINESS"',
            'FARM_WORKER_ROUTE_HOLDER_ONLY=false',
            '"$SCRIPT_DIR/verify-dynamic-route-state" --post-business',
        ]
        offsets = [apply.index(value) for value in ordered]
        self.assertEqual(offsets, sorted(offsets))
        self.assertIn('business_started=false', apply)
        self.assertIn('business_started=true', apply)
        self.assertIn('os.link(temp,path)', apply)
        self.assertIn('trap \'record_failure $?\' EXIT', apply)
        self.assertIn('dynamic-route-formal-run.lock', apply)
        self.assertLess(
            apply.index('flock -n 9 ||'),
            apply.index('trap \'record_failure $?\' EXIT'),
        )
        self.assertIn("https://127.0.0.1:6443", apply)
        self.assertIn("verify_holder_init_images", apply)
        self.assertIn("expected exactly two Holder StatefulSets", apply)
        self.assertIn("ACTOR_HOLDER_WORKLOAD_UID=$player_workload_uid", apply)
        self.assertIn("ACTOR_HOLDER_WORKLOAD_UID=$farm_workload_uid", apply)
        self.assertIn("profile/Redis namespace binding mismatch", apply)
        self.assertIn("runtime imageID mismatch", apply)
        self.assertIn("dynamic-h5-image-", apply)
        self.assertNotIn("frozen-images-local-registry", apply)
        self.assertIn("application Secret content hash mismatch", apply)
        secret_source = (REPO / "scripts/local-k8s/materialize-dynamic-route-secrets").read_text()
        for secret_key in (
            "authority-redis-url",
            "runtime-redis-url",
            "projection-redis-url",
            "load-test-token-secret",
        ):
            self.assertIn(secret_key, secret_source)
        self.assertIn('"required_key_count":9', secret_source)
        self.assertIn('"redis_failure_domains_claimed":False', secret_source)
        self.assertIn('--ignore-not-found', apply)
        self.assertNotIn('set env deployment/route-controller', apply)
        verify = (REPO / "scripts/local-k8s/verify-dynamic-route-state").read_text()
        for required in (
            "READY_ZERO_SHARD", "FINAL_4X4X4_VERIFIED_WORKER_AUTHORITY_HOLDERS_ONLY",
            "lease_until_ms", "serving_shard_count", "snapshot_revision",
            "Holder process restart observed", "company_redis_contacted",
            "run_ctl snapshot", "five coherent", "lease_expire_at_ms",
            "Snapshot canonical checksum mismatch", "revision-1 checksum mismatch",
            "Snapshot active holder cardinality mismatch", "('FARM_WORKER','farm_worker')",
            "POST_BUSINESS_4X4X4_STABLE_NOT_G1_VERIFIED", "farm_actor_route_snapshot_revision",
            "farm_opportunity_wakeup_owned_shards", "FARM_WORKER ownership mismatch",
            "Pod UID/restart/readiness drift across five samples", "verification_attempts=180",
            "deep Controller revision drift", "deep_snapshot_revision",
            "regressed below", "FARM_WORKER assignment revision mismatch",
            "business consumers are not on one complete revision",
            "deep Snapshot/status sandwich mismatch", "business_metrics_revision",
            'setsid timeout --foreground 240s "$KUBECTL_BIN"',
            'kill -- "-$pf_pid"',
        ):
            self.assertIn(required, verify)
        bootstrap = (
            REPO / "scripts/local-k8s/bootstrap-dynamic-route"
        ).read_text()
        self.assertIn(
            'setsid timeout --foreground 240s "$KUBECTL_BIN"',
            bootstrap,
        )
        self.assertIn('kill -- "-$pf_pid"', bootstrap)
        for script in (
            "apply-dynamic-route-profile", "bootstrap-dynamic-route", "verify-dynamic-route-state",
            "materialize-dynamic-route-secrets", "materialize-dynamic-route-tls",
        ):
            source = (REPO / "scripts/local-k8s" / script).read_text()
            self.assertNotRegex(source, r"(?m)^\s*assert\s")

    def test_runtime_entrypoint_rotates_holder_process_boot(self) -> None:
        dockerfile = (REPO / "Dockerfile.runtime.prebuilt").read_text()
        for required in (
            "HOLDER_PROCESS_BOOT_MODE", "pod-uid-then-random", "POD_UID",
            "/proc/sys/kernel/random/uuid", "process-started", "current-boot-id",
        ):
            self.assertIn(required, dockerfile)
        self.assertIn('export ACTOR_HOLDER_BOOT_ID="$POD_NAME-$suffix"', dockerfile)


if __name__ == "__main__":
    unittest.main()
