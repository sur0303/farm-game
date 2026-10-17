import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import agentctl  # noqa: E402


class FakeTmux:
    """Replace tmux while still running the controller's real worker argv."""

    def __init__(self):
        self.sessions = {}
        self.capture_text = "fake pane output\n"

    def has_session(self, session):
        return session in self.sessions

    def new_session(self, session, cwd, command):
        if session in self.sessions:
            raise agentctl.AgentCtlError("fake session already exists")
        process = subprocess.Popen(
            list(command),
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.sessions[session] = {"process": process, "cwd": Path(cwd), "command": list(command)}

    def capture(self, session, lines=80):
        if session not in self.sessions:
            raise agentctl.AgentCtlError("fake session missing")
        return self.capture_text

    def kill_session(self, session):
        item = self.sessions.pop(session, None)
        if item is None:
            return
        process = item["process"]
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class SlowFakeTmux(FakeTmux):
    """Make an unprotected capacity check observable in a test."""

    def new_session(self, session, cwd, command):
        time.sleep(0.1)
        super().new_session(session, cwd, command)


class AgentControllerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self._git("init", "-b", "main")
        self._git("config", "user.email", "agentctl-test@example.invalid")
        self._git("config", "user.name", "agentctl test")
        (self.repo / "prompt.md").write_text("Implement the scoped task.\n", encoding="utf-8")
        (self.repo / "authority.md").write_text(
            "AUTHORITATIVE BODY MUST NOT BE COPIED\n", encoding="utf-8"
        )
        self._git("add", "prompt.md", "authority.md")
        self._git("commit", "-m", "test base")
        self.base = self._git("rev-parse", "HEAD").stdout.strip()
        self.manifest_dir = self.root / "manifests"
        self.manifest_dir.mkdir()
        self.state_dir = self.root / "state"
        self.codex_script = self.root / "fake-codex"
        self.codex_script.write_text(
            """#!/usr/bin/env python3
import json, os, sys, time

args = sys.argv[1:]
if '--help' in args:
    print('fake codex --json --reasoning-effort')
    raise SystemExit(0)
if '--version' in args:
    print('codex fake 1.0')
    raise SystemExit(0)
output = None
for flag in ('--output-last-message', '-o'):
    if flag in args:
        output = args[args.index(flag) + 1]
if os.environ.get('FAKE_CODEX_SLEEP'):
    time.sleep(float(os.environ['FAKE_CODEX_SLEEP']))
print(json.dumps({'type': 'thread.started', 'thread_id': 'thread-one'}), flush=True)
if os.environ.get('FAKE_CODEX_CAPACITY') == '1':
    print(json.dumps({'type': 'error', 'message': '14003 full capacity resource limit'}), flush=True)
    raise SystemExit(1)
print(json.dumps({'type': 'turn.completed', 'status': 'ok'}), flush=True)
if output:
    with open(output, 'w', encoding='utf-8') as handle:
        handle.write('fake last message\\n')
raise SystemExit(int(os.environ.get('FAKE_CODEX_EXIT', '0')))
""",
            encoding="utf-8",
        )
        self.codex_script.chmod(0o755)

    def tearDown(self):
        self.temp.cleanup()

    def _git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.repo), *args],
            text=True,
            capture_output=True,
            check=True,
        )

    def _manifest(
        self,
        task_id="task-one",
        owned=None,
        authority_sha=None,
        role="writer",
        semantic_locks=None,
        dependencies=None,
        review_of=None,
    ):
        value = {
            "task_id": task_id,
            "repo": str(self.repo),
            "worktree": str(self.repo),
            "branch": "main",
            "base_commit": self.base,
            "model": "Luna Max",
            "reasoning_effort": "high",
            "prompt_file": "prompt.md",
            "owned_write_set": owned if owned is not None else [f"src/{task_id}/**"],
            "forbidden_write_set": ["src/game-protocol/**", "src/actors/**"],
            "authoritative_inputs": [
                {
                    "path": "authority.md",
                    "section": "whole test authority",
                    **({"sha256": authority_sha} if authority_sha else {}),
                }
            ],
            "acceptance_commands": ["python3 -m unittest discover -s tools/agentctl/tests -v"],
            "review_scope": ["tools/agentctl/**"],
            "frozen_summary": ["Only the cited authority reference is in scope."],
        }
        if role != "writer":
            value["role"] = role
        if semantic_locks is not None:
            value["semantic_locks"] = semantic_locks
        if dependencies is not None:
            value["dependencies"] = dependencies
        if review_of is not None:
            value["review_of"] = review_of
        return value

    def _write_manifest(self, value, name=None):
        path = self.manifest_dir / (name or f"{value['task_id']}.json")
        path.write_text(json.dumps(value, indent=2), encoding="utf-8")
        return path

    def _controller(self, tmux=None, stall_seconds=900, cursor=None, codebuddy=None):
        return agentctl.Controller(
            state_dir=self.state_dir,
            codex=agentctl.CodexAdapter(str(self.codex_script)),
            cursor=cursor or agentctl.CursorCliRunner(str(self.codex_script)),
            codebuddy=codebuddy or agentctl.CodeBuddyCliRunner(str(self.codex_script)),
            tmux=tmux or FakeTmux(),
            stall_seconds=stall_seconds,
        )

    def _install_fake_cursor_fixer(self):
        """Fake Cursor agent that repairs the synthetic pilot quota bug."""

        script = self.root / "fake-cursor-agent"
        quota_path = Path(agentctl.__file__).resolve().parent / "pilot" / "quota.py"
        script.write_text(
            f"""#!/usr/bin/env python3
import json, sys
from pathlib import Path
args = sys.argv[1:]
if '--help' in args or '--version' in args:
    print('fake cursor agent --print stream-json --resume')
    raise SystemExit(0)
print(json.dumps({{'type': 'system', 'session_id': 'cursor-session-one'}}), flush=True)
target = Path({str(quota_path)!r})
text = target.read_text(encoding='utf-8')
text = text.replace('return limit - used + 1', 'return limit - used')
target.write_text(text, encoding='utf-8')
print(json.dumps({{'type': 'assistant', 'message': 'fixed pilot off-by-one'}}), flush=True)
raise SystemExit(0)
""",
            encoding="utf-8",
        )
        script.chmod(0o755)
        return script

    def _wait_for_exit(self, task_id, controller, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            view = controller.status(task_id)
            exit_code = view["runtime"].get("exit_code")
            if exit_code is not None:
                return view
            time.sleep(0.05)
        self.fail("fake Codex did not write an exit code")

    def test_git_changed_paths_preserves_first_tracked_path(self):
        (self.repo / "authority.md").write_text("changed\n", encoding="utf-8")

        self.assertEqual(agentctl._git_changed_paths(self.repo), ["authority.md"])

    def test_manifest_validation_and_hash_drift(self):
        manifest = self._manifest(authority_sha="0" * 64)
        with self.assertRaises(agentctl.ManifestError):
            self._controller().register(self._write_manifest(manifest))
        self.assertFalse((self.state_dir / "tasks" / "task-one.json").exists())

        manifest = self._manifest()
        controller = self._controller()
        state = controller.register(self._write_manifest(manifest))
        self.assertEqual(state["status"], "REGISTERED")
        stored_state = controller.store.load("task-one")
        self.assertEqual(
            stored_state["context_pack"]["authoritative_inputs"][0]["sha256"],
            agentctl.sha256_file(self.repo / "authority.md"),
        )
        self.assertNotIn("AUTHORITATIVE BODY", json.dumps(stored_state["context_pack"]))

    def test_write_set_conflict_is_fail_closed(self):
        controller = self._controller()
        first = self._manifest("first", owned=["tools/agentctl/**"])
        controller.register(self._write_manifest(first))
        second = self._manifest("second", owned=["tools/agentctl/tests/**"])
        with self.assertRaises(agentctl.StateError) as raised:
            controller.register(self._write_manifest(second))
        self.assertIn("write-set conflict", str(raised.exception))
        self.assertFalse((self.state_dir / "tasks" / "second.json").exists())

    def test_exit_diff_gate_marks_policy_violation(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest(owned=["src/owned/**"])))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "0.5"}):
            controller.start("task-one")
            (self.repo / "outside.txt").write_text("out of scope\n", encoding="utf-8")
            self._wait_for_exit("task-one", controller)
        watched = controller.watch_once("task-one")
        self.assertEqual(watched["guard"], "POLICY_VIOLATION")
        self.assertEqual(watched["status"], "CONTRACT_CONFLICT")
        controller.stop("task-one")

    def test_semantic_lock_conflict_is_fail_closed(self):
        controller = self._controller()
        controller.register(
            self._write_manifest(self._manifest("locked-one", semantic_locks=["workflow:plant"]))
        )
        with self.assertRaises(agentctl.StateError) as raised:
            controller.register(
                self._write_manifest(
                    self._manifest("locked-two", semantic_locks=["workflow:plant/receipt"])
                )
            )
        self.assertIn("semantic lock", str(raised.exception))

    def test_state_transitions_accept_pipeline_and_reject_jump(self):
        valid = [
            ("ICR", "IMPLEMENTING"),
            ("IMPLEMENTING", "FOCUSED_REVIEW"),
            ("FOCUSED_REVIEW", "FIXING"),
            ("FIXING", "FOCUSED_REVIEW"),
            ("FOCUSED_REVIEW", "PREMERGE"),
            ("PREMERGE", "DONE"),
        ]
        for current, target in valid:
            agentctl.validate_phase_transition(current, target)
        with self.assertRaises(agentctl.StateError):
            agentctl.validate_phase_transition("ICR", "DONE")
        with self.assertRaises(agentctl.StateError):
            agentctl.validate_state_transition("DONE", "IMPLEMENTING")

    def test_atomic_state_update_leaves_no_partial_file(self):
        target = self.state_dir / "atomic" / "state.json"
        agentctl.atomic_write_json(target, {"version": 1, "status": "REGISTERED"})
        agentctl.atomic_write_json(target, {"version": 2, "status": "READY"})
        self.assertEqual(json.loads(target.read_text())["version"], 2)
        self.assertEqual(list(target.parent.glob(".*")), [])

    def test_command_arguments_are_shell_safe(self):
        malicious = "Luna; touch /tmp/agentctl-injection #"
        command = agentctl.TmuxAdapter.shell_command(["python3", "--model", malicious])
        self.assertEqual(__import__("shlex").split(command), ["python3", "--model", malicious])
        self.assertIn("'Luna; touch /tmp/agentctl-injection #'", command)
        adapter = agentctl.CodexAdapter(str(self.codex_script))
        argv = adapter.build_initial_argv(
            model=malicious,
            reasoning_effort="high",
            worktree=self.repo,
            last_message=self.state_dir / "last.txt",
        )
        self.assertIn(malicious, argv)
        self.assertEqual(argv.count(malicious), 1)

    def test_codex_adapter_sets_explicit_sandbox_for_writer_and_reviewer(self):
        adapter = agentctl.CodexAdapter(str(self.codex_script))
        writer = adapter.build_initial_argv(
            model="test-model",
            reasoning_effort="high",
            worktree=self.repo,
            last_message=self.state_dir / "writer-last.txt",
        )
        reviewer = adapter.build_initial_argv(
            model="test-model",
            reasoning_effort="high",
            worktree=self.repo,
            last_message=self.state_dir / "reviewer-last.txt",
            read_only=True,
        )
        resumed_writer = adapter.build_resume_argv(
            session_id="session-one",
            model="test-model",
            reasoning_effort="high",
            worktree=self.repo,
            last_message=self.state_dir / "resume-last.txt",
        )

        self.assertEqual(writer[writer.index("--sandbox") + 1], "danger-full-access")
        # Reviewer also uses host sandbox so Findings JSON can be written under
        # the controller state dir; mutation safety is post-exit fingerprinting.
        self.assertEqual(reviewer[reviewer.index("--sandbox") + 1], "danger-full-access")
        self.assertEqual(resumed_writer[resumed_writer.index("--sandbox") + 1], "danger-full-access")

    def test_capacity_classifier(self):
        self.assertTrue(agentctl.classify_capacity_text("HTTP 14003"))
        self.assertTrue(agentctl.classify_capacity_text("full capacity"))
        self.assertTrue(agentctl.classify_capacity_text("resource_limit reached"))
        self.assertFalse(agentctl.classify_capacity_text("normal completed turn"))

    def test_capacity_event_classifier_ignores_normal_tool_and_document_output(self):
        self.assertFalse(
            agentctl.classify_capacity_event(
                {
                    "type": "item.completed",
                    "item": {
                        "type": "command_execution",
                        "aggregated_output": "docs mention 14003 full capacity resource limit",
                    },
                }
            )
        )
        self.assertFalse(
            agentctl.classify_capacity_event(
                {"type": "agent_message", "message": "I am documenting the full capacity error"}
            )
        )
        self.assertTrue(
            agentctl.classify_capacity_event(
                {"type": "error", "message": "model is at full capacity", "code": 14003}
            )
        )
        self.assertTrue(
            agentctl.classify_capacity_event(
                {
                    "type": "turn.failed",
                    "error": {"message": "resource limit reached", "code": 14003},
                }
            )
        )

    def test_capacity_log_classifier_ignores_jsonl_command_output(self):
        jsonl = self.root / "codex.jsonl"
        plain = self.root / "codex.log"
        jsonl.write_text(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "type": "command_execution",
                        "aggregated_output": "review notes: 14003 full capacity resource limit",
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        plain.write_text("", encoding="utf-8")
        self.assertFalse(agentctl.logs_classify_capacity([jsonl, plain]))
        with jsonl.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps({"type": "error", "message": "full capacity", "code": 14003})
                + "\n"
            )
        self.assertTrue(agentctl.logs_classify_capacity([jsonl, plain]))

    def test_session_id_extraction(self):
        path = self.root / "events.jsonl"
        path.write_text(
            "not json\n"
            + json.dumps({"type": "thread.started", "thread_id": "thread-123"})
            + "\n"
            + json.dumps({"type": "turn.started", "session_id": "session-456"})
            + "\n",
            encoding="utf-8",
        )
        self.assertEqual(agentctl.extract_session_ids_from_jsonl(path), ("session-456", "thread-123"))

    def test_fake_tmux_codex_start_resume_status_and_stop(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest()))
        started = controller.start("task-one")
        self.assertEqual(started["status"], "RUNNING")
        self.assertEqual(fake_tmux.sessions["ac1-task-one"]["cwd"], self.repo)
        finished = self._wait_for_exit("task-one", controller)
        self.assertEqual(finished["codex_session_id"], "thread-one")
        reviewed = controller.watch_once("task-one")
        self.assertEqual(reviewed["status"], "READY_FOR_REVIEW")

        resumed = controller.resume(
            "task-one", model="Sol", reasoning_effort="xhigh", prompt=str(self.repo / "prompt.md")
        )
        self.assertEqual(resumed["status"], "RUNNING")
        self.assertEqual(resumed["model"], "Sol")
        self.assertEqual(resumed["reasoning_effort"], "xhigh")
        self.assertEqual(fake_tmux.sessions["ac1-task-one"]["cwd"], self.repo)
        self._wait_for_exit("task-one", controller)
        self.assertEqual(controller.watch_once("task-one")["status"], "READY_FOR_REVIEW")

        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "30"}):
            running = controller.resume("task-one")
            self.assertEqual(running["status"], "RUNNING")
            stopped = controller.stop("task-one")
        self.assertEqual(stopped["status"], "STOPPED")
        self.assertTrue(self.repo.exists())
        self.assertEqual(self._git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip(), "main")
        self.assertTrue((self.repo / "prompt.md").exists())
        self.assertTrue(Path(stopped["logs"]["jsonl"]).exists())

    def test_watch_capacity_waits_without_cleanup(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        with mock.patch.dict(os.environ, {"FAKE_CODEX_CAPACITY": "1"}):
            controller.register(self._write_manifest(self._manifest()))
            controller.start("task-one")
            self._wait_for_exit("task-one", controller)
            waiting = controller.watch_once("task-one")
        self.assertEqual(waiting["status"], "WAITING_CAPACITY")
        self.assertTrue(self.repo.exists())
        self.assertTrue((self.state_dir / "tasks" / "task-one.json").exists())
        fake_tmux.kill_session("ac1-task-one")

    def test_capacity_resume_keeps_thread_and_worktree(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest()))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_CAPACITY": "1"}):
            controller.start("task-one")
            self._wait_for_exit("task-one", controller)
            waiting = controller.watch_once("task-one")
        self.assertEqual(waiting["status"], "WAITING_CAPACITY")
        self.assertEqual(waiting["codex_session_id"], "thread-one")
        with mock.patch.dict(os.environ, {"FAKE_CODEX_CAPACITY": "0"}):
            resumed = controller.resume("task-one", model="Sol", reasoning_effort="xhigh")
        command = fake_tmux.sessions["ac1-task-one"]["command"]
        command_text = " ".join(command)
        self.assertIn('"resume"', command_text)
        self.assertIn('"thread-one"', command_text)
        self.assertEqual(fake_tmux.sessions["ac1-task-one"]["cwd"], self.repo)
        self.assertEqual(resumed["model"], "Sol")
        self._wait_for_exit("task-one", controller)
        controller.watch_once("task-one")
        controller.stop("task-one")

    def test_watch_recovers_false_capacity_state_while_process_is_live(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest()))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "2"}):
            controller.start("task-one")
            state_path = self.state_dir / "tasks" / "task-one.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["status"] = "WAITING_CAPACITY"
            state["runtime"]["warning"] = "WAITING_CAPACITY: stale false positive"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            running = controller.watch_once("task-one")
            controller.stop("task-one")
        self.assertEqual(running["status"], "RUNNING")
        self.assertIsNone(running["runtime"]["warning"])

    def test_three_concurrent_tasks_fourth_queued_then_dispatches(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        for index in range(1, 5):
            task_id = f"task-{index}"
            controller.register(
                self._write_manifest(
                    self._manifest(task_id, owned=[f"src/{task_id}/**"])
                )
            )
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            dispatch = controller.dispatch_once()
        self.assertEqual(len(dispatch["started"]), 3)
        self.assertEqual(controller.status("task-4")["status"], "QUEUED")
        controller.stop("task-1")
        resumed = controller.dispatch_once()
        self.assertIn("task-4", resumed["started"])
        self.assertEqual(controller.status("task-4")["status"], "RUNNING")
        for task_id in ("task-2", "task-3", "task-4"):
            controller.stop(task_id)

    def test_concurrent_starts_cannot_breach_third_slot(self):
        fake_tmux = SlowFakeTmux()
        controller = self._controller(fake_tmux)
        for index in range(1, 5):
            task_id = f"race-{index}"
            controller.register(
                self._write_manifest(
                    self._manifest(task_id, owned=[f"src/{task_id}/**"])
                )
            )

        errors = []
        barrier = threading.Barrier(3)

        def start_task(task_id):
            try:
                barrier.wait(timeout=5)
                controller.start(task_id)
            except Exception as exc:  # pragma: no cover - assertion below reports it
                errors.append(exc)

        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            controller.start("race-1")
            controller.start("race-2")
            threads = [
                threading.Thread(target=start_task, args=(task_id,))
                for task_id in ("race-3", "race-4")
            ]
            for thread in threads:
                thread.start()
            barrier.wait(timeout=5)
            for thread in threads:
                thread.join(timeout=5)

        self.assertFalse(errors)
        states = [controller.status(task_id) for task_id in ("race-3", "race-4")]
        self.assertEqual(sum(item["status"] == "RUNNING" for item in states), 1)
        self.assertEqual(sum(item["status"] == "QUEUED" for item in states), 1)
        self.assertLessEqual(
            controller.dispatch_once()["active"], controller.max_concurrency
        )
        for task_id in ("race-1", "race-2", "race-3", "race-4"):
            controller.stop(task_id)

    def test_resume_respects_capacity_and_dispatches_original_thread_later(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest("waiting")))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_CAPACITY": "1"}):
            controller.start("waiting")
            self._wait_for_exit("waiting", controller)
            self.assertEqual(controller.watch_once("waiting")["status"], "WAITING_CAPACITY")

        for index in range(1, 4):
            task_id = f"occupier-{index}"
            controller.register(
                self._write_manifest(
                    self._manifest(task_id, owned=[f"src/{task_id}/**"])
                )
            )
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            for index in range(1, 4):
                controller.start(f"occupier-{index}")
            waiting = controller.resume("waiting", model="Sol", reasoning_effort="xhigh")
        self.assertEqual(waiting["status"], "QUEUED")
        self.assertEqual(waiting["run_count"], 1)
        self.assertEqual(waiting["codex_session_id"], "thread-one")

        for index in range(1, 4):
            controller.stop(f"occupier-{index}")
        resumed = controller.dispatch_once()
        self.assertIn("waiting", resumed["started"])
        running = controller.status("waiting")
        self.assertEqual(running["status"], "RUNNING")
        self.assertEqual(running["run_count"], 2)
        self.assertEqual(running["codex_session_id"], "thread-one")
        self.assertEqual(fake_tmux.sessions["ac1-waiting"]["cwd"], self.repo)
        controller.stop("waiting")

    def test_context_stale_blocks_resume(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest()))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            controller.start("task-one")
        controller.stop("task-one")
        (self.repo / "authority.md").write_text("drifted authority\n", encoding="utf-8")
        with self.assertRaises(agentctl.StateError) as raised:
            controller.resume("task-one")
        self.assertIn("CONTEXT_STALE", str(raised.exception))
        state = controller.store.load("task-one")
        self.assertEqual(state["guard"], "CONTEXT_STALE")

    def test_jsonl_summary_is_short_and_structured(self):
        path = self.root / "summary.jsonl"
        path.write_text(
            json.dumps({"type": "thread.started", "thread_id": "thread-summary"})
            + "\n"
            + json.dumps(
                {
                    "type": "agent_message",
                    "message": "Running python3 -m unittest test_controller; test passed",
                    "command": "python3 -m unittest test_controller",
                }
            )
            + "\n"
            + json.dumps({"type": "turn.completed", "status": "ok"})
            + "\n",
            encoding="utf-8",
        )
        summary = agentctl.summarize_jsonl(path)
        self.assertEqual(summary["session_id"], "thread-summary")
        self.assertEqual(summary["current_command"], "python3 -m unittest test_controller")
        self.assertIn("test passed", summary["last_agent_message"])
        self.assertTrue(summary["tests"])

    def test_stall_marks_suspected_without_killing(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux, stall_seconds=1)
        controller.register(self._write_manifest(self._manifest()))
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            controller.start("task-one")
        state = controller.store.load("task-one")
        old = "2000-01-01T00:00:00Z"
        state["runtime"]["run_started_at"] = old
        state["runtime"]["last_log_growth_at"] = old
        controller.store.write(state)
        watched = controller.watch_once("task-one")
        self.assertEqual(watched["status"], "SUSPECTED_STALL")
        self.assertTrue(fake_tmux.has_session("ac1-task-one"))
        self.assertIsNone(fake_tmux.sessions["ac1-task-one"]["process"].poll())
        controller.stop("task-one")

    def test_reviewer_read_only_and_fixer_uses_same_thread_worktree(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest("writer")))
        controller.start("writer")
        finished = self._wait_for_exit("writer", controller)
        controller.watch_once("writer")
        controller.checkpoint("writer", phase="IMPLEMENT")
        controller.checkpoint("writer", phase="FOCUSED_REVIEW")
        reviewer_manifest = self._manifest(
            "reviewer",
            owned=[],
            role="reviewer",
            review_of="writer",
        )
        controller.register(self._write_manifest(reviewer_manifest))
        with self.assertRaises(agentctl.StateError):
            controller.fix("reviewer")
        controller.start("reviewer")
        self._wait_for_exit("reviewer", controller)
        reviewed = controller.watch_once("reviewer")
        self.assertEqual(reviewed["status"], "READY_FOR_REVIEW")
        self.assertTrue(controller.store.load("reviewer")["review_completed"])

        with self.assertRaises(agentctl.StateError):
            controller.register(
                self._write_manifest(
                    self._manifest(
                        "reviewer-again",
                        owned=[],
                        role="reviewer",
                        review_of="writer",
                    )
                )
            )

        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            fixed = controller.fix("writer")
        self.assertEqual(fixed["status"], "RUNNING")
        self.assertEqual(fixed["phase"], "FIX")
        self.assertEqual(fixed["codex_session_id"], finished["codex_session_id"])
        self.assertEqual(fake_tmux.sessions["ac1-writer"]["cwd"], self.repo)
        controller.stop("writer")
        controller.stop("reviewer")

    def test_review_is_invalidated_when_target_evidence_changes(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest("writer")))
        controller.start("writer")
        self._wait_for_exit("writer", controller)
        controller.watch_once("writer")
        controller.checkpoint("writer", phase="IMPLEMENT")
        controller.checkpoint("writer", phase="FOCUSED_REVIEW")
        controller.register(
            self._write_manifest(
                self._manifest("reviewer", owned=[], role="reviewer", review_of="writer")
            )
        )
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "0.4"}):
            controller.start("reviewer")
            target = controller.store.load("writer")
            target["evidence_revision"] = int(target.get("evidence_revision") or 0) + 1
            controller.store.write(target)
            self._wait_for_exit("reviewer", controller)
        invalidated = controller.watch_once("reviewer")
        self.assertEqual(invalidated["status"], "CONTRACT_CONFLICT")
        self.assertEqual(invalidated["guard"], "CONTEXT_STALE")
        self.assertFalse(controller.store.load("reviewer")["review_completed"])
        self.assertTrue(controller.store.load("reviewer")["review_invalidated"])
        controller.stop("writer")
        controller.stop("reviewer")

    def test_legacy_implementing_fixing_state_migrates_for_watch_resume_and_fix(self):
        fake_tmux = FakeTmux()
        controller = self._controller(fake_tmux)
        controller.register(self._write_manifest(self._manifest("legacy")))
        controller.start("legacy")
        self._wait_for_exit("legacy", controller)
        legacy = controller.store.load("legacy")
        legacy["phase"] = "IMPLEMENTING"
        legacy["status"] = "IMPLEMENTING"
        legacy["schema_version"] = 1
        controller.store.write(legacy)
        migrated = controller.store.load("legacy")
        self.assertEqual(migrated["phase"], "IMPLEMENT")
        self.assertEqual(migrated["status"], "IMPLEMENT")
        self.assertEqual(migrated["schema_version"], agentctl.STATE_SCHEMA_VERSION)
        watched = controller.watch_once("legacy")
        self.assertEqual(watched["status"], "READY_FOR_REVIEW")

        legacy = controller.store.load("legacy")
        legacy["phase"] = "FIXING"
        legacy["status"] = "FIXING"
        legacy["schema_version"] = 1
        controller.store.write(legacy)
        with mock.patch.dict(os.environ, {"FAKE_CODEX_SLEEP": "20"}):
            fixed = controller.fix("legacy")
        self.assertEqual(fixed["phase"], "FIX")
        self.assertEqual(fixed["status"], "RUNNING")
        controller.stop("legacy")

    def test_legacy_manifest_maps_to_execution_policy_and_runner_session(self):
        controller = self._controller()
        registered = controller.register(self._write_manifest(self._manifest("profile-legacy")))
        self.assertIn("execution_policy", registered)
        self.assertTrue(str(registered["active_profile"]).startswith("legacy-codex:"))
        self.assertEqual(registered["runner_kind"], "codex-cli")
        self.assertEqual(registered["repair_cycle_count"], 0)
        stored = controller.store.load("profile-legacy")
        self.assertEqual(stored["schema_version"], agentctl.STATE_SCHEMA_VERSION)
        controller.start("profile-legacy")
        finished = self._wait_for_exit("profile-legacy", controller)
        self.assertEqual(finished["runner_session_id"], "thread-one")
        self.assertEqual(finished["codex_session_id"], "thread-one")
        self.assertTrue(str(finished["current_run_id"]).startswith("run-001-"))

    def test_execution_policy_manifest_uses_named_codex_profile(self):
        value = self._manifest("profile-named")
        del value["model"]
        del value["reasoning_effort"]
        value["execution_policy"] = {
            "initial_writer_profile": "codex-luna-primary",
            "fixer_profile": "cursor-grok45-fixer",
            "reviewer_profile": "codex-luna-review",
            "diagnoser_profile": "codex-sol-xhigh-diagnose",
            "backup_profile": "codebuddy-kimi-k3-backup",
            "max_fix_cycles_before_diagnosis": 2,
            "max_post_diagnosis_fix_cycles": 1,
            "require_fresh_review_session": True,
            "automatic_provider_switch": False,
        }
        controller = self._controller()
        registered = controller.register(self._write_manifest(value))
        self.assertEqual(registered["active_profile"], "codex-luna-primary")
        self.assertEqual(registered["model"], "GPT-5.6 Luna Max")
        self.assertEqual(registered["runner_kind"], "codex-cli")
        self.assertEqual(
            registered["execution_policy"]["fixer_profile"], "cursor-grok45-fixer"
        )

    def test_cp1_rejects_non_codex_launch_profile(self):
        value = self._manifest("bad-launch")
        del value["model"]
        del value["reasoning_effort"]
        value["execution_policy"] = {
            "initial_writer_profile": "cursor-grok45-fixer",
            "fixer_profile": "cursor-grok45-fixer",
            "reviewer_profile": "codex-luna-review",
            "diagnoser_profile": "codex-sol-xhigh-diagnose",
        }
        with self.assertRaises(agentctl.ManifestError):
            agentctl.validate_manifest(value)

    def test_load_profiles_and_doctor_include_profiles_check(self):
        profiles = agentctl.load_profiles()
        self.assertIn("codex-luna-primary", profiles)
        self.assertIn("cursor-grok45-fixer", profiles)
        self.assertEqual(profiles["cursor-grok45-fixer"]["runner"], "cursor-cli")
        report = agentctl.doctor_report(
            codex_binary=str(self.codex_script),
        )
        self.assertTrue(report["checks"]["profiles"]["ok"])
        self.assertGreaterEqual(report["checks"]["profiles"]["count"], 5)

    def test_cursor_runner_uses_argv_prompt_mode_and_workspace(self):
        adapter = agentctl.CursorCliRunner("/tmp/fake-agent")
        argv = adapter.build_initial_argv(
            model="grok-4.5-high-fast",
            reasoning_effort="high",
            worktree=self.repo,
            last_message=self.state_dir / "cursor-last.txt",
        )
        self.assertEqual(adapter.prompt_mode, "argv")
        self.assertIn("--print", argv)
        self.assertIn("stream-json", argv)
        self.assertIn("--workspace", argv)
        self.assertEqual(argv[argv.index("--workspace") + 1], str(self.repo))
        self.assertIn("--force", argv)

    def test_verify_and_profile_fix_repairs_pilot_quota_bug(self):
        pilot_dir = Path(agentctl.__file__).resolve().parent / "pilot"
        quota = pilot_dir / "quota.py"
        original = quota.read_text(encoding="utf-8")
        self.addCleanup(quota.write_text, original, "utf-8")
        if "return limit - used + 1" not in original:
            self.fail("pilot quota.py no longer contains the intentional off-by-one")

        # Point the temporary repo at the real pilot package via acceptance cmd.
        acceptance = (
            f"python3 -m unittest discover -s {pilot_dir} -p 'test_*.py' -v"
        )
        value = self._manifest("mm-pilot-quota")
        value["owned_write_set"] = ["tools/agentctl/pilot/**"]
        value["acceptance_commands"] = [acceptance]
        value["execution_policy"] = {
            "initial_writer_profile": "codex-luna-primary",
            "fixer_profile": "cursor-grok45-fixer",
            "reviewer_profile": "codex-luna-review",
            "diagnoser_profile": "codex-sol-xhigh-diagnose",
            "backup_profile": "codebuddy-kimi-k3-backup",
            "max_fix_cycles_before_diagnosis": 2,
            "max_post_diagnosis_fix_cycles": 1,
            "require_fresh_review_session": True,
            "automatic_provider_switch": False,
        }
        del value["model"]
        del value["reasoning_effort"]

        fake_cursor = self._install_fake_cursor_fixer()
        controller = self._controller(cursor=agentctl.CursorCliRunner(str(fake_cursor)))
        controller.register(self._write_manifest(value))

        failed = controller.verify("mm-pilot-quota")
        self.assertEqual(failed["verify"]["result"], "CODE_FAILURE")
        self.assertTrue(failed["verify"]["failure_signatures"])

        fixed_run = controller.fix(
            "mm-pilot-quota",
            profile="cursor-grok45-fixer",
            from_verify=failed["verify"]["verify_id"],
        )
        self.assertEqual(fixed_run["runner_kind"], "cursor-cli")
        self.assertEqual(fixed_run["active_profile"], "cursor-grok45-fixer")
        finished = self._wait_for_exit("mm-pilot-quota", controller, timeout=8)
        self.assertEqual(finished["runner_session_id"], "cursor-session-one")

        passed = controller.verify("mm-pilot-quota")
        self.assertEqual(passed["verify"]["result"], "PASS")
        self.assertIn("return limit - used", quota.read_text(encoding="utf-8"))
        self.assertNotIn("return limit - used + 1", quota.read_text(encoding="utf-8"))
        runs = controller.runs("mm-pilot-quota")
        self.assertGreaterEqual(runs["run_count"], 1)
        self.assertTrue(any("cursor" in str(item.get("profile_id") or "") for item in runs["runs"]))
        nxt = controller.next_action("mm-pilot-quota")
        self.assertIn("SUGGEST_LUNA_REVIEW", nxt["suggested_next_actions"])

    def test_review_writes_findings_and_next_suggests_fix_when_required(self):
        pilot_dir = Path(agentctl.__file__).resolve().parent / "pilot"
        quota = pilot_dir / "quota.py"
        original = quota.read_text(encoding="utf-8")
        # Ensure acceptance currently passes for this review-only test.
        quota.write_text(original.replace("return limit - used + 1", "return limit - used"), encoding="utf-8")
        self.addCleanup(quota.write_text, original, "utf-8")

        fake_reviewer = self.root / "fake-reviewer"
        fake_reviewer.write_text(
            """#!/usr/bin/env python3
import json, sys
from pathlib import Path
# Codex prompt_mode=stdin: review pack arrives on stdin, not argv.
prompt = sys.stdin.read()
print(json.dumps({'type': 'thread.started', 'thread_id': 'review-thread'}), flush=True)
target = None
for line in prompt.splitlines():
    if 'Write JSON to `' in line:
        target = line.split('`')[1]
        break
if not target:
    raise SystemExit('fake reviewer missing findings path in prompt')
Path(target).write_text(json.dumps({
    'verdict': 'REQUIRED_FIX',
    'findings': [{
        'finding_id': 'F-PILOT-01',
        'severity': 'HIGH',
        'invariant': 'remaining must be exact',
        'file': 'tools/agentctl/pilot/quota.py',
        'evidence': 'reviewer synthetic finding',
        'required_fix': 'keep formula exact',
        'acceptance': 'unittest pilot'
    }]
}), encoding='utf-8')
raise SystemExit(0)
""",
            encoding="utf-8",
        )
        fake_reviewer.chmod(0o755)

        value = self._manifest("mm-pilot-review")
        value["owned_write_set"] = ["tools/agentctl/pilot/**"]
        value["acceptance_commands"] = [
            f"python3 -m unittest discover -s {pilot_dir} -p 'test_*.py' -v"
        ]
        value["execution_policy"] = {
            "initial_writer_profile": "codex-luna-primary",
            "fixer_profile": "cursor-grok45-fixer",
            "reviewer_profile": "codex-luna-review",
            "diagnoser_profile": "codex-sol-xhigh-diagnose",
            "backup_profile": "codebuddy-kimi-k3-backup",
            "max_fix_cycles_before_diagnosis": 2,
            "max_post_diagnosis_fix_cycles": 1,
            "require_fresh_review_session": True,
            "automatic_provider_switch": False,
        }
        del value["model"]
        del value["reasoning_effort"]
        controller = agentctl.Controller(
            state_dir=self.state_dir,
            codex=agentctl.CodexCliRunner(str(fake_reviewer)),
            cursor=agentctl.CursorCliRunner(str(self.codex_script)),
            codebuddy=agentctl.CodeBuddyCliRunner(str(self.codex_script)),
            tmux=FakeTmux(),
        )
        controller.register(self._write_manifest(value))
        verified = controller.verify("mm-pilot-review")
        self.assertEqual(verified["verify"]["result"], "PASS")
        controller.review("mm-pilot-review", profile="codex-luna-review")
        finished = self._wait_for_exit("mm-pilot-review", controller, timeout=8)
        watched = controller.watch_once("mm-pilot-review")
        self.assertEqual(watched.get("last_findings_verdict") or controller.store.load("mm-pilot-review").get("last_findings_verdict"), "REQUIRED_FIX")
        nxt = controller.next_action("mm-pilot-review")
        self.assertIn("SUGGEST_GROK_FIX", nxt["suggested_next_actions"])
        self.assertGreaterEqual(controller.store.load("mm-pilot-review").get("review_round_count", 0), 1)

    def test_diagnose_requires_repair_cycles_and_fallback_needs_reason(self):
        controller = self._controller()
        value = self._manifest("mm-diag")
        value["execution_policy"] = {
            "initial_writer_profile": "codex-luna-primary",
            "fixer_profile": "cursor-grok45-fixer",
            "reviewer_profile": "codex-luna-review",
            "diagnoser_profile": "codex-sol-xhigh-diagnose",
            "backup_profile": "codebuddy-kimi-k3-backup",
            "max_fix_cycles_before_diagnosis": 2,
            "max_post_diagnosis_fix_cycles": 1,
            "require_fresh_review_session": True,
            "automatic_provider_switch": False,
        }
        del value["model"]
        del value["reasoning_effort"]
        controller.register(self._write_manifest(value))
        with self.assertRaises(agentctl.StateError):
            controller.diagnose("mm-diag")
        state = controller.store.load("mm-diag")
        state["repair_cycle_count"] = 2
        state["last_verify_result"] = "CODE_FAILURE"
        state["last_verify_id"] = "verify-001"
        controller.store.write(state)
        # create a fake verify artifact for diagnose/fallback
        verify_dir = controller.store.task_dir("mm-diag") / "verify"
        verify_dir.mkdir(parents=True, exist_ok=True)
        (verify_dir / "verify-001.json").write_text(
            json.dumps(
                {
                    "verify_id": "verify-001",
                    "result": "CODE_FAILURE",
                    "commands": [{"command": "false", "exit_code": 1, "stdout": "", "stderr": "FAIL"}],
                    "failure_signatures": ["FAIL"],
                }
            ),
            encoding="utf-8",
        )
        started = controller.diagnose("mm-diag")
        self.assertEqual(started["phase"], "DIAGNOSE")
        self.assertTrue(str(started["current_run_id"]).startswith("run-"))
        controller.stop("mm-diag")
        with self.assertRaises(agentctl.ManifestError):
            controller.fallback("mm-diag", reason="")
        backed = controller.fallback("mm-diag", reason="cursor unavailable for pilot", profile="codebuddy-kimi-k3-backup")
        self.assertEqual(backed["runner_kind"], "codebuddy-cli")
        self.assertEqual(backed["active_profile"], "codebuddy-kimi-k3-backup")


if __name__ == "__main__":
    unittest.main()
