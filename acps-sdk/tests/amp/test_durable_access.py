"""Real local process exits, ownership, immutable replay and middleware behavior."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from acps_sdk.amp import AccessBody, AccessParticipant, AccessRequest, AccessResponse
from acps_sdk.amp.durable_access import AccessLogIntegrityError, DurableAccessEmitter
from acps_sdk.amp.group_access import GroupAccessMiddleware

FIXTURE = Path(__file__).with_name("fixtures") / "durable_access_process.py"
KEY = ("session", "task", "receive")


def intent():
    return AccessBody(request=AccessRequest(method="start", route="database.inspect"),
        caller=AccessParticipant(aic="caller"), callee=AccessParticipant(aic="worker"))


def success():
    return intent().model_copy(update={"response": AccessResponse(status_code=200), "duration_ms": 1.0})


class DurableAccessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        base = os.environ.get("INCIDENT_TEST_TMPDIR")
        self.temp = tempfile.TemporaryDirectory(dir=base)
        self.path = Path(self.temp.name) / "amp_access_worker.jsonl"
        self.emitters = []

    def tearDown(self):
        for emitter in self.emitters:
            emitter.close_sync()
        self.temp.cleanup()

    def emitter(self, aic="worker", cls=DurableAccessEmitter):
        value = cls(self.path, aic)
        self.emitters.append(value)
        return value

    def begin(self, emitter):
        return emitter.begin_sync(KEY, intent(), trace_id=KEY[0], correlation_id=KEY[1])

    def fixture_inputs(self, mode):
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path and Path(path).is_dir())
        executable = getattr(sys, "_base_executable", sys.executable)
        return [executable, str(FIXTURE), mode, str(self.path)], env

    def run_fixture(self, mode, expected):
        command, env = self.fixture_inputs(mode)
        value = subprocess.run(command,
            stdin=subprocess.DEVNULL, capture_output=True, timeout=30, env=env)
        self.assertEqual(value.returncode, expected, value.stderr.decode(errors="replace"))
        return value

    def record_in_journal(self):
        with closing(sqlite3.connect(self.path.with_suffix(".outbox.sqlite3"))) as connection:
            return connection.execute("SELECT log_id,record,written FROM attempts").fetchone()

    def rows(self):
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines()]

    def test_hard_exit_during_handler_recovers_honest_unknown_in_fresh_process(self):
        self.run_fixture("handler", 71)
        original_id, original_record, _ = self.record_in_journal()
        self.assertIsNone(original_record)
        effect = self.path.with_suffix(".effect.txt").read_bytes()
        self.run_fixture("recover", 0)
        row = self.rows()[0]
        self.assertEqual(row["log_id"], original_id)
        self.assertEqual((row["trace_id"], row["correlation_id"]), KEY[:2])
        self.assertEqual(row["body"]["error"]["code"], "OutcomeUnconfirmed")
        self.assertNotIn("response", row["body"])
        self.assertNotIn("durationMs", row["body"])
        before = self.path.read_bytes()
        self.run_fixture("recover", 0)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.path.with_suffix(".effect.txt").read_bytes(), effect)

    def test_hard_exit_after_ready_snapshot_replays_original_known_outcome(self):
        self.run_fixture("ready", 72)
        log_id, record, written = self.record_in_journal()
        self.assertEqual(written, 0)
        self.run_fixture("recover", 0)
        self.assertEqual(self.rows(), [json.loads(record)])
        self.assertEqual(self.record_in_journal(), (log_id, record, 1))

    def test_hard_exit_after_append_before_ack_adds_no_duplicate(self):
        self.run_fixture("append", 73)
        before = self.path.read_bytes()
        original = self.record_in_journal()
        self.assertEqual(original[2], 0)
        self.run_fixture("recover", 0)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.record_in_journal(), (*original[:2], 1))

    def test_visible_line_from_pre_fsync_crash_is_synced_before_ack(self):
        self.run_fixture("before_fsync", 75)
        before = self.path.read_bytes()
        emitter = self.emitter()
        with patch("acps_sdk.amp.durable_access.os.fsync", wraps=os.fsync) as synced:
            emitter.start_sync()
        self.assertGreater(synced.call_count, 0)
        emitter.close_sync()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.record_in_journal()[2], 1)

    def test_partial_line_is_retained_and_recovery_refuses_to_ack(self):
        self.run_fixture("partial", 74)
        before = self.path.read_bytes()
        emitter = self.emitter()
        with self.assertRaises(AccessLogIntegrityError):
            emitter.start_sync()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.record_in_journal()[2], 0)

    def test_only_one_owner_and_identity_binding_survive_close(self):
        first, second = self.emitter(), self.emitter()
        first.start_sync()
        with self.assertRaises(sqlite3.OperationalError):
            second.start_sync()
        first.close_sync()
        second.start_sync()
        second.close_sync()
        other = self.emitter("other-worker")
        with self.assertRaises(AccessLogIntegrityError):
            other.start_sync()

    def test_other_process_cannot_own_same_canonical_log(self):
        command, env = self.fixture_inputs("hold")
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        try:
            ready = self.path.with_suffix(".ready.json")
            deadline = time.monotonic() + 15
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(ready.exists())
            self.assertEqual(json.loads(ready.read_bytes())["pid"], process.pid)
            self.assertIsNone(process.poll())
            alias = self.path.parent / "unused" / ".." / self.path.name
            competitor = DurableAccessEmitter(alias, "worker")
            self.emitters.append(competitor)
            with self.assertRaises(sqlite3.OperationalError):
                competitor.start_sync()
        finally:
            try:
                process.communicate(b"release\n", timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0)
        competitor.start_sync()

    def test_close_cannot_release_owner_while_callback_is_active(self):
        emitter, second = self.emitter(), self.emitter()
        self.assertTrue(self.begin(emitter))
        with self.assertRaises(RuntimeError):
            emitter.close_sync()
        with self.assertRaises(sqlite3.OperationalError):
            second.start_sync()
        emitter.finish_sync(KEY, success())
        emitter.close_sync()
        second.start_sync()
        self.assertEqual(len(self.rows()), 1)

    def test_same_key_after_restart_keeps_first_outcome_immutable(self):
        emitter = self.emitter()
        self.begin(emitter)
        emitter.finish_sync(KEY, success())
        emitter.close_sync()
        before = self.path.read_bytes()
        self.assertFalse(self.begin(emitter))
        emitter.finish_sync(KEY, intent().model_copy(update={"response": AccessResponse(status_code=500)}))
        self.assertEqual(self.path.read_bytes(), before)

    def test_changed_request_metadata_is_rejected_before_reuse(self):
        emitter = self.emitter()
        self.begin(emitter)
        emitter.finish_sync(KEY, success())
        changed = intent().model_copy(update={"callee": AccessParticipant(aic="different")})
        with self.assertRaises(AccessLogIntegrityError):
            emitter.begin_sync(KEY, changed, trace_id=KEY[0], correlation_id=KEY[1])

    def test_conflicting_existing_log_id_is_retained_and_rejected(self):
        self.run_fixture("ready", 72)
        _, record, _ = self.record_in_journal()
        value = json.loads(record)
        value["body"]["response"]["statusCode"] = 500
        self.path.write_text(json.dumps(value) + "\n", encoding="utf-8")
        before = self.path.read_bytes()
        with self.assertRaises(AccessLogIntegrityError):
            self.emitter().start_sync()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.record_in_journal()[2], 0)

    def test_failed_append_is_not_acknowledged_and_retries_same_id(self):
        class Flaky(DurableAccessEmitter):
            fail = True
            def _append(self, record):
                if self.fail:
                    raise OSError("fixture append refused")
                return super()._append(record)
        emitter = self.emitter(cls=Flaky)
        with self.assertRaises(OSError):
            emitter.emit_sync(success(), trace_id="session", correlation_id="task")
        emitter.fail = False
        emitter.close_sync()
        log_id, record, written = self.record_in_journal()
        self.assertEqual((self.rows()[0]["log_id"], written), (log_id, 1))
        self.assertEqual(self.rows()[0], json.loads(record))

    def test_concurrent_completed_events_have_unique_complete_records(self):
        emitter = self.emitter()
        with ThreadPoolExecutor(max_workers=4) as pool:
            ids = list(pool.map(lambda n: emitter.emit_sync(success(), correlation_id=str(n)), range(12)))
        self.assertEqual(len(set(ids)), 12)
        self.assertEqual({row["log_id"] for row in self.rows()}, set(ids))

    def test_unrecorded_finished_outcome_recovers_unknown_without_restart(self):
        class CannotFreezeSuccess(DurableAccessEmitter):
            def _record(self, log_id, body, **kwargs):
                if body.response is not None:
                    raise OSError("fixture final snapshot refused")
                return super()._record(log_id, body, **kwargs)
        emitter = self.emitter(cls=CannotFreezeSuccess)
        self.begin(emitter)
        emitter.flush_sync()
        self.assertFalse(self.path.exists())
        with self.assertRaises(OSError):
            emitter.finish_sync(KEY, success())
        emitter.flush_sync()
        self.assertEqual(self.rows()[0]["body"]["error"]["code"], "OutcomeUnconfirmed")
        self.assertNotIn("response", self.rows()[0]["body"])

    async def test_durable_middleware_records_intent_before_handler_and_not_payload(self):
        emitter = self.emitter()
        command = SimpleNamespace(command=SimpleNamespace(value="start"), sessionId="session", taskId="task",
            senderId="caller", mentions=["worker"], groupId="group", commandParams={"skill": "database.inspect", "secret": "do-not-store-payload"}, dataItems=[])
        invoked = []
        async def callback():
            invoked.append(True)
            self.assertFalse(self.path.exists())
            with self.assertRaises(sqlite3.OperationalError):
                self.emitter().start_sync()
            return {"status": "ok"}
        middleware = GroupAccessMiddleware(emitter, "worker")
        self.assertEqual(await middleware.run(command, callback), {"status": "ok"})
        emitter.close_sync()
        self.assertNotIn(b"do-not-store-payload", emitter.journal_path.read_bytes())
        self.assertEqual(len(self.rows()), 1)
        async def redelivered():
            invoked.append(True)
            return {"status": "tool_error"}
        await GroupAccessMiddleware(emitter, "worker").run(command, redelivered)
        self.assertEqual(len(invoked), 2)
        self.assertEqual(self.rows()[0]["body"]["response"]["statusCode"], 200)

    async def test_live_middleware_checks_changed_duplicate_metadata(self):
        emitter = self.emitter()
        command = SimpleNamespace(command=SimpleNamespace(value="start"), sessionId="session", taskId="task",
            senderId="caller", mentions=["worker"], groupId="group", commandParams={}, dataItems=[])
        invoked = []
        async def callback():
            invoked.append(True)
            return {"status": "ok"}
        middleware = GroupAccessMiddleware(emitter, "worker")
        await middleware.run(command, callback)
        command.senderId = "different-caller"
        with self.assertRaises(AccessLogIntegrityError):
            await middleware.run(command, callback)
        self.assertEqual(invoked, [True])

    async def test_failed_intent_persistence_prevents_handler_execution(self):
        owner, other = self.emitter(), self.emitter()
        owner.start_sync()
        command = SimpleNamespace(command=SimpleNamespace(value="start"), sessionId="session", taskId="task",
            senderId="caller", mentions=["worker"], groupId="group", commandParams={}, dataItems=[])
        invoked = []
        async def callback():
            invoked.append(True)
        with self.assertRaises(sqlite3.OperationalError):
            await GroupAccessMiddleware(other, "worker").run(command, callback)
        self.assertEqual(invoked, [])


if __name__ == "__main__":
    unittest.main()
