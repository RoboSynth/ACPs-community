"""Real interpreter restart checks for the public task journal/wrapper APIs."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path

SDK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SDK_ROOT))
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "coordinator_process.py"


def fixture_command(phase, journal, receipt, *extra):
    # A Windows venv executable may be a launcher with a child interpreter.
    # Hold the interpreter itself so terminating this fixture cannot orphan it.
    interpreter = getattr(sys, "_base_executable", sys.executable)
    bootstrap = "import json,runpy,sys; sys.path[:0]=json.loads(sys.argv.pop(1)); runpy.run_path(sys.argv.pop(1),run_name='__main__')"
    return [interpreter, "-I", "-B", "-c", bootstrap, json.dumps(sys.path), str(FIXTURE),
            phase, "--journal", str(journal), "--receipt", str(receipt), *extra]


class CoordinatorProcessTests(unittest.TestCase):
    def run_restart_case(self, exit_mode):
        evidence_root = os.environ.get("INCIDENT_T30_EVIDENCE_DIR")
        evidence = Path(evidence_root) / exit_mode if evidence_root else None
        if evidence:
            evidence.mkdir(parents=True, exist_ok=False)
        process_receipts = []
        outcome = {"schema_version": "coordinator-restart-test/1", "exit_mode": exit_mode,
                   "status": "failed", "platform_or_business_started": False}
        working_directory = nullcontext(evidence) if evidence else tempfile.TemporaryDirectory(prefix="coordinator-restart-")
        with working_directory as directory:
            temporary = Path(directory)
            journal = temporary / "tasks.sqlite"
            writer_receipt, reader_receipt = temporary / "writer.json", temporary / "reader.json"
            marker = temporary / "finally-ran.txt"
            env = dict(os.environ, PYTHONPATH=str(SDK_ROOT), PYTHONDONTWRITEBYTECODE="1")
            command = fixture_command("seed", journal, writer_receipt,
                "--finally-marker", str(marker), "--exit-mode", exit_mode)
            child = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            terminated_after_ready = False
            try:
                if exit_mode == "wait-for-parent":
                    deadline = time.monotonic() + 15
                    while not writer_receipt.exists() and child.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.02)
                    self.assertTrue(writer_receipt.exists(), "writer did not reach its durable-ready handshake")
                    self.assertIsNone(child.poll(), "writer must be alive when parent requests termination")
                    child.terminate()  # Only the exact Popen handle created by this test.
                    terminated_after_ready = True
                stdout, stderr = child.communicate(timeout=15)
                process_receipts.append({"phase": "writer", "pid": child.pid, "actual_returncode": child.returncode,
                    "terminated_after_durable_ready": terminated_after_ready, "stdout": stdout, "stderr": stderr})
                if exit_mode == "hard-exit":
                    self.assertEqual(child.returncode, 37)
                else:
                    self.assertNotEqual(child.returncode, 0)
                self.assertEqual(stderr, "")
                self.assertFalse(marker.exists(), "hard termination must bypass Python finally cleanup")
                before = json.loads(writer_receipt.read_text(encoding="utf-8"))
                self.assertEqual(before["pid"], child.pid)
                self.assertEqual(before["parent_pid"], os.getpid())
                self.assertEqual(before["coordinator_term"], 1)

                reader = subprocess.Popen(fixture_command("recover", journal, reader_receipt),
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    stdout, stderr = reader.communicate(timeout=15)
                finally:
                    if reader.poll() is None:
                        reader.kill()
                        reader.communicate(timeout=5)
                process_receipts.append({"phase": "reader", "pid": reader.pid, "actual_returncode": reader.returncode,
                                         "stdout": stdout, "stderr": stderr})
                self.assertEqual((reader.returncode, stderr), (0, ""))
                after = json.loads(reader_receipt.read_text(encoding="utf-8"))
                self.assertEqual(after["pid"], reader.pid)
                self.assertEqual(after["parent_pid"], os.getpid())
                self.assertEqual(after["coordinator_term"], 2)
                for field in ("tasks", "in_flight", "events", "source_sha256"):
                    self.assertEqual(before[field], after[field], field)
                self.assertEqual(after["transport_calls"], [], "reopening must not send or replay tasks")
                self.assertEqual(len(after["tasks"]), 5)
                self.assertTrue(all(row["coordinator_term"] == 1 for row in after["tasks"]))
                self.assertEqual({(row["logical_task_id"], row["task_id"], row["partner_aic"], row["command"], row["state"])
                    for row in after["in_flight"]}, {
                    ("logical-observe", "task-observe", "application", "start", "sent"),
                    ("logical-observe", "task-observe", "database-primary", "continue", "working"),
                    ("logical-unknown", "task-unknown", "database-backup", "start", "delivery_unknown")})
                self.assertEqual(sum(row["state"] == "completed" for row in after["tasks"]), 1)
                histories = {tuple(event["state"] for event in history) for history in after["events"].values()}
                self.assertEqual(histories, {("dispatching", "sent"), ("dispatching", "sent", "working"),
                    ("dispatching", "sent", "completed"), ("dispatching", "delivery_unknown")})
                self.assertEqual(sum(len(history) for history in after["events"].values()), 13)
                outcome.update(status="passed", task_attempts=5, in_flight_attempts=3, persisted_events=13,
                    recovered_term=2, cleanup_finally_ran=marker.exists(), replayed_transport_calls=0)
            finally:
                if child.poll() is None:
                    child.kill()
                    stdout, stderr = child.communicate(timeout=5)
                    process_receipts.append({"phase": "writer-cleanup", "pid": child.pid,
                        "actual_returncode": child.returncode, "stdout": stdout, "stderr": stderr})
                if evidence:
                    for receipt in (writer_receipt, reader_receipt):
                        if receipt.exists() and receipt != evidence / receipt.name:
                            shutil.copyfile(receipt, evidence / receipt.name)
                    outcome["process_receipts"] = process_receipts
                    outcome["source_sha256"] = {Path(__file__).resolve().relative_to(SDK_ROOT).as_posix():
                        hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
                    (evidence / "test-result.json").write_text(json.dumps(outcome, indent=2) + "\n", encoding="utf-8")

    def test_hard_exit_recovers_all_durable_inflight_attempts(self):
        self.run_restart_case("hard-exit")

    def test_parent_termination_recovers_all_durable_inflight_attempts(self):
        self.run_restart_case("wait-for-parent")


if __name__ == "__main__":
    unittest.main()
