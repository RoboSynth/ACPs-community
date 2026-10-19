"""Offline coordinator process fixture; never opens a platform connection."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

SDK_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SDK_ROOT))

print("fixture: importing SDK", flush=True)
from acps_sdk.aip.aip_base_model import TaskResult, TaskState, TaskStatus
from acps_sdk.aip.resumable import CoordinatorTerm, ResumableGroupLeader, TaskJournal
print("fixture: SDK ready", flush=True)


class OfflineLeader:
    """Transport double: real SDK wrapper/journal, no broker or resource action."""

    def __init__(self):
        self.group_sessions = {"session-fixture": SimpleNamespace(group_id="group-fixture")}
        self.listeners = []
        self.calls = []

    def add_task_listener(self, listener):
        self.listeners.append(listener)

    async def start_task(self, session_id, *, task_content, task_id, target_partners):
        self.calls.append({"command": "start", "task_id": task_id, "partners": target_partners})
        if task_id == "task-unknown":
            raise TimeoutError("synthetic transport outcome is unknown")
        return task_id

    async def continue_task(self, session_id, task_id, content, target_partner):
        self.calls.append({"command": "continue", "task_id": task_id, "partners": [target_partner]})

    def deliver_update(self, task_id, partner, state):
        task = TaskResult(id="result-" + task_id + "-" + state.value,
            senderId=partner, senderRole="partner", sentAt="2026-10-19T00:00:00Z",
            sessionId="session-fixture", groupId="group-fixture", taskId=task_id,
            status=TaskStatus(state=state, stateChangedAt="2026-10-19T00:00:00Z"))
        for listener in self.listeners:
            listener(SimpleNamespace(session_id="session-fixture", group_id="group-fixture"), task)


def source_hashes():
    paths = [Path(__file__).resolve(), SDK_ROOT / "acps_sdk/aip/resumable/journal.py",
             SDK_ROOT / "acps_sdk/aip/_task_order.py", SDK_ROOT / "acps_sdk/aip/aip_base_model.py"]
    return {path.relative_to(SDK_ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in paths}


def snapshot(journal, term, leader, phase):
    rows = journal.list_tasks()
    return {"schema_version": "coordinator-process-fixture/1", "phase": phase,
        "pid": os.getpid(), "parent_pid": os.getppid(), "coordinator_term": int(term),
        "tasks": rows, "in_flight": journal.list_tasks(in_flight_only=True),
        "events": {row["attempt_id"]: journal.events(row["attempt_id"]) for row in rows},
        "transport_calls": leader.calls, "source_sha256": source_hashes()}


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


async def seed(wrapper, leader):
    await wrapper.start_task("session-fixture", task_content="observe fixture",
        task_id="task-observe", target_partners=["database-primary", "application"], logical_task_id="logical-observe")
    leader.deliver_update("task-observe", "database-primary", TaskState.Working)
    await wrapper.continue_task("session-fixture", "task-observe", "continue fixture", "database-primary",
        logical_task_id="logical-observe")
    leader.deliver_update("task-observe", "database-primary", TaskState.Working)
    await wrapper.start_task("session-fixture", task_content="completed fixture",
        task_id="task-completed", target_partners=["verification"], logical_task_id="logical-completed")
    leader.deliver_update("task-completed", "verification", TaskState.Completed)
    try:
        await wrapper.start_task("session-fixture", task_content="unknown fixture",
            task_id="task-unknown", target_partners=["database-backup"], logical_task_id="logical-unknown")
    except TimeoutError:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("seed", "recover"))
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--finally-marker", type=Path)
    parser.add_argument("--exit-mode", choices=("hard-exit", "wait-for-parent"), default="hard-exit")
    args = parser.parse_args()
    print("fixture: opening journal", flush=True)
    journal = TaskJournal(args.journal)
    print("fixture: journal opened", flush=True)
    term = CoordinatorTerm(journal)
    print("fixture: term allocated", flush=True)
    leader = OfflineLeader()
    wrapper = ResumableGroupLeader(leader, journal, term)
    if args.phase == "recover":
        save(args.receipt, snapshot(journal, term, leader, "recovered"))
        return
    print("fixture: starting asyncio", flush=True)
    asyncio.run(seed(wrapper, leader))
    print("fixture: tasks committed", flush=True)
    save(args.receipt, snapshot(journal, term, leader, "durable-ready"))
    try:
        if args.exit_mode == "hard-exit":
            os._exit(37)
        while True:
            time.sleep(0.05)
    finally:
        if args.finally_marker:
            args.finally_marker.write_text("normal fixture cleanup ran\n", encoding="utf-8")


if __name__ == "__main__":
    main()
