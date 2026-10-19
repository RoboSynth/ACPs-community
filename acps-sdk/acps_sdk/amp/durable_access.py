"""Process-crash recovery for local access logs, with immutable outbox records.

The derived journal is the exclusive owner of one canonical JSONL file. Recovery
records an unconfirmed outcome, never an invented response or request duration.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sqlite3
import threading

from .access_emitter import AccessEmitter
from .models import AccessBody, AccessLogRecord, ErrorInfo

logger = logging.getLogger(__name__)


class AccessLogIntegrityError(ValueError):
    """An existing log cannot safely accept an outbox replay."""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class DurableAccessEmitter(AccessEmitter):
    """Opt-in access emitter; SQLite FULL transactions precede handler execution.

    ``begin`` and ``finish`` are used by GroupAccessMiddleware. Ordinary ``emit``
    remains available for completed standalone events. The original AccessEmitter
    is unchanged. Use this emitter as the sole writer of its JSONL file.
    """

    def __init__(self, log_file: Path, aic: str, *, resource=None):
        super().__init__(Path(log_file).resolve(), aic, resource=resource)
        self.journal_path = self._log_file.with_suffix(".outbox.sqlite3")
        self._lock = threading.RLock()
        self._connection = None
        self._active = set()
        self._known = {}
        self._offset = 0
        self._file_identity = None

    def _record(self, log_id, body, **kwargs):
        record = AccessLogRecord(log_id=log_id,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            aic=self._aic, body=body, **kwargs)
        value = record.model_dump(mode="json", by_alias=True, exclude_none=True)
        if self._resource:
            value["resource"] = self._resource
        return _json(value)

    def start_sync(self):
        with self._lock:
            if self._connection is not None:
                return
            self._log_file.parent.mkdir(parents=True, exist_ok=True)
            if self.journal_path.is_symlink():
                raise AccessLogIntegrityError("Access journal must not be a symlink")
            for path in (self._log_file, self.journal_path):
                if path.exists() and path.stat().st_nlink > 1:
                    raise AccessLogIntegrityError("Access files must not have hard-link aliases")
            connection = sqlite3.connect(self.journal_path, timeout=0, check_same_thread=False)
            try:
                connection.execute("PRAGMA locking_mode=EXCLUSIVE")
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("PRAGMA synchronous=FULL")
                with connection:
                    connection.execute("CREATE TABLE IF NOT EXISTS owner (aic TEXT, log_path TEXT)")
                    connection.execute("CREATE TABLE IF NOT EXISTS attempts (event_key TEXT PRIMARY KEY, "
                        "log_id TEXT UNIQUE NOT NULL, intent TEXT NOT NULL, record TEXT, written INTEGER NOT NULL DEFAULT 0)")
                    owner = connection.execute("SELECT aic, log_path FROM owner").fetchone()
                    identity = (self._aic, os.path.normcase(str(self._log_file)))
                    if owner is None:
                        connection.execute("INSERT INTO owner VALUES (?, ?)", identity)
                    elif owner != identity:
                        raise AccessLogIntegrityError("Access journal identity or canonical log path mismatch")
                    # A real write acquires a lock retained by EXCLUSIVE mode after commit.
                    connection.execute("UPDATE owner SET aic=aic")
                self._connection = connection
                self._flush_locked()
            except BaseException:
                connection.close()
                self._connection = None
                self._known, self._offset, self._file_identity = {}, 0, None
                raise

    def _scan_existing(self):
        if not self._log_file.exists():
            if self._offset:
                raise AccessLogIntegrityError("Access log disappeared while owned")
            return
        stat = self._log_file.stat()
        identity = (stat.st_dev, stat.st_ino)
        if (self._file_identity is not None and identity != self._file_identity) or stat.st_size < self._offset:
            raise AccessLogIntegrityError("Access log was replaced or truncated while owned")
        with self._log_file.open("rb") as stream:
            stream.seek(self._offset)
            while line := stream.readline():
                if not line.endswith(b"\n"):
                    raise AccessLogIntegrityError("Partial access line retained; replay stopped")
                try:
                    value = json.loads(line)
                except (UnicodeError, ValueError) as error:
                    raise AccessLogIntegrityError("Malformed access line retained; replay stopped") from error
                if (not isinstance(value, dict) or value.get("aic") != self._aic
                        or not isinstance(value.get("log_id"), str) or not value["log_id"]):
                    raise AccessLogIntegrityError("Existing access identity is invalid")
                log_id, content = value["log_id"], _json(value)
                if log_id in self._known and self._known[log_id] != content:
                    raise AccessLogIntegrityError("Same access log ID has different content")
                self._known[log_id] = content
                self._offset = stream.tell()
        self._file_identity = identity

    def _append(self, record):
        with self._log_file.open("ab") as stream:
            raw = (record + "\n").encode("utf-8")
            if stream.write(raw) != len(raw):
                raise OSError("Incomplete access append")
            stream.flush()
            os.fsync(stream.fileno())

    def _flush_locked(self):
        self._scan_existing()
        connection = self._connection
        with connection:
            for key, log_id, intent in connection.execute(
                    "SELECT event_key, log_id, intent FROM attempts WHERE record IS NULL").fetchall():
                if key in self._active:
                    continue
                saved = json.loads(intent)
                body = AccessBody.model_validate(saved["body"])
                body.error = ErrorInfo(code="OutcomeUnconfirmed", message=
                    "Recovery observed no durable outcome for an inactive request")
                connection.execute("UPDATE attempts SET record=? WHERE event_key=?",
                    (self._record(log_id, body, **saved["trace"]), key))
        for key, log_id, record in connection.execute(
                "SELECT event_key, log_id, record FROM attempts WHERE record IS NOT NULL AND written=0 ORDER BY rowid").fetchall():
            if log_id in self._known:
                if self._known[log_id] != record:
                    raise AccessLogIntegrityError("Outbox replay conflicts with an existing access record")
                # A visible line may come from a crash before its original fsync.
                with self._log_file.open("r+b") as stream:
                    os.fsync(stream.fileno())
            else:
                self._append(record)
                self._scan_existing()
            with connection:
                connection.execute("UPDATE attempts SET written=1 WHERE event_key=?", (key,))

    def begin_sync(self, key, body, *, trace_id, correlation_id):
        if (len(key) != 3 or any(not isinstance(value, str) or not value for value in key)
                or key[:2] != (trace_id, correlation_id) or key[2] not in ("send", "receive")):
            raise ValueError("Durable group access requires session, task and direction")
        if body.response is not None or body.duration_ms is not None or body.error is not None:
            raise ValueError("A request intent must not assert an outcome")
        event_key = _json(key)
        intent = _json(dict(body=body.model_dump(mode="json", by_alias=True, exclude_none=True),
            trace=dict(trace_id=trace_id, correlation_id=correlation_id),
            received_at=datetime.now(timezone.utc).isoformat()))
        with self._lock:
            self.start_sync()
            previous = self._connection.execute("SELECT intent FROM attempts WHERE event_key=?", (event_key,)).fetchone()
            if previous:
                original, current = json.loads(previous[0]), json.loads(intent)
                if original["body"] != current["body"] or original["trace"] != current["trace"]:
                    raise AccessLogIntegrityError("Redelivered access key changed request metadata")
                return False
            with self._connection:
                self._connection.execute("INSERT INTO attempts(event_key,log_id,intent) VALUES (?,?,?)",
                    (event_key, AccessLogRecord.new_log_id(), intent))
            self._active.add(event_key)
            return True

    def finish_sync(self, key, body):
        event_key = _json(key)
        with self._lock:
            if self._connection is None:
                raise RuntimeError("Access owner closed before callback finalization")
            row = self._connection.execute("SELECT log_id,intent,record FROM attempts WHERE event_key=?", (event_key,)).fetchone()
            if row is None:
                raise ValueError("No durable request intent")
            try:
                if row[2] is None:
                    if event_key not in self._active:
                        raise RuntimeError("Only the active owner may finalize a request")
                    original = AccessBody.model_validate(json.loads(row[1])["body"])
                    if (body.request, body.caller, body.callee) != (original.request, original.caller, original.callee):
                        raise AccessLogIntegrityError("Final access changed its durable request identity")
                    record = self._record(row[0], body, **json.loads(row[1])["trace"])
                    with self._connection:
                        self._connection.execute("UPDATE attempts SET record=? WHERE event_key=?", (record, event_key))
                self._flush_locked()
                return row[0]
            finally:
                self._active.discard(event_key)

    def emit_sync(self, body, **kwargs):
        with self._lock:
            self.start_sync()
            log_id = AccessLogRecord.new_log_id()
            with self._connection:
                self._connection.execute("INSERT INTO attempts(event_key,log_id,intent,record) VALUES (?,?,?,?)",
                    ("event:" + log_id, log_id, "{}", self._record(log_id, body, **kwargs)))
            self._flush_locked()
            return log_id

    async def emit(self, body, **kwargs):
        # Complete the small local transaction before cancellation can release
        # its owner. No detached thread may reopen the journal after shutdown.
        return self.emit_sync(body, **kwargs)

    def flush_sync(self):
        with self._lock:
            self.start_sync()
            self._flush_locked()

    async def run_periodic(self, interval=5):
        while True:
            await asyncio.sleep(interval)
            try:
                self.flush_sync()
            except Exception:
                logger.warning("Durable access flush pending", exc_info=True)

    def close_sync(self):
        with self._lock:
            if self._active:
                raise RuntimeError("Cannot release access ownership while callbacks are active")
            if self._connection is not None:
                try:
                    self._flush_locked()
                finally:
                    self._connection.close()
                    self._connection = None
                    self._known, self._offset, self._file_identity = {}, 0, None
