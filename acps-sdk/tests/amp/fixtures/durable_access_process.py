"""Owned local subprocesses for access-log crash windows; no network or resources."""
import asyncio
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from acps_sdk.amp.durable_access import DurableAccessEmitter
from acps_sdk.amp.group_access import GroupAccessMiddleware


def main():
    mode, filename = sys.argv[1:]
    path = Path(filename).resolve()

    class CrashEmitter(DurableAccessEmitter):
        def _append(self, record):
            if mode == "ready":
                os._exit(72)
            if mode == "partial":
                with self._log_file.open("wb") as stream:
                    stream.write(record[:40].encode())
                    stream.flush()
                os._exit(74)
            if mode == "before_fsync":
                with self._log_file.open("ab") as stream:
                    stream.write((record + "\n").encode())
                    stream.flush()
                os._exit(75)
            super()._append(record)
            if mode == "append":
                os._exit(73)

    emitter = CrashEmitter(path, "worker")
    if mode == "recover":
        emitter.start_sync()
        emitter.close_sync()
        print(json.dumps(dict(pid=os.getpid(), status="recovered")), flush=True)
        return
    if mode == "hold":
        emitter.start_sync()
        path.with_suffix(".ready.json").write_text(json.dumps(dict(pid=os.getpid())), encoding="utf-8")
        sys.stdin.readline()
        emitter.close_sync()
        return
    command = SimpleNamespace(command=SimpleNamespace(value="start"), sessionId="session", taskId="task",
        senderId="caller", mentions=["worker"], groupId="group", commandParams={"skill": "database.inspect"}, dataItems=[])

    async def callback():
        path.with_suffix(".effect.txt").write_text("callback entered once", encoding="utf-8")
        if mode == "handler":
            os._exit(71)
        return {"status": "ok"}

    asyncio.run(GroupAccessMiddleware(emitter, "worker").run(command, callback))
    raise AssertionError("The requested crash window was not reached")


if __name__ == "__main__":
    main()
