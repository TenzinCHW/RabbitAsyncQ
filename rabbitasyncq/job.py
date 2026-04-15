import json
import multiprocessing
from typing import Callable, Any
import pika

from .messaging import Messenger


def process_worker(job_id: str, job_fn: Callable, body: bytes, ipc_queue, stop_event):
    print(f"Starting job {job_id}")
    try:
        for result in job_fn(body):
            try:
                if stop_event.is_set():
                    ipc_queue.put({"job_id": job_id, "type": "stopped"})
                    print(f"Stopped job {job_id}")
                    return
            except (EOFError, BrokenPipeError, ConnectionResetError):
                return

            job_id_res = result.get("job_id")
            if job_id_res is None or job_id_res != job_id:
                result["job_id"] = job_id
            result["status"] = "RUNNING"

            try:
                ipc_queue.put({"job_id": job_id, "type": "result", "payload": result})
            except (EOFError, BrokenPipeError, ConnectionResetError):
                return

        print(f"Finished job {job_id}")
        try:
            ipc_queue.put({"job_id": job_id, "type": "done"})
        except (EOFError, BrokenPipeError, ConnectionResetError):
            return
    except Exception as e:
        if type(e).__name__ in ["EOFError", "BrokenPipeError", "ConnectionResetError"]:
            return
        err_msg = repr(e)
        try:
            ipc_queue.put({"job_id": job_id, "type": "error", "message": err_msg})
        except (EOFError, BrokenPipeError, ConnectionResetError):
            pass


class ProcessJobContext:
    def __init__(
        self, job_id: str, method, ch, name: str, stop_event, messenger: Messenger
    ):
        self.job_id = job_id
        self.method = method
        self.ch = ch
        self.name = name
        self.stop_event = stop_event
        self.messenger = messenger
        self.future: Any = None

    def stop(self):
        self.stop_event.set()
