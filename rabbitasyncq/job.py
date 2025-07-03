import json
import threading
from typing import Callable

import pika

from .messaging import send_msg, send_err, send_stop, send_done, ack_msg


class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    @property
    def stopped(self):
        return self._stop_event.is_set()


class StoppableJob(StoppableThread):
    def __init__(self, method: pika.frame.Method, conn: pika.connection.Connection, ch: pika.channel.Channel, body: bytes, job_id: str, job_fn: Callable):
        # TODO add callback for logging
        super().__init__()
        self.job_id = job_id
        self.job_fn = job_fn
        self.body = body
        self.method = method
        self.conn = conn
        self.ch = ch

    def run(self):
        start = self.body.get("start")
        stop = self.body.get("stop")

        if not isinstance(start, int) or not isinstance(stop, int):
            err_msg = "Error with job {self.job_id}: job parameters must contain 'start' and 'stop' and they must be of type int."
            self.conn.add_callback_threadsafe(lambda: send_err(self.ch, err_msg))
            raise ValueError(err_msg)

        print(f"Starting job {self.job_id}")
        for i in range(start, stop):
            if self.stopped:
                self.conn.add_callback_threadsafe(lambda: send_stop(self.ch, self.job_id))
                self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
                print(f"Stopped job {self.job_id}")
                return

            try:
                result = self.job_fn(self.body, i)
            except Exception as e:
                err_msg = repr(e)
                self.conn.add_callback_threadsafe(lambda: send_err(self.ch, err_msg))
                self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
                raise e

            iteration = result.get("iteration")
            if iteration is not None and iteration != i:
                err_msg = f"Error with job {self.job_id}: result dict should not contain key 'iteration' that isn't the current iteration."
                self.conn.add_callback_threadsafe(lambda: send_err(self.ch, err_msg))
                self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
                raise ValueError(err_msg)
            elif iteration is None:
                result["iteration"] = i
            job_id = result.get("job_id")
            if job_id is None or job_id != self.job_id:
                result["job_id"] = self.job_id
            result["status"] = "RUNNING"

            self.conn.add_callback_threadsafe(lambda: send_msg(self.ch, "result", json.dumps(result)))
        print(f"Finished job {self.job_id}")
        self.conn.add_callback_threadsafe(lambda: send_done(self.ch, self.job_id))
        self.conn.add_callback_threadsafe(lambda: send_msg(self.ch, "stop job", json.dumps({"job_id": self.job_id})))
        self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
