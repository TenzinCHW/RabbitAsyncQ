import json
import threading


class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    @property
    def stopped(self):
        return self._stop_event.is_set()


def check_channel_open(ch):
    if not ch.is_open:
        raise ConnectionError("Channel is closed.")


def send_msg(ch, queue, data):
    check_channel_open(ch)
    ch.basic_publish(exchange="", routing_key=queue, body=data)


def ack_msg(ch, method):
    check_channel_open(ch)
    ch.basic_ack(delivery_tag=method.delivery_tag)


class StoppableJob(StoppableThread):
    def __init__(self, method, conn, ch, body, job_id, job_fn):
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
            raise ValueError("Error with job {self.job_id}: job parameters must contain 'start' and 'stop' and they must be of type int.")

        print(f"Starting job {self.job_id}")
        for i in range(start, stop):
            if self.stopped:
                self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
                print(f"Stopped job {self.job_id}")
                return
            result = self.job_fn(self.body, i)

            iteration = result.get("iteration")
            if iteration is not None and iteration != i:
                raise ValueError(f"Error with job {self.job_id}: result dict should not contain key 'iteration' that isn't the current iteration.")
            elif iteration is None:
                result["iteration"] = i
            job_id = result.get("job_id")
            if job_id is None or job_id != self.job_id:
                result["job_id"] = self.job_id

            self.conn.add_callback_threadsafe(lambda: send_msg(self.ch, "result", json.dumps(result)))
        print(f"Finished job {self.job_id}")
        self.conn.add_callback_threadsafe(lambda: ack_msg(self.ch, self.method))
        self.conn.add_callback_threadsafe(lambda: send_msg(self.ch, "stop job", json.dumps({"job_id": self.job_id})))
