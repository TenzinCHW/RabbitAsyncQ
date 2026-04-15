import os
import json
import threading
import multiprocessing
import functools
import signal
from concurrent.futures import ProcessPoolExecutor
from typing import Callable

import pika
import pika.connection
import pika.channel
import pika.frame
import pika.spec

from .job import process_worker, ProcessJobContext
from .messaging import Messenger


class JobManager:
    def __init__(
        self,
        name: str,
        conn: pika.connection.Connection,
        job_fn: Callable,
        result_fn: Callable,
        exchange_opt={},
        channel_opt={},
        max_workers: int | None = None,
    ):
        self.name = name
        self.job_fn = job_fn
        self.result_fn = result_fn
        self.conn = conn
        self.ch = conn.channel()

        if max_workers is None:
            try:
                self.max_workers = len(os.sched_getaffinity(0))
            except AttributeError:
                self.max_workers = os.cpu_count() or 1
        else:
            self.max_workers = max_workers

        self.ch.basic_qos(prefetch_count=self.max_workers)

        self.jobs = {}
        self.exchange_opt = exchange_opt
        self.messenger = Messenger(conn, self.ch, name)

        self.manager = multiprocessing.Manager()
        self.ipc_queue = self.manager.Queue()
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)

        if "queue" in channel_opt or "on_message_callback" in channel_opt:
            raise ValueError(
                "channel_opt should not have 'queue' or 'on_message_callback' keys."
            )

        input_job_qname = f"{name} input job"
        stop_job_qname = f"{name} stop job"
        result_qname = f"{name} result"
        self.ch.queue_declare(input_job_qname, **channel_opt)
        self.ch.basic_consume(input_job_qname, self.accept_job)
        self.ch.queue_declare(stop_job_qname, **channel_opt)
        self.ch.basic_consume(stop_job_qname, self.stop_job)
        self.ch.queue_declare(result_qname, **channel_opt)
        self.ch.basic_consume(result_qname, self.handle_result)
        if exchange_opt:
            self.ch.exchange_declare(**exchange_opt)
            self.ch.queue_bind(
                exchange=exchange_opt["exchange"],
                queue=input_job_qname,
                routing_key=input_job_qname,
            )
            self.ch.queue_bind(
                exchange=exchange_opt["exchange"],
                queue=stop_job_qname,
                routing_key=stop_job_qname,
            )
            self.ch.queue_bind(
                exchange=exchange_opt["exchange"],
                queue=result_qname,
                routing_key=result_qname,
            )

        self.ipc_thread = threading.Thread(target=self._consume_ipc)
        self.ipc_thread.start()

    def start(self):
        def _sig_handler(signum, frame):
            print(f"Received signal {signum}, stopping consumer...")
            self.conn.add_callback_threadsafe(self.ch.stop_consuming)

        try:
            signal.signal(signal.SIGINT, _sig_handler)
            signal.signal(signal.SIGTERM, _sig_handler)
        except ValueError:
            # signal only works in main thread
            pass

        self.ch.start_consuming()
        self.shutdown()

    def _consume_ipc(self):
        while True:
            msg = self.ipc_queue.get()
            if msg is None:
                break

            job_id = msg["job_id"]
            ctx = self.jobs.get(job_id)
            if not ctx:
                continue

            msg_type = msg["type"]

            if msg_type == "result":
                payload = msg["payload"]
                self.conn.add_callback_threadsafe(
                    lambda c=ctx, p=payload: c.messenger.send_msg(
                        f"{c.name} result", json.dumps(p)
                    )
                )
            elif msg_type == "stopped":
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.send_stop(c.job_id)
                )
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.ack_msg(c.method)
                )
                del self.jobs[job_id]
            elif msg_type == "done":
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.send_done(c.job_id)
                )
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.send_msg(
                        f"{c.name} stop job", json.dumps({"job_id": c.job_id})
                    )
                )
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.ack_msg(c.method)
                )
                del self.jobs[job_id]
            elif msg_type == "error":
                err_message = {
                    "status": "ERROR",
                    "message": msg["message"],
                    "job_id": job_id,
                }
                self.conn.add_callback_threadsafe(
                    lambda c=ctx, m=err_message: c.messenger.send_msg(
                        f"{c.name} result", json.dumps(m)
                    )
                )
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.messenger.ack_msg(c.method)
                )
                del self.jobs[job_id]

    def _handle_future_done(self, future, job_id=None):
        try:
            exc = future.exception()
        except Exception:
            return

        if exc is not None and type(exc).__name__ in [
            "BrokenProcessPool",
            "TerminatedWorkerError",
        ]:
            ctx = self.jobs.get(job_id)
            if ctx:
                self.conn.add_callback_threadsafe(
                    lambda c=ctx: c.ch.basic_nack(
                        delivery_tag=c.method.delivery_tag, requeue=False
                    )
                )
                del self.jobs[job_id]
            print(f"Worker process crashed for job {job_id}: {exc}")
            import sys
            import os

            os._exit(1)

    def accept_job(
        self,
        ch: pika.channel.Channel,
        method: pika.frame.Method,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        try:
            job_data = json.loads(body)
            job_id = job_data["job_id"]
        except:
            # if either the data wasn't parsed as json properly or there was no job id, there's something wrong. Fail immediately.
            return

        stop_event = self.manager.Event()
        job_ctx = ProcessJobContext(
            job_id, method, ch, self.name, stop_event, self.messenger
        )
        self.jobs[job_id] = job_ctx

        future = self.executor.submit(
            process_worker, job_id, self.job_fn, job_data, self.ipc_queue, stop_event
        )
        future.add_done_callback(
            functools.partial(self._handle_future_done, job_id=job_id)
        )
        job_ctx.future = future

    def handle_result(
        self,
        ch: pika.channel.Channel,
        method: pika.frame.Method,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        try:
            result_data = json.loads(body)
            job_id = result_data["job_id"]
            self.result_fn(result_data)
        except:
            # data not json or no job_id or result_fn errors, fail and ack job
            pass
        finally:
            self.messenger.ack_msg(method)

    def stop_job(
        self,
        ch: pika.channel.Channel,
        method: pika.frame.Method,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        try:
            job_data = json.loads(body)
            job_id = job_data["job_id"]
            ctx = self.jobs.get(job_id)
            if ctx:
                ctx.stop()
                print(f"Stopping job with ID: {job_id}...")
        except:
            # data not json, job_id not found or job_id not in job_thread, just fail
            pass
        finally:
            self.messenger.ack_msg(method)

    def shutdown(self):
        self.conn.add_callback_threadsafe(self.ch.stop_consuming)

        # Signal all running jobs to stop
        for job_ctx in self.jobs.values():
            job_ctx.stop()

        self.ipc_queue.put(None)
        if self.ipc_thread.is_alive():
            self.ipc_thread.join()

        # Wait for workers to clean up proxies before tearing down the manager
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.manager.shutdown()
