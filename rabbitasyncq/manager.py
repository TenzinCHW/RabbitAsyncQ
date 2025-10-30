import json
import threading
from typing import Callable

import pika

from .job import StoppableJob
from .messaging import Messenger


class JobManager:
    def __init__(self, name: str, conn: pika.connection.Connection, job_fn: Callable, result_fn: Callable, exchange_opt={}, channel_opt={}):
        self.name = name
        self.job_fn = job_fn
        self.result_fn = result_fn
        self.conn = conn
        self.ch = conn.channel()
        self.jobs = {}
        self.exchange_opt = exchange_opt
        self.messenger = Messenger(conn, self.ch, name)

        if "queue" in channel_opt or "on_message_callback" in channel_opt:
            raise ValueError("channel_opt should not have 'queue' or 'on_message_callback' keys.")

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
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=input_job_qname, routing_key=input_job_qname)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=stop_job_qname, routing_key=stop_job_qname)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=result_qname, routing_key=result_qname)
        consume_thread = threading.Thread(target=self.ch.start_consuming)
        consume_thread.start()
        #self.ch.start_consuming()

    def accept_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        try:
            job_data = json.loads(body)
            job_id = job_data["job_id"]
        except:
            # if either the data wasn't parsed as json properly or there was no job id, there's something wrong. Fail immediately.
            ...

        job_thread = StoppableJob(method, self.conn, ch, self.name, job_data, job_id, self.job_fn)
        self.jobs[job_id] = job_thread
        job_thread.start()

    def handle_result(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        try:
            result_data = json.loads(body)
            job_id = result_data["job_id"]
            self.result_fn(result_data)
        except:
            # data not json or no job_id or result_fn errors, fail and ack job
            ...
        finally:
            self.messenger.ack_msg(method)

    def stop_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        try:
            job_data = json.loads(body)
            job_id = job_data["job_id"]
            job_thread = self.jobs.get(job_id)
            job_thread.stop()
            job_thread.join()
            print(f"Stopped job with ID: {job_id}.")
            del self.jobs[job_id]
        except:
            # data not json, job_id not found or job_id not in job_thread, just fail
            ...
        finally:
            self.messenger.ack_msg(method)
