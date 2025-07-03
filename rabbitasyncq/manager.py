import json
import threading
from typing import Callable

import pika

from .job import StoppableJob
from .messaging import send_msg, send_err, ack_msg


def parse_json(ch: pika.channel.Channel, data: str):
    try:
        return json.loads(data)
    except Exception as e:
        err_msg = f"Incorrect JSON format for the following message:\n{body}"
        send_msg(ch, err_msg)
        e.add_note(err_msg)
        raise e


def get_job_id(ch: pika.channel.Channel, data: dict):
    job_id = data.get('job_id')
    if job_id is None:
        err_msg = "No job_id found in the following message:\n{body}"
        send_err(ch, err_msg)
        raise ValueError(err_msg)
    return job_id


class JobManager:
    def __init__(self, conn: pika.connection.Connection, job_fn: Callable, result_fn: Callable, exchange_opt={}, channel_opt={}):
        self.job_fn = job_fn
        self.result_fn = result_fn
        self.conn = conn
        self.ch = conn.channel()
        self.jobs = {}
        self.exchange_opt = exchange_opt

        if "queue" in channel_opt or "on_message_callback" in channel_opt:
            raise ValueError("channel_opt should not have 'queue' or 'on_message_callback' keys.")

        self.ch.queue_declare("input job", **channel_opt)
        self.ch.basic_consume("input job", self.accept_job)
        self.ch.queue_declare("stop job", **channel_opt)
        self.ch.basic_consume("stop job", self.stop_job)
        self.ch.queue_declare("result", **channel_opt)
        self.ch.basic_consume("result", self.handle_result)
        if exchange_opt:
            self.ch.exchange_declare(**exchange_opt)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue="input job", routing_key="input job")
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue="stop job", routing_key="stop job")
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue="result", routing_key="result")
        self.ch.start_consuming()

    def accept_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        job_data = parse_json(self.ch, body)

        job_id = get_job_id(self.ch, job_data)

        job_thread = StoppableJob(method, self.conn, ch, job_data, job_id, self.job_fn)
        self.jobs[job_id] = job_thread
        job_thread.start()


    def handle_result(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        result_data = parse_json(self.ch, body)

        job_id = get_job_id(self.ch, result_data)

        self.result_fn(result_data)
        ack_msg(ch, method)

    def stop_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        job_data = parse_json(self.ch, body)

        job_id = get_job_id(self.ch, job_data)

        try:
            job_thread = self.jobs.get(job_id)
            if job_thread is None:
                err_msg = f"No job found with ID: {job_id}, skipping."
                send_err(self.ch, err_msg)
                print(err_msg)
            else:
                job_thread.stop()
                job_thread.join()
                print(f"Stopped job with ID: {job_id}.")
                del self.jobs[job_id]

        except Exception as e:
            err_msg = f"Error stopping job {job_id}."
            send_err(self.ch, err_msg)
            e.add_note(err_msg)
            raise e

        finally:
            ack_msg(ch, method)
