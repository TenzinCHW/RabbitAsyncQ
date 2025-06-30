import json
import threading
import pika

from .job import StoppableJob


class JobManager:
    def __init__(self, conn, job_fn, result_fn, exchange_opt={}, channel_opt={}):
        self.job_fn = job_fn
        self.result_fn = result_fn
        self.conn = conn
        self.ch = conn.channel()
        self.jobs = {}
        self.exchange_opt = exchange_opt

        if exchange_opt:
            self.ch.exchange_declare(**exchange_opt)
        if "queue" in channel_opt or "on_message_callback" in channel_opt:
            raise ValueError("channel_opt should not have 'queue' or 'on_message_callback' keys.")

        self.ch.queue_declare("input job", **channel_opt)
        self.ch.queue_declare("stop job", **channel_opt)
        self.ch.queue_declare("result", **channel_opt)
        self.ch.basic_consume("input job", self.accept_job)
        self.ch.basic_consume("stop job", self.stop_job)
        self.ch.basic_consume("result", self.handle_result)
        self.ch.start_consuming()

    def process_body(self, body):
        return json.loads(body)

    def accept_job(self, ch, method, properties, body):
        try:
            job_data = self.process_body(body)
        except Exception as e:
            e.add_note(f"Incorrect JSON format for the following message:\n{body}")
            raise e

        job_id = job_data.get('job_id')
        if job_id is None:
            raise ValueError("No job_id found in the following message:\n{body}")

        job_id = job_data["job_id"]
        job_thread = StoppableJob(method, self.conn, ch, job_data, job_id, self.job_fn)
        self.jobs[job_id] = job_thread
        job_thread.start()


    def handle_result(self, ch, method, properties, body):
        try:
            result_data = self.process_body(body)
        except Exception as e:
            e.add_note(f"Incorrect JSON format for the following message:\n{body}")
            raise e

        job_id = result_data.get('job_id')
        if job_id is None:
            raise ValueError("No job_id found in the following message:\n{body}")

        self.result_fn(result_data)
        ch.basic_ack(method.delivery_tag)

    def stop_job(self, ch, method, properties, body):
        try:
            job_data = self.process_body(body)
        except Exception as e:
            e.add_note(f"Incorrect json format for the following message:\n{body}")
            raise e

        job_id = job_data.get('job_id')
        if job_id is None:
            raise ValueError("Job ID is required to stop a job.")

        try:
            job_thread = self.jobs.get(job_id)
            if job_thread is None:
                print(f"No job found with ID: {job_id}, skipping.")
            else:
                job_thread.stop()
                job_thread.join()
                print(f"Stopped job with ID: {job_id}.")
                del self.jobs[job_id]

        except Exception as e:
            e.add_note(f"Error stopping job {job_id}.")
            raise e

        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
