from pytest import fixture
from rabbitasyncq import JobManager
import os
import pika
import json
import time
import threading


def dummy_run(body, i):
    time.sleep(1)
    return {"nyaa": i * 5}


def handle_result(body):
    print(f"Received results: {body}")


@fixture
def job_manager():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    jm = lambda: JobManager(conn, dummy_run, handle_result)
    t = threading.Thread(target=jm)
    t.start()


def test_job(job_manager):
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(pika.ConnectionParameters(host="localhost")) as connection:
        channel = connection.channel()
        channel.basic_publish(exchange="", routing_key="input job", body=json.dumps({"start": 0, "stop": 3, "job_id": job_id}))

def test_cancel(job_manager):
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(pika.ConnectionParameters(host="localhost")) as connection:
        channel = connection.channel()
        channel.basic_publish(exchange="", routing_key="input job", body=json.dumps({"start": 0, "stop": 3, "job_id": job_id}))
        time.sleep(0.5)
        channel.basic_publish(exchange="", routing_key="stop job", body=json.dumps({"job_id": job_id}))
