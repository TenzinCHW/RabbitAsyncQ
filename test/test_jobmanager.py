from pytest import fixture
from rabbitasyncq import JobManager
import os
import pika
import json
import time
import threading


def dummy_run(body):
    for i in range(body["var"]):
        time.sleep(1)
        yield {"nyaa": i * 5}


def exception_run(body):
    raise ValueError(
        "I'm supposed to raise to make sure library is handling exceptions in job handlers."
    )


def handle_result(body):
    if body.get("status") == "SUCCESS":
        assert body["nyaa"] % 5 == 0
    print(f"Received results: {body}")


def handle_exception_result(body):
    if body.get("status") == "ERROR":
        print("Got an error from job function")
    print(f"Received results: {body}")


@fixture(scope="session")
def job_manager():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    jm = JobManager("test_job_name", conn, dummy_run, handle_result)
    yield jm
    jm.shutdown()
    jm.conn.close()


@fixture
def job_manager_exception():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    jm = JobManager("fail_test_job_name", conn, exception_run, handle_exception_result)
    yield jm
    jm.shutdown()
    jm.conn.close()


def test_job(job_manager):
    print(job_manager)
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key="test_job_name input job",
            body=json.dumps({"var": 2, "job_id": job_id}),
        )
        time.sleep(3.0)  # Wait for job to finish


def test_cancel(job_manager):
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key="test_job_name input job",
            body=json.dumps({"var": 2, "job_id": job_id}),
        )
        time.sleep(1.0)
        channel.basic_publish(
            exchange="",
            routing_key="test_job_name stop job",
            body=json.dumps({"job_id": job_id}),
        )
        time.sleep(1.0)  # Wait for stop to propagate


def test_exception_run(job_manager_exception):
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key="fail_test_job_name input job",
            body=json.dumps({"var": 2, "job_id": job_id}),
        )
        time.sleep(1.0)  # Wait for exception to propagate
