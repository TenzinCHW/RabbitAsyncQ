from pytest import fixture
from rabbitasyncq import JobManager
import os
import pika
import json
import time
import threading

results_list = []


def dummy_run(body):
    for i in range(body["var"]):
        time.sleep(1)
        yield {"nyaa": i * 5}


def exception_run(body):
    raise ValueError(
        "I'm supposed to raise to make sure library is handling exceptions in job handlers."
    )


def crash_run(body):
    import os
    import signal

    os.kill(os.getpid(), signal.SIGKILL)


def handle_result(body):
    results_list.append(body)
    print(f"Received results: {body}")


def handle_exception_result(body):
    results_list.append(body)
    print(f"Received results: {body}")


@fixture(scope="function")
def job_manager():
    global results_list
    results_list = []
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    jm = JobManager("test_job_name", conn, dummy_run, handle_result)
    t = threading.Thread(target=jm.start)
    t.start()
    time.sleep(0.5)
    yield jm
    if not jm.conn.is_closed:
        jm.conn.add_callback_threadsafe(jm.ch.stop_consuming)
    t.join()
    if not jm.conn.is_closed:
        jm.conn.close()


@fixture(scope="function")
def job_manager_exception():
    global results_list
    results_list = []
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    jm = JobManager("fail_test_job_name", conn, exception_run, handle_exception_result)
    t = threading.Thread(target=jm.start)
    t.start()
    time.sleep(0.5)
    yield jm
    if not jm.conn.is_closed:
        jm.conn.add_callback_threadsafe(jm.ch.stop_consuming)
    t.join()
    if not jm.conn.is_closed:
        jm.conn.close()


@fixture(scope="function")
def job_manager_crash():
    global results_list
    results_list = []
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    jm = JobManager("crash_test_job_name", conn, crash_run, handle_exception_result)
    t = threading.Thread(target=jm.start)
    t.start()
    time.sleep(0.5)
    yield jm
    if not jm.conn.is_closed:
        jm.conn.add_callback_threadsafe(jm.ch.stop_consuming)
    t.join()
    if not jm.conn.is_closed:
        jm.conn.close()


def test_job(job_manager):
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

    # We should have two running results and one success
    assert len(results_list) == 3
    assert results_list[0]["status"] == "RUNNING"
    assert results_list[0]["nyaa"] == 0
    assert results_list[1]["status"] == "RUNNING"
    assert results_list[1]["nyaa"] == 5
    assert results_list[2]["status"] == "SUCCESS"


def test_cancel(job_manager):
    job_id = os.urandom(15).hex()
    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key="test_job_name input job",
            body=json.dumps({"var": 5, "job_id": job_id}),
        )
        time.sleep(1.5)
        channel.basic_publish(
            exchange="",
            routing_key="test_job_name stop job",
            body=json.dumps({"job_id": job_id}),
        )
        time.sleep(1.5)  # Wait for stop to propagate

    assert len(results_list) > 0
    assert any(r["status"] == "STOPPED" for r in results_list)
    assert not any(r["status"] == "SUCCESS" for r in results_list)


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

    assert len(results_list) == 1
    assert results_list[0]["status"] == "ERROR"
    assert "ValueError" in results_list[0]["message"]


from unittest.mock import patch, ANY


@patch("os._exit")
def test_worker_hard_crash(mock_os_exit, job_manager_crash):
    job_id = os.urandom(15).hex()

    with patch.object(job_manager_crash.ch, "basic_nack") as mock_basic_nack:
        with pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        ) as connection:
            channel = connection.channel()
            channel.basic_publish(
                exchange="",
                routing_key="crash_test_job_name input job",
                body=json.dumps({"var": 2, "job_id": job_id}),
            )
            time.sleep(2.0)  # Wait for crash and callback to execute

        # Verify basic_nack was called with requeue=False
        assert mock_basic_nack.called
        mock_basic_nack.assert_called_with(delivery_tag=ANY, requeue=False)

        # Verify os._exit(1) was called
        mock_os_exit.assert_called_with(1)
