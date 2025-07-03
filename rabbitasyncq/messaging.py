import json

import pika


def check_channel_open(ch: pika.channel.Channel):
    if not ch.is_open:
        raise ConnectionError("Channel is closed.")


def send_msg(ch: pika.channel.Channel, queue: str, data: str):
    check_channel_open(ch)
    ch.basic_publish(exchange="", routing_key=queue, body=data)


def send_err(ch: pika.channel.Channel, data: str):
    err = {"status": "ERROR", "error message": data}
    send_msg(ch, "result", json.dumps(err))


def send_stop(ch: pika.channel.Channel, job_id: str):
    stop = {"status": "STOPPED", "job_id": job_id}
    send_msg(ch, "result", json.dumps(stop))


def send_done(ch: pika.channel.Channel, job_id: str):
    done = {"status": "SUCCESS", "job_id": job_id}
    send_msg(ch, "result", json.dumps(done))


def ack_msg(ch: pika.channel.Channel, method: pika.frame.Method):
    check_channel_open(ch)
    ch.basic_ack(delivery_tag=method.delivery_tag)
