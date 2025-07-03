# RabbitAsyncQ
A lightweight Python package for managing asynchronous job processing using RabbitMQ. Built with simplicity and flexibility in mind, this package provides a structured way to handle job execution, graceful stopping, and result publishing via RabbitMQ queues.

## Overview
RabbitAsyncQ is designed for distributed and event-driven systems where tasks are queued and processed asynchronously. It provides:
- A JobManager class to accept and manage jobs from a RabbitMQ queue.
- A StoppableJob class that runs in a thread and can be gracefully stopped.
- A result queue to store job outputs for later retrieval.
- Compatibility with RabbitMQ's callback signature for message handling.

## Installation
This package is not currently available on PyPI. You can install it from the GitHub repository.

### Installing with poetry
`poetry add git+https://github.com/TenzinCHW/RabbitAsyncQ.git`

### Installing with pip
`pip install git+https://github.com/TenzinCHW/RabbitAsyncQ.git`

You also need to install and run the [RabbitMQ server](https://www.rabbitmq.com/docs/download).

## Getting Started
### Define a Job Function and Results Handler
First, define a job function which accepts a `job_data` dictionary and `iteration` integer specifying which iteration of the job it should run and returns a dictionary of the result. Note that the `job_id` and `iteration` keys of the returned dictionary are reserved. If the job function does not insert them into the returned dictionary, they will be added automatically.
Then, define a result handler function which accepts the results dict produced by your job function. Use the `job_id` and `iteration` keys to 
```
def handle_job(job_data, iteration):
    print(f"Job {job_data['job_id']} - Iteration {iteration}: Processing...")
    time.sleep(0.5)
    return {"result": job_data["inputs"][iteration], "iteration": iteration}

def handle_result(result_data):
    print(f"Writing results to db... {result_data['job_id']}, {result_data['iteration']}")
```

### Set up the Job Manager
A RabbitMQ cluster can be used but we are just using localhost here as an example. Note that the thread that creates a `JobManager` will block.
```
import pika
from rabbitasyncq import JobManager

with pika.BlockingConnection(pika.ConnectionParameters('localhost')) as conn:
    job_manager = JobManager(conn, handle_job, handle_result)
```

On the publisher side, we need to format the message as a json string with the keys `start` and `stop`, indicating the range over which to loop (equivalent to looping over `range(start, stop)`), as well as the `job_id` string.
```
import os
import pika
import json

job_id = os.urandom(15).hex()  # generate a random job_id
with pika.BlockingConnection(pika.ConnectionParameters(host="localhost")) as conn:
    channel = conn.channel()
    channel.basic_publish(routing_key="input job", body=json.dumps({"job_id": job_id, "start": 0, "stop": 3}))
```

You can interrupt a job by sending a json-formatted string with the `job_id` key of the job to be cancelled.
```
channel.basic_publish(exchange="", routing_key="stop job", body=json.dumps({"job_id": job_id}))
```

## Contributing
Contributions are welcome! To get started:
1. Fork the repository.
2. Create a new branch for your feature or fix.
3. Submit a pull request with a clear description.

## License
This project is licensed under the MIT License — see the LICENSE file for details.

## Contact
If you have any questions, suggestions, or issues, feel free to reach out via email or open a GitHub issue.
