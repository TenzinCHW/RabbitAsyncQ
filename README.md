# RabbitAsyncQ
A lightweight Python package for managing asynchronous, compute-intensive job processing using RabbitMQ. Built with simplicity, performance, and resilience in mind, this package provides a structured way to handle job execution in isolated process pools, graceful cancellation, and real-time result streaming back to RabbitMQ queues without blocking the Python GIL.

## Overview
RabbitAsyncQ is designed for distributed and event-driven systems where tasks are queued and processed asynchronously. Recently upgraded to a **multi-processing architecture**, it provides:
- A `JobManager` class to accept and distribute jobs to a robust `ProcessPoolExecutor`.
- Lock-free Inter-Process Communication (IPC) to safely stream real-time yielded results from isolated worker processes back to the main connection thread.
- Full process-boundary cancellation support (detecting `SIGINT`/`SIGTERM` and routing `stop job` messages).
- Worker crash resilience (auto-detecting OOM kills, segfaults, and `BrokenProcessPool` errors, routing poisoned messages to Dead-Letter Exchanges via `basic_nack`).

## Installation
This package is not currently available on PyPI. You can install it directly from the GitHub repository.

### Installing with uv or poetry
`uv pip install git+https://github.com/TenzinCHW/RabbitAsyncQ.git`

`poetry add git+https://github.com/TenzinCHW/RabbitAsyncQ.git`

### Installing with pip
`pip install git+https://github.com/TenzinCHW/RabbitAsyncQ.git`

You also need a running [RabbitMQ server](https://www.rabbitmq.com/docs/download) accessible from your application.

## Getting Started

### Define a Job Generator and Result Handler
First, define a **generator function** which accepts a `job_data` dictionary (parsed from the RabbitMQ message body). It should `yield` intermediate dictionaries representing the progress or results of the computation. Note that the library will automatically inject the `job_id` and a `"status": "RUNNING"` key into every yielded dictionary for tracking.

Next, define a result handler function which accepts the results dictionary streamed from your job generator as it executes.

```python
import time

def process_data_job(job_data):
    """A compute-intensive generator function running in an isolated process."""
    print(f"Job {job_data['job_id']} started...")
    
    # Simulate a multi-step computation yielding intermediate results
    for i in range(job_data["steps"]):
        time.sleep(1) # Intensive work here
        yield {"progress": i + 1, "total": job_data["steps"]}

def handle_result(result_data):
    """Callback triggered on the main process for every yielded result."""
    if result_data.get("status") == "SUCCESS":
        print(f"Job {result_data['job_id']} completed successfully!")
    elif result_data.get("status") == "ERROR":
        print(f"Job {result_data['job_id']} failed: {result_data.get('message')}")
    else:
        print(f"Progress update for {result_data['job_id']}: {result_data}")
```

### Set up the Job Manager
A RabbitMQ cluster can be used, but we are using `localhost` here as an example. The `JobManager` will automatically scale the process pool size based on the available cgroups/CPU cores.

To start processing messages, call the blocking `.start()` method on your `JobManager` instance. It will automatically intercept `SIGINT`/`SIGTERM` to gracefully cancel all currently running jobs and safely wait for the process pool to clean up, preventing orphaned processes or errors.

```python
import pika
from rabbitasyncq import JobManager

with pika.BlockingConnection(pika.ConnectionParameters('localhost')) as conn:
    # Set up JobManager reading from "{name} input job" queue
    job_manager = JobManager(
        name="my_worker_queue", 
        conn=conn, 
        job_fn=process_data_job, 
        result_fn=handle_result
    )
    
    # Blocks the main thread and starts consuming
    print("Starting worker node...")
    job_manager.start()
```

### Publishing Jobs and Controlling Execution

On the publisher side, format the message as a JSON string with any arguments your job needs, alongside a unique `job_id` string. Publish to the `{name} input job` queue:

```python
import os
import pika
import json

job_id = os.urandom(15).hex()  # generate a random job_id
payload = {"job_id": job_id, "steps": 5}

with pika.BlockingConnection(pika.ConnectionParameters(host="localhost")) as conn:
    channel = conn.channel()
    channel.basic_publish(
        exchange="", 
        routing_key="my_worker_queue input job", 
        body=json.dumps(payload)
    )
    print(f"Dispatched job {job_id}")
```

You can gracefully interrupt and cancel a running job across the process boundary by sending a JSON-formatted string with the `job_id` key to the `{name} stop job` queue. The generator will safely abort before its next `yield`.

```python
channel.basic_publish(
    exchange="", 
    routing_key="my_worker_queue stop job", 
    body=json.dumps({"job_id": job_id})
)
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