# Dead Letter Queue

A Flask-based Dead Letter Queue (DLQ) that adds retry handling and failed-job isolation to a background job system.

## Features

- FIFO job processing
- Maximum retry policy
- Automatic retry after temporary failures
- Automatic dead-lettering after max retries
- Dead-letter inspection
- Manual dead-letter requeue
- Thread-safe operations
- Job status and statistics
- Health endpoint
- Pytest tests

## Job Lifecycle

```text
QUEUED -> PROCESSING -> COMPLETED
                  |
                  +-> FAILED -> retry -> QUEUED
                              |
                              +-> max retries -> DEAD
                                                   |
                                                   +-> requeue -> QUEUED
```

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/api/jobs` | Create a job |
| POST | `/api/jobs/claim` | Claim next job |
| GET | `/api/jobs/<job_id>` | Get job |
| POST | `/api/jobs/<job_id>/complete` | Complete job |
| POST | `/api/jobs/<job_id>/fail` | Fail/retry job |
| GET | `/api/dead-letters` | List dead jobs |
| POST | `/api/dead-letters/<job_id>/requeue` | Requeue dead job |
| GET | `/api/jobs/stats` | Queue statistics |

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Windows:

```powershell
.venv\Scripts\activate
```

Tests:

```bash
pytest -q
```

## Learning Goals

Retry policies, poison-message handling, failure isolation, dead-letter queues, worker reliability, and operational recovery. The in-memory implementation can later be replaced by Redis, RabbitMQ, Kafka, or a cloud message broker.
