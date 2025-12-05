import asyncio
import os

from celery import Celery

from jobs.celery_tasks import (
    _asr_task, _docqa_task, _classify_task, 
    _summarize_task, _is_damaged_task, _normalize_task
)
try:
    from prometheus_client import Counter
except Exception:
    Counter = None


broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
app = Celery(
    "shopdesk",
    broker=broker_url,
    backend=broker_url,
    include=[
        "worker.celery_app",
        "worker.jobs.gmail_poll",
    ],
)

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

def run_coro(coro):
    return _loop.run_until_complete(coro)


_failure_counter = Counter("shopdesk_pipeline_failures_total", "Pipeline failures", ["step"]) if Counter else None

def mark_failure(step: str) -> None:
    if not _failure_counter:
        return
    try:
        _failure_counter.labels(step=step).inc()
    except Exception:
        pass

@app.task(name="ping")
def ping():
    return "pong"


@app.task(name="pipeline.asr", bind=True, max_retries=3, default_retry_delay=10)
def asr_task(self, attachment_id: str) -> str | None:
    try:
        return run_coro(_asr_task(attachment_id))
    except Exception as exc:
        mark_failure("asr")
        raise self.retry(exc=exc)


@app.task(name="pipeline.docqa", bind=True, max_retries=3, default_retry_delay=10)
def docqa_task(self, attachment_id: str) -> dict | None:
    try:
        return run_coro(_docqa_task(attachment_id))
    except Exception as exc:
        mark_failure("docqa")
        raise self.retry(exc=exc)


@app.task(name="pipeline.zeroshot", bind=True, max_retries=3, default_retry_delay=10)
def classify_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_classify_task(message_id))
    except Exception as exc:
        mark_failure("classify")
        raise self.retry(exc=exc)


@app.task(name="pipeline.summarize", bind=True, max_retries=3, default_retry_delay=10)
def summarize_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_summarize_task(message_id))
    except Exception as exc:
        mark_failure("summarize")
        raise self.retry(exc=exc)


@app.task(name="pipeline.vqa", bind=True, max_retries=3, default_retry_delay=10)
def is_damaged_task(self, attachment_id: str) -> bool:
    try:
        return run_coro(_is_damaged_task(attachment_id))
    except Exception as exc:
        mark_failure("vqa")
        raise self.retry(exc=exc)


@app.task(name="pipeline.normalized", bind=True, max_retries=3, default_retry_delay=10)
def normalized_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_normalize_task(message_id))
    except Exception as exc:
        mark_failure("normalize")
        raise self.retry(exc=exc)


app.conf.beat_schedule = {
    "gmail-poll-every-60s": {
        "task": "worker.jobs.gmail_poll.poll_gmail",
        "schedule": 60.0,
        "args": (25,),
    }
}
app.conf.timezone = "UTC"

if __name__ == "__main__":
    res = ping.delay()
    print("Celery app delay:", res.get(timeout=5))
