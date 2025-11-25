from celery import Celery
import os

broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
app = Celery("shopdesk", broker=broker_url, backend=broker_url, include=["worker.jobs.gmail_poll"])

@app.task
def ping():
    return "pong"

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
