from celery import Celery
import os

broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
app = Celery("shopdesk", broker=broker_url, backend=broker_url)

@app.task
def ping():
    return "pong"

if __name__ == "__main__":
    res = ping.delay()
    print("Celery app delay:", res.get(timeout=5))