import os, random, time
from fastapi import FastAPI
from anticip8_sdk import Anticip8Middleware

from fastapi import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


SERVICE_NAME = os.getenv("SERVICE_NAME", "orders-api")
CORE_URL = os.getenv("ANTICIP8_CORE_URL", "http://anticip8-core:8000")
OPTIONS_BASE = os.getenv("OPTIONS_BASE_URL", "http://options-api:8000")

app = FastAPI(title="Orders API")

app.add_middleware(
    Anticip8Middleware,
    core_url=CORE_URL,
    service_name=SERVICE_NAME,
    base_urls={
        "orders-api": "http://orders-api:8000",
        "options-api": "http://options-api:8000",
    },
    prefetch_enabled=True,
)

@app.get("/orders")
def orders():
    time.sleep(0.02)
    return [{"id": i, "title": f"Order {i}"} for i in range(1, 21)]

@app.get("/orders/{order_id}")
def order(order_id: int):
    time.sleep(0.03)
    return {"id": order_id, "items": random.randint(1, 5)}

@app.get("/_whoami")
def whoami():
    return {
        "service": "orders-api",
        "env_SERVICE_NAME": os.getenv("SERVICE_NAME"),
        "env_CORE": os.getenv("ANTICIP8_CORE_URL"),
        "env_OPTIONS_BASE": os.getenv("OPTIONS_BASE_URL"),
        "cwd": __import__("os").getcwd(),
        "file": __file__,
    }

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
