import os, time, random
from fastapi import FastAPI, Request
from anticip8_sdk import Anticip8Middleware, cache_response

from fastapi import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

import redis, json

r = redis.Redis(host="redis", port=6379, decode_responses=True)
SERVICE_NAME = os.getenv("SERVICE_NAME", "options-api")
CORE_URL = os.getenv("ANTICIP8_CORE_URL", "http://anticip8-core:8000")

app = FastAPI(title="Options API")

app.add_middleware(
    Anticip8Middleware,
    core_url=CORE_URL,
    service_name=SERVICE_NAME,
    base_urls={
        "orders-api": "http://orders-api:8000",
        "options-api": "http://options-api:8000",
    },
    prefetch_enabled=False,
)

@app.get("/order-options/{order_id}")
@cache_response(ttl=60, namespace="options")
def order_options(order_id: int, request: Request):
    time.sleep(0.15)
    return {
        "order_id": order_id,
        "options": ["A", "B", "C"],
    }

@app.get("/contacts")
def contacts():
    time.sleep(0.06)
    return [{"id": 1, "name": "Neo"}, {"id": 2, "name": "Trinity"}]

@app.get("/_whoami")
def whoami():
    return {
        "service": "options-api",
        "env_SERVICE_NAME": os.getenv("SERVICE_NAME"),
        "env_CORE": os.getenv("ANTICIP8_CORE_URL"),
        "cwd": __import__("os").getcwd(),
        "file": __file__,
    }
    
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
