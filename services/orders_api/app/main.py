import os
import random
import time
from fastapi import FastAPI, Response, Request
from anticip8_sdk import Anticip8Middleware, cache_response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = os.getenv("SERVICE_NAME", "orders-api")
CORE_URL = os.getenv("ANTICIP8_CORE_URL", "http://anticip8-core:8000")

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


# ---------------- utils ----------------

def _sleep(ms_min: int, ms_max: int):
    time.sleep(random.randint(ms_min, ms_max) / 1000.0)

def _rng(seed: int):
    return random.Random(seed)

# ---------------- endpoints ----------------

@app.get("/orders")
def orders(status: str = "new", page: int = 1, q: str = ""):
    """
    Список заказов — НЕ кешируем (слишком много вариаций)
    """
    _sleep(15, 45)
    base = (page - 1) * 20
    return {
        "status": status,
        "page": page,
        "q": q,
        "items": [{"id": base + i, "title": f"Order {base + i}"} for i in range(1, 21)],
    }


@app.get("/orders/{order_id}")
@cache_response(ttl=60, namespace="orders")
def order(order_id: int, request: Request):
    """
    Карточка заказа — стабильная → кеш
    """
    _sleep(25, 70)
    rng = _rng(order_id)
    return {
        "id": order_id,
        "items": rng.randint(1, 7),
        "customer_id": rng.randint(1, 200),
        "total": round(rng.uniform(20, 500), 2),
    }


@app.get("/orders/{order_id}/items")
@cache_response(ttl=90, namespace="orders")
def order_items(order_id: int, request: Request):
    """
    Тяжёлая ручка → идеальна для префетча
    """
    _sleep(50, 130)
    rng = _rng(order_id)
    return {
        "order_id": order_id,
        "items": [
            {"sku": f"SKU-{i}", "qty": rng.randint(1, 3)}
            for i in range(1, rng.randint(4, 8))
        ],
    }


@app.get("/orders/{order_id}/payment")
@cache_response(ttl=30, namespace="orders")
def payment(order_id: int, request: Request):
    """
    Полустабильная → короткий TTL
    """
    _sleep(70, 160)
    rng = _rng(order_id)
    return {
        "order_id": order_id,
        "method": rng.choice(["card", "invoice", "cash"]),
        "paid": rng.choice([True, False]),
    }


@app.get("/orders/{order_id}/status")
def order_status(order_id: int):
    """
    Живой статус — НЕ кешируем
    """
    _sleep(20, 50)
    return {
        "order_id": order_id,
        "status": random.choice(
            ["new", "paid", "packed", "shipped", "delivered", "canceled"]
        ),
    }


@app.get("/orders/{order_id}/cancel")
def cancel(order_id: int, reason: str = "changed_mind"):
    """
    Mutating endpoint — НИКОГДА не кешируем
    """
    _sleep(30, 80)
    return {"order_id": order_id, "canceled": True, "reason": reason}


@app.get("/basket/summary")
@cache_response(ttl=30, namespace="orders")
def basket_summary(user_id: str = "u_test", request: Request = None):
    _sleep(40, 100)
    rng = _rng(hash(user_id))
    return {
        "user_id": user_id,
        "items": rng.randint(1, 10),
        "total": round(rng.uniform(10, 300), 2),
    }


@app.get("/basket/items")
@cache_response(ttl=45, namespace="orders")
def basket_items(user_id: str = "u_test", request: Request = None):
    _sleep(50, 120)
    rng = _rng(hash(user_id))
    return {
        "user_id": user_id,
        "items": [
            {"product_id": i, "qty": rng.randint(1, 3)}
            for i in rng.sample(range(1, 300), 6)
        ],
    }


@app.get("/profile")
@cache_response(ttl=120, namespace="orders")
def profile(user_id: str = "u_test", request: Request = None):
    _sleep(25, 80)
    rng = _rng(hash(user_id))
    return {
        "user_id": user_id,
        "tier": rng.choice(["bronze", "silver", "gold"]),
        "created_at": "2024-01-01",
    }


@app.get("/profile/history")
@cache_response(ttl=120, namespace="orders")
def history(user_id: str = "u_test", page: int = 1, request: Request = None):
    _sleep(60, 140)
    rng = _rng(hash(user_id) + page)
    return {
        "user_id": user_id,
        "page": page,
        "items": [{"order_id": i} for i in rng.sample(range(1, 500), 20)],
    }


@app.get("/search")
def search(q: str = "neo", limit: int = 20):
    """
    Поиск — не кешируем, слишком динамичный
    """
    _sleep(50, 140)
    return {
        "q": q,
        "items": [
            {"id": i, "type": random.choice(["order", "product", "customer"])}
            for i in range(1, limit + 1)
        ],
    }


@app.get("/feed")
def feed(user_id: str = "u_test"):
    """
    Главный экран — точка разветвления
    """
    _sleep(40, 110)
    return {
        "user_id": user_id,
        "widgets": random.sample(
            ["orders", "basket", "promos", "recommendations", "support", "profile", "catalog"],
            4,
        ),
    }


# ---------------- infra ----------------

@app.get("/_whoami")
def whoami():
    return {
        "service": SERVICE_NAME,
        "env_SERVICE_NAME": os.getenv("SERVICE_NAME"),
        "env_CORE": os.getenv("ANTICIP8_CORE_URL"),
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
