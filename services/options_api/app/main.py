import os, time, random
from fastapi import FastAPI, Request, Response
from anticip8_sdk import Anticip8Middleware, cache_response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

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
    prefetch_enabled=True,
    # Keep this low: more concurrency usually increases deadline skips without improving hit-rate.
    max_prefetch_concurrency=2,
)
# --- helpers ---
def _sleep(ms_min: int, ms_max: int):
    time.sleep(random.randint(ms_min, ms_max) / 1000.0)

def _rand_city():
    return random.choice(["Helsinki", "Tampere", "Turku", "Oulu", "Espoo"])

# --- existing ---
@app.get("/order-options/{order_id}")
@cache_response(ttl=60, namespace="options")
def order_options(order_id: int, request: Request):
    _sleep(120, 200)
    return {"order_id": order_id, "options": ["A", "B", "C"]}

@app.get("/contacts")
def contacts():
    _sleep(40, 90)
    return [{"id": 1, "name": "Neo"}, {"id": 2, "name": "Trinity"}]

# --- new endpoints ---
@app.get("/customers/{customer_id}")
def customer(customer_id: int):
    _sleep(30, 70)
    return {"id": customer_id, "name": f"Customer {customer_id}", "city": _rand_city()}

@app.get("/customers/{customer_id}/addresses")
@cache_response(ttl=120, namespace="addr")
def customer_addresses(customer_id: int, request: Request):
    _sleep(80, 160)
    return [{"id": i, "customer_id": customer_id, "city": _rand_city()} for i in range(1, 4)]

@app.get("/orders/{order_id}/pricing")
@cache_response(ttl=60, namespace="pricing")
def pricing(order_id: int, request: Request):
    _sleep(90, 180)
    return {"order_id": order_id, "total": round(random.uniform(10, 500), 2), "currency": "EUR"}

@app.get("/orders/{order_id}/delivery")
@cache_response(ttl=60, namespace="delivery")
def delivery(order_id: int, request: Request):
    _sleep(70, 140)
    return {"order_id": order_id, "eta_min": random.randint(15, 120), "courier": random.choice(["DHL", "Posti", "Wolt"])}

@app.get("/orders/{order_id}/timeline")
def timeline(order_id: int):
    _sleep(50, 120)
    return {
        "order_id": order_id,
        "events": [{"ts": i, "event": random.choice(["created", "paid", "packed", "shipped", "delivered"])} for i in range(5)]
    }

@app.get("/catalog/categories")
def categories():
    _sleep(20, 60)
    return [{"id": i, "name": n} for i, n in enumerate(["Food", "Tech", "Home", "Books", "Clothes"], start=1)]

@app.get("/catalog/products")
def products(category: int = 1, q: str = "", page: int = 1):
    _sleep(60, 140)
    base = (page - 1) * 20
    return {
        "page": page,
        "category": category,
        "q": q,
        "items": [{"id": base + i, "title": f"Product {base+i}", "price": round(random.uniform(1, 100), 2)} for i in range(1, 21)]
    }

@app.get("/catalog/products/{product_id}")
@cache_response(ttl=120, namespace="product")
def product(product_id: int, request: Request):
    _sleep(90, 200)
    return {"id": product_id, "title": f"Product {product_id}", "desc": "lorem", "stock": random.randint(0, 50)}

@app.get("/catalog/products/{product_id}/reviews")
def reviews(product_id: int, page: int = 1):
    _sleep(80, 160)
    return {
        "product_id": product_id,
        "page": page,
        "items": [{"id": i, "rating": random.randint(1, 5), "text": "ok"} for i in range(1, 6)]
    }

@app.get("/recommendations")
def recommendations(user_id: str = "u_test"):
    _sleep(40, 120)
    return {"user_id": user_id, "items": random.sample(range(1, 200), 10)}

@app.get("/promotions/active")
def promos():
    _sleep(20, 70)
    return [{"id": 1, "title": "SALE -10%"}, {"id": 2, "title": "FREE DELIVERY"}]

@app.get("/support/tickets")
def tickets(user_id: str = "u_test", status: str = "open"):
    _sleep(40, 100)
    return {"user_id": user_id, "status": status, "items": [{"id": i, "title": f"Ticket {i}"} for i in range(1, 6)]}

@app.get("/support/tickets/{ticket_id}")
def ticket(ticket_id: int):
    _sleep(30, 80)
    return {"id": ticket_id, "messages": [{"from": "user", "text": "help"}, {"from": "support", "text": "ok"}]}

@app.get("/_whoami")
def whoami():
    return {"service": "options-api", "env_SERVICE_NAME": os.getenv("SERVICE_NAME"), "env_CORE": os.getenv("ANTICIP8_CORE_URL")}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
