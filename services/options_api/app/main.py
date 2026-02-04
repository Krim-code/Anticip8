# app/main.py
import os
import json
import random
import asyncio
import hashlib
from typing import Optional

from fastapi import FastAPI, Request, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from anticip8_sdk import Anticip8Middleware, cache_response
from anticip8_sdk.cache import RedisCache, build_cache_key

# optional internal prefetch
try:
    from anticip8_sdk.middleware import register_internal_prefetch, PrefetchCtx
    INTERNAL_PREFETCH = True
except Exception:
    register_internal_prefetch = None  # type: ignore
    PrefetchCtx = object  # type: ignore
    INTERNAL_PREFETCH = False


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
    max_prefetch_concurrency=int(os.getenv("ANTICIP8_PREFETCH_MAX_CONCURRENCY", "2")),
)

_cache = RedisCache()

@app.on_event("shutdown")
async def _shutdown():
    await _cache.close()

# ---------------- utils ----------------
async def _sleep(ms_min: int, ms_max: int):
    await asyncio.sleep(random.randint(ms_min, ms_max) / 1000.0)

def _seed(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:8], 16)

def _rng(n: int) -> random.Random:
    return random.Random(n)

def _user(request: Request) -> str:
    return request.headers.get("x-user", "anon")

def _ctx_user(ctx: PrefetchCtx) -> str:
    u = getattr(ctx, "user_key", None) or getattr(ctx, "user", None)
    return u or "anon"

def _rand_city(rng: random.Random) -> str:
    return rng.choice(["Helsinki", "Tampere", "Turku", "Oulu", "Espoo"])

async def _cache_put_json(
    namespace: str,
    path: str,
    ttl: int,
    payload: dict,
    *,
    vary_user: bool = False,
    user_key: Optional[str] = None,
    query_params: Optional[dict] = None,
):
    key = build_cache_key(
        namespace=namespace,
        path=path,
        method="GET",
        route_params={},
        query_params=query_params or {},
        vary_user=vary_user,
        user_key=user_key,
    )
    await _cache.setex(
        key,
        ttl,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )

# =====================================================
# Implementations
# =====================================================
async def _order_options_impl(order_id: int):
    await _sleep(120, 200)
    return {"order_id": order_id, "options": ["A", "B", "C"]}

async def _customer_impl(customer_id: int):
    await _sleep(30, 70)
    rng = _rng(customer_id * 17)
    return {"id": customer_id, "name": f"Customer {customer_id}", "city": _rand_city(rng)}

async def _customer_addresses_impl(customer_id: int):
    await _sleep(80, 160)
    rng = _rng(customer_id * 101)
    return [{"id": i, "customer_id": customer_id, "city": _rand_city(rng)} for i in range(1, 4)]

async def _pricing_impl(order_id: int):
    await _sleep(90, 180)
    rng = _rng(order_id * 313)
    return {"order_id": order_id, "total": round(rng.uniform(10, 500), 2), "currency": "EUR"}

async def _delivery_impl(order_id: int):
    await _sleep(70, 140)
    rng = _rng(order_id * 911)
    return {"order_id": order_id, "eta_min": rng.randint(15, 120), "courier": rng.choice(["DHL", "Posti", "Wolt"])}

async def _timeline_impl(order_id: int):
    await _sleep(50, 120)
    rng = _rng(order_id * 123)
    return {
        "order_id": order_id,
        "events": [{"ts": i, "event": rng.choice(["created", "paid", "packed", "shipped", "delivered"])} for i in range(5)],
    }

async def _categories_impl():
    await _sleep(20, 60)
    names = ["Food", "Tech", "Home", "Books", "Clothes"]
    return [{"id": i, "name": n} for i, n in enumerate(names, start=1)]

async def _products_impl(category: int, q: str, page: int):
    await _sleep(60, 140)
    rng = _rng(_seed(f"{category}:{q}:{page}"))
    base = (page - 1) * 20
    return {
        "page": page,
        "category": category,
        "q": q,
        "items": [{"id": base + i, "title": f"Product {base+i}", "price": round(rng.uniform(1, 100), 2)} for i in range(1, 21)],
    }

async def _product_impl(product_id: int):
    await _sleep(90, 200)
    rng = _rng(product_id * 37)
    return {"id": product_id, "title": f"Product {product_id}", "desc": "lorem", "stock": rng.randint(0, 50)}

async def _reviews_impl(product_id: int, page: int):
    await _sleep(80, 160)
    rng = _rng(product_id * 73 + page * 11)
    return {"product_id": product_id, "page": page, "items": [{"id": i, "rating": rng.randint(1, 5), "text": "ok"} for i in range(1, 6)]}

async def _recommendations_impl(uid: str):
    await _sleep(40, 120)
    rng = _rng(_seed(uid) + 555)
    return {"user_id": uid, "items": rng.sample(range(1, 200), 10)}

async def _promos_impl():
    await _sleep(20, 70)
    return [{"id": 1, "title": "SALE -10%"}, {"id": 2, "title": "FREE DELIVERY"}]

async def _tickets_impl(uid: str, status: str):
    await _sleep(40, 100)
    rng = _rng(_seed(uid) + _seed(status))
    return {"user_id": uid, "status": status, "items": [{"id": i, "title": f"Ticket {i}"} for i in range(1, 6)]}

async def _ticket_impl(ticket_id: int):
    await _sleep(30, 80)
    return {"id": ticket_id, "messages": [{"from": "user", "text": "help"}, {"from": "support", "text": "ok"}]}

async def _contacts_impl():
    await _sleep(40, 90)
    return [{"id": 1, "name": "Neo"}, {"id": 2, "name": "Trinity"}]

# =====================================================
# Internal prefetch: warm cache directly (NO ctx.request bullshit)
# =====================================================
if INTERNAL_PREFETCH:

    @register_internal_prefetch("/order-options/{order_id}")
    async def pf_order_options(ctx: PrefetchCtx, order_id: int):
        data = await _order_options_impl(order_id)
        await _cache_put_json("options", f"/order-options/{order_id}", 60, data)

    @register_internal_prefetch("/orders/{order_id}/pricing")
    async def pf_pricing(ctx: PrefetchCtx, order_id: int):
        data = await _pricing_impl(order_id)
        await _cache_put_json("pricing", f"/orders/{order_id}/pricing", 60, data)

    @register_internal_prefetch("/orders/{order_id}/delivery")
    async def pf_delivery(ctx: PrefetchCtx, order_id: int):
        data = await _delivery_impl(order_id)
        await _cache_put_json("delivery", f"/orders/{order_id}/delivery", 60, data)

    @register_internal_prefetch("/catalog/products/{product_id}")
    async def pf_product(ctx: PrefetchCtx, product_id: int):
        data = await _product_impl(product_id)
        await _cache_put_json("product", f"/catalog/products/{product_id}", 120, data)

    @register_internal_prefetch("/customers/{customer_id}/addresses")
    async def pf_customer_addresses(ctx: PrefetchCtx, customer_id: int):
        data = await _customer_addresses_impl(customer_id)
        await _cache_put_json("addr", f"/customers/{customer_id}/addresses", 120, data)

    # опционально: user-scoped штуки тоже можно греть
    @register_internal_prefetch("/recommendations")
    async def pf_recommendations(ctx: PrefetchCtx):
        uid = _ctx_user(ctx)
        data = await _recommendations_impl(uid)
        await _cache_put_json("reco", "/recommendations", 30, data, vary_user=True, user_key=uid)

    @register_internal_prefetch("/support/tickets")
    async def pf_tickets(ctx: PrefetchCtx):
        uid = _ctx_user(ctx)
        status = "open"
        data = await _tickets_impl(uid, status)
        # status — query param, его надо учесть в ключе
        await _cache_put_json(
            "tickets",
            "/support/tickets",
            30,
            data,
            vary_user=True,
            user_key=uid,
            query_params={"status": status},
        )

# =====================================================
# Endpoints
# =====================================================
@app.get("/order-options/{order_id}")
@cache_response(ttl=60, namespace="options")
async def order_options(order_id: int, request: Request):
    return await _order_options_impl(order_id)

@app.get("/contacts")
async def contacts():
    return await _contacts_impl()

@app.get("/customers/{customer_id}")
async def customer(customer_id: int):
    return await _customer_impl(customer_id)

@app.get("/customers/{customer_id}/addresses")
@cache_response(ttl=120, namespace="addr")
async def customer_addresses(customer_id: int, request: Request):
    return await _customer_addresses_impl(customer_id)

@app.get("/orders/{order_id}/pricing")
@cache_response(ttl=60, namespace="pricing")
async def pricing(order_id: int, request: Request):
    return await _pricing_impl(order_id)

@app.get("/orders/{order_id}/delivery")
@cache_response(ttl=60, namespace="delivery")
async def delivery(order_id: int, request: Request):
    return await _delivery_impl(order_id)

@app.get("/orders/{order_id}/timeline")
async def timeline(order_id: int):
    return await _timeline_impl(order_id)

@app.get("/catalog/categories")
async def categories():
    return await _categories_impl()

@app.get("/catalog/products")
async def products(category: int = 1, q: str = "", page: int = 1):
    return await _products_impl(category, q, page)

@app.get("/catalog/products/{product_id}")
@cache_response(ttl=120, namespace="product")
async def product(product_id: int, request: Request):
    return await _product_impl(product_id)

@app.get("/catalog/products/{product_id}/reviews")
async def reviews(product_id: int, page: int = 1):
    return await _reviews_impl(product_id, page)

@app.get("/recommendations")
@cache_response(ttl=30, namespace="reco", vary_user=True)
async def recommendations(request: Request, user_id: str = "u_test"):
    uid = _user(request) or user_id
    return await _recommendations_impl(uid)

@app.get("/promotions/active")
async def promos():
    return await _promos_impl()

@app.get("/support/tickets")
@cache_response(ttl=30, namespace="tickets", vary_user=True)
async def tickets(request: Request, user_id: str = "u_test", status: str = "open"):
    uid = _user(request) or user_id
    return await _tickets_impl(uid, status)

@app.get("/support/tickets/{ticket_id}")
async def ticket(ticket_id: int):
    return await _ticket_impl(ticket_id)

@app.get("/_whoami")
async def whoami(request: Request):
    return {
        "service": SERVICE_NAME,
        "x_user": _user(request),
        "env_SERVICE_NAME": os.getenv("SERVICE_NAME"),
        "env_CORE": os.getenv("ANTICIP8_CORE_URL"),
    }

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
