# app/main.py
import os
import json
import random
import asyncio
import hashlib
from typing import Optional

from fastapi import FastAPI, Response, Request
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from anticip8_sdk import Anticip8Middleware, cache_response
from anticip8_sdk.middleware import register_internal_prefetch, PrefetchCtx
from anticip8_sdk.cache import RedisCache, build_cache_key

SERVICE_NAME = os.getenv("SERVICE_NAME", "orders-api")
CORE_URL = os.getenv("ANTICIP8_CORE_URL", "http://anticip8-core:8000")

app = FastAPI(title="Orders API")

# ---------------- Anticip8 middleware ----------------
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

async def _cache_put_json(
    namespace: str,
    path: str,
    ttl: int,
    payload: dict,
    *,
    vary_user: bool = False,
    user_key: Optional[str] = None,
):
    key = build_cache_key(
        namespace=namespace,
        path=path,
        method="GET",
        route_params={},
        query_params={},
        vary_user=vary_user,
        user_key=user_key,
    )
    await _cache.setex(
        key,
        ttl,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )

# =====================================================
# Internal implementations (NO Request in signature)
# =====================================================

async def _order_impl(order_id: int):
    await _sleep(25, 70)
    rng = _rng(order_id)
    return {
        "id": order_id,
        "items": rng.randint(1, 7),
        "customer_id": rng.randint(1, 200),
        "total": round(rng.uniform(20, 500), 2),
    }

async def _order_items_impl(order_id: int):
    await _sleep(50, 130)
    rng = _rng(order_id * 991)
    return {
        "order_id": order_id,
        "items": [{"sku": f"SKU-{i}", "qty": rng.randint(1, 3)} for i in range(1, rng.randint(4, 8))],
    }

async def _payment_impl(order_id: int):
    await _sleep(70, 160)
    rng = _rng(order_id * 31337)
    return {
        "order_id": order_id,
        "method": rng.choice(["card", "invoice", "cash"]),
        "paid": rng.choice([True, False]),
    }

async def _basket_summary_impl(uid: str):
    await _sleep(40, 100)
    rng = _rng(_seed(uid))
    return {"user_id": uid, "items": rng.randint(1, 10), "total": round(rng.uniform(10, 300), 2)}

async def _basket_items_impl(uid: str):
    await _sleep(50, 120)
    rng = _rng(_seed(uid) + 7)
    return {"user_id": uid, "items": [{"product_id": i, "qty": rng.randint(1, 3)} for i in rng.sample(range(1, 300), 6)]}

async def _profile_impl(uid: str):
    await _sleep(25, 80)
    rng = _rng(_seed(uid) + 101)
    return {"user_id": uid, "tier": rng.choice(["bronze", "silver", "gold"]), "created_at": "2024-01-01"}

async def _history_impl(uid: str, page: int):
    await _sleep(60, 140)
    rng = _rng(_seed(uid) + page * 13)
    return {"user_id": uid, "page": page, "items": [{"order_id": i} for i in rng.sample(range(1, 500), 20)]}

async def _feed_impl(uid: str):
    await _sleep(40, 110)
    rng = _rng(_seed(uid) + 404)
    widgets = ["orders", "basket", "promos", "recommendations", "support", "profile", "catalog"]
    return {"user_id": uid, "widgets": rng.sample(widgets, 4)}

# =====================================================
# Internal prefetch: warm Redis cache DIRECTLY
# =====================================================

@register_internal_prefetch("/orders/{order_id}")
async def pf_order(ctx: PrefetchCtx, order_id: int):
    data = await _order_impl(order_id)
    await _cache_put_json("orders", f"/orders/{order_id}", 60, data)

@register_internal_prefetch("/orders/{order_id}/items")
async def pf_order_items(ctx: PrefetchCtx, order_id: int):
    data = await _order_items_impl(order_id)
    await _cache_put_json("orders", f"/orders/{order_id}/items", 90, data)

@register_internal_prefetch("/orders/{order_id}/payment")
async def pf_order_payment(ctx: PrefetchCtx, order_id: int):
    data = await _payment_impl(order_id)
    await _cache_put_json("orders", f"/orders/{order_id}/payment", 30, data)

@register_internal_prefetch("/basket/summary")
async def pf_basket_summary(ctx: PrefetchCtx):
    uid = _ctx_user(ctx)
    data = await _basket_summary_impl(uid)
    await _cache_put_json("orders", "/basket/summary", 30, data, vary_user=True, user_key=uid)

@register_internal_prefetch("/basket/items")
async def pf_basket_items(ctx: PrefetchCtx):
    uid = _ctx_user(ctx)
    data = await _basket_items_impl(uid)
    await _cache_put_json("orders", "/basket/items", 45, data, vary_user=True, user_key=uid)

@register_internal_prefetch("/profile")
async def pf_profile(ctx: PrefetchCtx):
    uid = _ctx_user(ctx)
    data = await _profile_impl(uid)
    await _cache_put_json("orders", "/profile", 120, data, vary_user=True, user_key=uid)

@register_internal_prefetch("/profile/history")
async def pf_profile_history(ctx: PrefetchCtx):
    uid = _ctx_user(ctx)
    page = 1
    data = await _history_impl(uid, page)

    key = build_cache_key(
        namespace="orders",
        path="/profile/history",
        method="GET",
        route_params={},
        query_params={"page": str(page)},
        vary_user=True,
        user_key=uid,
    )
    await _cache.setex(
        key,
        120,
        json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )

@register_internal_prefetch("/feed")
async def pf_feed(ctx: PrefetchCtx):
    uid = _ctx_user(ctx)
    data = await _feed_impl(uid)
    await _cache_put_json("orders", "/feed", 25, data, vary_user=True, user_key=uid)

# ---------------- endpoints ----------------

@app.get("/orders")
async def orders(status: str = "new", page: int = 1, q: str = ""):
    await _sleep(15, 45)
    base = (page - 1) * 20
    return {
        "status": status,
        "page": page,
        "q": q,
        "items": [{"id": base + i, "title": f"Order {base + i}"} for i in range(1, 21)],
    }

@app.get("/orders/{order_id}")
@cache_response(ttl=180, namespace="orders")
async def order(order_id: int, request: Request):
    return await _order_impl(order_id)

@app.get("/orders/{order_id}/items")
@cache_response(ttl=240, namespace="orders")
async def order_items(order_id: int, request: Request):
    return await _order_items_impl(order_id)

@app.get("/orders/{order_id}/payment")
@cache_response(ttl=30, namespace="orders")
async def payment(order_id: int, request: Request):
    return await _payment_impl(order_id)

@app.get("/orders/{order_id}/status")
async def order_status(order_id: int):
    await _sleep(20, 50)
    return {"order_id": order_id, "status": random.choice(["new", "paid", "packed", "shipped", "delivered", "canceled"])}

@app.get("/orders/{order_id}/cancel")
async def cancel(order_id: int, reason: str = "changed_mind"):
    await _sleep(30, 80)
    return {"order_id": order_id, "canceled": True, "reason": reason}

@app.get("/basket/summary")
@cache_response(ttl=30, namespace="orders", vary_user=True)
async def basket_summary(request: Request):
    uid = _user(request)
    return await _basket_summary_impl(uid)

@app.get("/basket/items")
@cache_response(ttl=45, namespace="orders", vary_user=True)
async def basket_items(request: Request):
    uid = _user(request)
    return await _basket_items_impl(uid)

@app.get("/profile")
@cache_response(ttl=300, namespace="orders", vary_user=True)
async def profile(request: Request):
    uid = _user(request)
    return await _profile_impl(uid)

@app.get("/profile/history")
@cache_response(ttl=300, namespace="orders", vary_user=True)
async def history(request: Request, page: int = 1):
    uid = _user(request)
    return await _history_impl(uid, page)

@app.get("/feed")
@cache_response(ttl=25, namespace="orders", vary_user=True)
async def feed(request: Request):
    uid = _user(request)
    return await _feed_impl(uid)

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
