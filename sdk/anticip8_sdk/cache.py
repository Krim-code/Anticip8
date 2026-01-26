import os
import json
import hashlib
from typing import Any, Callable, Optional, Union
import inspect
from functools import wraps
from .metrics import cache_hits, cache_misses
import os

import redis

Jsonable = Union[dict, list, str, int, float, bool, None]

def _stable_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

class RedisCache:
    def __init__(self, url: Optional[str] = None):
        url = url or os.getenv("ANTICIP8_REDIS_URL") or os.getenv("REDIS_URL") or "redis://redis:6379/0"
        self.r = redis.Redis.from_url(url, decode_responses=True)

    def get(self, key: str) -> Optional[str]:
        return self.r.get(key)

    def setex(self, key: str, ttl: int, value: str) -> None:
        self.r.setex(key, ttl, value)

def default_key_builder(
    namespace: str,
    path: str,
    method: str,
    route_params: Optional[dict] = None,
    query_params: Optional[dict] = None,
    vary_user: bool = False,
    user_key: Optional[str] = None,
) -> str:
    rp = route_params or {}
    qp = query_params or {}
    base = {
        "ns": namespace,
        "m": method.upper(),
        "p": path,
        "rp": rp,
        "qp": qp,
        "u": user_key if vary_user else None,
    }
    return f"anticip8:cache:{namespace}:{_hash(_stable_json(base))}"

def cache_response(
    ttl: int = 60,
    namespace: str = "default",
    vary_user: bool = False,
    key_builder: Callable[..., str] = default_key_builder,
    cache: Optional[RedisCache] = None,
):
    cache = cache or RedisCache()

    def deco(fn: Callable[..., Any]):
        sig = inspect.signature(fn)
        is_async = inspect.iscoroutinefunction(fn)

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            path = getattr(getattr(request, "url", None), "path", None) or fn.__name__
            method = getattr(request, "method", "GET")

            route_params = dict(getattr(request, "path_params", {}) or {})
            query_params = dict(getattr(request, "query_params", {}) or {})

            user_key = None
            if vary_user and request is not None:
                user_key = request.headers.get("x-user", "anon")

            key = key_builder(
                namespace=namespace,
                path=path,
                method=method,
                route_params=route_params,
                query_params=query_params,
                vary_user=vary_user,
                user_key=user_key,
            )
            
            service = os.getenv("SERVICE_NAME", "unknown")
            hit = cache.get(key)
            if hit is not None:
                cache_hits.labels(service=service, namespace=namespace).inc()
                return json.loads(hit)

            cache_misses.labels(service=service, namespace=namespace).inc()

            data = await fn(*args, **kwargs) if is_async else fn(*args, **kwargs)
            cache.setex(key, ttl, _stable_json(data))
            return data


        # ВОТ ЭТО — главное: говорим FastAPI “сигнатура как у оригинала”
        wrapper.__signature__ = sig
        return wrapper

    return deco
