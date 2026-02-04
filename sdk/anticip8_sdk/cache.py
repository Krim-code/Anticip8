import os
import json
import hashlib
import inspect
from functools import wraps
from typing import Any, Callable, Optional, Union, Awaitable

import anyio
from redis.asyncio import Redis

from .metrics import cache_hits, cache_misses

Jsonable = Union[dict, list, str, int, float, bool, None]


def _stable_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class RedisCache:
    """
    Async Redis cache (non-blocking).
    """
    def __init__(self, url: Optional[str] = None):
        url = (
            url
            or os.getenv("ANTICIP8_REDIS_URL")
            or os.getenv("REDIS_URL")
            or "redis://redis:6379/0"
        )
        self.r: Redis = Redis.from_url(url, decode_responses=True)

    async def get(self, key: str) -> Optional[str]:
        return await self.r.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        # SETEX exists, but set(ex=ttl) is also fine.
        await self.r.set(key, value, ex=ttl)

    async def close(self) -> None:
        try:
            await self.r.aclose()
        except Exception:
            pass


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
    # Если Redis упал — не валим ручку.
    fail_open: bool = True,
):
    cache = cache or RedisCache()

    def deco(fn: Callable[..., Any]):
        sig = inspect.signature(fn)
        is_async = inspect.iscoroutinefunction(fn)

        async def _call_handler(*args, **kwargs):
            if is_async:
                return await fn(*args, **kwargs)
            # sync handler -> threadpool (не блокируем loop)
            return await anyio.to_thread.run_sync(lambda: fn(*args, **kwargs))

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            # FastAPI обычно прокидывает request как kwarg, но иногда он в args
            request = kwargs.get("request")
            if request is None:
                for a in args:
                    if hasattr(a, "url") and hasattr(a, "method"):
                        request = a
                        break

            path = getattr(getattr(request, "url", None), "path", None) or fn.__name__
            method = getattr(request, "method", "GET")

            route_params = dict(getattr(request, "path_params", {}) or {}) if request else {}
            query_params = dict(getattr(request, "query_params", {}) or {}) if request else {}

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

            # ---- GET ----
            try:
                hit = await cache.get(key)
            except Exception:
                hit = None
                if not fail_open:
                    raise

            if hit is not None:
                cache_hits.labels(service=service, namespace=namespace).inc()
                try:
                    return json.loads(hit)
                except Exception:
                    # corrupted cache entry
                    cache_misses.labels(service=service, namespace=namespace).inc()

            cache_misses.labels(service=service, namespace=namespace).inc()

            # ---- COMPUTE ----
            data = await _call_handler(*args, **kwargs)

            # Если это Response/Streaming/etc — не кешируем
            if hasattr(data, "body") and hasattr(data, "status_code"):
                return data

            # Only cache JSONable
            try:
                payload = _stable_json(data)
            except Exception:
                return data

            # ---- SET ----
            try:
                await cache.setex(key, ttl, payload)
            except Exception:
                if not fail_open:
                    raise

            return data

        wrapper.__signature__ = sig  # FastAPI signature reflection
        return wrapper

    return deco

def build_cache_key(
    namespace: str,
    path: str,
    method: str = "GET",
    route_params: Optional[dict] = None,
    query_params: Optional[dict] = None,
    vary_user: bool = False,
    user_key: Optional[str] = None,
    key_builder: Callable[..., str] = default_key_builder,
) -> str:
    return key_builder(
        namespace=namespace,
        path=path,
        method=method,
        route_params=route_params or {},
        query_params=query_params or {},
        vary_user=vary_user,
        user_key=user_key,
    )

