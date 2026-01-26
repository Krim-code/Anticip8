import json
import os
import hashlib
from functools import wraps
from typing import Callable, Any, Optional

import redis
from fastapi import Request

CACHE_MODE = os.getenv("CACHE_MODE", "NO_CACHE")  # IO_CACHE | NO_CACHE
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/1")

_rds: Optional[redis.Redis] = None

def _redis() -> redis.Redis:
    global _rds
    if _rds is None:
        _rds = redis.from_url(REDIS_URL, decode_responses=True)
    return _rds

def _key(namespace: str, request: Request) -> str:
    # Важно: одинаково для тестов. Хедер x-user участвует, чтобы кеш был “пользовательский”.
    user = request.headers.get("x-user", "")
    raw = f"{request.method}:{request.url.path}?{request.url.query}|u={user}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{namespace}:{h}"

def cached(ttl: int, namespace: str):
    def deco(fn: Callable[..., Any]):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if CACHE_MODE != "IO_CACHE":
                return fn(*args, **kwargs)

            request: Request = kwargs.get("request")
            if request is None:
                # если забыли Request — считаем что кеш нельзя
                return fn(*args, **kwargs)

            k = _key(namespace, request)
            r = _redis()

            hit = r.get(k)
            if hit is not None:
                return json.loads(hit)

            val = fn(*args, **kwargs)
            r.setex(k, ttl, json.dumps(val, ensure_ascii=False))
            return val
        return wrapper
    return deco
