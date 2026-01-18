import time
import asyncio
import httpx
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .metrics import (
    prefetch_total, prefetch_errors, prefetch_latency,
    policy_requests, policy_errors
)
import os

from .client import Anticip8Client

import re
RE_LAST_INT = re.compile(r"/(\d+)(?:/|$)")

class Anticip8Middleware(BaseHTTPMiddleware):
    """
    Ловим переходы: prev_path -> current_path (по user_key).
    После ответа: просим policy и делаем prefetch.
    """
    def __init__(self, app, core_url: str, service_name: str, base_urls: dict, prefetch_enabled: bool = True):
        super().__init__(app)
        self.client = Anticip8Client(core_url, service_name)
        self.base_urls = {k: v.rstrip("/") for k, v in (base_urls or {}).items()}
        self.prefetch_enabled = prefetch_enabled
        self._prev = {}


    async def dispatch(self, request: Request, call_next):
        


        user_key = request.headers.get("x-user", "anon")
        path = request.url.path
        
        if path.startswith(("/docs", "/openapi.json", "/metrics", "/_whoami")):
            return await call_next(request)
        
        prev_path = self._prev.get(user_key)
        self._prev[user_key] = path

        t0 = time.perf_counter()
        resp: Response = await call_next(request)
        latency_ms = int((time.perf_counter() - t0) * 1000)

        if prev_path and prev_path != path:
            # не блокируем ответ: отправку события делаем таской
            asyncio.create_task(self.client.ingest(user_key, prev_path, path, resp.status_code, latency_ms))

        if self.prefetch_enabled:
            asyncio.create_task(self._prefetch(user_key, path))

        return resp

    async def _prefetch(self, user_key: str, path: str):
        service = os.getenv("SERVICE_NAME", "unknown")

        # === policy request metric ===
        policy_requests.labels(service=service).inc()
        try:
            pol = await self.client.get_policy(user_key, path, limit=3)
        except Exception as e:
            policy_errors.labels(service=service, reason=type(e).__name__).inc()
            return

        next_paths = pol.get("next_paths", [])
        max_prefetch = int(pol.get("max_prefetch", 0) or 0)
        if max_prefetch <= 0 or not next_paths:
            return

        next_paths = sorted(
            next_paths,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:max_prefetch]

        async with httpx.AsyncClient(timeout=1.0) as c:
                for item in next_paths:
                    svc = item.get("service")
                    p = item.get("path")
                    if not svc or not p:
                        continue
                    
                    if "{id}" in p:
                        m = RE_LAST_INT.search(path)  # path = текущий реальный путь, типа /orders/15
                        if not m:
                            continue
                        p = p.replace("{id}", m.group(1))

                    base = self.base_urls.get(svc)
                    if not base:
                        continue

                    prefetch_total.labels(service=service).inc()
                    t0 = time.perf_counter()
                    status = 0
                    try:
                        resp = await c.get(f"{base}{p}", headers={"x-user": user_key, "x-prefetch": "1"})
                        status = resp.status_code
                    except Exception as e:
                        prefetch_errors.labels(service=service, reason=type(e).__name__).inc()
                    finally:
                        prefetch_latency.labels(service=service).observe(time.perf_counter() - t0)

                    # логируем межсервисное ребро только если хоть как-то сходили
                    if status:
                        try:
                            await self.client.ingest_prefetch(
                                user_key=user_key,
                                src_path=path,
                                dst_service=svc,
                                dst_path=p,
                                status=status,
                                latency_ms=int((time.perf_counter() - t0) * 1000),
                            )
                        except Exception:
                            pass