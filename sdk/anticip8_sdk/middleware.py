import os
import re
import time
import asyncio
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Any

import httpx
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .client import Anticip8Client
from .metrics import (
    prefetch_total, prefetch_errors, prefetch_latency,
    policy_requests, policy_errors
)

RE_LAST_INT = re.compile(r"/(\d+)(?:/|$)")
SKIP_PREFIXES = ("/docs", "/openapi.json", "/metrics", "/_whoami", "/favicon.ico")


@dataclass
class _PrevState:
    path: str
    ts: float


class Anticip8Middleware(BaseHTTPMiddleware):
    """
    - tracks transitions prev_path -> current_path per user_key
    - sends ingest async (fire-and-forget)
    - requests policy + prefetch async (fire-and-forget)
    - concurrent prefetch + inflight dedup + budget
    """

    def __init__(
        self,
        app,
        core_url: str,
        service_name: str,
        base_urls: Optional[dict] = None,
        prefetch_enabled: bool = True,
        # --- tuning ---
        prev_ttl_sec: int = 1800,
        prev_max_users: int = 5000,
        max_prefetch_concurrency: int = 5,
        inflight_ttl_sec: float = 2.0,
        default_prefetch_budget_ms: int = 250,
        # httpx timeouts
        http_connect_timeout: float = 1.0,
        http_read_timeout: float = 3.0,
        http_write_timeout: float = 1.0,
        http_pool_timeout: float = 1.0,
        http_max_connections: int = 50,
        http_max_keepalive: int = 20,
    ):
        super().__init__(app)

        self.service_name = service_name
        self.client = Anticip8Client(core_url, service_name)

        self.base_urls = {k: v.rstrip("/") for k, v in (base_urls or {}).items()}
        self.prefetch_enabled = prefetch_enabled

        self.prev_ttl_sec = prev_ttl_sec
        self.prev_max_users = prev_max_users

        self.inflight_ttl_sec = inflight_ttl_sec
        self.default_prefetch_budget_ms = default_prefetch_budget_ms

        self.prefetch_sem = asyncio.Semaphore(max_prefetch_concurrency)

        self.timeout = httpx.Timeout(
            connect=http_connect_timeout,
            read=http_read_timeout,
            write=http_write_timeout,
            pool=http_pool_timeout,
        )
        self.limits = httpx.Limits(
            max_connections=http_max_connections,
            max_keepalive_connections=http_max_keepalive,
        )

        self._prev: Dict[str, _PrevState] = {}
        self._inflight: Dict[Tuple[str, str, str], float] = {}  # (user, svc, path) -> ts
        self._lock = asyncio.Lock()

        self._http: Optional[httpx.AsyncClient] = None

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout, limits=self.limits)
        return self._http

    async def dispatch(self, request: Request, call_next):
        user_key = request.headers.get("x-user", "anon")
        path = request.url.path

        if path.startswith(SKIP_PREFIXES):
            return await call_next(request)

        # --- prev tracking ---
        prev_path = None
        now = time.time()

        async with self._lock:
            # cleanup prev if too many
            if len(self._prev) > self.prev_max_users:
                # drop oldest ~20%
                items = sorted(self._prev.items(), key=lambda kv: kv[1].ts)
                for k, _st in items[: max(1, len(items)//5)]:
                    self._prev.pop(k, None)

            st = self._prev.get(user_key)
            if st and (now - st.ts) <= self.prev_ttl_sec:
                prev_path = st.path

            self._prev[user_key] = _PrevState(path=path, ts=now)

        # --- execute request ---
        t0 = time.perf_counter()
        resp: Response = await call_next(request)
        latency_ms = int((time.perf_counter() - t0) * 1000)

        # --- ingest transition (fire-and-forget) ---
        if prev_path and prev_path != path:
            # !!! positional args to match your Anticip8Client.ingest signature
            asyncio.create_task(self.client.ingest(user_key, prev_path, path, resp.status_code, latency_ms))

        # --- prefetch (fire-and-forget) ---
        if self.prefetch_enabled:
            asyncio.create_task(self._prefetch(user_key, path))

        return resp

    async def _prefetch(self, user_key: str, path: str):
        service = self.service_name

        policy_requests.labels(service=service).inc()
        try:
            # keep call style compatible with your client
            pol = await self.client.get_policy(user_key, path, limit=3)
        except Exception as e:
            policy_errors.labels(service=service, reason=type(e).__name__).inc()
            return

        next_paths = pol.get("next_paths") or []
        max_prefetch = int(pol.get("max_prefetch") or 0)
        if max_prefetch <= 0 or not next_paths:
            return

        # budget for whole prefetch batch
        budget_ms = int(pol.get("max_prefetch_time_ms") or self.default_prefetch_budget_ms)
        deadline = time.perf_counter() + (budget_ms / 1000.0)

        next_paths = sorted(next_paths, key=lambda x: x.get("score", 0), reverse=True)[:max_prefetch]

        http = await self._get_http()
        tasks = [self._prefetch_one(http, user_key, path, item, deadline) for item in next_paths]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _prefetch_one(
        self,
        http: httpx.AsyncClient,
        user_key: str,
        src_path: str,
        item: Dict[str, Any],
        deadline: float,
    ):
        if time.perf_counter() >= deadline:
            return

        svc = item.get("service")
        p = item.get("path")
        if not svc or not p:
            return

        # substitute {id} using last int from src_path
        if "{id}" in p:
            m = RE_LAST_INT.search(src_path)
            if not m:
                return
            p = p.replace("{id}", m.group(1))

        base = self.base_urls.get(svc)
        if not base:
            return

        # inflight dedup with TTL
        key = (user_key, svc, p)
        now = time.time()

        async with self._lock:
            # cleanup inflight
            for k, ts in list(self._inflight.items()):
                if (now - ts) > self.inflight_ttl_sec:
                    self._inflight.pop(k, None)

            if key in self._inflight:
                return
            self._inflight[key] = now

        try:
            async with self.prefetch_sem:
                if time.perf_counter() >= deadline:
                    return

                prefetch_total.labels(service=self.service_name).inc()

                t0 = time.perf_counter()
                status = 0
                try:
                    resp = await http.get(f"{base}{p}", headers={"x-user": user_key, "x-prefetch": "1"})
                    status = resp.status_code
                except Exception as e:
                    prefetch_errors.labels(service=self.service_name, reason=type(e).__name__).inc()
                finally:
                    dur = time.perf_counter() - t0
                    prefetch_latency.labels(service=self.service_name).observe(dur)

                if status:
                    try:
                        await self.client.ingest_prefetch(
                            user_key=user_key,
                            src_path=src_path,
                            dst_service=svc,
                            dst_path=p,
                            status=status,
                            latency_ms=int(dur * 1000),
                        )
                    except Exception:
                        pass
        finally:
            async with self._lock:
                self._inflight.pop(key, None)

    async def close(self):
        if self._http is not None:
            await self._http.aclose()
            self._http = None
