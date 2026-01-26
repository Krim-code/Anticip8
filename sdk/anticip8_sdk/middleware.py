import os
import re
import time
import json
import asyncio
import random
from typing import Dict, Optional, Tuple, Any, List

import httpx
import redis
from starlette.types import ASGIApp, Receive, Scope, Send

from .client import Anticip8Client
from .metrics import (
    prefetch_total, prefetch_errors, prefetch_latency,
    policy_requests, policy_errors, policy_latency,
    prefetch_hits, prefetch_misses,
    prefetch_deadline_skips, prefetch_dedup_skips, prefetch_budget_overrun,
)

# =========================
# Debug
# =========================
DEBUG_PREFETCH = os.getenv("ANTICIP8_DEBUG_PREFETCH", "0") == "1"
DEBUG_SAMPLE = float(os.getenv("ANTICIP8_DEBUG_PREFETCH_SAMPLE", "0.05"))
DEBUG_MAX_ITEMS = int(os.getenv("ANTICIP8_DEBUG_PREFETCH_MAX_ITEMS", "5"))

# =========================
# Regex: extract last id/uuid from src_path
# =========================
RE_LAST_INT = re.compile(r"/(\d+)(?:/|$)")
RE_LAST_UUID = re.compile(
    r"/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12})(?:/|$)"
)

# =========================
# Skip endpoints
# =========================
SKIP_PREFIXES = (
    "/docs", "/openapi.json", "/metrics", "/_whoami", "/favicon.ico", "/health",
    "/redoc",
)

# =========================
# Redis keys
# =========================
def _sess_key(user_key: str) -> str:
    return f"anticip8:sess:{user_key}"

def _pf_key(user_key: str, svc: str, path: str) -> str:
    return f"anticip8:pf:{user_key}:{svc}:{path}"

def _intent_key(user_key: str, svc: str, dst_path: str) -> str:
    # intent: "we expected this concrete dst path to be used soon"
    return f"anticip8:intent:{user_key}:{svc}:{dst_path}"


class Anticip8Middleware:
    def __init__(
        self,
        app: ASGIApp,
        core_url: str,
        service_name: str,
        base_urls: Optional[dict] = None,
        prefetch_enabled: bool = True,
        redis_url: Optional[str] = None,
        session_ttl_sec: int = 1800,
        prefetch_mark_ttl_sec: int = 60,
        intent_ttl_sec: int = 45,
        max_prefetch_concurrency: int = 5,
        inflight_ttl_sec: float = 2.0,
        default_prefetch_budget_ms: int = 250,
        http_connect_timeout: float = 1.0,
        http_read_timeout: float = 3.0,
        http_write_timeout: float = 1.0,
        http_pool_timeout: float = 1.0,
        http_max_connections: int = 50,
        http_max_keepalive: int = 20,
    ):
        self.app = app
        self.service_name = service_name
        self.prefetch_enabled = prefetch_enabled

        self.client = Anticip8Client(
            core_url=core_url,
            service_name=service_name,
            timeout_sec=1.0,
            max_connections=http_max_connections,
            max_keepalive=http_max_keepalive,
        )

        # base_urls: {"orders-api":"http://orders-api:18001", "options-api":"http://options-api:18002"}
        self.base_urls = {k: v.rstrip("/") for k, v in (base_urls or {}).items()}

        rurl = redis_url or os.getenv("REDIS_URL") or "redis://redis:6379/0"
        self.r = redis.Redis.from_url(rurl, decode_responses=True)

        self.session_ttl_sec = session_ttl_sec
        self.prefetch_mark_ttl_sec = prefetch_mark_ttl_sec
        self.intent_ttl_sec = intent_ttl_sec

        self.inflight_ttl_sec = inflight_ttl_sec
        self.default_prefetch_budget_ms = default_prefetch_budget_ms
        self.prefetch_sem = asyncio.Semaphore(max_prefetch_concurrency)

        # EARLY cutoff: if remaining budget < this window - don't start
        self.min_prefetch_window_sec = float(os.getenv("ANTICIP8_MIN_PREFETCH_WINDOW_SEC", "0.05"))

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
        self._http: Optional[httpx.AsyncClient] = None

        self._inflight: Dict[Tuple[str, str, str], float] = {}
        self._lock = asyncio.Lock()

    # =========================
    # Debug helpers
    # =========================
    def _dbg_enabled(self, user_key: str) -> bool:
        if not DEBUG_PREFETCH:
            return False
        return random.random() <= DEBUG_SAMPLE

    def _dbg(self, msg: str, **kv):
        payload = {"ts": int(time.time()), "svc": self.service_name, "msg": msg, **kv}
        print("PREFETCH_DBG", json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    # =========================
    # http client
    # =========================
    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout, limits=self.limits)
        return self._http

    # =========================
    # Session tracking
    # =========================
    def _get_prev_and_set_current(self, user_key: str, path: str) -> Tuple[Optional[str], Optional[str]]:
        key = _sess_key(user_key)
        prev_svc = prev_path = None

        raw = self.r.get(key)
        if raw:
            try:
                obj = json.loads(raw)
                prev_svc = obj.get("svc")
                prev_path = obj.get("path")
            except Exception:
                prev_svc = prev_path = None

        self.r.setex(
            key,
            self.session_ttl_sec,
            json.dumps({"svc": self.service_name, "path": path, "ts": int(time.time())}),
        )
        return prev_svc, prev_path

    # =========================
    # Hit / miss accounting (FIXED)
    # =========================
    def _check_prefetch_hit(self, user_key: str, path: str):
        """
        IMPORTANT FIX:
        miss is counted only if we previously planned prefetch for this src_path (intent key exists).
        Otherwise we'd count miss for every request (which is garbage).
        """
        ik = _intent_key(user_key, self.service_name, path)
        intent = self.r.get(ik)
        if intent is None:
            return

        k = _pf_key(user_key, self.service_name, path)
        if self.r.get(k) is not None:
            prefetch_hits.labels(service=self.service_name).inc()
            self.r.delete(k)
        else:
            prefetch_misses.labels(service=self.service_name).inc()

        # clear intent after accounting
        self.r.delete(ik)

    # =========================
    # ASGI entry
    # =========================
    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "") or ""
        headers = dict(scope.get("headers") or [])

        def h(name: str) -> Optional[str]:
            v = headers.get(name.lower().encode())
            return v.decode() if v else None

        user_key = h("x-user") or "anon"
        is_prefetch = (h("x-prefetch") == "1")

        start = time.perf_counter()
        status_code: Optional[int] = None

        # skip prefetch requests + noise
        if is_prefetch or path.startswith(SKIP_PREFIXES):
            await self.app(scope, receive, send)
            return

        # hit/miss check BEFORE request
        self._check_prefetch_hit(user_key, path)

        prev_svc, prev_path = self._get_prev_and_set_current(user_key, path)

        async def send_wrapper(message):
            nonlocal status_code

            if message["type"] == "http.response.start":
                status_code = int(message.get("status", 0) or 0)

            await send(message)

            if message["type"] == "http.response.body" and not message.get("more_body", False):
                latency_ms = int((time.perf_counter() - start) * 1000)
                sc = status_code or 0

                # ingest transition (async fire-and-forget after response)
                if prev_path and prev_path != path:
                    if prev_svc and prev_svc != self.service_name:
                        asyncio.create_task(
                            self.client.ingest_edge(
                                user_key=user_key,
                                src_service=prev_svc,
                                src_path=prev_path,
                                dst_service=self.service_name,
                                dst_path=path,
                                status=sc,
                                latency_ms=latency_ms,
                            )
                        )
                    else:
                        asyncio.create_task(
                            self.client.ingest(
                                user_key=user_key,
                                from_path=prev_path,
                                to_path=path,
                                status=sc,
                                latency_ms=latency_ms,
                            )
                        )

                # schedule prefetch
                if self.prefetch_enabled:
                    asyncio.create_task(self._prefetch(user_key, path))

        await self.app(scope, receive, send_wrapper)

    # =========================
    # Prefetch orchestration
    # =========================
    async def _prefetch(self, user_key: str, path: str):
        dbg = self._dbg_enabled(user_key)
        if dbg:
            self._dbg("prefetch.start", user=user_key, src=path)

        policy_requests.labels(service=self.service_name).inc()
        t0 = time.perf_counter()
        try:
            pol = await self.client.get_policy(user_key, path, limit=3)
        except Exception as e:
            policy_errors.labels(service=self.service_name, reason=type(e).__name__).inc()
            if dbg:
                self._dbg("prefetch.policy.error", user=user_key, src=path, err=repr(e))
            return
        finally:
            policy_latency.labels(service=self.service_name).observe(time.perf_counter() - t0)

        next_paths: List[Dict[str, Any]] = pol.get("next_paths") or []
        max_prefetch = int(pol.get("max_prefetch") or 0)
        budget_ms = int(pol.get("max_prefetch_time_ms") or self.default_prefetch_budget_ms)

        if dbg:
            self._dbg(
                "prefetch.policy.ok",
                user=user_key,
                src=path,
                max_prefetch=max_prefetch,
                got=len(next_paths),
                top=next_paths[:DEBUG_MAX_ITEMS],
                budget_ms=budget_ms,
            )

        if max_prefetch <= 0 or not next_paths:
            if dbg:
                self._dbg("prefetch.stop.no_candidates", user=user_key, src=path)
            return

        # build deadline
        deadline = time.perf_counter() + (budget_ms / 1000.0)

        # EARLY cutoff: don't start if it's already too late
        remaining = deadline - time.perf_counter()
        if remaining < self.min_prefetch_window_sec:
            prefetch_deadline_skips.labels(service=self.service_name).inc()
            if dbg:
                self._dbg("prefetch.stop.too_late", user=user_key, src=path, budget_ms=budget_ms, remaining_ms=int(remaining * 1000))
            return

        # prefilter: skip templates requiring id/uuid if src doesn't have token
        src_has_int = bool(RE_LAST_INT.search(path))
        src_has_uuid = bool(RE_LAST_UUID.search(path))

        filtered: List[Dict[str, Any]] = []
        dropped: List[Dict[str, Any]] = []

        for it in next_paths:
            p_tpl = (it.get("path") or "")
            if "{id}" in p_tpl and not src_has_int:
                if dbg:
                    dropped.append({"why": "need_id", "item": it})
                continue
            if "{uuid}" in p_tpl and not src_has_uuid:
                if dbg:
                    dropped.append({"why": "need_uuid", "item": it})
                continue
            filtered.append(it)

        next_paths = filtered
        if dbg:
            self._dbg("prefetch.filter", user=user_key, src=path, kept=len(next_paths), dropped=dropped[:DEBUG_MAX_ITEMS])

        if not next_paths:
            if dbg:
                self._dbg("prefetch.stop.filtered_out", user=user_key, src=path)
            return

        # sort by score and cap by max_prefetch
        next_paths = sorted(next_paths, key=lambda x: x.get("score", 0.0), reverse=True)[:max_prefetch]

        # mark intent ONLY if we will actually try to prefetch something from this src
        # this is what enables correct hit/miss accounting
        http = await self._get_http()
        tasks = [self._prefetch_one(http, user_key, path, item, deadline, dbg=dbg) for item in next_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        if dbg:
            self._dbg("prefetch.done", user=user_key, src=path, results=results)

        # optional: if we got a lot of deadline skips, treat as budget overrun symptom
        # (don't overcount: budget_overrun is intended as "we didn't make it in time")
        if any(isinstance(r, dict) and r.get("reason", "").startswith("Deadline") for r in results):
            prefetch_budget_overrun.labels(service=self.service_name).inc()

    # =========================
    # Prefetch single target
    # =========================
    async def _prefetch_one(
        self,
        http: httpx.AsyncClient,
        user_key: str,
        src_path: str,
        item: Dict[str, Any],
        deadline: float,
        dbg: bool = False,
    ) -> Dict[str, Any]:

        def ret(ok: bool, reason: str, **extra):
            return {"ok": ok, "reason": reason, **extra}

        if time.perf_counter() >= deadline:
            prefetch_deadline_skips.labels(service=self.service_name).inc()
            if dbg:
                self._dbg("prefetch.one.skip.deadline.enter", user=user_key, src=src_path, item=item)
            return ret(False, "DeadlineEnter")

        svc = item.get("service")
        p_tpl = item.get("path")

        if not svc or not p_tpl:
            if dbg:
                self._dbg("prefetch.one.skip.bad_item", user=user_key, src=src_path, item=item)
            return ret(False, "BadItem", item=item)

        p = p_tpl

        # template -> concrete
        if "{id}" in p:
            m = RE_LAST_INT.search(src_path)
            if not m:
                prefetch_errors.labels(service=self.service_name, reason="NoIdInSrcPath").inc()
                if dbg:
                    self._dbg("prefetch.one.skip.no_id", user=user_key, src=src_path, dst_tpl=p_tpl)
                return ret(False, "NoIdInSrcPath", dst_tpl=p_tpl)
            p = p.replace("{id}", m.group(1))

        if "{uuid}" in p:
            m = RE_LAST_UUID.search(src_path)
            if not m:
                prefetch_errors.labels(service=self.service_name, reason="NoUuidInSrcPath").inc()
                if dbg:
                    self._dbg("prefetch.one.skip.no_uuid", user=user_key, src=src_path, dst_tpl=p_tpl)
                return ret(False, "NoUuidInSrcPath", dst_tpl=p_tpl)
            p = p.replace("{uuid}", m.group(1))

        base = self.base_urls.get(svc)
        if not base:
            prefetch_errors.labels(service=self.service_name, reason="NoBaseUrl").inc()
            if dbg:
                self._dbg("prefetch.one.skip.no_base", user=user_key, src=src_path, dst_svc=svc, dst=p, dst_tpl=p_tpl, base_urls=self.base_urls)
            return ret(False, "NoBaseUrl", dst_svc=svc, dst=p, dst_tpl=p_tpl)

        url = f"{base}{p}"

        # dedup inflight
        key = (user_key, svc, p)
        now = time.time()

        async with self._lock:
            # cleanup old inflight
            for k, ts in list(self._inflight.items()):
                if (now - ts) > self.inflight_ttl_sec:
                    self._inflight.pop(k, None)

            if key in self._inflight:
                prefetch_dedup_skips.labels(service=self.service_name).inc()
                if dbg:
                    self._dbg("prefetch.one.skip.dedup", user=user_key, src=src_path, dst_svc=svc, dst=p, url=url)
                return ret(False, "Dedup", dst_svc=svc, dst=p, url=url)

            self._inflight[key] = now

        try:
            if time.perf_counter() >= deadline:
                prefetch_deadline_skips.labels(service=self.service_name).inc()
                if dbg:
                    self._dbg("prefetch.one.skip.deadline.before_sem", user=user_key, src=src_path, dst_svc=svc, dst=p, url=url)
                return ret(False, "DeadlineBeforeSem", dst_svc=svc, dst=p, url=url)

            if dbg:
                self._dbg("prefetch.one.try", user=user_key, src=src_path, dst_svc=svc, dst=p, dst_tpl=p_tpl, url=url)

            async with self.prefetch_sem:
                if time.perf_counter() >= deadline:
                    prefetch_deadline_skips.labels(service=self.service_name).inc()
                    if dbg:
                        self._dbg("prefetch.one.skip.deadline.inside_sem", user=user_key, src=src_path, dst_svc=svc, dst=p, url=url)
                    return ret(False, "DeadlineInsideSem", dst_svc=svc, dst=p, url=url)

                prefetch_total.labels(service=self.service_name).inc()
                
            
                t0 = time.perf_counter()
                status: int = 0
                err_name: Optional[str] = None

                try:
                    resp = await http.get(url, headers={"x-user": user_key, "x-prefetch": "1"})
                    status = int(resp.status_code or 0)
                except Exception as e:
                    err_name = type(e).__name__
                    prefetch_errors.labels(service=self.service_name, reason=err_name).inc()
                    if dbg:
                        self._dbg("prefetch.one.err", user=user_key, src=src_path, dst_svc=svc, dst=p, url=url, err=repr(e))
                finally:
                    dur = time.perf_counter() - t0
                    prefetch_latency.labels(service=self.service_name).observe(dur)

                ms = int(dur * 1000)

                if status > 0:
                    # mark: so when real request comes we can count hit
                    self.r.setex(_pf_key(user_key, svc, p), self.prefetch_mark_ttl_sec, "1")
                    self.r.setex(_intent_key(user_key, svc, p), self.intent_ttl_sec, "1")

                    # ingest attempt (optional)
                    try:
                        await self.client.ingest_prefetch(
                            user_key=user_key,
                            src_path=src_path,
                            dst_service=svc,
                            dst_path=p,
                            status=status,
                            latency_ms=ms,
                        )
                    except Exception as e:
                        if dbg:
                            self._dbg("prefetch.one.ingest_prefetch.err", user=user_key, src=src_path, dst_svc=svc, dst=p, err=repr(e))

                    if dbg:
                        self._dbg("prefetch.one.ok", user=user_key, src=src_path, dst_svc=svc, dst=p, url=url, status=status, ms=ms)

                    return ret(True, "OK", dst_svc=svc, dst=p, url=url, status=status, ms=ms)

                return ret(False, err_name or "NoStatus", dst_svc=svc, dst=p, url=url, status=0, ms=ms)

        finally:
            async with self._lock:
                self._inflight.pop(key, None)

    async def close(self):
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        await self.client.close()
