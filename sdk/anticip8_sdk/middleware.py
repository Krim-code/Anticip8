# middleware.py
import os
import re
import time
import json
import asyncio
import random
from dataclasses import dataclass
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
    prefetch_mark_not_ready, intent_seen, prefetch_mark_ready, intent_missing
)

import logging
LOG = logging.getLogger("uvicorn.error")


# =========================
# Debug / logging toggles
# =========================
DEBUG_PREFETCH = os.getenv("ANTICIP8_DEBUG_PREFETCH", "0") == "1"
DEBUG_SAMPLE = float(os.getenv("ANTICIP8_DEBUG_PREFETCH_SAMPLE", "0.05"))
DEBUG_MAX_ITEMS = int(os.getenv("ANTICIP8_DEBUG_PREFETCH_MAX_ITEMS", "5"))

LOG_REAL = os.getenv("ANTICIP8_LOG_REAL", "0") == "1"
LOG_PREFETCH = os.getenv("ANTICIP8_LOG_PREFETCH", "0") == "1"
LOG_HITMISS = os.getenv("ANTICIP8_LOG_HITMISS", "0") == "1"
LOG_ONLY_USER = os.getenv("ANTICIP8_LOG_ONLY_USER", "").strip()  # e.g. "u254"
LOG_SAMPLE = float(os.getenv("ANTICIP8_LOG_SAMPLE", "1.0"))  # 0..1


def _log_ok(user_key: str) -> bool:
    if LOG_ONLY_USER and user_key != LOG_ONLY_USER:
        return False
    if LOG_SAMPLE >= 1.0:
        return True
    return random.random() <= LOG_SAMPLE


def _jlog(tag: str, payload: dict):
    payload = {"ts": int(time.time()), "tag": tag, **payload}
    LOG.info("%s %s", tag, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


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
    "/docs", "/openapi.json", "/metrics", "/_whoami", "/favicon.ico", "/health", "/redoc",
)

# =========================
# Redis keys
# =========================
def _sess_key(user_key: str) -> str:
    return f"anticip8:sess:{user_key}"

def _pf_key(user_key: str, svc: str, req_key: str) -> str:
    return f"anticip8:pf:{user_key}:{svc}:{req_key}"

def _intent_key(user_key: str, svc: str, req_key: str) -> str:
    return f"anticip8:intent:{user_key}:{svc}:{req_key}"


# =========================
# Query / request-key normalization
# =========================
QUERY_MODE = os.getenv("ANTICIP8_QUERY_MODE", "ignore").strip().lower()
QUERY_ALLOWLIST = [
    s.strip()
    for s in os.getenv("ANTICIP8_QUERY_ALLOWLIST", "status,category,q").split(",")
    if s.strip()
]


def _parse_query(qs: bytes) -> List[Tuple[str, str]]:
    if not qs:
        return []
    out: List[Tuple[str, str]] = []
    try:
        s = qs.decode("utf-8", errors="ignore")
    except Exception:
        return out
    for part in s.split("&"):
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
        else:
            k, v = part, ""
        out.append((k, v))
    return out


def _make_req_key(path: str, query_string: bytes) -> str:
    if QUERY_MODE == "ignore":
        return path

    pairs = _parse_query(query_string)

    if QUERY_MODE == "full":
        pairs.sort(key=lambda x: (x[0], x[1]))
        if not pairs:
            return path
        qs = "&".join([f"{k}={v}" if v != "" else k for k, v in pairs])
        return f"{path}?{qs}"

    # stable
    allow = set(QUERY_ALLOWLIST)
    filtered = [(k, v) for (k, v) in pairs if k in allow]
    filtered.sort(key=lambda x: (x[0], x[1]))
    if not filtered:
        return path
    qs = "&".join([f"{k}={v}" if v != "" else k for k, v in filtered])
    return f"{path}?{qs}"


# =========================
# Policy cache
# =========================
@dataclass
class _PolicyCacheItem:
    exp: float
    value: Dict[str, Any]


class _TTLCache:
    def __init__(self, maxsize: int = 2048):
        self.maxsize = maxsize
        self._d: Dict[str, _PolicyCacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if it.exp <= now:
                self._d.pop(key, None)
                return None
            return it.value

    async def set(self, key: str, value: Dict[str, Any], ttl_sec: float):
        now = time.time()
        exp = now + ttl_sec
        async with self._lock:
            if len(self._d) >= self.maxsize:
                for k in list(self._d.keys())[: max(1, self.maxsize // 20)]:
                    self._d.pop(k, None)
            self._d[key] = _PolicyCacheItem(exp=exp, value=value)


# =========================
# Circuit breaker
# =========================
class _Breaker:
    def __init__(self, trip_errors: int, window_sec: float, cooloff_sec: float):
        self.trip_errors = trip_errors
        self.window_sec = window_sec
        self.cooloff_sec = cooloff_sec
        self._errs: List[float] = []
        self._cooloff_until: float = 0.0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            return time.time() >= self._cooloff_until

    async def report_error(self):
        now = time.time()
        async with self._lock:
            cutoff = now - self.window_sec
            self._errs = [t for t in self._errs if t >= cutoff]
            self._errs.append(now)
            if len(self._errs) >= self.trip_errors:
                self._cooloff_until = now + self.cooloff_sec
                self._errs.clear()


# =========================
# Middleware
# =========================
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

        self.base_urls = {k: v.rstrip("/") for k, v in (base_urls or {}).items()}

        rurl = redis_url or os.getenv("REDIS_URL") or "redis://redis:6379/0"
        self.r = redis.Redis.from_url(rurl, decode_responses=True)

        # Prefer env overrides if present (so you don't forget to pass in add_middleware)
        self.session_ttl_sec = int(os.getenv("ANTICIP8_SESSION_TTL_SEC", str(session_ttl_sec)))
        self.prefetch_mark_ttl_sec = int(os.getenv("ANTICIP8_PREFETCH_MARK_TTL_SEC", str(prefetch_mark_ttl_sec)))
        self.intent_ttl_sec = int(os.getenv("ANTICIP8_INTENT_TTL_SEC", str(intent_ttl_sec)))

        self.inflight_ttl_sec = float(os.getenv("ANTICIP8_INFLIGHT_TTL_SEC", str(inflight_ttl_sec)))
        self.default_prefetch_budget_ms = int(os.getenv("ANTICIP8_PREFETCH_BUDGET_MS", str(default_prefetch_budget_ms)))

        self.prefetch_sem = asyncio.Semaphore(int(os.getenv("ANTICIP8_PREFETCH_MAX_CONCURRENCY", str(max_prefetch_concurrency))))

        self.max_batch_inflight = int(os.getenv("ANTICIP8_PREFETCH_MAX_BATCH_INFLIGHT", "2"))
        self.min_prefetch_window_sec = float(os.getenv("ANTICIP8_MIN_PREFETCH_WINDOW_SEC", "0.08"))

        self.max_items_cap = int(os.getenv("ANTICIP8_PREFETCH_MAX_ITEMS", "0"))  # 0 = disabled
        deny_raw = os.getenv("ANTICIP8_PREFETCH_DENY_REGEX", "").strip()
        self.deny_re = re.compile(deny_raw) if deny_raw else None

        self.policy_cache_ttl_sec = float(os.getenv("ANTICIP8_POLICY_CACHE_TTL_SEC", "0.8"))
        self.policy_cache = _TTLCache(maxsize=int(os.getenv("ANTICIP8_POLICY_CACHE_MAX", "4096")))

        self.breaker = _Breaker(
            trip_errors=int(os.getenv("ANTICIP8_BREAKER_TRIP_ERRORS", "25")),
            window_sec=float(os.getenv("ANTICIP8_BREAKER_WINDOW_SEC", "5")),
            cooloff_sec=float(os.getenv("ANTICIP8_BREAKER_COOLOFF_SEC", "3")),
        )

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

        # show resolved config once (helps you catch the "ttl=60 but env says 180" bullshit)
        if LOG_PREFETCH:
            _jlog("ANTICIP8_CFG", {
                "svc": self.service_name,
                "QUERY_MODE": QUERY_MODE,
                "intent_ttl": self.intent_ttl_sec,
                "pf_ttl": self.prefetch_mark_ttl_sec,
                "batch_inflight": self.max_batch_inflight,
                "min_window": self.min_prefetch_window_sec,
                "sem": int(getattr(self.prefetch_sem, "_value", 0)),
            })

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout, limits=self.limits)
        return self._http

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

    # -------------------------
    # Hit/miss accounting
    # -------------------------
    def _probe_intent_pf(self, user_key: str, req_key: str) -> Tuple[bool, bool]:
        ik = _intent_key(user_key, self.service_name, req_key)
        pk = _pf_key(user_key, self.service_name, req_key)
        return (self.r.get(ik) is not None, self.r.get(pk) is not None)

    def _check_prefetch_hit(self, user_key: str, req_key: str):
        ik = _intent_key(user_key, self.service_name, req_key)
        intent = self.r.get(ik)
        if intent is None:
            if LOG_HITMISS and _log_ok(user_key):
                _jlog("HITMISS", {
                    "svc": self.service_name,
                    "user": user_key,
                    "req_key": req_key,
                    "result": "NO_INTENT",
                })
            intent_seen.labels(service=self.service_name).inc()
        else:
            intent_missing.labels(service=self.service_name).inc()


        k = _pf_key(user_key, self.service_name, req_key)
        has_pf = self.r.get(k) is not None

        if has_pf:
            prefetch_hits.labels(service=self.service_name).inc()
            prefetch_mark_ready.labels(service=self.service_name).inc()
            self.r.delete(k)
            res = "HIT"
        else:
            prefetch_mark_not_ready.labels(service=self.service_name).inc()
            prefetch_misses.labels(service=self.service_name).inc()
            res = "MISS"

        self.r.delete(ik)

        if LOG_HITMISS and _log_ok(user_key):
            _jlog("HITMISS", {
                "svc": self.service_name,
                "user": user_key,
                "req_key": req_key,
                "result": res,
            })

    # -------------------------
    # ASGI entry
    # -------------------------
    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "") or ""
        qs = scope.get("query_string", b"") or b""
        headers = dict(scope.get("headers") or [])

        def h(name: str) -> Optional[str]:
            v = headers.get(name.lower().encode())
            return v.decode() if v else None

        user_key = h("x-user") or "anon"
        is_prefetch = (h("x-prefetch") == "1")

        start = time.perf_counter()
        status_code: Optional[int] = None
        prefetch_scheduled = False

        if is_prefetch or path.startswith(SKIP_PREFIXES):
            await self.app(scope, receive, send)
            return

        req_key = _make_req_key(path, qs)

        if LOG_REAL and _log_ok(user_key):
            _jlog("REAL_REQ", {
                "svc": self.service_name,
                "user": user_key,
                "path": path,
                "qs": qs.decode(errors="ignore"),
                "req_key": req_key,
                "ua": h("user-agent"),
            })

        # IMPORTANT: probe BEFORE deletion
        if LOG_HITMISS and _log_ok(user_key):
            intent, pf = self._probe_intent_pf(user_key, req_key)
            _jlog("HITMISS_PROBE", {
                "svc": self.service_name,
                "user": user_key,
                "req_key": req_key,
                "intent": intent,
                "pf_mark": pf,
            })

        # hit/miss check BEFORE request (will delete keys)
        self._check_prefetch_hit(user_key, req_key)

        if LOG_HITMISS and _log_ok(user_key):
            intent2, pf2 = self._probe_intent_pf(user_key, req_key)
            _jlog("HITMISS_AFTER", {
                "svc": self.service_name,
                "user": user_key,
                "req_key": req_key,
                "intent": intent2,
                "pf_mark": pf2,
            })

        prev_svc, prev_path = self._get_prev_and_set_current(user_key, path)

        async def send_wrapper(message):
            nonlocal status_code, prefetch_scheduled

            if message["type"] == "http.response.start":
                status_code = int(message.get("status", 0) or 0)

                if self.prefetch_enabled and (status_code < 400) and (not prefetch_scheduled):
                    prefetch_scheduled = True
                    asyncio.create_task(self._prefetch(user_key, path, qs))

            await send(message)

            if message["type"] == "http.response.body" and not message.get("more_body", False):
                latency_ms = int((time.perf_counter() - start) * 1000)
                sc = status_code or 0

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

        await self.app(scope, receive, send_wrapper)

    # -------------------------
    # Prefetch orchestration
    # -------------------------
    async def _prefetch(self, user_key: str, src_path: str, src_qs: bytes):
        if not await self.breaker.allow():
            if LOG_PREFETCH and _log_ok(user_key):
                _jlog("PREFETCH_STOP_BREAKER", {"svc": self.service_name, "user": user_key, "src": src_path})
            return

        pol_cache_key = f"{self.service_name}:{src_path}"

        policy_requests.labels(service=self.service_name).inc()
        t0 = time.perf_counter()
        pol: Optional[Dict[str, Any]] = None
        try:
            if self.policy_cache_ttl_sec > 0:
                pol = await self.policy_cache.get(pol_cache_key)

            if pol is None:
                pol = await self.client.get_policy(user_key, src_path, limit=3)
                if self.policy_cache_ttl_sec > 0:
                    await self.policy_cache.set(pol_cache_key, pol, ttl_sec=self.policy_cache_ttl_sec)

        except Exception as e:
            policy_errors.labels(service=self.service_name, reason=type(e).__name__).inc()
            if LOG_PREFETCH and _log_ok(user_key):
                _jlog("PREFETCH_POLICY_ERR", {"svc": self.service_name, "user": user_key, "src": src_path, "err": repr(e)})
            return
        finally:
            policy_latency.labels(service=self.service_name).observe(time.perf_counter() - t0)

        next_paths: List[Dict[str, Any]] = (pol or {}).get("next_paths") or []
        max_prefetch = int((pol or {}).get("max_prefetch") or 0)
        budget_ms = int((pol or {}).get("max_prefetch_time_ms") or self.default_prefetch_budget_ms)

        if max_prefetch <= 0 or not next_paths:
            return

        if self.max_items_cap > 0:
            max_prefetch = min(max_prefetch, self.max_items_cap)

        deadline = time.perf_counter() + (budget_ms / 1000.0)
        if (deadline - time.perf_counter()) < self.min_prefetch_window_sec:
            prefetch_deadline_skips.labels(service=self.service_name).inc()
            return

        src_has_int = bool(RE_LAST_INT.search(src_path))
        src_has_uuid = bool(RE_LAST_UUID.search(src_path))

        filtered: List[Dict[str, Any]] = []
        for it in next_paths:
            p_tpl = (it.get("path") or "")
            if self.deny_re and self.deny_re.search(p_tpl):
                continue
            if "{id}" in p_tpl and not src_has_int:
                continue
            if "{uuid}" in p_tpl and not src_has_uuid:
                continue
            filtered.append(it)

        if not filtered:
            return

        targets = sorted(filtered, key=lambda x: x.get("score", 0.0), reverse=True)[:max_prefetch]

        per_req_timeout = float(os.getenv("ANTICIP8_PREFETCH_PER_REQUEST_TIMEOUT_SEC", "0"))
        if per_req_timeout <= 0:
            per_req_timeout = min(0.22, max(0.09, (budget_ms / 1000.0) / max(1, max_prefetch)))

        if LOG_PREFETCH and _log_ok(user_key):
            _jlog("PREFETCH_BATCH", {
                "svc": self.service_name,
                "user": user_key,
                "src_path": src_path,
                "src_qs": src_qs.decode(errors="ignore"),
                "max_prefetch": max_prefetch,
                "budget_ms": budget_ms,
                "timeout": per_req_timeout,
                "targets": targets[:8],
            })

        http = await self._get_http()

        results: List[Any] = []
        pending: set[asyncio.Task] = set()

        for item in targets:
            if (deadline - time.perf_counter()) < self.min_prefetch_window_sec:
                prefetch_deadline_skips.labels(service=self.service_name).inc()
                break

            t = asyncio.create_task(
                self._prefetch_one(
                    http=http,
                    user_key=user_key,
                    src_path=src_path,
                    src_qs=src_qs,
                    item=item,
                    deadline=deadline,
                    per_request_timeout=per_req_timeout,
                )
            )
            pending.add(t)

            if len(pending) >= max(1, self.max_batch_inflight):
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    if not d.cancelled():
                        results.append(d.result())

        if pending:
            timeout = max(0.0, deadline - time.perf_counter())
            done, not_done = await asyncio.wait(pending, timeout=timeout)
            for d in done:
                if not d.cancelled():
                    results.append(d.result())

            if not_done:
                for t in not_done:
                    t.cancel()
                prefetch_budget_overrun.labels(service=self.service_name).inc()
                prefetch_deadline_skips.labels(service=self.service_name).inc(len(not_done))  # type: ignore

    # -------------------------
    # Prefetch single target
    # -------------------------
    async def _prefetch_one(
        self,
        http: httpx.AsyncClient,
        user_key: str,
        src_path: str,
        src_qs: bytes,
        item: Dict[str, Any],
        deadline: float,
        per_request_timeout: float,
    ) -> Dict[str, Any]:

        def ret(ok: bool, reason: str, **extra):
            return {"ok": ok, "reason": reason, **extra}

        if time.perf_counter() >= deadline:
            prefetch_deadline_skips.labels(service=self.service_name).inc()
            return ret(False, "DeadlineEnter")

        svc = item.get("service")
        p_tpl = item.get("path")
        if not svc or not p_tpl:
            return ret(False, "BadItem", item=item)

        if self.deny_re and self.deny_re.search(p_tpl):
            return ret(False, "DeniedByRegexTpl", dst_svc=svc, dst_tpl=p_tpl)

        p = p_tpl

        if "{id}" in p:
            m = RE_LAST_INT.search(src_path)
            if not m:
                prefetch_errors.labels(service=self.service_name, reason="NoIdInSrcPath").inc()
                return ret(False, "NoIdInSrcPath", dst_tpl=p_tpl)
            p = p.replace("{id}", m.group(1))

        if "{uuid}" in p:
            m = RE_LAST_UUID.search(src_path)
            if not m:
                prefetch_errors.labels(service=self.service_name, reason="NoUuidInSrcPath").inc()
                return ret(False, "NoUuidInSrcPath", dst_tpl=p_tpl)
            p = p.replace("{uuid}", m.group(1))

        if self.deny_re and self.deny_re.search(p):
            return ret(False, "DeniedByRegex", dst_svc=svc, dst=p, dst_tpl=p_tpl)

        base = self.base_urls.get(svc)
        if not base:
            prefetch_errors.labels(service=self.service_name, reason="NoBaseUrl").inc()
            return ret(False, "NoBaseUrl", dst_svc=svc, dst=p, dst_tpl=p_tpl)

        url = f"{base}{p}"

        # IMPORTANT: outgoing prefetch has no query => always path-only req_key
        dst_req_key = p

        if LOG_PREFETCH and _log_ok(user_key):
            _jlog("PREFETCH_REQ", {
                "src_svc": self.service_name,
                "user": user_key,
                "src_path": src_path,
                "src_qs": src_qs.decode(errors="ignore"),
                "dst_svc": svc,
                "dst_path": p,
                "dst_req_key": dst_req_key,
                "url": url,
                "timeout": per_request_timeout,
            })

        # Mark intent before attempt
        try:
            self.r.setex(_intent_key(user_key, svc, dst_req_key), self.intent_ttl_sec, "1")
            if LOG_PREFETCH and _log_ok(user_key):
                _jlog("PREFETCH_INTENT_SET", {
                    "user": user_key, "svc": svc, "dst_req_key": dst_req_key, "dst_path": p, "ttl": self.intent_ttl_sec
                })
        except Exception as e:
            if LOG_PREFETCH and _log_ok(user_key):
                _jlog("PREFETCH_INTENT_ERR", {"user": user_key, "svc": svc, "dst_req_key": dst_req_key, "err": repr(e)})
            # intent failing doesn't forbid doing prefetch attempt
            pass

        key = (user_key, svc, dst_req_key)
        now = time.time()

        async with self._lock:
            for k, ts in list(self._inflight.items()):
                if (now - ts) > self.inflight_ttl_sec:
                    self._inflight.pop(k, None)

            if key in self._inflight:
                prefetch_dedup_skips.labels(service=self.service_name).inc()
                return ret(False, "Dedup", dst_svc=svc, dst=p, url=url)

            self._inflight[key] = now

        try:
            if time.perf_counter() >= deadline:
                prefetch_deadline_skips.labels(service=self.service_name).inc()
                return ret(False, "DeadlineBeforeSem", dst_svc=svc, dst=p, url=url)

            async with self.prefetch_sem:
                if time.perf_counter() >= deadline:
                    prefetch_deadline_skips.labels(service=self.service_name).inc()
                    return ret(False, "DeadlineInsideSem", dst_svc=svc, dst=p, url=url)

                if (deadline - time.perf_counter()) < self.min_prefetch_window_sec:
                    prefetch_deadline_skips.labels(service=self.service_name).inc()
                    return ret(False, "DeadlineTooClose", dst_svc=svc, dst=p, url=url)

                prefetch_total.labels(service=self.service_name).inc()

                t0 = time.perf_counter()
                status: int = 0
                err_name: Optional[str] = None

                try:
                    resp = await http.get(
                        url,
                        headers={"x-user": user_key, "x-prefetch": "1"},
                        timeout=per_request_timeout,
                    )
                    status = int(resp.status_code or 0)

                except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as e:
                    err_name = type(e).__name__
                    prefetch_errors.labels(service=self.service_name, reason=err_name).inc()
                    await self.breaker.report_error()

                except Exception as e:
                    err_name = type(e).__name__
                    prefetch_errors.labels(service=self.service_name, reason=err_name).inc()
                    await self.breaker.report_error()

                finally:
                    dur = time.perf_counter() - t0
                    prefetch_latency.labels(service=self.service_name).observe(dur)

                ms = int((time.perf_counter() - t0) * 1000)

                if LOG_PREFETCH and _log_ok(user_key):
                    _jlog("PREFETCH_RES", {
                        "src_svc": self.service_name,
                        "user": user_key,
                        "dst_svc": svc,
                        "dst_path": p,
                        "dst_req_key": dst_req_key,
                        "status": status,
                        "ms": ms,
                        "err": err_name,
                    })

                if status > 0:
                    try:
                        self.r.setex(_pf_key(user_key, svc, dst_req_key), self.prefetch_mark_ttl_sec, "1")
                        if LOG_PREFETCH and _log_ok(user_key):
                            _jlog("PREFETCH_MARK_SET", {
                                "user": user_key, "svc": svc, "dst_req_key": dst_req_key,
                                "dst_path": p, "ttl": self.prefetch_mark_ttl_sec
                            })
                    except Exception as e:
                        if LOG_PREFETCH and _log_ok(user_key):
                            _jlog("PREFETCH_MARK_ERR", {"user": user_key, "svc": svc, "dst_req_key": dst_req_key, "err": repr(e)})

                    # optional ingest
                    try:
                        await self.client.ingest_prefetch(
                            user_key=user_key,
                            src_path=src_path,
                            dst_service=svc,
                            dst_path=p,
                            status=status,
                            latency_ms=ms,
                        )
                    except Exception:
                        pass

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
