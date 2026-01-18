import os
import re
from typing import List, Tuple

import redis
from fastapi import FastAPI, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Anticip8 Core")

# --- path normalization ---
RE_UUID = re.compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}(?=/|$)"
)
RE_INT = re.compile(r"/\d+(?=/|$)")

NOISE_PREFIXES = ("/docs", "/openapi.json", "/redoc", "/metrics", "/_whoami", "/health")


def norm_path(p: str) -> str:
    if not p:
        return p
    # fast skip for noisy endpoints
    if p.startswith(NOISE_PREFIXES):
        return p
    # trim trailing slash
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    # uuid -> {uuid}
    p = RE_UUID.sub("/{uuid}", p)
    # int -> {id}
    p = RE_INT.sub("/{id}", p)
    return p


# --- schemas ---
class Event(BaseModel):
    service: str
    user_key: str = Field(default="anon")
    from_path: str
    to_path: str
    status: int = 200
    latency_ms: int = 0


class PrefetchEdge(BaseModel):
    src_service: str
    user_key: str = Field(default="anon")
    src_path: str
    dst_service: str
    dst_path: str
    status: int = 200
    latency_ms: int = 0


class NextPath(BaseModel):
    service: str
    path: str
    score: float


class PolicyResp(BaseModel):
    next_paths: List[NextPath]
    max_prefetch: int = 2
    max_prefetch_time_ms: int = 80


# --- redis keys ---
def _k_trans(service: str, from_path: str) -> str:
    # intra-service transitions: to_path -> count
    return f"trans:{service}:{from_path}"


def _k_total(service: str) -> str:
    # intra-service totals: from_path -> total_outgoing_count
    return f"tot:{service}"


def _k_trans_any(src_service: str, from_path: str) -> str:
    # cross-service transitions: "<dst_service>|<dst_path>" -> count
    return f"trans2:{src_service}:{from_path}"


def _k_total_any(service: str) -> str:
    # cross-service totals: from_path -> total_outgoing_count (for trans2)
    return f"tot2:{service}"


def _pack(dst_service: str, dst_path: str) -> str:
    return f"{dst_service}|{dst_path}"


def _unpack(v: str) -> Tuple[str, str]:
    svc, p = v.split("|", 1)
    return svc, p


# --- ingest endpoints ---
@app.post("/ingest/event")
def ingest_event(ev: Event):
    f = norm_path(ev.from_path)
    if f.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    t = norm_path(ev.to_path)
    if t.startswith(NOISE_PREFIXES):
        # можно считать переход на шум как "неинтересный"
        return {"ok": True, "skipped": True}

    r.hincrby(_k_trans(ev.service, f), t, 1)
    r.hincrby(_k_total(ev.service), f, 1)
    return {"ok": True}


@app.post("/ingest/prefetch")
def ingest_prefetch(edge: PrefetchEdge):
    src = norm_path(edge.src_path)
    if src.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    dst = norm_path(edge.dst_path)
    if dst.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    r.hincrby(_k_trans_any(edge.src_service, src), _pack(edge.dst_service, dst), 1)
    r.hincrby(_k_total_any(edge.src_service), src, 1)
    return {"ok": True}


# --- policy ---
@app.get("/policy/next", response_model=PolicyResp)
def policy_next(service: str, path: str, user_key: str = "anon", limit: int = 3):
    p = norm_path(path)

    # noise guard
    if p.startswith(NOISE_PREFIXES):
        return PolicyResp(next_paths=[], max_prefetch=0, max_prefetch_time_ms=0)

    items: List[Tuple[str, str, float]] = []

    # 1) intra-service P(to|from) = count(from->to)/total(from)
    total1_raw = r.hget(_k_total(service), p)
    total1 = int(total1_raw) if total1_raw else 0
    if total1 > 0:
        trans = r.hgetall(_k_trans(service, p)) or {}
        for to_path, cnt in trans.items():
            c = int(cnt)
            items.append((service, to_path, c / total1))

    # 2) cross-service P(dst|src) = count(src->dst)/total2(src)
    total2_raw = r.hget(_k_total_any(service), p)
    total2 = int(total2_raw) if total2_raw else 0
    if total2 > 0:
        trans2 = r.hgetall(_k_trans_any(service, p)) or {}
        for packed, cnt in trans2.items():
            dst_svc, dst_path = _unpack(packed)
            c = int(cnt)
            items.append((dst_svc, dst_path, c / total2))

    # rank
    items.sort(key=lambda x: x[2], reverse=True)
    top = items[: max(0, limit)]

    return PolicyResp(
        next_paths=[NextPath(service=s, path=pp, score=sc) for s, pp, sc in top],
        max_prefetch=2,
        max_prefetch_time_ms=80,
    )


# --- misc ---
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
