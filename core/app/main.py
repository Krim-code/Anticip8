import os
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel, Field
import redis
import re
from fastapi import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Anticip8 Core")



RE_UUID = re.compile(r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}(?=/|$)")
RE_INT  = re.compile(r"/\d+(?=/|$)")
RE_LAST_INT = re.compile(r"/(\d+)(?:/|$)")


def norm_path(p: str) -> str:
    
    if not p:
        return p
    # убираем хвостовые /
    if p.startswith(("/docs", "/openapi.json", "/redoc", "/metrics", "/_whoami", "/health")):
        return p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    # uuid -> {uuid}
    p = RE_UUID.sub("/{uuid}", p)
    # int -> {id}
    p = RE_INT.sub("/{id}", p)
    return p



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


def _k_trans(service: str, from_path: str) -> str:
    return f"trans:{service}:{from_path}"


def _k_total(service: str) -> str:
    return f"tot:{service}"

def _k_trans_any(src_service: str, from_path: str) -> str:
    return f"trans2:{src_service}:{from_path}"  # hash: "<dst_service>|<dst_path>" -> count

def _pack(dst_service: str, dst_path: str) -> str:
    return f"{dst_service}|{dst_path}"

def _unpack(v: str):
    svc, p = v.split("|", 1)
    return svc, p

@app.post("/ingest/event")
def ingest_event(ev: Event):
    f = norm_path(ev.from_path)
    
    if f.startswith(("/docs", "/openapi.json", "/redoc", "/metrics", "/_whoami", "/health")):
        return {"ok": True, "skipped": True}
                
    t = norm_path(ev.to_path)

    r.hincrby(_k_trans(ev.service, f), t, 1)
    r.hincrby(_k_total(ev.service), f, 1)
    return {"ok": True}


@app.post("/ingest/prefetch")
def ingest_prefetch(edge: PrefetchEdge):
    src = norm_path(edge.src_path)
    dst = norm_path(edge.dst_path)

    r.hincrby(_k_trans_any(edge.src_service, src), _pack(edge.dst_service, dst), 1)
    r.hincrby(_k_total(edge.src_service), src, 1)
    return {"ok": True}




@app.get("/policy/next", response_model=PolicyResp)
def policy_next(service: str, path: str, user_key: str = "anon", limit: int = 3):
    total = r.hget(_k_total(service), path)
    total_i = int(total) if total else 0

    items = []

    if total_i > 0:
        # 1) intra-service (старые события)
        trans = r.hgetall(_k_trans(service, path))
        for to_path, cnt in (trans or {}).items():
            c = int(cnt)
            items.append((service, to_path, c / total_i))

        # 2) cross-service (prefetch edges)
        trans2 = r.hgetall(_k_trans_any(service, path))
        for packed, cnt in (trans2 or {}).items():
            dst_svc, dst_path = _unpack(packed)
            c = int(cnt)
            items.append((dst_svc, dst_path, c / total_i))

    items.sort(key=lambda x: x[2], reverse=True)
    top = items[: max(0, limit)]

    return PolicyResp(
        next_paths=[NextPath(service=s, path=p, score=sc) for s, p, sc in top],
        max_prefetch=2,
        max_prefetch_time_ms=80,
    )



@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)