import os
import re
import json
from typing import List, Tuple, Dict

import redis
from fastapi import FastAPI, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# =========================
# Config
# =========================
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

NOISE_PREFIXES = ("/docs", "/openapi.json", "/redoc", "/metrics", "/_whoami", "/health")

# Item2Vec
I2V_PREFIX = os.getenv("I2V_PREFIX", "anticip8:i2v:topk:")
I2V_ALPHA = float(os.getenv("I2V_ALPHA", "0.7"))   # weight for embeddings
I2V_TOPK = int(os.getenv("I2V_TOPK", "30"))

# Policy behavior:
# If true -> allow "prefetch attempted" edges (trans2p) to contribute to policy (usually BAD unless you also have hit-confirmation)
ALLOW_PREFETCH_ATTEMPTS_IN_POLICY = os.getenv("ALLOW_PREFETCH_ATTEMPTS_IN_POLICY", "0") == "1"
PREFETCH_ATTEMPT_WEIGHT = float(os.getenv("PREFETCH_ATTEMPT_WEIGHT", "0.15"))  # only used if ALLOW_... == 1

DEFAULT_MAX_PREFETCH = int(os.getenv("MAX_PREFETCH", "2"))
DEFAULT_PREFETCH_BUDGET_MS = int(os.getenv("PREFETCH_BUDGET_MS", "120"))
I2V_ALPHA = float(os.getenv("I2V_ALPHA", "0.65"))          # i2v weight
MARKOV_SMOOTH = float(os.getenv("MARKOV_SMOOTH", "1.0"))   # Laplace smoothing strength (0 disables)
MIN_PROB = float(os.getenv("MIN_PROB", "0.001"))           # drop near-zero edges
DROP_SELF_LOOPS = os.getenv("DROP_SELF_LOOPS", "1") == "1"

ALLOW_PREFETCH_ATTEMPTS_IN_POLICY = os.getenv("ALLOW_PREFETCH_ATTEMPTS", "0") == "1"
PREFETCH_ATTEMPT_WEIGHT = float(os.getenv("PREFETCH_ATTEMPT_WEIGHT", "0.15"))

app = FastAPI(title="Anticip8 Core v2")

# =========================
# Path normalization
# =========================
RE_UUID = re.compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}(?=/|$)"
)
RE_INT = re.compile(r"/\d+(?=/|$)")

def norm_path(p: str) -> str:
    if not p:
        return p
    if p.startswith(NOISE_PREFIXES):
        return p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    p = RE_UUID.sub("/{uuid}", p)
    p = RE_INT.sub("/{id}", p)
    return p

# =========================
# Item2Vec helpers
# =========================
def _i2v_key(service: str, path: str) -> str:
    return f"{I2V_PREFIX}{service}::{path}"

def _parse_node(node: str) -> Tuple[str, str]:
    svc, p = node.split("::", 1)
    return svc, p

def get_i2v_candidates(service: str, path: str) -> List[Tuple[str, str, float]]:
    raw = r.get(_i2v_key(service, path))
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []

    out: List[Tuple[str, str, float]] = []
    for item in data[:I2V_TOPK]:
        n = item.get("item")
        cos = float(item.get("cos", 0.0))
        if not n:
            continue
        try:
            svc, p = _parse_node(n)
        except Exception:
            continue
        out.append((svc, p, cos))
    return out

# =========================
# Schemas
# =========================
class Event(BaseModel):
    service: str
    user_key: str = Field(default="anon")
    from_path: str
    to_path: str
    status: int = 200
    latency_ms: int = 0

class EdgeEvent(BaseModel):
    src_service: str
    user_key: str = Field(default="anon")
    src_path: str
    dst_service: str
    dst_path: str
    status: int = 200
    latency_ms: int = 0

class PrefetchAttempt(BaseModel):
    # this is NOT a real user transition, only "we tried to prefetch"
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

# =========================
# Redis keys
# =========================
def _k_trans(service: str, from_path: str) -> str:
    # intra-service transitions: to_path -> count
    return f"trans:{service}:{from_path}"

def _k_total(service: str) -> str:
    # totals for intra: from_path -> total_outgoing_count
    return f"tot:{service}"

def _pack(dst_service: str, dst_path: str) -> str:
    return f"{dst_service}|{dst_path}"

def _unpack(v: str) -> Tuple[str, str]:
    svc, p = v.split("|", 1)
    return svc, p

def _k_trans_any(src_service: str, from_path: str) -> str:
    # REAL cross-service transitions: "<dst_service>|<dst_path>" -> count
    return f"trans2:{src_service}:{from_path}"

def _k_total_any(service: str) -> str:
    # totals for REAL cross-service: from_path -> total_outgoing_count
    return f"tot2:{service}"

def _k_trans_prefetch(src_service: str, from_path: str) -> str:
    return f"ptrans:{src_service}:{from_path}"

def _k_total_prefetch(service: str) -> str:
    return f"ptot:{service}"

# =========================
# Ingest endpoints
# =========================
@app.post("/ingest/event")
def ingest_event(ev: Event):
    f = norm_path(ev.from_path)
    t = norm_path(ev.to_path)

    if f.startswith(NOISE_PREFIXES) or t.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    r.hincrby(_k_trans(ev.service, f), t, 1)
    r.hincrby(_k_total(ev.service), f, 1)
    return {"ok": True}

@app.post("/ingest/edge")
def ingest_edge(ev: EdgeEvent):
    src = norm_path(ev.src_path)
    dst = norm_path(ev.dst_path)

    if src.startswith(NOISE_PREFIXES) or dst.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    r.hincrby(_k_trans_any(ev.src_service, src), _pack(ev.dst_service, dst), 1)
    r.hincrby(_k_total_any(ev.src_service), src, 1)
    return {"ok": True}

@app.post("/ingest/prefetch")
def ingest_prefetch_attempt(edge: PrefetchAttempt):
    # WARNING: this is attempt, not real user transition
    src = norm_path(edge.src_path)
    dst = norm_path(edge.dst_path)

    if src.startswith(NOISE_PREFIXES) or dst.startswith(NOISE_PREFIXES):
        return {"ok": True, "skipped": True}

    r.hincrby(_k_trans_prefetch(edge.src_service, src), _pack(edge.dst_service, dst), 1)
    r.hincrby(_k_total_prefetch(edge.src_service), src, 1)
    return {"ok": True}

# =========================
# Policy
# =========================
@app.get("/policy/next", response_model=PolicyResp)
def policy_next(service: str, path: str, user_key: str = "anon", limit: int = 3):
    p = norm_path(path)

    if p.startswith(NOISE_PREFIXES):
        return PolicyResp(next_paths=[], max_prefetch=0, max_prefetch_time_ms=0)

    # markov probabilities for current node
    markov: Dict[Tuple[str, str], float] = {}

    # ---------------------------
    # 1) intra-service transitions
    # ---------------------------
    trans = r.hgetall(_k_trans(service, p)) or {}
    # total = sum counts from trans itself (no tot hash dependency)
    if trans:
        # (optional) smoothing: add MARKOV_SMOOTH mass across observed outgoing edges
        counts: Dict[str, int] = {}
        total = 0
        for to_path, cnt in trans.items():
            try:
                c = int(cnt)
            except Exception:
                continue
            if c <= 0:
                continue
            if DROP_SELF_LOOPS and to_path == p:
                continue
            counts[to_path] = c
            total += c

        if total > 0:
            k = len(counts)
            denom = total + (MARKOV_SMOOTH * k if MARKOV_SMOOTH > 0 and k > 0 else 0)
            for to_path, c in counts.items():
                num = c + (MARKOV_SMOOTH if MARKOV_SMOOTH > 0 else 0)
                prob = num / denom
                if prob >= MIN_PROB:
                    markov[(service, to_path)] = prob

    # ---------------------------
    # 2) cross-service transitions (REAL, from ingest/prefetch edges)
    # ---------------------------
    trans2 = r.hgetall(_k_trans_any(service, p)) or {}
    if trans2:
        counts2: Dict[Tuple[str, str], int] = {}
        total2 = 0
        for packed, cnt in trans2.items():
            try:
                c = int(cnt)
            except Exception:
                continue
            if c <= 0:
                continue
            try:
                dst_svc, dst_path = _unpack(packed)
            except Exception:
                continue
            if DROP_SELF_LOOPS and dst_svc == service and dst_path == p:
                continue
            counts2[(dst_svc, dst_path)] = c
            total2 += c

        if total2 > 0:
            k2 = len(counts2)
            denom2 = total2 + (MARKOV_SMOOTH * k2 if MARKOV_SMOOTH > 0 and k2 > 0 else 0)
            for (dst_svc, dst_path), c in counts2.items():
                num = c + (MARKOV_SMOOTH if MARKOV_SMOOTH > 0 else 0)
                prob = num / denom2
                if prob >= MIN_PROB:
                    # keep max if duplicated across signals
                    markov[(dst_svc, dst_path)] = max(markov.get((dst_svc, dst_path), 0.0), prob)

    # ---------------------------
    # 3) OPTIONAL: prefetch attempts as weak hint
    # ---------------------------
    if ALLOW_PREFETCH_ATTEMPTS_IN_POLICY:
        # ptot:{service} field p ; ptrans:{service}:{p} packed->count
        totalp_raw = r.hget(_k_total_prefetch(service), p)
        totalp = int(totalp_raw) if totalp_raw else 0
        if totalp > 0:
            trans2p = r.hgetall(_k_trans_prefetch(service, p)) or {}
            for packed, cnt in trans2p.items():
                try:
                    c = int(cnt)
                except Exception:
                    continue
                if c <= 0:
                    continue
                try:
                    dst_svc, dst_path = _unpack(packed)
                except Exception:
                    continue
                # weak probability mass
                prob = (c / totalp) * PREFETCH_ATTEMPT_WEIGHT
                if prob >= MIN_PROB:
                    markov[(dst_svc, dst_path)] = max(markov.get((dst_svc, dst_path), 0.0), prob)

    # ---------------------------
    # 4) candidates from item2vec
    # ---------------------------
    cands = get_i2v_candidates(service, p)

    # Fallback: pure markov if no i2v
    if not cands:
        items = [(s, pp, sc) for (s, pp), sc in markov.items()]
        items.sort(key=lambda x: x[2], reverse=True)
        top = items[: max(0, limit)]
        return PolicyResp(
            next_paths=[NextPath(service=s, path=pp, score=sc) for s, pp, sc in top],
            max_prefetch=DEFAULT_MAX_PREFETCH,
            max_prefetch_time_ms=DEFAULT_PREFETCH_BUDGET_MS,
        )

    # ---------------------------
    # 5) Hybrid scoring (robust)
    # score = alpha*cos + (1-alpha)*prob
    # + "insurance": keep top markov edges even if i2v doesn't include them
    # ---------------------------
    alpha = I2V_ALPHA
    best: Dict[Tuple[str, str], float] = {}

    # i2v candidates
    for svc, pp, cos in cands:
        if DROP_SELF_LOOPS and svc == service and pp == p:
            continue
        prob = markov.get((svc, pp), 0.0)
        sc = alpha * float(cos) + (1.0 - alpha) * float(prob)
        key = (svc, pp)
        if sc > best.get(key, -1e9):
            best[key] = sc

    # insurance: add top markov edges (cap to avoid polluting)
    # take only strongest outgoing probs
    markov_items = sorted(markov.items(), key=lambda kv: kv[1], reverse=True)
    for (svc, pp), prob in markov_items[: max(5, limit * 3)]:
        sc = (1.0 - alpha) * float(prob)
        key = (svc, pp)
        if sc > best.get(key, -1e9):
            best[key] = sc

    items = [(svc, pp, sc) for (svc, pp), sc in best.items()]
    items.sort(key=lambda x: x[2], reverse=True)
    top = items[: max(0, limit)]

    return PolicyResp(
        next_paths=[NextPath(service=s, path=pp, score=float(sc)) for s, pp, sc in top],
        max_prefetch=DEFAULT_MAX_PREFETCH,
        max_prefetch_time_ms=DEFAULT_PREFETCH_BUDGET_MS,
    )
# =========================
# Misc
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug/policy_raw")
def debug_policy_raw(service: str, path: str, limit: int = 10):
    p = norm_path(path)
    trans = r.hgetall(_k_trans(service, p)) or {}
    trans2 = r.hgetall(_k_trans_any(service, p)) or {}
    return {
        "p": p,
        "trans_keys": len(trans),
        "trans2_keys": len(trans2),
        "top_trans": sorted([(k, int(v)) for k, v in trans.items()], key=lambda x: x[1], reverse=True)[:10],
        "top_trans2": sorted([(k, int(v)) for k, v in trans2.items()], key=lambda x: x[1], reverse=True)[:10],
        "i2v": get_i2v_candidates(service, p)[:10],
    }

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
