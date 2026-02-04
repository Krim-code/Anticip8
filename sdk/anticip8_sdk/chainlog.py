# anticip8_sdk/chainlog.py
import os
from typing import Optional
from redis.asyncio import Redis

LAST_TTL_SEC = int(os.getenv("ANTICIP8_CHAINLOG_LAST_TTL_SEC", "3600"))
USER_TOP_TTL_SEC = int(os.getenv("ANTICIP8_CHAINLOG_USER_TOP_TTL_SEC", "0"))  # 0 = no expire

def _step_key(service: str, req_key: str) -> str:
    return f"{service}|{req_key}"

def _k_last(user: str) -> str:
    return f"anticip8:chain:last:{user}"

def _k_prev(user: str) -> str:
    return f"anticip8:chain:prev:{user}"

def _k_top2_global() -> str:
    return "anticip8:chain:top2"

def _k_top3_global() -> str:
    return "anticip8:chain:top3"

def _k_top2_user(user: str) -> str:
    return f"anticip8:chain:u:{user}:top2"

def _k_top3_user(user: str) -> str:
    return f"anticip8:chain:u:{user}:top3"

async def log_step(
    r: Redis,
    service_name: str,
    user_key: str,
    req_key: str,
    *,
    per_user: bool = True,
    enable_trigram: bool = True,
) -> None:
    """
    Updates:
      - last step per user
      - global top bigrams
      - optional global top trigrams
      - optional per-user top bigrams/trigrams
    """
    # "svc|path?qs" already normalized by your middleware as req_key
    cur = _step_key(service_name, req_key)

    last_k = _k_last(user_key)
    prev_k = _k_prev(user_key)

    try:
        # read last and prev in one go
        last, prev = await r.mget(last_k, prev_k)

        pipe = r.pipeline(transaction=False)

        # ---- bigram: last -> cur
        if last and last != cur:
            b = f"{last} -> {cur}"
            pipe.zincrby(_k_top2_global(), 1.0, b)
            if per_user:
                pipe.zincrby(_k_top2_user(user_key), 1.0, b)

        # ---- trigram: prev -> last -> cur
        if enable_trigram and prev and last and (prev != last) and (last != cur):
            t = f"{prev} -> {last} -> {cur}"
            pipe.zincrby(_k_top3_global(), 1.0, t)
            if per_user:
                pipe.zincrby(_k_top3_user(user_key), 1.0, t)

        # shift window:
        # prev = last, last = cur
        pipe.setex(last_k, LAST_TTL_SEC, cur)
        pipe.setex(prev_k, LAST_TTL_SEC, last or cur)

        # optional TTL for per-user zsets (if you want them to “fade”)
        if per_user and USER_TOP_TTL_SEC > 0:
            pipe.expire(_k_top2_user(user_key), USER_TOP_TTL_SEC)
            if enable_trigram:
                pipe.expire(_k_top3_user(user_key), USER_TOP_TTL_SEC)

        await pipe.execute()

    except Exception:
        # fail-open, analytics must never break requests
        return
