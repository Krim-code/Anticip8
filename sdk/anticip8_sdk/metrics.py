import os
from prometheus_client import Counter, Histogram

SERVICE = os.getenv("SERVICE_NAME", "unknown")

# Cache
cache_hits = Counter(
    "anticip8_cache_hits_total",
    "Cache hits",
    ["service", "namespace"],
)
cache_misses = Counter(
    "anticip8_cache_misses_total",
    "Cache misses",
    ["service", "namespace"],
)

# Prefetch execution
prefetch_total = Counter(
    "anticip8_prefetch_total",
    "Prefetch requests started",
    ["service"],
)
prefetch_errors = Counter(
    "anticip8_prefetch_errors_total",
    "Prefetch errors",
    ["service", "reason"],
)
prefetch_latency = Histogram(
    "anticip8_prefetch_latency_seconds",
    "Prefetch latency (seconds)",
    ["service"],
)

# Prefetch quality (legacy: HIT/MISS based on intent + mark)
prefetch_hits = Counter(
    "anticip8_prefetch_hits_total",
    "Prefetch hits (user later requested a prefetched path)",
    ["service"],
)
prefetch_misses = Counter(
    "anticip8_prefetch_misses_total",
    "Prefetch misses (user requested a path not prefetched recently)",
    ["service"],
)

# Core policy calls (from SDK)
policy_requests = Counter(
    "anticip8_policy_requests_total",
    "Requests to Anticip8 core policy endpoint",
    ["service"],
)
policy_errors = Counter(
    "anticip8_policy_errors_total",
    "Errors calling Anticip8 core policy endpoint",
    ["service", "reason"],
)
policy_latency = Histogram(
    "anticip8_policy_latency_seconds",
    "Latency of Anticip8 core policy call",
    ["service"],
)

# Prefetch control-plane
prefetch_budget_overrun = Counter(
    "anticip8_prefetch_budget_overrun_total",
    "Prefetch batch stopped due to budget deadline",
    ["service"],
)
prefetch_dedup_skips = Counter(
    "anticip8_prefetch_dedup_skips_total",
    "Prefetch skipped due to inflight dedup",
    ["service"],
)
prefetch_deadline_skips = Counter(
    "anticip8_prefetch_deadline_skips_total",
    "Prefetch skipped because deadline already passed",
    ["service"],
)

# NEW: separate “policy guessed right” vs “prefetch didn’t make it in time”
intent_seen = Counter(
    "anticip8_intent_seen_total",
    "Real request observed an intent marker for this req_key",
    ["service"],
)
intent_missing = Counter(
    "anticip8_intent_missing_total",
    "Real request had no intent marker for this req_key",
    ["service"],
)
prefetch_mark_ready = Counter(
    "anticip8_prefetch_mark_ready_total",
    "Real request observed prefetched mark present at arrival time",
    ["service"],
)
prefetch_mark_not_ready = Counter(
    "anticip8_prefetch_mark_not_ready_total",
    "Real request had intent but prefetched mark was not ready yet (race/slow prefetch)",
    ["service"],
)
race_grace_hits = Counter(
    "anticip8_race_grace_hits_total",
    "After small grace wait, prefetched mark appeared (race window)",
    ["service"],
)
race_grace_misses = Counter(
    "anticip8_race_grace_misses_total",
    "After grace wait, prefetched mark still missing (slow/failed prefetch)",
    ["service"],
)

# Optional: how long we waited (if you want a graph)
race_grace_wait = Histogram(
    "anticip8_race_grace_wait_seconds",
    "Grace wait duration for race recheck",
    ["service"],
)
