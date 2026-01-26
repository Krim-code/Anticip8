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

# Prefetch
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

# Prefetch quality
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
