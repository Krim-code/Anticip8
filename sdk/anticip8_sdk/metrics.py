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
