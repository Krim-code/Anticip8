import httpx
from typing import Any, Dict, Optional
from .metrics import policy_latency
import time


class Anticip8Client:
    def __init__(
        self,
        core_url: str,
        service_name: str,
        timeout_sec: float = 1.0,
        max_connections: int = 50,
        max_keepalive: int = 20,
    ):
        self.core_url = core_url.rstrip("/")
        self.service = service_name

        self._timeout = httpx.Timeout(timeout_sec)
        self._limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive,
        )
        self._http: Optional[httpx.AsyncClient] = None

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self._timeout, limits=self._limits)
        return self._http

    async def close(self):
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def ingest(self, user_key: str, from_path: str, to_path: str, status: int, latency_ms: int):
        payload = {
            "service": self.service,
            "user_key": user_key,
            "from_path": from_path,
            "to_path": to_path,
            "status": status,
            "latency_ms": latency_ms,
        }
        try:
            c = await self._get_http()
            await c.post(f"{self.core_url}/ingest/event", json=payload)
        except Exception:
            pass

    async def ingest_edge(
        self,
        user_key: str,
        src_service: str,
        src_path: str,
        dst_service: str,
        dst_path: str,
        status: int,
        latency_ms: int,
    ):
        payload = {
            "src_service": src_service,
            "user_key": user_key,
            "src_path": src_path,
            "dst_service": dst_service,
            "dst_path": dst_path,
            "status": status,
            "latency_ms": latency_ms,
        }
        try:
            c = await self._get_http()
            await c.post(f"{self.core_url}/ingest/edge", json=payload)
        except Exception:
            pass

    async def ingest_prefetch(
        self,
        user_key: str,
        src_path: str,
        dst_service: str,
        dst_path: str,
        status: int,
        latency_ms: int,
    ):
        payload = {
            "src_service": self.service,
            "user_key": user_key,
            "src_path": src_path,
            "dst_service": dst_service,
            "dst_path": dst_path,
            "status": status,
            "latency_ms": latency_ms,
        }
        try:
            c = await self._get_http()
            await c.post(f"{self.core_url}/ingest/prefetch", json=payload)
        except Exception:
            pass

    async def get_policy(self, user_key: str, path: str, limit: int = 3) -> Dict[str, Any]:
        t0 = time.perf_counter()
        try:
            c = await self._get_http()
            resp = await c.get(
                f"{self.core_url}/policy/next",
                params={"service": self.service, "path": path, "user_key": user_key, "limit": limit},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return {"next_paths": [], "max_prefetch": 0, "max_prefetch_time_ms": 0}
        finally:
            policy_latency.labels(service=self.service).observe(time.perf_counter() - t0)
        
