import os
import httpx

class Anticip8Client:
    def __init__(self, core_url: str, service_name: str):
        self.core_url = core_url.rstrip("/")
        self.service = service_name

    async def ingest(self, user_key: str, from_path: str, to_path: str, status: int, latency_ms: int):
        payload = {
            "service": self.service,
            "user_key": user_key,
            "from_path": from_path,
            "to_path": to_path,
            "status": status,
            "latency_ms": latency_ms,
        }
        async with httpx.AsyncClient(timeout=1.0) as c:
            # fire-and-forget-ish: try, ignore errors
            try:
                await c.post(f"{self.core_url}/ingest/event", json=payload)
            except Exception:
                pass

    async def get_policy(self, user_key: str, path: str, limit: int = 3):
        async with httpx.AsyncClient(timeout=1.0) as c:
            try:
                resp = await c.get(
                    f"{self.core_url}/policy/next",
                    params={"service": self.service, "path": path, "user_key": user_key, "limit": limit},
                )
                resp.raise_for_status()
                return resp.json()
            except Exception:
                return {"next_paths": [], "max_prefetch": 0, "max_prefetch_time_ms": 0}
            
    async def ingest_prefetch(self, user_key: str, src_path: str, dst_service: str, dst_path: str, status: int, latency_ms: int):
        payload = {
            "src_service": self.service,
            "user_key": user_key,
            "src_path": src_path,
            "dst_service": dst_service,
            "dst_path": dst_path,
            "status": status,
            "latency_ms": latency_ms,
        }
        async with httpx.AsyncClient(timeout=1.0) as c:
            try:
                await c.post(f"{self.core_url}/ingest/prefetch", json=payload)
            except Exception:
                pass