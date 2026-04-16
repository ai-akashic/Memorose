"""Memorose Python SDK — reference client for the Memorose v1 REST API."""

import time
import uuid
from typing import Any, Dict, List, Literal, Optional, Union

import requests


class MemoroseError(Exception):
    """Base exception for Memorose client errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, body: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class MemoroseClient:
    """Client for the Memorose v1 REST API.

    Supports authentication via API key or dashboard JWT token,
    multimodal ingestion, hybrid retrieval, semantic corrections,
    task management, graph edges, and organization knowledge.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:3000",
        user_id: str = "demo-user",
        stream_id: Optional[str] = None,
        org_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        api_key: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 30,
        max_retries: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_id = user_id
        self.stream_id = stream_id or str(uuid.uuid4())
        self.org_id = org_id
        self.agent_id = agent_id
        self.api_key = api_key
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        elif self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = self._headers()
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        kwargs["headers"] = headers
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(method, f"{self.base_url}{path}", timeout=self.timeout, **kwargs)
                if resp.status_code == 503 and attempt < self.max_retries:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                if resp.status_code >= 400:
                    try:
                        body = resp.json()
                    except Exception:
                        body = resp.text
                    raise MemoroseError(f"HTTP {resp.status_code}: {resp.reason}", status_code=resp.status_code, body=body)
                return resp
            except requests.ConnectionError as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise MemoroseError(f"Connection failed: {exc}") from exc
        raise MemoroseError(f"Request failed after {self.max_retries + 1} attempts") from last_exc
    # APPEND_MARKER_1

    # ── Ingest ────────────────────────────────────────────────────────────

    def ingest_event(self, content: str, content_type: str = "text", *, org_id: Optional[str] = None, agent_id: Optional[str] = None, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """Ingest a single event."""
        payload: Dict[str, Any] = {"content": content, "content_type": content_type}
        if org_id or self.org_id:
            payload["org_id"] = org_id or self.org_id
        sid = stream_id or self.stream_id
        return self._request("POST", f"/v1/users/{self.user_id}/streams/{sid}/events", json=payload).json()

    def ingest_batch(self, events: List[Dict[str, Any]], *, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """Ingest multiple events in one request."""
        sid = stream_id or self.stream_id
        return self._request("POST", f"/v1/users/{self.user_id}/streams/{sid}/events/batch", json={"events": events}).json()

    def ingest_image(self, image_data: str, **kw: Any) -> Dict[str, Any]:
        """Ingest a base64-encoded image."""
        return self.ingest_event(image_data, content_type="image", **kw)

    def ingest_audio(self, audio_data: str, **kw: Any) -> Dict[str, Any]:
        """Ingest a base64-encoded audio clip."""
        return self.ingest_event(audio_data, content_type="audio", **kw)

    def ingest_video(self, video_url: str, **kw: Any) -> Dict[str, Any]:
        """Ingest a video URL."""
        return self.ingest_event(video_url, content_type="video", **kw)
    # APPEND_MARKER_2

    # ── Retrieve ──────────────────────────────────────────────────────────

    def retrieve_memory(self, query: str, *, limit: int = 10, org_id: Optional[str] = None, agent_id: Optional[str] = None, token_budget: Optional[int] = None, min_score: Optional[float] = None, graph_depth: Optional[int] = None, enable_arbitration: bool = False, start_time: Optional[str] = None, end_time: Optional[str] = None, as_of: Optional[str] = None, image: Optional[str] = None, audio: Optional[str] = None, video: Optional[str] = None, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """Hybrid search over the user's memory store."""
        payload: Dict[str, Any] = {"query": query, "limit": limit}
        if org_id or self.org_id:
            payload["org_id"] = org_id or self.org_id
        if agent_id or self.agent_id:
            payload["agent_id"] = agent_id or self.agent_id
        for k, v in [("token_budget", token_budget), ("min_score", min_score), ("graph_depth", graph_depth)]:
            if v is not None:
                payload[k] = v
        if enable_arbitration:
            payload["enable_arbitration"] = True
        for k, v in [("start_time", start_time), ("end_time", end_time), ("as_of", as_of), ("image", image), ("audio", audio), ("video", video)]:
            if v:
                payload[k] = v
        sid = stream_id or self.stream_id
        return self._request("POST", f"/v1/users/{self.user_id}/streams/{sid}/retrieve", json=payload).json()

    def get_memory(self, memory_id: str) -> Dict[str, Any]:
        """Get a single memory unit by ID."""
        return self._request("GET", f"/v1/users/{self.user_id}/memories/{memory_id}").json()

    # ── Context (sidecar) ─────────────────────────────────────────────────

    def build_context(self, query: str, *, token_budget: int = 800, limit: int = 12, org_id: Optional[str] = None, agent_id: Optional[str] = None, format: Literal["text", "xml"] = "text", min_score: Optional[float] = None, graph_depth: Optional[int] = None, image: Optional[str] = None, audio: Optional[str] = None, video: Optional[str] = None) -> Dict[str, Any]:
        """Build a prompt-ready memory context block (sidecar pattern)."""
        payload: Dict[str, Any] = {"user_id": self.user_id, "query": query, "limit": limit, "token_budget": token_budget, "format": format}
        if org_id or self.org_id:
            payload["org_id"] = org_id or self.org_id
        if agent_id or self.agent_id:
            payload["agent_id"] = agent_id or self.agent_id
        for k, v in [("min_score", min_score), ("graph_depth", graph_depth), ("image", image), ("audio", audio), ("video", video)]:
            if v is not None:
                payload[k] = v
        return self._request("POST", "/v1/memory/context", json=payload).json()
    # APPEND_MARKER_3

    # ── Semantic corrections ──────────────────────────────────────────────

    def semantic_preview(self, instruction: str, *, mode: str = "auto", forget_mode: str = "logical", limit: int = 10) -> Dict[str, Any]:
        """Preview a semantic update/forget plan."""
        payload: Dict[str, Any] = {"instruction": instruction, "mode": mode, "forget_mode": forget_mode, "limit": limit}
        if self.org_id:
            payload["org_id"] = self.org_id
        return self._request("POST", f"/v1/users/{self.user_id}/memories/semantic/preview", json=payload).json()

    def semantic_execute(self, plan_id: str, *, reviewer: Optional[str] = None, note: Optional[str] = None) -> Dict[str, Any]:
        """Execute a previously previewed semantic plan."""
        payload: Dict[str, Any] = {"plan_id": plan_id, "confirm": True}
        if self.org_id:
            payload["org_id"] = self.org_id
        if reviewer:
            payload["reviewer"] = reviewer
        if note:
            payload["note"] = note
        return self._request("POST", f"/v1/users/{self.user_id}/memories/semantic/execute", json=payload).json()

    def semantic_update(self, instruction: str, *, reviewer: Optional[str] = None, note: Optional[str] = None) -> Dict[str, Any]:
        """Preview + execute a semantic update in one call."""
        preview = self.semantic_preview(instruction, mode="update")
        return {"preview": preview, "execute": self.semantic_execute(preview["plan_id"], reviewer=reviewer, note=note)}

    def semantic_forget(self, instruction: str, *, forget_mode: str = "logical", reviewer: Optional[str] = None, note: Optional[str] = None) -> Dict[str, Any]:
        """Preview + execute a semantic forget in one call."""
        preview = self.semantic_preview(instruction, mode="forget", forget_mode=forget_mode)
        return {"preview": preview, "execute": self.semantic_execute(preview["plan_id"], reviewer=reviewer, note=note)}

    # ── Tasks (L3) ────────────────────────────────────────────────────────

    def get_task_trees(self, *, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """Get all goal/task trees for the user."""
        path = f"/v1/users/{self.user_id}/streams/{stream_id}/tasks/tree" if stream_id else f"/v1/users/{self.user_id}/tasks/tree"
        return self._request("GET", path).json()

    def get_ready_tasks(self) -> Dict[str, Any]:
        """Get tasks that are ready to execute."""
        return self._request("GET", f"/v1/users/{self.user_id}/tasks/ready").json()

    def update_task_status(self, task_id: str, status: str, *, progress: Optional[float] = None, result_summary: Optional[str] = None) -> Dict[str, Any]:
        """Update a task's status."""
        payload: Dict[str, Any] = {"status": status}
        if progress is not None:
            payload["progress"] = progress
        if result_summary is not None:
            payload["result_summary"] = result_summary
        return self._request("PUT", f"/v1/users/{self.user_id}/tasks/{task_id}/status", json=payload).json()

    # ── Graph ─────────────────────────────────────────────────────────────

    def add_edge(self, source_id: str, target_id: str, relation: str, *, weight: Optional[float] = None) -> Dict[str, Any]:
        """Add a relationship edge between two memory units."""
        payload: Dict[str, Any] = {"source_id": source_id, "target_id": target_id, "relation": relation}
        if weight is not None:
            payload["weight"] = weight
        return self._request("POST", f"/v1/users/{self.user_id}/graph/edges", json=payload).json()

    # ── Organization knowledge ────────────────────────────────────────────

    def list_org_knowledge(self, org_id: Optional[str] = None) -> Dict[str, Any]:
        """List organization knowledge entries."""
        oid = org_id or self.org_id
        if not oid:
            raise MemoroseError("org_id is required")
        return self._request("GET", f"/v1/organizations/{oid}/knowledge").json()

    def get_org_knowledge(self, knowledge_id: str, org_id: Optional[str] = None) -> Dict[str, Any]:
        """Get a single organization knowledge entry."""
        oid = org_id or self.org_id
        if not oid:
            raise MemoroseError("org_id is required")
        return self._request("GET", f"/v1/organizations/{oid}/knowledge/{knowledge_id}").json()

    def get_org_knowledge_metrics(self, org_id: Optional[str] = None) -> Dict[str, Any]:
        """Get organization knowledge automation metrics."""
        oid = org_id or self.org_id
        if not oid:
            raise MemoroseError("org_id is required")
        return self._request("GET", f"/v1/organizations/{oid}/knowledge/metrics").json()

    # ── Status ────────────────────────────────────────────────────────────

    def pending_count(self) -> Dict[str, Any]:
        """Get the number of pending events in the pipeline."""
        return self._request("GET", "/v1/status/pending").json()

    # ── Dashboard ─────────────────────────────────────────────────────────

    def list_pending_reviews(self, limit: int = 20) -> Dict[str, Any]:
        """List pending correction reviews (dashboard API)."""
        query = f"?user_id={self.user_id}&status=pending&limit={limit}"
        if self.org_id:
            query += f"&org_id={self.org_id}"
        return self._request("GET", f"/v1/dashboard/corrections/reviews{query}").json()


def main() -> None:
    print("\n" + "=" * 20 + " Memorose Python Client " + "=" * 20)
    client = MemoroseClient(base_url="http://localhost:3000", user_id="demo-python-user", stream_id="demo-session")

    print("\n--- 1. Ingesting Events ---")
    for line in ["I used to live in Shanghai.", "My email is old@example.com.", "I prefer Rust over Python."]:
        print(client.ingest_event(line))
        time.sleep(0.1)

    print("\n--- 2. Batch Ingest ---")
    print(client.ingest_batch([{"content": "I have a cat named Mochi."}, {"content": "My favorite color is blue."}]))

    print("\n--- 3. Retrieving Memory ---")
    print(client.retrieve_memory("Where do I live now?"))

    print("\n--- 4. Building Sidecar Context ---")
    print(client.build_context("What should I keep in mind?", token_budget=240))

    print("\n--- 5. Semantic Update ---")
    print(client.semantic_update("I now live in Beijing", reviewer="sdk-demo"))

    print("\n--- 6. Task Trees ---")
    try:
        print(client.get_task_trees())
    except MemoroseError as e:
        print(f"  (no tasks yet: {e})")

    print("\n--- 7. Pending Count ---")
    print(client.pending_count())


if __name__ == "__main__":
    main()
