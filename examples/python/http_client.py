import time
import uuid
from typing import Any, Dict, Optional

import requests


class MemoroseClient:
    def __init__(
        self,
        base_url: str = "http://localhost:3000",
        user_id: str = "demo-user",
        stream_id: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_id = user_id
        self.stream_id = stream_id or str(uuid.uuid4())
        self.org_id = org_id

    def _headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json"}

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> requests.Response:
        headers = self._headers()
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers
        response = requests.request(method, f"{self.base_url}{path}", timeout=30, **kwargs)
        response.raise_for_status()
        return response

    def ingest_event(self, content: str, content_type: str = "text") -> Dict[str, Any]:
        payload = {
            "content": content,
            "content_type": content_type,
        }
        response = self._request(
            "POST",
            f"/v1/users/{self.user_id}/streams/{self.stream_id}/events",
            json=payload,
        )
        return response.json()

    def retrieve_memory(
        self,
        query: str,
        *,
        limit: int = 10,
        org_id: Optional[str] = None,
        token_budget: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"query": query, "limit": limit}
        if org_id or self.org_id:
            payload["org_id"] = org_id or self.org_id
        if token_budget is not None:
            payload["token_budget"] = token_budget
        response = self._request(
            "POST",
            f"/v1/users/{self.user_id}/streams/{self.stream_id}/retrieve",
            json=payload,
        )
        return response.json()

    def build_context(
        self,
        query: str,
        *,
        token_budget: int = 800,
        limit: int = 12,
        org_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        format: str = "text",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "user_id": self.user_id,
            "query": query,
            "limit": limit,
            "token_budget": token_budget,
            "format": format,
        }
        if org_id or self.org_id:
            payload["org_id"] = org_id or self.org_id
        if agent_id:
            payload["agent_id"] = agent_id
        response = self._request(
            "POST",
            "/v1/memory/context",
            json=payload,
        )
        return response.json()

    def semantic_preview(
        self,
        instruction: str,
        *,
        mode: str = "auto",
        forget_mode: str = "logical",
        limit: int = 10,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "instruction": instruction,
            "mode": mode,
            "forget_mode": forget_mode,
            "limit": limit,
        }
        if self.org_id:
            payload["org_id"] = self.org_id
        response = self._request(
            "POST",
            f"/v1/users/{self.user_id}/memories/semantic/preview",
            json=payload,
        )
        return response.json()

    def semantic_execute(
        self,
        plan_id: str,
        *,
        reviewer: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "plan_id": plan_id,
            "confirm": True,
        }
        if self.org_id:
            payload["org_id"] = self.org_id
        if reviewer:
            payload["reviewer"] = reviewer
        if note:
            payload["note"] = note
        response = self._request(
            "POST",
            f"/v1/users/{self.user_id}/memories/semantic/execute",
            json=payload,
        )
        return response.json()

    def semantic_update(
        self,
        instruction: str,
        *,
        reviewer: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        preview = self.semantic_preview(instruction, mode="update")
        return {
            "preview": preview,
            "execute": self.semantic_execute(
                preview["plan_id"],
                reviewer=reviewer,
                note=note,
            ),
        }

    def semantic_forget(
        self,
        instruction: str,
        *,
        forget_mode: str = "logical",
        reviewer: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        preview = self.semantic_preview(
            instruction,
            mode="forget",
            forget_mode=forget_mode,
        )
        return {
            "preview": preview,
            "execute": self.semantic_execute(
                preview["plan_id"],
                reviewer=reviewer,
                note=note,
            ),
        }

    def list_pending_reviews(
        self,
        limit: int = 20,
        *,
        dashboard_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        query = f"?user_id={self.user_id}&status=pending&limit={limit}"
        if self.org_id:
            query += f"&org_id={self.org_id}"
        headers = self._headers()
        if dashboard_token:
            headers["Authorization"] = f"Bearer {dashboard_token}"
        response = self._request("GET", f"/v1/dashboard/corrections/reviews{query}", headers=headers)
        return response.json()


def main() -> None:
    print("\n" + "=" * 20 + " Memorose Python Client " + "=" * 20)

    client = MemoroseClient(
        base_url="http://localhost:3000",
        user_id="demo-python-user",
        stream_id="demo-session",
    )

    print("\n--- 1. Ingesting Events ---")
    for line in [
        "I used to live in Shanghai.",
        "My email is old@example.com.",
        "I prefer Rust over Python.",
    ]:
        print(client.ingest_event(line))
        time.sleep(0.1)

    print("\n--- 2. Retrieving Memory ---")
    print(client.retrieve_memory("Where do I live now?"))

    print("\n--- 3. Building Sidecar Context ---")
    context = client.build_context(
        "What should I keep in mind before helping this user?",
        token_budget=240,
    )
    print(context)

    print("\n--- 4. Semantic Update Preview/Execute ---")
    print(
        client.semantic_update(
            "I now live in Beijing and changed my email from old@example.com to new@example.com",
            reviewer="sdk-demo",
        )
    )

    print("\n--- 5. Pending Review Queue ---")
    print(client.list_pending_reviews(dashboard_token="replace-with-dashboard-jwt-if-needed"))


if __name__ == "__main__":
    main()
