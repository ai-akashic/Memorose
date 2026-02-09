import requests
import json
import uuid
import time
import sys

class MemoroseClient:
    def __init__(self, seed_nodes=["http://localhost:3000"], tenant_id="default"):
        self.nodes = seed_nodes
        self.leader_idx = 0
        self.tenant_id = tenant_id
        self.stream_id = str(uuid.uuid4())

    def _get_current_node(self):
        return self.nodes[self.leader_idx]

    def _request(self, method, path, **kwargs):
        """Internal request handler with Raft redirection support."""
        max_retries = len(self.nodes)
        
        for _ in range(max_retries + 1):
            url = f"{self._get_current_node()}{path}"
            
            # Ensure headers
            headers = kwargs.get("headers", {})
            headers["x-tenant-id"] = self.tenant_id
            headers["Content-Type"] = "application/json"
            kwargs["headers"] = headers

            try:
                if method == "GET":
                    resp = requests.get(url, **kwargs)
                elif method == "DELETE":
                    resp = requests.delete(url, **kwargs)
                else:
                    resp = requests.post(url, **kwargs)
                
                # Check for Raft redirection
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and data.get("status") == "redirect":
                        print(f"🔄 Node {self._get_current_node()} redirected to leader ID {data.get('leader_id')}")
                        self.leader_idx = (self.leader_idx + 1) % len(self.nodes)
                        continue
                    return resp
                elif resp.status_code == 503:
                    try:
                        err = resp.json()
                        if err.get("error") == "Not Leader":
                            hint = err.get("hint", "Unknown")
                            print(f"🔄 Node {self._get_current_node()} is not leader. Hint: {hint}. Retrying...")
                            self.leader_idx = (self.leader_idx + 1) % len(self.nodes)
                            continue
                    except:
                        pass
                    return resp
                else:
                    return resp
                    
            except requests.exceptions.ConnectionError:
                print(f"⚠️  Node {self._get_current_node()} is offline. Trying next...")
                self.leader_idx = (self.leader_idx + 1) % len(self.nodes)
                continue
        
        raise Exception("❌ All nodes in cluster are unreachable or failed.")

    def ingest_event(self, content):
        path = f"/v1/streams/{self.stream_id}/events"
        payload = {
            "tenant_id": self.tenant_id,
            "content": content
        }
        
        print(f"📥 Sending Event to {self._get_current_node()}: '{content[:30]}...'")
        resp = self._request("POST", path, json=payload)
        
        if resp.status_code == 200:
            print(f"✅ Success: {resp.json().get('event_id')}")
        else:
            print(f"❌ Failed: {resp.text}")

    def retrieve_memory(self, query, start_time=None, end_time=None):
        path = f"/v1/streams/{self.stream_id}/retrieve"
        payload = {
            "query": query,
            "start_time": start_time,
            "end_time": end_time
        }
        
        print(f"🔍 Searching on {self._get_current_node()} for: '{query}'")
        resp = self._request("POST", path, json=payload)
        
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            print(f"✨ Found {len(results)} results:")
            for item in results:
                if isinstance(item, list) and len(item) == 2:
                    unit, score = item
                    print(f"   - [{score:.2f}] {unit.get('content', '???')}")
                else:
                    print(f"   - {item}")
        else:
            print(f"❌ Failed: {resp.text}")

    def delete_tenant(self, tenant_id=None):
        target = tenant_id or self.tenant_id
        path = f"/v1/tenants/{target}"
        print(f"🗑️ Deleting tenant {target}...")
        resp = self._request("DELETE", path)
        if resp.status_code == 200:
            print(f"✅ Success: {resp.json()}")
        else:
            print(f"❌ Failed: {resp.text}")

def main():
    print("\n" + "="*20 + " Memorose Smart Client " + "="*20)
    
    # Connect to potential cluster nodes
    client = MemoroseClient(
        seed_nodes=["http://localhost:3000", "http://localhost:3001", "http://localhost:3002"],
        tenant_id="tenant-demo-python"
    )

    # 1. Ingest
    print("\n--- 1. Ingesting Events ---")
    conversations = [
        "Rust is a high-performance systems language.",
        "Raft is a consensus algorithm for distributed systems.",
        "Memorose uses RocksDB and LanceDB for storage."
    ]

    for line in conversations:
        client.ingest_event(line)
        time.sleep(0.1)

    print("\n⏳ Waiting for consolidation...")
    time.sleep(3)

    # 2. Retrieve
    print("\n--- 2. Retrieving Memories ---")
    client.retrieve_memory("What storage does Memorose use?")
    client.retrieve_memory("Explain Raft.")

if __name__ == "__main__":
    main()