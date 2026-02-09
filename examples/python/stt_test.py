import requests
import uuid
import time
import base64
import os

BASE_URL = "http://localhost:3000"
TENANT_ID = "stt-test-tenant"
STREAM_ID = str(uuid.uuid4())

# Smallest valid MP3 frame (approx) - Silence
MP3_B64 = "SUQzBAAAAAAAI1RTU0UAAAAPAAADTGF2ZjU4LjI5LjEwMAAAAAAAAAAAAAAA//uQZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWgAAAA0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABYaW5nAAAABwAAAAEAAACQAOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs//uQZAAAAAAAIAAAAAAAAEAAABAAAAAAAAAAAAAAAExBTUUzLjEwMKqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq//sQZAAP8AAAaQAAAAgAAA0gAAABAAABpAAAACAAADSAAAAEAAfmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZm"

def test_stt_ingest():
    print(f"Stream ID: {STREAM_ID}")
    
    payload = {
        "tenant_id": TENANT_ID,
        "content": MP3_B64,
        "content_type": "audio"
    }
    
    print("Sending audio event...")
    resp = requests.post(f"{BASE_URL}/v1/streams/{STREAM_ID}/events", json=payload)
    if resp.status_code != 200:
        print(f"Failed to ingest: {resp.text}")
        return
    
    print("Ingest success. Waiting for consolidation...")
    time.sleep(5) 

    # Retrieve
    search_payload = {
        "query": "audio",
        "include_vector": False
    }
    
    print("Searching...")
    search_resp = requests.post(f"{BASE_URL}/v1/streams/{STREAM_ID}/retrieve", json=search_payload, headers={"x-tenant-id": TENANT_ID})
    
    if search_resp.status_code == 200:
        results = search_resp.json().get("results", [])
        print(f"Found {len(results)} results.")
        for r, score in results:
            print(f" - [{score:.2f}] {r['content']}")
            # We expect either a transcript or the fallback "Audio asset at..."
            if "Audio asset" in r['content'] or "Transcribe" in r['content'] or "audio" in r['content'].lower():
                print("PASS: Audio pipeline processed event.")
    else:
        print(f"Search failed: {search_resp.text}")

if __name__ == "__main__":
    test_stt_ingest()
