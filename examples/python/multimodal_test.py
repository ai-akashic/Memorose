import requests
import uuid
import time
import base64
import os

# Configuration
BASE_URL = "http://localhost:3000"
TENANT_ID = "multimodal-test-tenant"
STREAM_ID = str(uuid.uuid4())

# 1. Create a dummy image (1x1 red pixel)
# Base64 of a 1x1 red PNG
RED_PIXEL_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="

def test_multimodal_ingest():
    print(f"Stream ID: {STREAM_ID}")
    
    # 2. Ingest Image Event
    # Note: Memorose expects "content" to be URL or Data. 
    # Since we don't have a public URL for this test, we pass base64 directly as content 
    # and rely on the updated gemini.rs to handle it (it treats non-http string as base64/data).
    
    payload = {
        "tenant_id": TENANT_ID,
        "content": RED_PIXEL_B64,
        "content_type": "image"
    }
    
    print("Sending image event...")
    resp = requests.post(f"{BASE_URL}/v1/streams/{STREAM_ID}/events", json=payload)
    if resp.status_code != 200:
        print(f"Failed to ingest: {resp.text}")
        return
    
    print("Ingest success. Waiting for consolidation (Worker cycle)...")
    time.sleep(5) # Wait for worker to pick it up (consolidation interval is usually short)

    # 3. Retrieve - Search for "red"
    # The Vision model should describe it as a red color or pixel.
    
    search_payload = {
        "query": "red color",
        "include_vector": False
    }
    
    print("Searching for 'red color'...")
    search_resp = requests.post(f"{BASE_URL}/v1/streams/{STREAM_ID}/retrieve", json=search_payload, headers={"x-tenant-id": TENANT_ID})
    
    if search_resp.status_code == 200:
        results = search_resp.json().get("results", [])
        print(f"Found {len(results)} results.")
        for r, score in results:
            print(f" - [{score:.2f}] {r['content']}")
            if "red" in r['content'].lower() or "pixel" in r['content'].lower():
                print("PASS: Vision-to-Text worked!")
    else:
        print(f"Search failed: {search_resp.text}")

if __name__ == "__main__":
    test_multimodal_ingest()
