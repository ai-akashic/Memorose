---
title: Python
---

# Python

There is no stable Python SDK documented here for the current `v1` runtime.

Use a thin HTTP client instead.

```python
import requests

base_url = "http://127.0.0.1:3000"
token = "<dashboard-jwt>"
stream_id = "<uuid>"

response = requests.post(
    f"{base_url}/v1/users/dylan/streams/{stream_id}/retrieve",
    headers={"Authorization": f"Bearer {token}"},
    json={"query": "What does Dylan prefer?", "limit": 5},
    timeout=30,
)

print(response.json())
```
