---
title: Go
---

# Go

There is no stable Go SDK documented here for the current `v1` runtime.

Use the standard library or your preferred HTTP client.

```go
payload := strings.NewReader(`{"query":"What does Dylan prefer?","limit":5}`)
req, _ := http.NewRequest(
    http.MethodPost,
    "http://127.0.0.1:3000/v1/users/dylan/streams/<uuid>/retrieve",
    payload,
)
req.Header.Set("Authorization", "Bearer "+token)
req.Header.Set("Content-Type", "application/json")

resp, err := http.DefaultClient.Do(req)
if err != nil {
    panic(err)
}
defer resp.Body.Close()
```
