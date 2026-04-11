---
title: Node.js
---

# Node.js

There is no stable Node.js SDK documented here for the current `v1` runtime.

Use a thin HTTP client instead.

```ts
const response = await fetch(
  "http://127.0.0.1:3000/v1/users/dylan/streams/<uuid>/retrieve",
  {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: "What does Dylan prefer?",
      limit: 5,
    }),
  },
);

console.log(await response.json());
```
