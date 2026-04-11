---
title: Java
---

# Java

There is no stable Java SDK documented here for the current `v1` runtime.

Use your existing HTTP client stack and target the documented REST routes directly.

Typical flow:

1. log in through `/v1/dashboard/auth/login`
2. store the bearer token
3. call `/v1/users/:user_id/streams/:stream_id/events`
4. call `/v1/users/:user_id/streams/:stream_id/retrieve`
