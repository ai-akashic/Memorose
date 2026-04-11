---
title: Memory Domains
---

# Memory Domains

The current Memorose model uses three primary domains:

- `agent`
- `user`
- `organization`

Older docs versions described an `app`-centric hierarchy. That is no longer the primary product model.

## Domain Responsibilities

### Agent Memory

Agent memory is mainly procedural.

Use it for:

- tool usage patterns
- execution traces
- recovery strategies
- planning heuristics
- agent-specific reflections

### User Memory

User memory is mainly factual and preferential.

Use it for:

- identity facts
- preferences
- constraints
- long-lived personal context
- goals tied to one user

### Organization Memory

Organization memory is reusable shared knowledge. It should be evolved from authorized source memory, not created by mixing all raw user events into one global bucket.

Use it for:

- policies
- terminology
- shared workflows
- generalized best practices
- reusable higher-order insights

## Native vs Projected

- `agent` and `user` memory are native domains
- `organization` memory is often projected from source memory after filtering, abstraction, and sharing policy checks

## How Domains Interact With L0-L3

The hierarchy and the domains answer different questions:

- `L0-L3`: how memory evolves over time
- `agent/user/organization`: whose memory it is and who may reuse it

You need both dimensions to reason about the system correctly.
