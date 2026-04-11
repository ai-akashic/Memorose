---
title: Memory Hierarchy
---

# The L0-L3 Memory Hierarchy

Memorose models memory as an explicit hierarchy, not as one flat document store.

## Layer Summary

| Layer | Primary Type | Role | Typical Content |
| --- | --- | --- | --- |
| `L0` | `Event` | Raw experience stream | chat turns, tool output, image/audio/video/json input, task-completion events |
| `L1` | `MemoryUnit (level=1)` | Stable first-order memory | compressed facts, preferences, procedures, grounded summaries |
| `L2` | `MemoryUnit (level=2)` | Reflective and clustered memory | topics, communities, insights, shared knowledge |
| `L3` | `L3Task` plus level-3 goal units | Goal and task execution | goal structures, task trees, dependencies, progress, outcomes |

## L0: Raw Events

L0 is append-oriented. It preserves what happened before the system decides what is worth remembering long term.

Important point: `L0` is not yet a memory unit.

## L1: Stable Memory

L1 is where raw events become useful memory. Facts and procedural traces are compressed into retrievable units.

Examples:

- user preferences
- recurring constraints
- successful tool-use patterns
- concise grounded summaries

## L2: Insights

L2 stores higher-order structure. This can include community summaries, longer-horizon themes, graph-linked insights, and projected organization knowledge.

## L3: Goals & Task Memory

L3 is the execution layer. In the current implementation it behaves primarily as goal and task memory for planning, coordination, and execution state, not just a more abstract memory bucket.

L3 includes:

- goal roots and structures
- milestones
- parent-child task trees
- dependency tracking
- progress updates
- completion summaries

## Why This Matters

`L0-L3` answers: how abstract and action-oriented is this memory?

It does not answer ownership. Ownership is handled separately by the domain model: `agent`, `user`, and `organization`.
