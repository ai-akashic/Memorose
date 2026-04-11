---
title: Forgetting Strategies
---

# Forgetting Strategies

Memorose is designed to forget on purpose. The goal is not maximum retention. The goal is durable, useful retention.

## L0: Entropy Filtering

Low-signal events can be filtered before or during consolidation using the entropy threshold configured under `[consolidation]`.

This keeps greetings, filler, and repeated low-value chatter from turning into long-term state.

## L1 And L2: Decay And Pruning

Long-lived memory should lose weight if it stops being useful. The `[forgetting]` section controls:

- decay half-life
- minimum importance
- pruning interval

This is what prevents a long-running agent from turning into an unbounded junk drawer.

## Graph And Community Compaction

Graph auto-linking and community detection let the system replace repeated local evidence with higher-level structure when that structure is more useful than replaying every source fragment.

## L3 Sedimentation

When a task completes, its outcome can sediment back into the event stream as a new fact about what happened. This lets execution outcomes become memory instead of staying trapped inside task state.

## Operational Principle

Memorose treats forgetting as part of the architecture, not as a cleanup script that runs after the design is already wrong.
