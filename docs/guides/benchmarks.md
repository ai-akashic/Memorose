---
title: Benchmark Notes & Methodology
---

# Benchmark Notes & Methodology

This page explains the benchmark figures referenced on the Memorose website and documentation.

## Important Scope Note

The current numbers are **project-reported engineering benchmarks** gathered from Memorose's internal evaluation setup. They are useful for understanding directional performance and capability, but they should **not** be read as an independent third-party audit.

## Reported Figures

- **HaluMem Recall**: 100% hallucination-free recall in the project's benchmark run
- **Persona Consistency**: 100% retention in the project's benchmark run
- **LoCoMo**: 100% long-conversation quality in the project's benchmark run
- **Cache Speedup**: 1273x acceleration for repeated queries in the project's benchmark run

## What These Numbers Are Intended To Show

- Memorose can preserve user and agent context across long-running interactions.
- Hybrid retrieval plus memory consolidation can reduce repeated-query latency dramatically when relevant memory is already structured and available.
- The system is designed for **agent memory quality**, not only document retrieval accuracy.

## What They Do Not Yet Prove

- They are not a substitute for a public, reproducible benchmark suite.
- They do not guarantee the same outcomes for every model, dataset, or deployment topology.
- They should not be treated as a formal independent certification.

## Recommended Interpretation

Use these benchmark figures as:

- evidence of current engineering direction,
- a signal that Memorose is optimized for persistent AI memory,
- and a starting point for your own workload-specific evaluation.

## Reproducibility Roadmap

The Memorose project is moving toward:

- public benchmark inputs and evaluation scripts,
- clearer hardware and model configuration disclosure,
- and reproducible benchmark packages for external validation.

Until that work is complete, teams evaluating Memorose for production should run their own benchmark pass against the workloads, models, and latency budgets they care about.
