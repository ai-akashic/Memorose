# Memorose TODOs

## Architecture & Integration

- [ ] **Implement Native Gateway / Sidecar Injection Pattern for Memory**
  - **Context:** Relying solely on MCP (Model Context Protocol) for memory integration leads to high latency (extra RTT), wasted tokens (tool call + redundant context generation), and unpredictable context window usage. Memorose should offer a native integration path.
  - **Proposed Solutions:**
    1.  **Strict Token-Aware Retrieval (Gateway Pattern):**
        - [ ] Create a `tokenizer` utility module in `memorose-common` (using `tiktoken-rs` or `tokenizers` crate) with a `count_tokens(text: &str) -> usize` function.
        - [ ] Add `token_budget: Option<usize>` parameter to the retrieval engine functions in `memorose-core`.
        - [ ] Implement a dynamic truncation loop during `MemoryUnit` assembly: iterate through ranked results, accumulate token counts, and break when the budget is reached.
        - [ ] Expose the budget parameter via HTTP headers (e.g., `X-Memory-Budget`) or request payload in `memorose-server`.
    2.  **Context-as-a-Service (Sidecar Pattern):**
        - Expose a `/v1/memory/context` endpoint.
        - Clients query this endpoint with their input and token limits before calling the LLM.
        - Memorose returns a highly condensed text block (or XML) that the client explicitly prepends to the prompt, ensuring total control over context length by the client orchestrator.
    3.  **Adaptive Compression via L1-L3 Layers:**
        - Implement adaptive fallback logic based on the token budget.
        - If token budget is large: retrieve detailed L1 procedural/factual memory.
        - If token budget is extremely small: fallback to retrieving concise L2 graph insights or L3 goal summaries to maximize information density.

- [x] **Implement Active Forgetting & Correction Mechanisms**
  - **Context:** While passive importance decay is implemented via background workers, there is no explicit way for users or agents to instantly delete or overwrite a specific memory (e.g., handling contradictions, right-to-be-forgotten, privacy requests).
  - **Current Status:** Hard delete, structured correction actions, logical tombstoning, correction guards, slot-aware candidate retrieval, multi-fact extraction/compatibility matching, and a hybrid fact extraction path (rule-based facts + LLM fallback extraction) are implemented. The L1 consolidation worker now runs a pre-store RAC reconciliation hook before writing new factual memories into the stable retrieval set, including same-batch staged reconciliation before commit. The extractor path now also supports richer structured fact fields (subject refs/names, temporal status, polarity, evidence spans), persists normalized extracted facts onto `MemoryUnit`, reuses those persisted facts before falling back to live extraction, includes a first bilingual (English/Chinese) rule/tokenization/eval pass, and now exposes explicit manual correction + RAC review/approve/reject APIs for guarded relation-only cases. Follow-up work here is now quality hardening/evaluation breadth rather than missing core product behavior.
  - **Proposed Solutions:**
    - [x] **Expose Hard Delete API:** Add an HTTP endpoint (e.g., `DELETE /v1/users/:user_id/memories/:id`) in `memorose-server` to allow explicit removal of a memory unit across all storage layers (KV, LanceDB, Tantivy).
    - [ ] **Contradiction Resolution via Retrieval-Augmented Consolidation (RAC):**
      - Enhance the L1 consolidation worker to perform a "Self-Query" during consolidation so contradiction handling happens before the new fact becomes part of the stable retrieval set.
        - [x] **Step 1:** Extract key entities from new events and query the existing L1 memory store for related historical facts.
          - Current implementation now covers the full Step 1 path with hybrid fact extraction (rule-based / heuristic first, then LLM fallback), rule-side multi-fact extraction, inline list-value extraction for common preference/skill/ownership patterns, broader multilingual relationship/status/schedule variants, expanded employment/contact/historical-negation phrasing coverage, better organization/agent subject inference, explicit old→new transition extraction (e.g. `changed from X to Y` / `从旧值改成新值`), same-sentence mixed-slot transition handling, pronoun-aware/table-driven transition markers (e.g. `changed his email from ...` / `changed her job from ...`), organization/field-based residence transition extraction (e.g. `changed its headquarters from Berlin to Paris` / `把地址从上海改成北京`), and stricter employment-transition disambiguation to avoid `moved from city A to city B` being misread as a job change. The extractor lexicon layer is now driven by a config rule pack (`crates/memorose-core/src/fact_extraction_multilingual.json`) instead of continuing to grow hardcoded per-language literals in Rust; that pack now owns the core English/Chinese attribute/subject/segmentation lexicons as well as the non-core multilingual additions and change-marker/transition-marker lexicons. It currently covers a first-pass Spanish/Japanese set for high-value slots such as residence/contact/employment/preference/status (e.g. `Vivo en Madrid`, `Mi correo es ...`, `私は東京に住んでいます`, `私はOpenAIで働いています`, `私は寿司が好きです`). Multi-clause historical→current fact extraction, cross-segment subject carry/linking, slot-aware multi-fact candidate retrieval, richer subject/temporal normalization, persisted extracted-fact reuse, multilingual fallback coverage, fixture-based extractor scorecards plus rule-retrieval end-to-end tests, subject keys, canonical/typed value normalization, and a pre-store consolidation hook are all in place. Remaining follow-up work is now mainly broader language/coverage hardening and deeper end-to-end evaluation breadth, rather than a missing Step 1 capability.
        - [x] **Step 2:** Pass the new event and retrieved historical facts to an LLM "Arbitrator" prompt to detect contradictions or updates.
        - [x] **Step 3:** Parse structured JSON actions (e.g., `{"target_id": "...", "action": "OBSOLETE"}`) from the LLM output.
          - Current action set also supports `REAFFIRM` and `IGNORE` in addition to `OBSOLETE` and `CONTRADICTS`, and `OBSOLETE` now has a stricter guard with compatibility checks, minimum confidence, and target recency checks before tombstoning.
        - [x] **Step 4:** Execute logical tombstoning on outdated memory units and attach correction relations (`EvolvedTo` / `Contradicts`) so outdated facts are removed from future retrieval contexts while preserving traceability.
          - `OBSOLETE` is now guarded so only compatible fact slots can overwrite prior memories.

- [ ] **Enhance Multimodal Asset Representation in Retrieval**
  - **Context:** While the engine handles multimodal data via native embeddings or fallback descriptions, the retrieved `MemoryUnit` should explicitly expose the generated description (e.g., text from STT or image caption) alongside the original source URL, so the LLM can leverage both the raw link and the textual context.
  - **Proposed Solutions:**
    - [ ] **Extend Asset Struct:** Add a `description: Option<String>` field to the `Asset` struct in `memorose-common`.
    - [ ] **Store Descriptions:** Update `worker.rs` (in `extract_assets_from_event` or the embedding pipeline) to persist the result of `describe_image`, `describe_video`, or `transcribe` directly into the `Asset`'s metadata or description field.
    - [ ] **Enrich Retrieval Formatting:** Ensure that when a `MemoryUnit` is retrieved, its formatted text block (passed to the LLM) explicitly appends multimodal descriptions and source URLs (e.g., `[Image: <description>] (Source: <url>)`).
