# Memorose TODOs

## Architecture & Integration

- [x] **Implement Native Gateway / Sidecar Injection Pattern for Memory**
  - **Context:** Relying solely on MCP (Model Context Protocol) for memory integration leads to high latency (extra RTT), wasted tokens (tool call + redundant context generation), and unpredictable context window usage. Memorose should offer a native integration path.
  - **Current Status:** Core delivery is complete. Token-budget-aware retrieval, prompt-ready context assembly, `/v1/memory/context` sidecar access, and adaptive L1/L2/L3 compression fallback are all implemented. Remaining follow-up, if any, is productization polish rather than a missing architecture capability.
  - **Proposed Solutions:**
    1.  **Strict Token-Aware Retrieval (Gateway Pattern):**
        - [x] Create a `tokenizer` utility module in `memorose-common` with a `count_tokens(text: &str) -> usize` function.
        - [x] Add token-budget-aware retrieval entry points in `memorose-core`.
        - [x] Implement a dynamic truncation loop during ranked result assembly: accumulate estimated token counts and stop when the budget is reached.
        - [x] Expose the budget parameter via HTTP headers (`X-Memory-Budget`) and request payload in `memorose-server`.
    2.  **Context-as-a-Service (Sidecar Pattern):**
        - [x] Expose a `/v1/memory/context` endpoint.
        - [x] Clients query this endpoint with their input and token limits before calling the LLM.
        - [x] Memorose returns a condensed prompt-ready context block in `text` or `xml`, along with token-usage metadata and included-hit metadata so the caller can prepend it deterministically.
    3.  **Adaptive Compression via L1-L3 Layers:**
        - [x] Implement adaptive fallback logic based on the token budget.
        - [x] If token budget is large: prefer detailed L1 procedural/factual memory formatting.
        - [x] If token budget is extremely small: prioritize dense L2/L3 context before falling back to L1, while enforcing the final token budget during response assembly.

- [x] **Implement Active Forgetting & Correction Mechanisms**
  - **Context:** While passive importance decay is implemented via background workers, there is no explicit way for users or agents to instantly delete or overwrite a specific memory (e.g., handling contradictions, right-to-be-forgotten, privacy requests).
  - **Current Status:** Core product behavior is complete. Hard delete, structured correction actions, logical tombstoning, correction guards, slot-aware candidate retrieval, persisted-fact-aware candidate ranking, pre-store RAC reconciliation in the L1 consolidation worker, manual RAC review APIs, and a config-driven hybrid fact extractor are all in place. The remaining open work here is long-tail quality hardening only: more end-to-end eval coverage, more noisy/colloquial samples, and broader language/generalization expansion.
  - **Proposed Solutions:**
    - [x] **Expose Hard Delete API:** Add an HTTP endpoint (e.g., `DELETE /v1/users/:user_id/memories/:id`) in `memorose-server` to allow explicit removal of a memory unit across all storage layers (KV, LanceDB, Tantivy).
    - [x] **Contradiction Resolution via Retrieval-Augmented Consolidation (RAC):**
      - Enhance the L1 consolidation worker to perform a "Self-Query" during consolidation so contradiction handling happens before the new fact becomes part of the stable retrieval set.
        - [x] **Step 1:** Extract key entities from new events and query the existing L1 memory store for related historical facts.
          - Step 1 is complete on the mainline: config-driven hybrid extraction, multi-fact segmentation, persisted extracted-fact reuse, mixed forget/update handling, noisy/non-assertive filtering, quote/attribution-aware subject carry, and multilingual first-pass coverage (`zh/en/es/ja/fr/de/pt`) are already in place. Remaining follow-up is long-tail hardening only, not a missing capability.
        - [x] **Step 2:** Pass the new event and retrieved historical facts to an LLM "Arbitrator" prompt to detect contradictions or updates.
        - [x] **Step 3:** Parse structured JSON actions (e.g., `{"target_id": "...", "action": "OBSOLETE"}`) from the LLM output.
          - Current action set also supports `REAFFIRM` and `IGNORE` in addition to `OBSOLETE` and `CONTRADICTS`, and `OBSOLETE` now has a stricter guard with compatibility checks, minimum confidence, and target recency checks before tombstoning.
        - [x] **Step 4:** Execute logical tombstoning on outdated memory units and attach correction relations (`EvolvedTo` / `Contradicts`) so outdated facts are removed from future retrieval contexts while preserving traceability.
          - `OBSOLETE` is now guarded so only compatible fact slots can overwrite prior memories.

- [x] **Enhance Multimodal Asset Representation in Retrieval**
  - **Context:** While the engine handles multimodal data via native embeddings or fallback descriptions, the retrieved `MemoryUnit` should explicitly expose the generated description (e.g., text from STT or image caption) alongside the original source URL, so the LLM can leverage both the raw link and the textual context.
  - **Proposed Solutions:**
    - [x] **Extend Asset Struct:** `Asset` already carries `description: Option<String>` in `memorose-common`.
    - [x] **Store Descriptions:** The worker persists image/video/audio descriptions/transcripts directly onto `Asset.description`.
    - [x] **Enrich Retrieval Formatting:** Retrieval/dashboard formatting now includes multimodal descriptions and source references in the assembled memory context.
