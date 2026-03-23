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

- [ ] **Implement Active Forgetting & Correction Mechanisms**
  - **Context:** While passive importance decay is implemented via background workers, there is no explicit way for users or agents to instantly delete or overwrite a specific memory (e.g., handling contradictions, right-to-be-forgotten, privacy requests).
  - **Proposed Solutions:**
    - [ ] **Expose Hard Delete API:** Add an HTTP endpoint (e.g., `DELETE /v1/users/:user_id/memories/:id`) in `memorose-server` to allow explicit removal of a memory unit across all storage layers (KV, LanceDB, Tantivy).
    - [ ] **Contradiction Resolution via Retrieval-Augmented Consolidation (RAC):**
        - Enhance the L1 consolidation worker to perform a "Self-Query" before saving new facts.
        - [ ] **Step 1:** Extract key entities from new events and query the existing L1 memory store for related historical facts.
        - [ ] **Step 2:** Pass the new event and retrieved historical facts to an LLM "Arbitrator" prompt to detect contradictions or updates.
        - [ ] **Step 3:** Parse structured JSON actions (e.g., `{"type": "OBSOLETE", "target_unit_id": "..."}`) from the LLM output.
        - [ ] **Step 4:** Execute "Logical Tombstoning" on outdated memory units (e.g., setting `importance` to `0.0` or linking via an `obsoleted_by` graph edge) to immediately remove them from future retrieval contexts.

- [ ] **Enhance Multimodal Asset Representation in Retrieval**
  - **Context:** While the engine handles multimodal data via native embeddings or fallback descriptions, the retrieved `MemoryUnit` should explicitly expose the generated description (e.g., text from STT or image caption) alongside the original source URL, so the LLM can leverage both the raw link and the textual context.
  - **Proposed Solutions:**
    - [ ] **Extend Asset Struct:** Add a `description: Option<String>` field to the `Asset` struct in `memorose-common`.
    - [ ] **Store Descriptions:** Update `worker.rs` (in `extract_assets_from_event` or the embedding pipeline) to persist the result of `describe_image`, `describe_video`, or `transcribe` directly into the `Asset`'s metadata or description field.
    - [ ] **Enrich Retrieval Formatting:** Ensure that when a `MemoryUnit` is retrieved, its formatted text block (passed to the LLM) explicitly appends multimodal descriptions and source URLs (e.g., `[Image: <description>] (Source: <url>)`).
