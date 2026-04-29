# Memorose Self-Growing Memory Roadmap

This roadmap turns Memorose from a durable memory runtime into a self-growing, self-correcting knowledge network while keeping the core system predictable, auditable, and cost-controlled.

The plan is intentionally staged. The early phases improve correctness and cost. Later phases add proactive behavior only after provenance, review, and policy controls exist.

## Principles

- **Core memory must stay trustworthy.** Model-generated insights, web research, and inferred relationships need provenance and review before they become durable memory.
- **User input is not the same as model inference.** Every memory artifact should carry a source type and trust level.
- **Proactive behavior must be opt-in.** Background research and insight generation should never silently mutate personal memory by default.
- **Cost must be explicit.** Any LLM, embedding, web research, or graph analysis loop needs budgets, rate limits, and observability.
- **Current state and history are both valuable.** Conflicting facts should not always be overwritten; some should become an explicit timeline of belief or context changes.

## Current Baseline

Memorose already has several building blocks needed for this roadmap:

- L0-L3 layered memory.
- Hybrid retrieval with vector, full-text, graph expansion, and reranking.
- Organization knowledge records.
- Semantic update / forget preview and execute flows.
- Correction actions such as obsolete, contradicts, reaffirm, and ignore.
- Arbitrator support for conflict resolution and dynamic reranking.
- Dashboard surfaces for memory, graph, corrections, and cluster state.
- LanceDB index guardrails and startup degradation behavior.

The roadmap should extend these instead of creating a parallel memory system.

## Phase 0: Product And Safety Vocabulary

Goal: define the shared language used by core, API, dashboard, docs, and future plugins.

### Deliverables

- Define memory artifact categories:
  - `event`: raw user/tool/application input.
  - `memory_unit`: durable L1/L2/L3 memory.
  - `insight`: model-generated pattern or graph-derived connection.
  - `research_note`: externally sourced or tool-generated knowledge.
  - `correction`: update, obsolete, contradicts, reaffirm, ignore.
- Define source types:
  - `user_input`
  - `agent_observation`
  - `tool_result`
  - `model_inference`
  - `web_research`
  - `manual_review`
- Define trust levels:
  - `direct`
  - `derived`
  - `external`
  - `unverified`
  - `reviewed`
- Define lifecycle states:
  - `active`
  - `historical`
  - `obsolete`
  - `contested`
  - `hidden`
  - `deleted`

### Implementation Notes

- Start with schema constants and docs before changing storage behavior.
- Prefer additive metadata fields over destructive migrations.
- Update README and docs only after the terms are reflected in code or planned API contracts.

### Exit Criteria

- A short architecture note explains artifact type, source type, trust level, and lifecycle state.
- New roadmap terms are used consistently in issues and implementation plans.

## Phase 1: Content Hash And Drift Detection

Goal: reduce duplicate event ingestion, unnecessary embedding work, and repeated memory pollution.

This is the lowest-risk, highest-return feature from the llm_wiki-style ideas.

### Core Behavior

- Canonicalize incoming event payloads before hashing.
- Compute `content_hash = sha256(canonical_payload)`.
- Store hash metadata on L0 events.
- Add a dedupe index scoped by:
  - `user_id`
  - `stream_id`
  - `namespace`
  - `domain`
  - `content_hash`
- On duplicate input:
  - skip embedding and consolidation by default,
  - update `last_seen_at`,
  - increment `seen_count`,
  - optionally append a lightweight reference event if audit mode requires it.

### API Surface

- Add optional ingest flags:
  - `dedupe: true | false`
  - `dedupe_scope: "stream" | "user" | "organization"`
  - `on_duplicate: "skip" | "touch" | "append_reference" | "force"`
- Return duplicate status:
  - `accepted`
  - `duplicate`
  - `content_hash`
  - `original_event_id`
  - `seen_count`

### Dashboard

- Show duplicate counts in event and memory detail views.
- Add a filter for high-duplicate streams.
- Add dashboard metrics:
  - duplicate events skipped,
  - embedding calls avoided,
  - consolidation jobs avoided.

### Tests

- Same payload in same stream dedupes.
- Same payload in different stream respects `dedupe_scope`.
- Whitespace / key order changes in JSON do not create false negatives.
- `force` bypasses dedupe.
- Binary and multimodal payloads hash stable asset bytes or canonical asset references.

### Exit Criteria

- Duplicate text payloads do not trigger duplicate embedding/consolidation by default.
- Operators can see dedupe savings.
- Existing ingest behavior remains available through `force`.

## Phase 2: Provenance And Review Queue

Goal: make proactive memory safe before adding proactive intelligence.

### Core Behavior

- Add provenance metadata to derived artifacts:
  - `source_event_ids`
  - `source_memory_ids`
  - `source_urls`
  - `source_tool`
  - `generated_by_model`
  - `generated_at`
  - `confidence`
  - `trust_level`
- Add review states:
  - `pending_review`
  - `approved`
  - `rejected`
  - `auto_approved`
- Do not publish model-generated insights or research notes to active retrieval by default unless policy allows it.

### API Surface

- Add review endpoints:
  - `GET /v1/users/:user_id/reviews`
  - `POST /v1/users/:user_id/reviews/:review_id/approve`
  - `POST /v1/users/:user_id/reviews/:review_id/reject`
- Reuse dashboard correction review patterns where possible.

### Dashboard

- Add a unified review inbox with tabs:
  - corrections,
  - graph insights,
  - research notes,
  - duplicate clusters.
- Each item must show:
  - proposed memory text,
  - source evidence,
  - confidence,
  - expected retrieval impact,
  - approve/reject controls.

### Tests

- Pending review items do not affect normal retrieval unless explicitly included.
- Approved insight becomes retrievable.
- Rejected insight remains auditable but hidden from retrieval.

### Exit Criteria

- Every derived artifact can answer: where did this come from, who/what generated it, and who approved it?

## Phase 3: Conflict Timeline

Goal: turn existing contradiction and correction machinery into an explicit product capability.

### Core Behavior

- Treat conflicts as structured memory relationships, not just cleanup operations.
- Add or standardize relation types:
  - `contradicts`
  - `supersedes`
  - `reaffirms`
  - `historical_version_of`
  - `derived_from`
- Preserve historical memory when it represents useful temporal context.
- Mark current preferred memory separately from historical or contested memory.

### Retrieval Policy

- Default retrieval should prefer current active memory.
- If relevant, return historical context as separate metadata:
  - `current`
  - `historical`
  - `contested`
  - `resolution_reason`
- Arbitrator mode should synthesize a concise narrative when multiple conflicting memories are relevant.

### API Surface

- Extend retrieve response with optional conflict metadata.
- Add a timeline endpoint:
  - `GET /v1/users/:user_id/memories/:memory_id/timeline`
- Add query flag:
  - `include_history: true | false`
  - `include_conflicts: true | false`

### Dashboard

- Add a conflict timeline panel:
  - before / after facts,
  - timestamps,
  - source events,
  - correction action,
  - confidence,
  - reviewer.
- Show “current view” vs “historical context”.

### Tests

- Residence update marks old residence obsolete or historical.
- Direct contradiction creates a `contradicts` relation.
- Medium-confidence obsolete action downgrades to relation-only behavior.
- Retrieval returns current fact first.
- `include_history=true` returns the older fact with state metadata.

### Exit Criteria

- Users can inspect how a belief or preference changed over time.
- Conflicting facts no longer silently compete as equally current retrieval hits.

## Phase 4: Proactive Graph Insights

Goal: detect useful hidden relationships across memories without turning the system into an uncontrolled agent.

### Core Behavior

- Periodically scan bounded memory windows for relationship opportunities.
- Candidate generation should use cheap signals first:
  - shared entities,
  - recurring topics,
  - temporal proximity,
  - graph neighborhoods,
  - lexical overlap,
  - embedding similarity.
- LLM synthesis should run only after cheap candidate filtering.
- Generated insights enter review by default.

### Insight Types

- `cross_time_connection`: two old topics may now be related.
- `recurring_pattern`: repeated user preference or workflow pattern.
- `missing_link`: two memories should be connected by an edge.
- `workflow_inference`: repeated steps form a reusable procedure.
- `organization_candidate`: private insight may be useful as org knowledge.

### API Surface

- Add insight preview endpoints:
  - `POST /v1/users/:user_id/insights/preview`
  - `GET /v1/users/:user_id/insights`
- Add execution endpoint:
  - `POST /v1/users/:user_id/insights/:insight_id/approve`
- Policy options:
  - `max_candidates`
  - `max_llm_calls`
  - `min_confidence`
  - `review_required`

### Dashboard

- Add “Insights” inbox:
  - title,
  - explanation,
  - source memory graph,
  - approve/reject,
  - “turn into organization knowledge”.

### Tests

- Insight generation respects cost limits.
- Same insight is not repeatedly generated.
- Insight source memories remain traceable.
- Unapproved insight does not affect retrieval.

### Exit Criteria

- Memorose can propose useful graph insights while preserving user control and evidence.

## Phase 5: Knowledge Gap Detection

Goal: identify incomplete knowledge areas without automatically fetching or storing external content.

### Core Behavior

- Detect gaps from:
  - repeated queries with low recall,
  - memory clusters with missing key fields,
  - unresolved TODO-like user statements,
  - graph nodes with high centrality but low evidence,
  - contradictions with no resolution.
- Create `knowledge_gap` records with:
  - topic,
  - missing fields,
  - evidence,
  - recommended next action,
  - priority,
  - scope.

### API Surface

- Add endpoints:
  - `GET /v1/users/:user_id/knowledge-gaps`
  - `POST /v1/users/:user_id/knowledge-gaps/:gap_id/dismiss`
  - `POST /v1/users/:user_id/knowledge-gaps/:gap_id/research`

### Dashboard

- Add “Knowledge gaps” view:
  - topic,
  - missing information,
  - why it matters,
  - source evidence,
  - recommended action.

### Tests

- Low-recall repeated queries create a gap candidate.
- Dismissed gaps do not reappear immediately.
- Gap records include provenance.

### Exit Criteria

- Memorose can say “I do not know enough about this topic” with evidence, without fabricating missing knowledge.

## Phase 6: Connector Framework And Integrations

Goal: let Memorose ingest trusted external context from user-approved systems without turning every integration into custom ingestion code.

Connectors should feed the same event, provenance, hash dedupe, review, and policy pipeline as native API ingestion.

### Connector Framework

- Define a connector trait / interface:
  - `list_sources`
  - `sync_incremental`
  - `fetch_item`
  - `normalize_item`
  - `revoke`
  - `health_check`
- Standardize connector output:
  - source system,
  - external ID,
  - content hash,
  - last modified timestamp,
  - author / actor,
  - permissions,
  - source URL,
  - raw metadata,
  - normalized event payload.
- Use content hash dedupe before embedding or consolidation.
- Preserve provenance from connector item to memory unit.
- Support connector-specific namespaces.

### Initial Connector Targets

- **GitHub**:
  - issues,
  - pull requests,
  - discussions,
  - repository README/docs,
  - commit messages.
- **Documentation sites**:
  - sitemap ingestion,
  - markdown pages,
  - API docs,
  - changelogs.
- **Local files**:
  - markdown,
  - text,
  - PDFs when parser support exists,
  - directory watch mode.
- **Chat systems**:
  - Slack,
  - Discord,
  - Feishu/Lark.
- **Support / CRM systems**:
  - Zendesk,
  - Intercom,
  - Linear,
  - HubSpot,
  - Salesforce.
- **Browser / web clipper**:
  - manually saved pages,
  - selected text,
  - screenshots,
  - source URL and timestamp.

### API Surface

- Connector management:
  - `GET /v1/connectors`
  - `POST /v1/connectors`
  - `GET /v1/connectors/:connector_id`
  - `DELETE /v1/connectors/:connector_id`
- Sync jobs:
  - `POST /v1/connectors/:connector_id/sync`
  - `GET /v1/connectors/:connector_id/jobs`
  - `GET /v1/connectors/:connector_id/jobs/:job_id`
  - `POST /v1/connectors/:connector_id/jobs/:job_id/cancel`
- Source browsing:
  - `GET /v1/connectors/:connector_id/sources`
  - `GET /v1/connectors/:connector_id/items/:external_id`

### Dashboard

- Connector setup wizard.
- OAuth / token status.
- Last sync, next sync, and error state.
- Per-connector budget and rate-limit display.
- Item preview before ingestion.
- Sync logs with skipped duplicate counts.
- Revoke connector and delete imported memory controls.

### Permissions And Safety

- Connector credentials must be encrypted at rest.
- Every imported memory must preserve source permissions.
- Connector memory should not cross user or organization boundaries unless explicitly promoted.
- Deleting or revoking a connector should support:
  - stop future sync,
  - hide imported memories,
  - hard-delete imported memories.
- External source content must remain distinguishable from user-authored memory.

### Incremental Sync

- Store connector cursors:
  - last sync timestamp,
  - external page token,
  - etag / revision ID,
  - last processed ID.
- Support tombstones for deleted external items.
- Re-ingest changed items only when hash or revision changes.
- Track sync metrics:
  - scanned,
  - imported,
  - updated,
  - skipped duplicate,
  - failed,
  - deleted.

### Tests

- Connector sync is idempotent.
- Changed external item updates memory provenance.
- Deleted external item creates a tombstone or hidden state according to policy.
- Permission metadata is preserved.
- Connector credentials are not returned by read APIs.
- Duplicate connector items do not trigger duplicate embeddings.
- Failed sync can resume from last safe cursor.

### Exit Criteria

- At least one repository/docs connector and one local-file connector can ingest incrementally with provenance, dedupe, and dashboard visibility.

## Phase 7: Optional Deep Research Plugin

Goal: let Memorose fill approved knowledge gaps through an opt-in research workflow.

This should be a plugin or separate worker, not default core behavior.

### Plugin Behavior

- Trigger only from approved knowledge gaps.
- Use configured research providers and approved connectors:
  - web search,
  - crawler,
  - internal docs connector,
  - repository connector,
  - user-provided sources.
- Generate `research_note` artifacts.
- Store source URLs, retrieval timestamps, excerpts, and confidence.
- Require review before promotion into active memory.

### Safety Requirements

- No silent writes to active personal memory.
- No source-less generated knowledge.
- No unlimited crawling.
- Respect robots, rate limits, and user-configured domains.
- Separate external knowledge from user-owned memory.

### API Surface

- Plugin configuration:
  - allowed domains,
  - blocked domains,
  - max pages,
  - max tokens,
  - provider,
  - review requirement.
- Research job endpoints:
  - `POST /v1/research/jobs`
  - `GET /v1/research/jobs/:job_id`
  - `POST /v1/research/jobs/:job_id/cancel`

### Dashboard

- Research job status.
- Source list and extracted claims.
- Promote to memory / organization knowledge controls.

### Tests

- Plugin cannot run without explicit policy.
- Research notes are hidden until approved.
- Source URLs are required.
- Budget limits stop jobs deterministically.

### Exit Criteria

- Memorose can support active research workflows without compromising memory trust.

## Phase 8: Local Privacy And Encryption

Goal: improve Memorose as a personal second-brain runtime for sensitive memory.

### Core Security Features

- At-rest encryption for local data directories.
- Per-user or per-organization encryption keys.
- Encrypted export / import archive.
- Secure key rotation plan.
- Clear separation between dashboard auth secret and memory encryption keys.

### Local-First Mode

- Document fully local deployment with:
  - local LLM provider,
  - local embedding provider,
  - local-only network policy,
  - disabled external research plugins.
- Provide a configuration preset:
  - `memorose local init`
  - or `config.local.toml`.

### Dashboard

- Show encryption status.
- Show provider mode:
  - cloud LLM,
  - local LLM,
  - mixed.
- Warn when external research or cloud LLMs are enabled.

### Tests

- Encrypted database cannot be opened without key.
- Key rotation preserves readable data.
- Export archive decrypts only with the expected key.
- Local mode blocks external research plugins by default.

### Exit Criteria

- Users can run Memorose with a clear local privacy posture.

## Phase 9: Policy Engine And Budgets

Goal: make background intelligence configurable and safe in production.

### Policy Dimensions

- Who can create derived memory.
- Which namespaces allow proactive insights.
- Which memory domains allow background jobs.
- Whether review is required.
- Which providers are allowed.
- Which connectors are allowed.
- Which connectors can write active memory.
- Maximum LLM calls per hour/day.
- Maximum embedding calls per hour/day.
- Maximum external research spend.
- Maximum connector sync volume.
- Data retention windows.

### API Surface

- `GET /v1/policies`
- `PUT /v1/policies`
- Per-user and per-organization overrides.

### Dashboard

- Policy editor.
- Cost and budget charts.
- Background job audit log.

### Tests

- Policies block disallowed background jobs.
- Budget exhaustion stops jobs.
- Connector policies block disallowed imports.
- Review-required policies prevent active retrieval from pending derived artifacts.

### Exit Criteria

- Proactive memory behavior is safe to enable selectively in real deployments.

## Phase 10: Product Packaging

Goal: expose the new capabilities without making Memorose feel complex.

### Documentation

- Update README with concise, accurate positioning.
- Add docs pages:
  - content hash dedupe,
  - conflict timeline,
  - graph insights,
  - knowledge gaps,
  - research plugin,
  - connectors,
  - local privacy mode,
  - policy engine.

### SDK

- Add helper methods:
  - `ingest(..., dedupe=True)`
  - `preview_update(...)`
  - `get_conflict_timeline(...)`
  - `list_insights(...)`
  - `approve_insight(...)`
  - `list_knowledge_gaps(...)`
  - `sync_connector(...)`
  - `run_research_job(...)`

### Website

- Add use cases:
  - self-growing memory,
  - research assistant memory,
  - connected knowledge base memory,
  - privacy-first local memory,
  - contradiction-aware personal memory.
- Add feature pages:
  - conflict timeline,
  - graph insights,
  - memory deduplication,
  - connectors,
  - local-first privacy.

### Release Criteria

- Docs explain what is default and what is opt-in.
- Dashboard exposes review and provenance.
- SDK supports common workflows.
- Benchmarks and cost metrics are reproducible.

## Suggested Execution Order

1. Phase 0: vocabulary and metadata model.
2. Phase 1: content hash dedupe.
3. Phase 2: provenance and review queue.
4. Phase 3: conflict timeline.
5. Phase 4: proactive graph insights.
6. Phase 5: knowledge gap detection.
7. Phase 6: connector framework and integrations.
8. Phase 7: optional deep research plugin.
9. Phase 8: local privacy and encryption.
10. Phase 9: policy engine and budgets.
11. Phase 10: packaging, docs, SDK, and website.

## Near-Term Milestone: v0.2 Self-Correcting Memory

Recommended first milestone:

- Content hash dedupe.
- Provenance fields for derived memory.
- Conflict timeline using existing correction actions.
- Dashboard timeline panel.
- README/docs update.

This milestone is small enough to ship without adding web research or autonomous background agents.

## Later Milestone: v0.3 Self-Growing Memory

Recommended second milestone:

- Proactive graph insight candidates.
- Review inbox for insights.
- Knowledge gap records.
- Connector framework design.
- Cost limits for background jobs.
- SDK helpers for insight approval.

This milestone introduces proactive behavior only after review and provenance are already reliable.

## Later Milestone: v0.4 Research-Aware Memory

Recommended third milestone:

- Optional deep research plugin.
- Repository/docs connector and local-file connector.
- Research job queue.
- Source-backed research notes.
- Promotion flow into user or organization memory.
- Local privacy mode documentation.

This milestone should stay opt-in and policy-gated.
