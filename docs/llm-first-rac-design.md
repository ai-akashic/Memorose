# Memorose LLM-First RAC 设计方案

本文档定义 Memorose 后续的 `RAC`（Retrieval-Augmented Consolidation）改造方向：从当前“规则优先 + LLM fallback”的混合实现，逐步收敛为“LLM 负责理解，代码负责约束与安全提交”的架构。

## 1. 目标

这次改造有三个核心目标：

- 提高覆盖度：
  - 更好支持中文、英文和长尾表达
  - 降低对硬编码 pattern 覆盖面的依赖
- 保持安全性：
  - 高风险动作不能只靠模型建议直接执行
  - 宁可漏改，不要误删或误 tombstone
- 提升可维护性：
  - 让 fact extraction、candidate retrieval、correction guard 分层清晰
  - 后续扩展 slot / 语言 / prompt 时不再污染主流程

## 2. 当前状态

当前实现已经具备以下能力：

- `fact_extraction` 模块负责：
  - rule-based fact detection
  - candidate focus term 构建
  - slot-aware candidate scoring
  - fact descriptor normalization
- `Arbitrator` 已支持：
  - `extract_memory_fact(...)`
  - `detect_memory_corrections(...)`
- `engine` 已支持：
  - candidate retrieval
  - `OBSOLETE / CONTRADICTS / REAFFIRM / IGNORE`
  - logical tombstone
  - correction relation (`EvolvedTo` / `Contradicts`)
  - slot compatibility guard

当前主要不足：

- 事实抽取仍偏规则和英文表达
- correction 仍在 memory store 之后触发，而不是 consolidation/save 前
- LLM 输出虽然有 JSON schema 约束，但还没有形成完整的“LLM-first + minimal guard”执行链

## 3. 设计原则

后续 RAC 采用以下原则：

- 模型负责理解：
  - 从自然语言中抽结构化 facts
  - 识别更新、否定、历史、补充、冲突
- 代码负责约束：
  - schema 校验
  - canonicalization
  - minimal guard
  - mutation commit
- 高风险动作必须审查：
  - `OBSOLETE`
  - logical tombstone
  - 任何会让旧 memory 从检索结果中消失或弱化的动作
- 不确定时降级：
  - `IGNORE`
  - `REAFFIRM`
  - 或保留旧事实并只建立冲突关系

## 4. 目标架构

### 4.1 流程总览

目标链路：

1. L0 events 被 consolidation worker 聚合
2. LLM 将聚合内容压缩为 L1 factual/procedural memory
3. 在 L1 factual memory 写入稳定检索集前，执行 RAC：
   - 抽取 facts
   - 召回候选旧记忆
   - 对候选做 correction arbitration
   - 通过 guard
   - 提交 mutation
4. mutation 提交后，最终写入新 memory 并建立图关系

### 4.2 分层职责

#### A. Fact Extraction Layer

职责：

- 把新记忆抽成 `facts[]`
- 支持多语言
- 输出结构化 schema

首选来源：

- LLM-first structured extraction

保留能力：

- `fact_extraction` 中的 rule table 作为：
  - fast path
  - fallback
  - canonicalization helper

#### B. Candidate Retrieval Layer

职责：

- 用新 facts 去召回可能被更新/冲突的旧 L1 factual memories
- 候选检索只负责“召回相关项”，不直接做状态变更

候选来源：

- full-text search
- recent L1 memory fallback
- slot-aware rerank

#### C. Correction Arbitration Layer

职责：

- 让 LLM 对：
  - `new facts`
  - `candidate old memories`
  - `candidate old facts`
  做语义判断

输出动作：

- `OBSOLETE`
- `CONTRADICTS`
- `REAFFIRM`
- `IGNORE`

#### D. Validation & Commit Layer

职责：

- 代码侧做最小必要的 guard
- 通过后才真正提交 tombstone / relation / cache invalidation

## 5. 数据契约

### 5.1 Fact Extraction Schema

目标让 LLM 输出严格 JSON，支持 0..N 条 facts：

```json
{
  "facts": [
    {
      "subject_type": "user|organization|agent|external",
      "subject_text": "Alice",
      "subject_key": "external:alice",
      "attribute": "residence|preference|employment|relationship|status|contact|ownership|skill|schedule",
      "value": "Beijing",
      "canonical_value": "beijing",
      "change_type": "update|contradiction|negation|historical|reaffirm|addition",
      "confidence": 0.93,
      "evidence_span": "now live in Beijing"
    }
  ]
}
```

说明：

- `subject_key` 可以由模型提供，也可以由代码重算
- `canonical_value` 可以由模型提供，但最终以代码 canonicalization 为准
- `evidence_span` 仅用于 debug / audit，不作为主逻辑判断依据

### 5.2 Correction Action Schema

对候选旧记忆的 LLM 输出：

```json
{
  "actions": [
    {
      "target_id": "uuid",
      "action": "OBSOLETE|CONTRADICTS|REAFFIRM|IGNORE",
      "reason": "Residence updated from Shanghai to Beijing",
      "confidence": 0.94
    }
  ]
}
```

## 6. Minimal Guard

规则在新方案中只保留四件事：

### 6.1 Schema 校验

- 输出必须是合法 JSON
- 枚举值必须在允许集合内
- 缺字段、脏字段、额外解释文本都直接拒绝

### 6.2 Canonicalization

- email 小写
- phone 数字化
- city / company / title / schedule 做稳定化
- `subject_key` 做统一化

### 6.3 高风险动作 Guard

高风险动作主要是 `OBSOLETE`。

执行前至少检查：

- `subject` 是否兼容
- `subject_key` 是否兼容
- `attribute` 是否兼容
- `change_type` 是否允许 replacement
- `confidence` 是否达到阈值
- target 是否早于新 memory

建议：

- `OBSOLETE` guard 比 `CONTRADICTS` 更严格
- guard 失败时降级为：
  - `IGNORE`
  - 或只保留 graph relation，不 tombstone

### 6.4 Fast Path / Fallback

规则仍保留，但只做：

- 高频模式快速命中
- LLM unavailable 时的 degrade 模式
- regression baseline

规则不再承担“覆盖全部语言表达”的主任务。

## 7. Consolidation 前移方案

### 7.1 当前问题

当前 correction 发生在 memory 已写入后：

- `worker.rs` 负责 consolidation pipeline：`crates/memorose-core/src/worker.rs`
- `engine.rs` 中 `reconcile_conflicting_memory_unit(...)` 当前在 stored memory 路径触发：`crates/memorose-core/src/engine.rs`

这会带来两个问题：

- 新 memory 先进入稳定检索集，再回头修正旧 memory
- correction 更像事后补救，不像 consolidation 的一部分

### 7.2 目标时序

目标改为：

1. compression 产出 summary / L1 candidate
2. 先做 fact extraction
3. 用 extracted facts 检索旧 L1 factual memory
4. 运行 correction arbitration
5. 提交 mutation
6. 再正式写入新 memory

### 7.3 兼容迁移策略

建议分两阶段：

- Phase 1：
  - 保留现有 post-store reconciliation
  - 新增 pre-store RAC，并打日志比较两者差异
- Phase 2：
  - pre-store RAC 稳定后，移除 post-store 主路径
  - post-store 只保留 repair / backfill 能力

## 8. 分阶段实施

### Phase 1：Schema 与模块边界

目标：

- 明确 `facts[]` schema
- 明确 `actions[]` schema
- 为 `Arbitrator` 提供 LLM-first structured extraction 接口

任务：

- 扩展 `extract_memory_fact(...)` 为 `extract_memory_facts(...)`
- 增加 schema parsing / validation
- 保留 `fact_extraction` 规则表作为 fallback

### Phase 2：Pre-Store RAC

目标：

- 将 RAC 从 post-store 前移到 consolidation pipeline

任务：

- 在 `worker.rs` 的 pipeline batch 中插入 pre-store reconciliation hook
- 让 `process_pipeline_batch(...)` 先执行 RAC mutation，再写入 L1

### Phase 3：LLM-First Candidate & Correction

目标：

- 让 LLM 主导 multi-fact extraction 和 correction arbitration

任务：

- 从 `facts[]` 构建 query plan
- 把 candidate 记忆的 normalized fact 一并送进 arbitration prompt
- 对 `OBSOLETE` 引入更严格阈值

### Phase 4：评估与观测

目标：

- 控制模型漂移
- 量化误判/漏判

任务：

- 构建中英 fixture
- 增加 metrics：
  - extraction success rate
  - action distribution
  - guard reject rate
  - tombstone count
  - contradiction count
- 增加 sampled audit logging

## 9. 测试策略

需要补三类测试：

### 9.1 Fact Extraction Fixtures

覆盖：

- 中文 / 英文
- 单事实 / 多事实
- 更新 / 历史 / 否定 / 补充
- 无主语 / 外部主体 / organization 主体

### 9.2 Guard Tests

覆盖：

- subject mismatch
- attribute mismatch
- unsafe `change_type`
- low confidence `OBSOLETE`
- invalid JSON / invalid enum

### 9.3 End-to-End RAC Tests

覆盖：

- consolidation -> extraction -> candidate retrieval -> correction -> commit
- pre-store mutation 结果和检索可见性
- cache / graph side effects

## 10. 与当前代码的映射

当前建议的主要改动入口：

- `crates/memorose-core/src/fact_extraction.rs`
  - 保留 rule table、canonicalization、guard helper
- `crates/memorose-core/src/arbitrator.rs`
  - 扩展为 LLM-first structured fact extraction / correction schema
- `crates/memorose-core/src/engine.rs`
  - 保留 candidate retrieval、mutation commit、guard
- `crates/memorose-core/src/worker.rs`
  - 插入 pre-store RAC hook

## 11. 非目标

这轮改造暂不追求：

- 一次性支持所有语言
- 一次性覆盖所有 slot
- 完全移除规则
- 完全依赖模型决定 destructive mutation

## 12. 建议的下一步

建议直接按下面顺序推进：

1. `Arbitrator` 支持 `extract_memory_facts(...) -> facts[]`
2. 为 facts / actions 引入统一 schema parser
3. 在 `worker` consolidation pipeline 中加入 pre-store RAC hook
4. 增加中文/英文 fixture 与 guard tests

如果后续继续实现，推荐先做第 1 步和第 2 步，因为这两步决定了后面整个 LLM-first RAC 的输入输出边界。
