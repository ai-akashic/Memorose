# Memorose 记忆领域与共享投影重构计划

本文档用于记录当前关于 `Agent / User / Organization` 记忆定义、共享投影机制、以及后续内核调整方向的讨论结果，作为后续修改计划的基线。

## 1. 当前重构方向

当前讨论已经形成如下方向：

- 保留三类核心记忆域：
  - `Agent`
  - `User`
  - `Organization`
- 去掉：
  - 旧的第四记忆域
- 弱化并逐步移除：
  - `应用作用域` 作为核心记忆建模维度

这意味着后续内核和文档都应尽量从“四域模型”收敛到“三域模型”。

## 2. 当前项目中的记忆领域定义

当前内核里，`L0-L3` 与 `Agent / User / Organization` 是两套正交概念：

- `L0-L3` 表示抽象层级
- `Agent / User / Organization` 表示归属域和共享边界

当前代码中的 `MemoryDomain` 仍然定义为：

- `Agent`
- `User`
- 旧的第四域
- `Organization`

实现位置：

- `MemoryDomain` 枚举：`crates/memorose-common/src/lib.rs`
- `MemoryUnit::infer_domain()`：`crates/memorose-common/src/lib.rs`
- `MemoryUnit::build_namespace_key()`：`crates/memorose-common/src/lib.rs`

### 2.0 三类记忆正式定义对照表

| Memory type | 核心问题 | 归属对象 | 建议主作用域 | 应存什么 | 不应存什么 |
|------------|----------|----------|--------------|----------|------------|
| **Agent Memory** | 这个 agent 怎么做事？ | 单个 agent | `org_id + agent_id` | 工具使用模式、执行策略、恢复路径、规划启发、可复用 procedural memory | 用户个体偏好、用户身份事实、只对单一用户成立的局部上下文 |
| **User Memory** | 这个用户是谁、要什么？ | 单个 user | `org_id + user_id` | 偏好、身份事实、长期目标、个体约束、长期稳定上下文 | agent 私有执行习惯、组织共享规范、只能在组织级复用的公共知识 |
| **Organization Memory** | 组织内什么知识应该被共享复用？ | 整个 organization | `org_id` | 由用户记忆演化出的去用户化知识、组织术语、组织级最佳实践、共享流程、稳定洞察 | 原始事件、个体敏感事实、单用户偏好、agent 私有技巧 |

这张表对应的收敛原则：

- `Agent Memory` 是 agent-centric
- `User Memory` 是 user-centric
- `Organization Memory` 是 shared-centric
- 任何介于 `Agent` 和 `User` 之间的局部工作上下文，后续如有必要，应单独建模为 interaction/session memory，而不是混入这三类主域

### 2.1 Agent Memory

当前 README 语义：

- 某个 Agent 是怎么做事的记忆
- 典型内容包括：工具使用模式、执行轨迹、恢复策略、规划启发、过程反思

当前实现状态：

- 只有当 `memory_type = Procedural` 且 `agent_id` 存在时，才会推断为 `Agent`
- 当前 `namespace_key` 形式为：
  - `agent:{user_id}:{应用作用域}:{agent_id}`

当前问题：

- 当前实现更接近 `Agent-for-User Memory`
- 它不是纯 `agent_id` 级别记忆，而是按用户切碎后的 agent procedural memory

当前目标定义：

- `Agent Memory` 应表示“某个 agent 如何做事”
- 不应默认按 `user_id` 切分

建议作用域模型：

- 推荐：`org_id + agent_id`
- 最少：`agent_id`

### 2.2 User Memory

当前 README 语义：

- 系统应该记住这个用户什么
- 典型内容包括：偏好、身份、目标、约束、长期个人上下文、用户事实

当前实现状态：

- 只要不满足 `Agent` 的判定条件，就会默认落入 `User`
- 当前 `namespace_key` 形式为：
  - `user:{user_id}`

当前结论：

- `User Memory` 是当前实现里最稳定、最原生的一类记忆域

当前目标定义：

- `User Memory` 应表示“这个用户是谁、要什么、长期稳定特征是什么”
- 它应是跨 agent 可复用的用户长期记忆
- 不应默认带 `agent_id`

建议作用域模型：

- 推荐：`org_id + user_id`
- 最少：`user_id`

### 2.3 Organization Memory

当前讨论后的目标定义：

- `Organization Memory` 应该是在组织范围内共享的公共知识
- 它的来源不是原始事件直接共享
- 它应主要由 `User Memory` 持续演化生成
- 它的目标是形成一套与具体用户无关的知识库

这类知识可以包括：

- 组织政策
- 组织术语
- 组织级最佳实践
- 组织共享流程
- 从大量用户案例中沉淀出的稳定洞察

它不应包含：

- 具体用户身份
- 个体偏好
- 个体敏感事实
- 只对单一用户成立的局部上下文

当前实现状态：

- `namespace_key` 形式为：
  - `org:{org_id}`
- 当前主要还是共享投影域
- 是否生成和消费，目前受 `SharePolicy` 控制

当前目标作用域模型：

- `org_id`

## 3. 旧第四域与应用作用域的处理结论

### 3.1 去掉旧第四域概念

当前讨论结论：

- 旧第四域与 `Agent Memory`、`Organization Memory` 边界重叠严重
- 很多所谓应用级知识，本质上不是 agent 私有，就是组织共享
- 因此旧第四域作为独立记忆概念位，当前判断为冗余

当前决定：

- 去掉旧第四域这个概念
- 将 `Organization Memory` 作为唯一共享层

### 3.2 去掉 应用作用域 作为核心记忆建模维度

当前讨论结论：

- `应用作用域` 在当前实现中参与了 scope、过滤和共享策略
- 但在新的三域模型里，它不再承担必要的核心语义
- 如果记忆模型收敛为 `Agent / User / Organization`，则 `应用作用域` 也不再需要作为一等记忆建模维度

当前决定：

- 去掉 `应用作用域` 在记忆模型中的核心地位
- 后续评估是否完全移除字段、索引、接口以及文档中的依赖

## 4. 当前共享投影机制的定义

### 4.0 概念区分：演化 / 投影 / 共享

为了避免后续设计和文档混用术语，这里先明确三个概念：

#### 演化

- 指记忆内容本身如何变化
- 关注的是：
  - 这条记忆会变成什么
- 在 Memorose 中主要包括：
  - 降噪
  - 压缩
  - 对齐
  - 关联
  - 反思
  - 遗忘

结论：

- 演化解决的是“记忆内容和结构如何变化”

#### 投影

- 指记忆作用域如何变化
- 关注的是：
  - 这条记忆应该进入哪个归属域
  - 这条记忆应该服务谁

在当前讨论后的模型中：

- 投影主要是指：
  - 从本地原生记忆进入组织共享记忆层的发布机制

结论：

- 投影解决的是“记忆进入哪个共享边界”

#### 共享

- 指投影完成之后的访问语义
- 关注的是：
  - 谁可以读取
  - 谁可以复用
  - 在什么边界内共享

在当前模型里：

- 共享主要指：
  - 在 `org_id` 范围内，多个用户和多个 agent 对组织记忆的复用

结论：

- 共享解决的是“谁可以使用这条记忆”

#### 三者关系

统一口径如下：

- 演化先于投影
- 投影承载共享

也就是说：

- `User Memory` 先经过演化
- 再将满足条件的知识投影到 `Organization Memory`
- 最后由组织内其他参与者共享消费

当前讨论结论：

- 共享投影不是把原记忆“移动”到共享域
- 共享投影也不是一个轻量引用
- 它本质上是：
  - 把本地原生 `MemoryUnit` 克隆成一个新的 `MemoryUnit`
  - 重新分配 `id`
  - 改写 `domain`
  - 重算 `namespace_key`
  - 来源关系由 canonical organization knowledge 的 membership/source 结构记录

当前代码行为：

- 投影对象来自 `MemoryUnit`
- 不来自 `L0 Event`
- 当前只对本地域的 `L1/L2` 生效

这意味着：

- `L0` 不参与共享投影
- `L1/L2` 当前可被投影到共享域
- `L3` 不在当前共享投影路径中

## 5. 已确认的问题

### 5.1 Agent Memory 的主作用域定义偏窄

当前实现：

- `Agent Memory` 的主作用域是 `user_id + 应用作用域 + agent_id`

当前讨论结论：

- 如果产品概念是“这个 agent 学会如何做事”，那么主作用域不应默认带 `user_id`
- `应用作用域` 也不应继续作为核心建模维度

建议方向：

- 将当前 `Agent Memory` 从“用户切分后的 agent 经验”改为“agent 级 procedural memory”
- 用户相关经验应沉淀到 `User Memory`，或单独定义成 interaction/session memory，而不是进入 `Agent Memory` 主域

### 5.2 Organization Memory 需要从“共享桶”收紧成“演化后的组织知识库”

当前讨论结论：

- `Organization Memory` 不能只是泛泛的“组织共享知识”
- 它应强调：
  - 来源：`User Memory`
  - 机制：记忆演化
  - 目标：去用户化知识库

### 5.3 克隆式共享投影存在明显的数据放大压力

当前讨论结论：

- 一条原生记忆最多可能变成：
  - 1 份本地原生副本
  - 1 份共享投影副本
- 放大的不只是 KV，还包括：
  - vector index
  - text index
  - backfill 写入量
  - 查询候选集

这在“一个组织很多用户”的场景下，会带来明显的存储和索引成本压力。

## 6. 当前目标口径

基于当前讨论，后续修改应尽量向以下定义收敛：

### 6.1 Agent Memory

- 定义：某个 agent 如何做事
- 主键语义：
  - `agent_id` 为主
  - 多租户时带 `org_id`
- 不应默认按 `user_id` 切分

建议作用域模型：

- 推荐：`org_id + agent_id`
- 最少：`agent_id`

### 6.2 User Memory

- 定义：系统应该记住这个用户什么
- 保留用户偏好、身份、长期事实和个体约束
- 不承载“agent 如何做事”的通用 procedural memory

建议作用域模型：

- 推荐：`org_id + user_id`
- 最少：`user_id`

### 6.3 Organization Memory

- 定义：组织范围内共享的公共知识
- 来源：由用户记忆演化生成
- 目标：形成与具体用户无关的组织级知识库
- 共享边界：`org_id`

组织记忆按层级的准入原则：

- `L0` 不直接进入组织记忆
- `L3` 不直接进入组织记忆
- `L1` 默认不直接进入组织记忆，只作为演化原料
- `L2` 是组织记忆的主要承载层

### 6.4 L1 进入 Organization Memory 的例外准入规则

默认规则：

- `L1` 不应直接落入 `Organization Memory`

只有同时满足以下条件时，`L1` 才可以例外进入组织记忆：

1. 已经去用户化
   - 不包含具体用户身份、个体偏好、敏感事实

2. 具有高复用价值
   - 对多个用户、多个 agent 都成立
   - 不是一次性的局部经验

3. 表达已经足够稳定
   - 内容噪声低
   - 不依赖原始会话上下文才能成立

4. 可以直接作为规则或模式复用
   - 例如：组织术语、稳定流程、通用恢复策略、共享规范

5. 通过显式组织共享判定
   - 不能仅因为“存在共享投影能力”就自动进入
   - 需要单独的筛选或准入逻辑

当前建议：

- `User L1/L2` 是 `Organization Memory` 的原料
- `Organization Memory` 的正式落库主体以 `L2` 为主
- `L1` 只在“准 L2 的高质量通用记忆”场景下例外进入

## 7. 共享投影的后续重构方向

当前实现是“克隆式投影”：

- 优点：简单、检索路径直观、权限边界容易理解
- 缺点：数据放大明显，长期不适合大规模多租户场景

后续可按三阶段演进：

### 阶段 A：先收缩投影范围

- 限制共享投影只覆盖高价值 `L2`
- 限制 `L1` 历史回填
- 控制共享域的规模

### 阶段 B：从克隆投影演进为轻量投影记录

- 保留单份 canonical memory
- 投影层只保存：
  - `source_id`
  - `target_domain`
  - `scope metadata`
  - `share metadata`

### 阶段 C：长期演进为“单份存储 + 共享关系表 + 查询时展开”

- 主体记忆只存一份
- 共享边界通过关系和策略控制
- 查询时按作用域展开候选集

## 8. 本文档对应的后续修改任务

后续建议按以下顺序推进：

1. 统一文档口径
   - README / README-zh / 网站文档统一为三域模型：
     - `Agent`
     - `User`
     - `Organization`

2. 去掉旧第四域概念
   - 删除文档和图示中的旧第四域描述
   - 删除内核中的旧共享域定义与说明

3. 去掉 `应用作用域` 作为核心记忆维度
   - 梳理所有 `应用作用域` 参与 scope / namespace / filter 的位置
   - 评估字段、索引、接口的删除或降级

4. 修正 Agent Memory 作用域模型
   - 重新定义 agent 域主键
   - 把 `user_id` 从 agent 主作用域中拆出去

5. 重定义 Organization Memory
   - 明确其来源是 `User Memory` 的演化结果
   - 明确其目标是组织级去用户化知识库

6. 梳理共享投影边界
   - 明确哪些层可投影
   - 明确是否允许 `L1` 大规模回填

7. 设计非克隆式共享方案
   - 输出一版 canonical memory + projection record 的新设计

8. 处理存量兼容
   - 评估历史投影数据迁移
   - 评估索引回建与清理策略

### 8.1 已完成的第一轮实现

截至当前代码状态，已经完成以下第一轮落地：

- `README.md` / `README-zh.md` / 统一记忆图示 已切换到三域模型
- `Agent Memory` 的 native namespace 已改为 `org_id + agent_id`
- `User Memory` 的 native namespace 已改为 `org_id + user_id`
- `MemoryDomain::App` 已从公共模型与 core 中移除
- core 中旧第四域共享投影、回填、共享策略 API 已删除
- core 本地检索不再把 `应用作用域` 作为记忆过滤维度
- organization 投影已收紧为显式准入：
  - 仅 `User L2` 进入组织域
  - `L1` 默认不进入组织域
  - `Agent` 域不再直接投影到组织域
- organization 投影已不再复用贡献者的本地 user 归属展示
- server 的旧 `memory-sharing` 接口已删除
- dashboard 已改为三域展示口径：
  - 不再把旧第四域作为正式共享域展示
  - 共享记忆默认只突出 `Organization`

这意味着：当前系统在实现层已经完成从四域模型到三域模型的收敛，不再保留额外的应用级记忆语义。

### 8.2 当前可后置的 Backlog

以下事项可以进入 backlog，后续按需要再做；它们不阻塞当前“canonical organization knowledge + 自动准入/自动发布”主线闭环：

1. organization knowledge 的独立详情页 / 跨页面上下文跳转
   - 当前组织页已经具备可分享 URL 状态和专用 detail 面板
   - 后续可以进一步拆成：
     - 独立详情页
     - 从 memories / search / graph 到 organization knowledge 的跳转
     - 更明确的前后文导航
   - 属于体验增强，不影响自动化流程本身

2. organization 自动化观测指标
   - 增加：
     - candidate -> active 自动批准计数
     - revoke / rebuild 计数
     - topic 合并次数
     - source lineage 分布
   - 用于运营和排障，不影响当前正确性

3. organization source lineage 的后台回收与压缩策略
   - 例如：
     - 过长 lineage 的摘要化
     - 被 revoke source 的衍生统计压缩
     - 低价值 source preview 的裁剪
   - 当前数据规模还小，这块可以后置

4. organization knowledge 的外部消费接口优化
   - 例如 SDK / dashboard 之外的专用只读 API 整理
   - 当前内部 dashboard 已可消费，不阻塞主线

5. 更细粒度的自动发布策略面板
   - 例如不同 org 的 topic merge 阈值、来源去重阈值、自动发布条件
   - 由于当前系统明确坚持“全部自动化”，这类配置能力可以后置，不应先于主模型稳定性

需要强调：

- 上述项可以放入 backlog
- organization knowledge 的独立详情页 / 深链接入口 已完成第一版
- organization 自动化观测指标与只读自动化策略面板 已完成第一版
- organization knowledge 的外部只读 API 已完成第一版
- organization view 残留的启动时自动回收 / canonical 驱动自愈 已完成第一版
- organization source membership 已从通用 relation 中独立出第一版显式结构
- organization legacy source relation 已切出主读路径，并在启动时自动清扫
- organization knowledge 列表的 topic 级自动聚合展示 已完成第一版
- organization knowledge 详情里的 contributor / source type 自动 summary 已完成第一版
- organization knowledge 的 dashboard 检索 / 过滤 / 排序 已完成第一版
- organization knowledge 的 URL 深链接 已完成第一版
- 更细粒度的可配置自动发布策略 仍可继续保留在 backlog
- 但“最终从 clone/materialize 进一步收敛到真正的单份 canonical 存储 + projection/membership 关系展开”不建议简单放进 backlog；它仍然属于终局架构主线
- 同样，组织知识本体与 projection/membership 关系的进一步拆分、以及自动回收/重建正确性，仍然应视为主线持续推进项

## 9. 当前阶段结论

当前最重要的结论不是“再补几条规则”，而是：

- `Agent Memory` 的主作用域定义需要调整
- 旧第四域这个概念可以去掉
- `应用作用域` 这个核心建模维度可以去掉
- `Organization Memory` 应该被重新定义为组织内共享、由用户记忆演化出来的去用户化知识库
- 克隆式共享投影可以作为早期工程方案，但不适合作为长期终局架构

以上结论作为后续内核修改、README 修订、以及架构重构的输入基线。
