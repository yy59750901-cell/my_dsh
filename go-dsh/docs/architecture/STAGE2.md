# Stage 2：Agent Loop 与 LLM

更新时间：2026-08-26

## 1. 文档定位

本文冻结 Go DSH Stage 2 的 Agent Inbox、Turn/Step/Attempt、Agent Runner、LLM Stream、取消、重试、错误归一化和崩溃修复语义。独立设计评审已完成，P0/P1 问题已在本文关闭；当前状态是“设计完成，待用户确认后编码”，不代表当前代码已经实现。

本文严格区分两类内容：

- **原版事实**：已从 DeepSeek Harness Agent Loop、Inbox、LLM、Retry、Cancel 和 Session Persistence 源码或测试确认的行为。
- **Go 版决策**：结合 Stage 1 的 Session Actor、EventStore、Replay、Ordered Surface、ActorLease 和 GenerationLease 所冻结的实现方案。

如两者存在差异，必须在本文显式记录，不允许在编码阶段隐式偏离。

## 2. 目标、范围与非目标

### 2.1 目标

Stage 2 完成后，系统应能跑通以下闭环：

```text
FollowUp / Steer / Inject
→ 持久 Inbox
→ 唤醒单 Session Driver
→ Turn / Step 推进
→ 派生 Model Request
→ LLM 流式响应
→ assistant/chunk 审计事件
→ assistant/message Surface 事件
→ Usage / Finish / Error
→ Retry 或 Step / Turn 收敛
```

本阶段同时要求：

1. 同一 Session 只有一个 Agent Driver 推进 Turn/Step。
2. LLM 网络调用不阻塞 Session Actor mailbox。
3. 进程重启后能从 Event 日志恢复 Inbox，并关闭不完整 Turn/Step。
4. 取消、重试和失败流不会把半截 tool call 或失败 attempt 的内容暴露给模型 Surface。
5. OpenAI-compatible Provider 能承载 DeepSeek、Qwen 等供应商的文本、reasoning、tool-call、usage 和错误。
6. 所有状态变化可由 Session Event 重建，前端轨迹不依赖进程内临时对象。

### 2.2 本阶段包含

- 持久化 Agent Inbox；
- FollowUp、Steer、Inject、Cancel；
- Agent Driver 和 Runner；
- Turn、Step、Model Attempt；
- 最小 System Prompt 装配入口；
- 结构化 Message、ContentBlock 和 ModelRequest；
- Provider-neutral StreamChunk；
- OpenAI-compatible Provider；
- BlockAssembler；
- LLM Failure 和稳定错误码；
- Retry、backoff 和取消传播；
- 不完整 Turn/Step 的恢复修复；
- Mock Provider、Replay、race 和故障测试；
- Stage 2 轨迹事件，但不实现完整前端页面。

### 2.3 本阶段不包含

- 真实 Tool Registry、Tool 参数校验、审批和 Tool 执行；
- MCP、Skill、Workspace、Sandbox、Subagent、Workflow；
- 完整 Prompt Contributor、Token Budget 和 Compaction；
- 多模态供应商的全部私有能力；
- 前端调试台和 WebSocket 断线回补；
- 自动重放未完成 Tool Call 或具有外部副作用的动作。

Stage 2 可以无损解析 `tool-call` block并以 `assistant/chunk` 保留审计，但 `ModelRequest.Tools` 在本阶段正常运行时固定为空。若供应商仍返回 tool call，Stage 2 以 `TOOL_EXECUTION_UNAVAILABLE` 将 Step/Turn 收敛为 `blocked`，不写入模型可见的 `assistant/message`，避免留下没有配对 `tool/result` 的历史。Stage 3 启用 Tool 后，才允许 tool-call-only `assistant/message → tool/call → tool/result → 下一 Step` 闭环。

## 3. Stage 1 不变量

Stage 2 不得破坏以下已冻结能力：

1. Session EventStore 是唯一事实来源。
2. 同一 Session 只有一个 Actor 串行提交事件。
3. Actor Command 只执行短决策和事件提交，不执行网络 I/O、流读取、sleep 或长时间等待。
4. Actor 只使用数据库回读后的 committed Event 更新 Snapshot 和 Surface。
5. `actor_epoch + expectedSeq` 是跨进程最终写入防线。
6. `ActorLease` 阻止活动 Session 被 idle eviction。
7. `GenerationLease` 保证运行中的调用不因配置热切换漂移。
8. Ordered Surface 只由规范 Surface Event 构建；`assistant/chunk` 不直接进入 Surface。
9. 未知 REQUIRED 事件阻止 Replay；未知 IGNORABLE 事件只推进 seq。
10. 任何新 Migration 必须新增版本，不得修改 `000001` 或 `000002`。

核心边界：

```text
Session Actor：串行决定并提交事实
Agent Runner：执行可取消的长流程和副作用
LLM Provider：适配上游协议
EventStore：持久化真相
Surface：模型可见投影
```

## 4. 原版行为基线

### 4.1 Inbox 不是三条内部队列

原版内部持久状态只有：

```text
next-turn
next-step
```

公开 API/UI 的 placement 是投影：

| 外部 placement | 内部 target | 消息来源 | 语义 |
|---|---|---|---|
| `queued` | `next-turn` | user | 等待独占一个普通 Turn |
| `steering` | `next-step` | user | 最近后续 Step 边界消费 |
| `context` | `next-step` | non-user | 最近后续 Step 边界消费，不作为普通用户 FollowUp |

原版入口：

```text
FollowUp → next-turn，唤醒 Driver
Steer    → next-step，唤醒 Driver
Inject   → next-step，不唤醒空闲 Driver
```

### 4.2 Inbox Claim 顺序

每个 Step 的领取顺序是：

```text
先领取全部 next-step
首个 Step 再领取一个 next-turn
```

因此：

1. 已存在的 steering/context 排在本 Turn 的普通 FollowUp 前。
2. 每条 queued FollowUp 独占自己的 Turn。
3. Inbox 接受消息时只写 Inbox splice，不提前写 `user/message`。
4. 消息真正进入 Step 后才写 `user/message`。

### 4.3 Turn、Step 和 Attempt

- Turn：从领取一次普通工作，到该工作暂时结束。
- Step：一次模型请求及后续输出处理。
- Attempt：同一个 Step 内的一次具体模型调用。
- Retry 只增加 Attempt，不创建新的 `step/start`。

准确主顺序：

```text
turn/start
→ Inbox claim
→ system prompt / pre-step
→ step/start
→ user/message*
→ request/header 或 request/context（需要时）
→ assistant/chunk*
→ assistant/message
→ step/end
→ 检查 next-step
→ 必要时进入下一个 Step
→ turn/end
```

### 4.4 Cancel

原版已确认：

- 空闲且没有活动任务时 Cancel 是 no-op。
- 默认 Cancel 清空 next-step、next-turn 和 wake latch，并中止当前活动。
- `keepInbox: true` 保留尚未领取的输入，但不恢复已领取输入。
- 第一个 Cancel cause 获胜。
- Abort 到真正 idle 的窗口内，新到达的唤醒输入被转入 next-turn，并设置 wake latch。
- 生命周期 `disposed` 不允许 latch 新工作。
- 正常 mid-stream Cancel 可以固化安全的 text/reasoning 前缀。
- 未完成或未执行的 tool-call block 必须丢弃。

### 4.5 Retry

原版已确认：

```text
失败 attempt
→ llm/retry
→ 可取消 backoff
→ llm/retry-started
→ 新 attempt
```

- `llm/retry` 必须先持久化，再开始等待。
- 失败 attempt 的 chunk 保留在审计日志中。
- 最终 `assistant/message` 不能引用失败 attempt 的 chunk。
- retry 次数可从 Event 历史恢复。
- `providerRetryAfterMs` 有效且不超过最大延迟时优先使用。
- 默认策略按稳定错误码白名单和最大次数决定。

### 4.6 LLM Stream

原版 Provider-neutral chunk 至少包含：

```text
block-start
text-delta
reasoning-delta
tool-call-delta
block-end
usage
finish
```

约束：

1. block index 关联交错到达的 delta。
2. usage 必须在 terminal finish 前出现。
3. finish 后不得再有 chunk。
4. Adapter 可以抛错，Runtime 应把错误转换为 terminal `error` 或 `aborted` finish。
5. tool arguments 保留原始 JSON 字符串。

## 5. Go 版总体运行模型

### 5.1 每 Session 两个串行域

Go 版保留两个不同的串行域：

```text
Session Actor mailbox
  - 串行提交 Event
  - 更新 Replay Projection
  - 不做慢 I/O

Agent Driver loop
  - 每 Session 最多一个
  - 驱动 Turn / Step / Attempt
  - 调用 LLM、等待 Stream、backoff
  - 通过短 Command 把事实提交给 Actor
```

二者不可合并。否则 LLM Stream、Retry delay 或上游超时会阻塞 FollowUp、Steer、Cancel 和状态查询。

### 5.2 Runner 生命周期

每个活动 Turn：

1. 获取一个 `ActorLease`，覆盖整个 Turn。
2. 获取一个 `GenerationLease`，覆盖整个 Turn。
3. 从该 generation 锁定 Provider、模型配置、基础 Prompt、RetryPolicy 和 ErrorNormalizer。
4. 创建 Turn 级带 cause 的可取消 Context。
5. 所有正常、错误、Cancel、fencing 和 panic 路径都必须释放两个 Lease。

普通 API 接受 Inbox 输入时只使用短 `ActorLease`；不能为等待队列中的消息长期持有 Lease。

### 5.3 禁止同步重入 Actor

Committed Event Publisher 是 observe-only。Runner 不得依赖 Publisher Handler 同步提交同一 Actor 的新 Command，否则可能形成 Actor 等待 Publisher、Publisher 等待 Actor 的死锁。

Runner 的唤醒使用独立的非阻塞 wake latch；事件广播只用于观察、轨迹和外部投影。

### 5.4 Agent Runtime Registry

现有 `internal/session.Registry` 只拥有 Actor，不能直接承担 Driver 生命周期，且 `internal/session` 不得依赖 `internal/agent`。Stage 2 在 `internal/agent` 新增 `RuntimeRegistry`：

```text
Agent RuntimeRegistry
  └── sessionID → RuntimeEntry
        ├── Driver
        ├── cancel
        ├── done
        └── phase/wake latch

Session Registry
  └── sessionID → Actor
```

冻结的外部 API 形态：

```go
type RuntimeRegistry interface {
    RequestWake(sessionID string) error
    CancelActive(sessionID string, cause CancelCause) bool
    Status(ctx context.Context, sessionID string) (Status, error)
    Remove(ctx context.Context, sessionID string, cause CancelCause) error
    Dispose(ctx context.Context) error
}
```

`RequestWake` 只设置容量为 1 的 latch，并由 Registry 自有 Context 驱动 lazy single-flight 创建；不得把 Driver 生命周期绑定到某个 HTTP/gRPC 请求 Context。`CancelActive` 只能安装 Actor 已提交的 committed cause。外部删除 Session 的编排入口必须先调用 Agent `Remove`，等待 Driver/Lease 退出，再调用 Session Registry `Remove`；禁止业务层直接绕过该顺序。

约束：

1. `SessionSchema`、Session Registry、GenerationManager、Agent RuntimeRegistry 和 WakeReconciler 均属于 application/root Scope；注册顺序固定为 `Session Registry → GenerationManager → Agent RuntimeRegistry → WakeReconciler`，利用 Lifecycle 的逆序销毁先停止 Reconciler，再按 `Agent RuntimeRegistry → GenerationManager → Session Registry` 退出。这样先取消 Driver并释放 GenerationLease，再销毁 GenerationManager，最后销毁 Actor；RuntimeRegistry 和 SessionSchema 都不属于某个模型 Generation。
2. `RuntimeRegistry` 对同一 Session 的 Driver 创建、移除和重建采用 single-flight，任何时刻最多一个 Driver。
3. Driver 不长期缓存无 Lease 的 Actor 指针；每个 Turn 通过 Session Registry 获取 `ActorLease`。
4. 获取顺序固定为 `ActorLease → GenerationLease`；后者失败立即释放前者。
5. 正常释放顺序反向：先关闭 Stream/Attempt，再释放 GenerationLease，最后释放 ActorLease。
6. `ErrWriterFenced` 或 `ErrActorClosed` 使当前 Turn 立即中止。Driver 等待旧 Actor 从 Session Registry 完全移除后，下一 Turn 才重新 `GetOrLoad`；旧活动不得向新 Actor 续写。
7. Agent RuntimeRegistry Dispose：先关闭准入，再以 `lifecycle-disposed` 取消全部 Driver，等待 `done` 和 Lease 释放，最后才允许上层 Dispose Session Registry。
8. Agent RuntimeRegistry 的 idle eviction 只能移除已无 active Turn、无 maintenance、无 wake latch 的 Driver；持久 Inbox 是否存在由 AgentProjection 判断，存在待唤醒工作时不得回收。
9. lazy 创建 Driver 或加载 Actor 失败时不得清除 `wakeRequested`。RuntimeEntry 保存结构化 `last_start_error`，使用 Registry 自有 Clock 做 500ms 起步、10s 封顶的有界指数重试；`AgentStatus.last_error` 承载该错误。只有成功进入 Driver loop、确认持久 Inbox 已空，或 committed Cancel/Dispose 明确清理后，才能清除 latch。
10. root Scope 增加一次性 `WakeReconciler`：服务启动后通过只读 `SessionCatalog.ListResumableSessionIDs(cursor, limit)` 分页枚举未归档 Session，以 SessionSchema 做只读 Replay；发现 next-turn 非空，或 next-step 中存在 Source=user 的 Steer 时，幂等调用 `RequestWake`。只有 Inject 的 next-step 不唤醒。Reconciler 不 ClaimWriter、不创建 Actor，不修改 EventStore；单 Session 失败记录并继续，最终汇总错误供 readiness/运维观察。
11. FollowUp/Steer 返回成功前仍要调用 `RequestWake`；若调用失败，API 返回“输入已持久化但调度失败”的结构化错误。客户端以相同幂等键重试时不重复 append，但只要 AgentProjection 仍有可唤醒工作，就必须再次调用 `RequestWake`。

这样既维持包依赖方向，也保证 Session Actor 被 fencing、回收或服务关闭时不会遗留旧 Driver、双 Driver或“已持久化但永远不再唤醒”的输入。

## 6. Agent Inbox 设计

### 6.1 领域类型

```go
type InboxTarget string

const (
    InboxNextTurn InboxTarget = "next-turn"
    InboxNextStep InboxTarget = "next-step"
)

type InputSource string

const (
    InputSourceUser      InputSource = "user"
    InputSourceExtension InputSource = "extension"
    InputSourceSystem    InputSource = "system"
)

type InboxItem struct {
    InputID       string
    RequestID     string
    IdempotencyKey string
    Source        InputSource
    Content       []ContentBlock
    AcceptedAt    time.Time
    Metadata      map[string]json.RawMessage
}
```

约束：

- `InputID` 全 Session 唯一。
- `RequestID` 用于端到端追踪。
- 同一 Session、同一命令类型、同一非空 `IdempotencyKey` 只能接受一次。
- ContentBlock 必须深拷贝，禁止 Replay Snapshot 暴露可变 slice/map 别名。
- Metadata 只允许 JSON object，且不得承载明文凭据。

### 6.2 持久事件

Inbox 只通过 REQUIRED Event 变更：

```text
agent/inbox/spliced
```

冻结 payload：

```json
{
  "target": "next-turn",
  "start": 0,
  "delete_count": 0,
  "items": [],
  "reason": "followup|steer|inject|claim|cancel",
  "command_kind": "followup|steer|inject|internal",
  "request_id": "...",
  "idempotency_key": "...",
  "payload_hash": "sha256:...",
  "original_target": "next-step",
  "actual_target": "next-turn",
  "claim_id": "..."
}
```

规则：

- 接受输入的 insertion splice 必须携带 `command_kind`、`request_id`、`idempotency_key`、规范化 `payload_hash`、`original_target` 和 `actual_target`。
- `payload_hash` 使用规范化领域命令的 RFC 8785 JSON Canonicalization Scheme 结果计算 SHA-256；不包含 request ID、接收时间和 idempotency key。
- `AcceptedSeq` 和 `AcceptedAt` 取 committed Event Envelope，不在调用方预估。
- Claim 和清空仍使用 splice 表达，但 Claim 还必须与 REQUIRED `agent/input-claimed` 同批提交，显式保存输入归属。
- `reason=claim` 时 `claim_id` 必须非空，同一 claim batch 的全部删除 splice 使用同一值，并与 `agent/input-claimed.claim_id` 一致；insertion/cancel splice 禁止携带 `claim_id`。
- AgentProjection 即使在 item 被 Claim/Cancel 删除后，也永久保留已接受 InputID/幂等键索引，供重复请求返回原 Receipt。

所有追加、Claim 和清空都使用 splice 表达，Replay 按序应用同一操作。

校验：

1. target 只能是 next-turn/next-step。
2. `start <= len(queue)`。
3. `start + delete_count <= len(queue)`。
4. 新增 item 的 InputID 不得与已有或历史已接受 ID 冲突。
5. 接受事件的 `(command_kind, idempotency_key)` 不得与历史不同 payload hash 冲突。
6. Claim 删除的 item 必须在同一 Append batch 内由 `agent/input-claimed` 完整归属；禁止静默丢失。

### 6.3 API 行为

#### FollowUp

```text
原子检查幂等键
→ append 到 next-turn
→ 返回 Receipt
→ 请求 Driver wake
```

#### Steer

```text
原子检查幂等键
→ append 到 next-step，Source=user
→ 返回 Receipt
→ 请求 Driver wake
```

当当前没有可继续的活动 Turn 时，Driver 将 Steering 作为新 Turn 的 next-step 输入处理；它不被伪装成普通 queued FollowUp。

#### Inject

```text
原子检查幂等键
→ append 到 next-step，Source=extension/system
→ 返回 Receipt
→ 不唤醒 idle Driver
```

Inject 只影响已经运行或之后由其他输入唤醒的 Agent。

### 6.4 外部 placement 投影

```text
next-turn                         → queued
next-step + Source=user           → steering
next-step + Source!=user          → context
```

placement 不持久化为第三套状态，避免双写和 Replay 分歧。

### 6.5 Claim 与输入所有权

仅将 item 从队列删除不足以形成可恢复语义。Go 版增加 REQUIRED：

```text
agent/input-claimed
```

其 payload 至少包含：

```text
claim_id
turn_id
proposed_step_index
ordered_input_ids
source_targets
claimed_at_seq（由 committed batch 推导，不由调用方填写）
```

完整输入内容不重复复制；AgentProjection 从历史接受索引按 InputID 解析。若索引缺失、顺序不一致或 item 不在本次 splice 删除集合中，整批候选事件必须在持久化前被拒绝。

首个 Step 使用一个 Actor Command：

```text
StartTurnAndClaim:
  同一 Append batch：
  1. turn/start
  2. 删除 next-step 全部 item 的 splice
  3. 删除 next-turn 第一条 item 的 splice
  4. agent/input-claimed
  返回顺序为 next-step items + next-turn item
```

因此不会出现 `turn/start` 已提交但尚未 Claim 的 crash gap。

后续 Step 仍不能提前写 `step/start`，因为 pre-step 可能 reject。使用：

```text
ClaimForProposedStep:
  同一 Append batch：
  1. 删除 next-step 全部 item 的 splice
  2. agent/input-claimed
```

`claim_id + proposed_step_index` 表示“由当前 Turn 拥有、尚未进入 Step”的批次。pre-step 通过后，`step/start` 必须引用该 `claim_id`；pre-step reject/空输入时，`turn/end` 必须引用该 `claim_id` 和被消费 InputID。

Claim 后输入不再属于 Inbox。Cancel 的 `keepInbox` 不得把它们恢复；崩溃修复也只关闭其所属边界，不重新排队。

## 7. Agent Projection 与 Session Snapshot 扩展

### 7.1 问题

当前 `session.Snapshot` 只有 Session/Turn/Step/Surface，不能表达 Inbox、Driver phase、attempt、retry、cancel、usage 和幂等状态。直接让 `internal/session` 依赖 `internal/agent` 会造成反向依赖。

### 7.2 决策：Session Schema + Composite Projection

现有 Stage 1 `Projector.Apply(*Snapshot, Event)` 无错误返回、只有单 Projector，且候选批次只试算 Surface，无法安全承载 Inbox splice。Stage 2 首先把它演进为可组合、可失败的 Session Schema；这属于 Session 内核扩展，不得让 `internal/session` 反向依赖 `internal/agent`。

冻结接口采用“非泛型存储 + 泛型读取 helper”，避免 Go interface method 不能声明独立类型参数的问题：

```go
type ProjectionKey string

type ProjectionBoundary string

const (
    BoundaryCandidateBatch ProjectionBoundary = "candidate-batch"
    BoundaryCommittedBatch ProjectionBoundary = "committed-batch"
    BoundaryReplayEnd      ProjectionBoundary = "replay-end"
)

type ProjectionSpec struct {
    Key              ProjectionKey
    Order            int
    New              func() any
    Clone            func(any) any
    Apply            func(any, Event) error
    ValidateBoundary func(any, ProjectionBoundary) error
}

type EventDefinition struct {
    EventType           string
    ReplayPolicy        ReplayPolicy
    CandidateValidators []CandidateEventValidator
    CommittedValidators []EventValidator
}

type SchemaContribution struct {
    Name        string
    Events      []EventDefinition
    Projections []ProjectionSpec
}

type SessionSchema struct {
    // 由 Builder 校验并冻结，运行期只读。
}

func ProjectionAs[T any](snapshot Snapshot, key ProjectionKey) (T, bool)
```

`Snapshot` 内部保存不可导出的 `ProjectionSet`；`Snapshot.Clone()` 必须逐项调用对应 `ProjectionSpec.Clone`。Clone 在 Schema 构建后定义为不会失败的纯内存操作；panic 属于实现缺陷，不降级为业务错误。`ProjectionAs` 只能从已克隆 Snapshot 读取 typed value，调用方不能取得 Actor 内 live map/slice 指针。首版不使用反射式自动深拷贝：每个模块必须显式提供 `New/Clone/Apply/ValidateBoundary`，缺失任一函数则 Schema 构建失败。

每个模块以一个不可拆分的 `SchemaContribution` 注册“事件定义 + Validator + Projector”；所有 contribution 在 application/root Scope 初始化期间、Session Registry 创建之前合并为进程级不可变 `SessionSchema`，不属于可热切换 Generation。同名 EventDefinition 或 ProjectionKey 冲突直接拒绝启动。Generation 只承载 Provider、Prompt、RetryPolicy、ErrorNormalizer 和 route capability。禁止分别维护 `KnownEventTypes` 和 Projector 列表后再临时拼装。

所有 Projector 按 `(Order, Key)` 稳定顺序接收每一个 Event，而不是由唯一 EventDefinition 绑定 ProjectionKey；Projector 自行忽略不关心的已知事件。这样 AgentProjector 可以订阅 Core 的 `turn/start` 等事件而不重复定义事件，SurfaceProjector 也能在未知 IGNORABLE Event 上推进 `throughSeq`。未知 REQUIRED 仍在投影前失败；未知 IGNORABLE 跳过业务 Validator，但仍送入所有 Projector 的 seq-only 路径。

`SessionSchema` 同时拥有 Core、Surface 和 Agent Projection；Actor/Replay 不再在 Schema 外单独调用 `Surface.Apply`。跨事件约束通过投影中的 transient validation marker 表达。对 Claim：marker 打开后，只允许继续出现相同 `claim_id` 的 claim-delete splice，随后必须由相同 `claim_id` 的 `agent/input-claimed` 完整消费，其间禁止其他 Event。候选 batch和 committed readback batch 在各自边界调用 `ValidateBoundary`；Replay 分页末不视为边界，只在完整 Replay 结束调用 `BoundaryReplayEnd`。

`internal/agent` 贡献 `AgentProjector`，负责从以下事件重建 AgentProjection：

- Inbox 两队列；
- 历史已接受 InputID、幂等键、payload hash 和 Receipt；
- claim_id 与已 Claim 输入所有权；
- Driver/Turn/Step/Attempt 的持久阶段；
- cancel cause；
- retry 状态；
- usage 汇总；
- 未完成边界检测所需信息。

Actor 对候选、committed readback 和 Replay 使用同一 EventDefinition、Validator 和 Projector 顺序：

```text
clone full Snapshot/ProjectionSet
→ 按 EventDefinition 校验 EventType 与 ReplayPolicy
→ 逐 Event 运行当前阶段对应的 Validator
→ 逐 Event 运行全部 Projector
→ 在当前路径的真实边界调用 ValidateBoundary
→ 任一步失败则拒绝或标记 unhealthy
```

候选阶段运行 `CandidateValidators + BoundaryCandidateBatch`；数据库回读运行 `CommittedValidators + BoundaryCommittedBatch`；Replay 逐 Event 运行 `CommittedValidators`，只在整个日志结束时运行 `BoundaryReplayEnd`，不把分页边界误当 Append 边界。EventStore 当前没有持久化 append batch ID，因此 Replay 不宣称能反证历史 Event 是否来自同一次 Append；它验证严格连续的 claim 结构、完整 InputID 集合和最终 marker 闭合，而“同一 Append 原子性”由候选/committed batch 校验、EventStore 事务和故障测试共同保证。持久化后如果数据库回读 Event 无法通过规则，继续沿用 Stage 1 的 unhealthy/recover-after-commit 语义。

约束：

1. Projection 只安装 committed Event 形成的状态；候选阶段只在 clone 上试算。
2. 实时 Apply 与 Replay 使用同一个 Projector 和 EventDefinition。
3. Snapshot 返回深拷贝或不可变 View。
4. Projector 不能执行 I/O。
5. 任何影响 Inbox、幂等、活动终态、Retry 次数或 Repair 判断的事件必须为 REQUIRED。
6. IGNORABLE Event 只能影响审计、实时展示或可缺失统计，不能决定控制流正确性。

### 7.3 持久化

Stage 2 不新增 Agent 状态表。Inbox、幂等和执行状态先完全从 Event Replay 重建，因此暂不创建 `000003_agent_loop` Migration。

只有在性能数据证明 Replay 成本不可接受时，才新增可丢弃、可重建的 projection/checkpoint 表；不得把它升级为事实来源。

## 8. Driver Phase 与 Wake Latch

### 8.1 Phase

```go
type DriverPhase string

const (
    DriverIdle        DriverPhase = "idle"
    DriverMaintenance DriverPhase = "maintenance"
    DriverRunning     DriverPhase = "running"
    DriverDisposed    DriverPhase = "disposed"
)
```

活动状态还包含：

```text
turn_id
step_id
step_index
attempt
cancel_cause
wake_requested
```

进程内 Phase 用于并发控制；可恢复事实必须由 Event 投影得到。

### 8.2 Wake Latch

使用容量为 1 的非阻塞 wake channel 或等价原子 latch：

```text
RequestWake:
  wakeRequested = true
  non-blocking send wake signal
```

Driver 每次醒来后循环运行：

```text
while work exists and not disposed:
    run one Turn
```

避免每条 FollowUp 启动一个 goroutine，也避免 wake signal 丢失。

### 8.3 Abort-to-idle 窗口

当前 Turn 已收到 Cancel、但 Driver 尚未收敛 idle 时：

- FollowUp：正常进入 next-turn，并设置 wake latch。
- Steer：如果当前 Turn 已不可继续，则重分类为 next-turn，保留 Source=user，并设置 wake latch。
- Inject：仍进入 next-step，但不单独唤醒。
- disposed：拒绝新输入，不设置 latch。

重分类必须由接收命令根据 committed AgentProjection 判断，并在 Event payload 中记录原始命令和实际 target，便于轨迹解释。

## 9. Turn、Step、Attempt 状态机

### 9.1 标识

- `turn_id`：ULID/UUID，不使用易复用的内存自增序号作为持久身份。
- `step_id`：ULID/UUID，并包含 `step_index` 便于排序。
- `attempt`：同 Step 内从 1 开始的 uint32。
- `provider_request_id`：由供应商返回时记录。

Event Envelope 继续使用既有 `turn_id`、`step_id`；attempt 放入模型相关 payload。除非后续多类 Event 都证明需要，否则本阶段不把 attempt 增加为 Envelope 顶层字段。

### 9.2 Turn 状态

```text
absent
→ running
→ completed | blocked | aborted | error | interrupted
```

### 9.3 Step 状态

```text
absent
→ running
→ completed | max-tokens | blocked | aborted | error | interrupted | tool-calls
```

### 9.4 Attempt 状态

```text
requested
→ streaming
→ stop | tool-calls | max-tokens | aborted | error
→ retry-scheduled（仅 error）
→ 下一 attempt
```

### 9.5 单 Turn 算法

```text
1. StartTurnAndClaim：同批提交 turn/start + Inbox splice + agent/input-claimed
2. 运行 pre-step waterfall
3. 如果 reject：append turn/end(blocked, claim_id)，结束
4. 如果消息为空：append turn/end(completed/no-input, claim_id)，结束
5. append step/start(claim_id)
6. 将 claimed/变换后的输入写为 user/message 或 context-visible event
7. 派生 ModelRequest
8. 运行 Attempt
9. stop/max-tokens：CommitAttemptTerminal 原子提交最终消息（如有）+ usage（如有）+ step/end
10. error：CommitAttemptFailure 原子提交 model/error + llm/retry，或 model/error + step/end(error)
11. aborted：CommitAttemptAborted 原子提交安全前缀（如有）+ step/end(aborted)
12. Step 完成后由 Actor Snapshot 检查 next-step；有输入则 ClaimForProposedStep
13. 没有 next-step 时运行 turn-stopping hook
14. FinishTurnIfQuiescent 原子复核 phase/cancel/inbox revision；满足条件才 append turn/end
15. 如果复核发现新 next-step，则回到 ClaimForProposedStep
```

Stage 2 的 `turn-stopping` 只保留扩展点和确定性顺序，首版不允许 Hook 执行无限等待。任何“检查 Inbox 后再提交结束”的流程都必须在 Actor 内二次校验，禁止 Runner 读取旧 Snapshot 后无条件关闭 Turn。

### 9.6 pre-step reject 和空输入

与原版对齐：

- pre-step reject 后，已 Claim 输入不恢复 Inbox。
- reject 不写 `step/start`，不写 `user/message`。
- Turn 以 `blocked` 结束，结束事件必须记录被消费 InputID 和原因。
- 初始消息被变换为空时可以只保留 Turn 边界，不调用模型、不创建 Step。

### 9.7 pre-step 与 turn-stopping Hook

Stage 2 冻结 `pre-step`、`request`、`turn-stopping` 三类进程内串行 Waterfall 扩展点，不提供远程 Hook。Hook 在 Driver goroutine 中运行，不占用 Actor mailbox；必须接收 Turn Context，禁止直接写 EventStore、持有 Actor live Snapshot 或绕过 Command 提交状态。`request` Waterfall 位于 `step/start/user-message` 已提交之后、`model/requested` 之前，必须纳入同一失败闭环。

默认预算：

```text
单 Hook timeout       = 2s
单次 Waterfall 总预算 = 5s
```

两者均可按 Generation 配置覆盖，但必须大于 0，且总预算不得小于单 Hook timeout。Hook 输出必须深拷贝并在提交 `user/message` 或终态事件前完成校验。

结果分类：

| Hook 结果 | 状态处理 | 是否进入 LLM Retry |
|---|---|---|
| continue/transform | 继续当前流程 | 否 |
| explicit reject | pre-step：`turn/end(blocked)`；request：`step/end(blocked) + turn/end(blocked)`；turn-stopping：`turn/end(blocked)`；均记录稳定 reason code | 否 |
| context cancelled | 按 committed Cancel cause 收敛 aborted | 否 |
| deadline exceeded | `agent/hook-failed(HOOK_TIMEOUT)` + 对应 Step/Turn `error` 终态 | 否 |
| 普通 error | `agent/hook-failed(HOOK_FAILED)` + 对应 Step/Turn `error` 终态 | 否 |
| panic | recover 为 `agent/hook-failed(HOOK_PANIC)`，测试模式可重新 panic | 否 |

失败 closer 按 Hook 所处边界确定：`pre-step` 尚无 Step，原子提交 `agent/hook-failed + turn/end(error)`；`request` 已有 Step，原子提交 `agent/hook-failed + step/end(error) + turn/end(error)`，且不得写 `model/requested`；`turn-stopping` 已无 active Step，原子提交 `agent/hook-failed + turn/end(error)`。显式 reject 不是 hook-failed，但同样按边界原子关闭：request reject 必须同时写 `step/end(blocked) + turn/end(blocked)`，不能直接越过 active Step 只关闭 Turn。

`turn-stopping` Hook 只能返回“允许结束”或一组待提交的 next-step 输入提案；提案仍须通过 Actor Command 原子校验/提交，Hook 本身不能修改 Inbox。Hook 超时或失败时不再启动新 Step。

## 10. 规范事件

### 10.1 必要执行事件

| Event | Replay | 作用 |
|---|---|---|
| `agent/inbox/spliced` | REQUIRED | 重建 Inbox 和幂等接收状态 |
| `agent/input-claimed` | REQUIRED | 将已删除输入原子归属到 Turn/候选 Step |
| `turn/start` | REQUIRED | 打开 Turn，并与首批 Claim 同批提交 |
| `step/start` | REQUIRED | 打开 Step，引用已存在的 claim_id |
| `user/message` | REQUIRED | 进入 Surface 的用户输入 |
| `model/requested` | REQUIRED | 记录 attempt、模型和可解释请求摘要 |
| `assistant/chunk` | IGNORABLE | 流式审计和实时轨迹 |
| `model/usage` | IGNORABLE | attempt usage；缺失不能破坏 Replay |
| `model/error` | REQUIRED | 稳定失败事实和 attempt 结果 |
| `llm/retry` | REQUIRED | 已调度 retry 和 delay |
| `llm/retry-started` | REQUIRED | backoff 完成，下一 attempt 即将开始 |
| `assistant/message` | REQUIRED | 最终进入 Ordered Surface 的模型消息 |
| `step/end` | REQUIRED | 关闭 Step |
| `turn/end` | REQUIRED | 关闭 Turn |
| `agent/cancel-requested` | REQUIRED | 记录首个 Cancel cause 和 Inbox 策略 |
| `agent/hook-failed` | REQUIRED | 记录会改变 Turn 终态的 Hook timeout/error/panic |
| `session/repaired` | IGNORABLE | 仅记录恢复诊断；真正控制流由 REQUIRED synthetic step/end、turn/end 决定 |

### 10.2 事件批次约束

以下必须在一个 Actor Command/Append batch 内保持原子：

- FollowUp/Steer/Inject 的幂等接受与 Inbox insertion splice；
- 首个 Claim 的 `turn/start + 删除 splice + agent/input-claimed`；
- 后续 Claim 的 `删除 splice + agent/input-claimed`；
- 默认 Cancel 的 cancel-requested 与 Inbox 清空 splice；
- Attempt 成功时的可选 usage、assistant/message 和 step/end；
- Attempt 失败时的 model/error 与 llm/retry，或 model/error 与 step/end(error)；
- Attempt 取消时的可选 interrupted assistant/message 与 step/end(aborted)；
- Hook timeout/error/panic 时的 agent/hook-failed 与对应 Step/Turn error closer；
- synthetic step/end、turn/end 和 repair 诊断事件。

`assistant/chunk` 已经是更早提交的审计事件，不能与最终消息放在同一批；最终提交必须验证 `sourceEventSeqs` 全部属于当前 attempt 且确实能组装出该消息。

### 10.3 Chunk 来源

`assistant/message.sourceEventSeqs` 只能引用当前成功 attempt，或当前正常取消 attempt 中被保留安全前缀的 `assistant/chunk` seq。

禁止引用：

- 失败 attempt；
- 已关闭 block 后的迟到 delta；
- 被 max-tokens 裁剪的 tool call；
- Cancel 时未执行的 tool call；
- 其他 Turn/Step 的 chunk。

## 11. Message 与 ContentBlock

### 11.1 不再使用字符串 Content

当前 `llm.Message.Content string` 无法无损表达 reasoning、tool-call 和多内容块，Stage 2 必须改为结构化模型：

```go
type Role string

const (
    RoleSystem    Role = "system"
    RoleUser      Role = "user"
    RoleAssistant Role = "assistant"
    RoleTool      Role = "tool"
)

type Message struct {
    Role    Role
    Name    string
    Source  MessageSource
    Content []ContentBlock
}
```

### 11.2 ContentBlock

首版：

```text
text
reasoning
tool-call
tool-result
```

保留未来扩展：

```text
image
file
audio
provider-defined
```

`tool-call` 至少包含：

```text
index
call_id
name
arguments_json_raw
complete
```

`arguments_json_raw` 保留供应商原始 JSON 字符串；组装期间不自动反序列化后再序列化，避免键顺序、数字和非法半截 JSON 被改写。

### 11.3 Surface 派生修复

当前 `deriveMessages()` 可能跳过只有 tool call、没有文本的 assistant message。Stage 2 实现必须改为：

```text
只要 assistant/message 含至少一个有效 ContentBlock，就进入 Model Message
```

不得以 `text != ""` 作为 assistant message 是否存在的条件。

## 12. ModelRequest

```go
type ModelRequest struct {
    RequestID    string
    SessionID    string
    TurnID       string
    StepID       string
    Attempt      uint32
    Model        string
    Messages     []Message
    Tools        []ToolDefinition
    Temperature  *float64
    MaxTokens    *int
    Reasoning    *ReasoningOptions
    Metadata     map[string]json.RawMessage
}
```

本阶段 `Tools` 可以为空；类型必须提前稳定，以承接 Stage 3。Reasoning 领域类型冻结为：

```go
type ReasoningOptions struct {
    Enabled   bool
    Effort    string
    MaxTokens *int
    Preset    string
}
```

未启用时为 nil。`Effort` 首版允许 `low/medium/high`，空值表示使用 Provider 默认；`MaxTokens` 必须为正数。Provider route capability 负责校验和映射，不能把不支持的 reasoning 参数静默发给上游。

请求构建顺序：

```text
锁定 Generation
→ 从 Ordered Surface 派生历史消息
→ 添加本 Step 已提交的输入
→ 运行基础 Prompt Contributor
→ 运行 request waterfall
→ 深拷贝并冻结 ModelRequest
→ append model/requested（记录摘要和哈希，不记录凭据）
→ 调用 Provider
```

`model/requested` 不应默认复制完整敏感 Prompt。至少记录：

- request_id；
- model；
- attempt；
- message 数量和各角色数量；
- tool 数量；
- request hash；
- generation id；
- timeout；
- 可选脱敏后的诊断摘要。

`request_hash` 冻结为：将实际送入 Provider Adapter 的 provider-neutral 请求投影为 JSON，按 RFC 8785 JCS 规范化后计算 SHA-256，编码为 `sha256:<lowercase-hex>`。哈希包含 model、messages、tools、temperature、max_tokens、reasoning 配置和会影响 wire payload 的 allowlisted provider options；不包含 API Key、Authorization/Header、CredentialRef、request/trace ID、时间、timeout 和仅诊断 metadata。`attempt` 单独记录但不进入哈希，因此相同 wire 请求的 Retry 得到相同 hash。Adapter 若对请求做供应商特定语义变换，还要在本地诊断中计算可选 `wire_request_hash`，但 Stage 2 不持久化包含敏感字段的原始 wire body。

完整上下文解释和安全预览在 Stage 4 完成。

## 13. LLM Provider 与 Stream 契约

### 13.1 接口决策

不继续使用裸 `<-chan StreamEvent` 作为唯一接口，因为它无法强制 terminal、区分 EOF 与异常、可靠 Close 或防止 goroutine 泄漏。

冻结为 pull-based Stream：

```go
type Provider interface {
    Stream(context.Context, ModelRequest) (Stream, error)
}

type Stream interface {
    Next(context.Context) (StreamChunk, error)
    Close() error
}
```

Provider Adapter 可以在创建或 Next 时返回原始错误。`LLMRuntime` 包装 Adapter，向 Agent Runner 提供规范 Stream。

这里分两层：

```text
Provider Adapter Stream
  - 解析 HTTP/SSE 和供应商字段
  - 只有在看到协议终止边界（如 [DONE]）或可证明的正常 EOF 后，才产出 provider terminal
  - provider terminal 进入内部 pending 状态，Adapter 的下一次 Next 必须本地立即返回 io.EOF，不再读网络

LLMRuntime Stream
  - 归一化 block/usage/failure
  - 对 pending terminal 做一次非阻塞 lookahead；确认 EOF 后才向 Runner 发布唯一规范 finish
```

规范化规则：

1. 所有 Adapter 错误先归一为 `LlmFailure`。
2. 创建阶段错误转换为只有一个 terminal finish 的规范流。
3. terminal 前出现非预期 `io.EOF` 转换为 `STREAM_CLOSED`；只有 Adapter 已依据协议确认完整结束时，EOF 才可用于确认 pending terminal。
4. Context cancel 转换为 `aborted` finish。
5. Runtime 向 Runner 发布 terminal finish 后，后续 `Next` 只允许立即返回 `io.EOF`。
6. `Next` 禁止并发调用；并发调用返回 `PROTOCOL`。
7. `Close` 幂等、必须有界，不得等待网络无限阻塞；Close 后 `Next` 立即返回 `io.EOF` 或已冻结的 terminal 结果。
8. Adapter 不得在已返回 provider terminal 后继续访问网络；因此 Runtime 的 terminal lookahead 不引入额外网络等待。

### 13.2 StreamChunk

```go
type StreamChunk struct {
    Kind        StreamChunkKind
    Index       uint32
    BlockType   ContentBlockType
    Text        string
    ToolCallID  string
    ToolName    string
    ArgumentsDelta string
    Block       *ContentBlock
    Usage       *TokenUsage
    Finish      *FinishReason
    ProviderRaw json.RawMessage
}
```

`ProviderRaw` 默认不进入 Event；只有显式开启且完成脱敏时才用于诊断。

### 13.3 Chunk 类型

```text
block-start
text-delta
reasoning-delta
tool-call-delta
block-end
usage
finish
```

### 13.4 Terminal contract

1. 每次规范 Stream 恰好一个 `finish`。
2. `usage` 只能出现在 finish 前。
3. finish 后不得出现其他 chunk，且下一次 `Next` 必须立即返回 `io.EOF`。
4. `error` 和 `aborted` 也通过 FinishReason 表达。
5. 双 provider terminal、terminal pending 后仍出现数据、非法 block index、并发 Next 或 Close 不收敛，均归一为 `PROTOCOL` failure。
6. Runner 只消费 LLMRuntime 的规范 Stream，不直接消费 Adapter Stream；因此看到 finish 时已经完成 terminal 完整性验证，可以立即提交终态并停止读取。

## 14. FinishReason、Usage 与 Failure

### 14.1 FinishReason

```text
stop
tool-calls
max-tokens
aborted
error
```

`aborted` 和 `error` 必须携带 `LlmFailure`。

### 14.2 TokenUsage

```go
type TokenUsage struct {
    InputTokens       int64
    OutputTokens      int64
    CacheReadTokens   *int64
    CacheWriteTokens  *int64
    ReasoningTokens   *int64
}
```

约束：

- 所有值非负。
- 同 attempt 多个 usage chunk 时，Provider Adapter 必须声明是累计值还是增量值；Runtime 统一输出最终累计值。
- Usage 缺失不把成功响应改判为失败；`usage_missing=true` 固定写入 REQUIRED `step/end` payload，不能依赖缺失的 IGNORABLE `model/usage` 自我表达。
- Retry 时各 attempt Usage 分开记录；Session/Turn 汇总不能只保留最后一次。Usage 汇总是诊断/计量投影，不参与 Agent 控制流，因此 `model/usage` 保持 IGNORABLE；若未来用量影响配额裁决，必须新增 REQUIRED 计量事实，不能直接改变本事件语义。

### 14.3 LlmFailure

```go
type LlmFailure struct {
    Message              string
    Code                 string
    Status               *int
    ProviderRetryAfterMs *int64
    ProviderRequestID    string
    RetryableHint        *bool
    CauseChain           []FailureCause
}
```

路由只使用 `Code`，不得解析 Message。

首版稳定错误码：

```text
CONTEXT_WINDOW_EXCEEDED
QUOTA
RATE_LIMIT
EMPTY_RESPONSE
INVALID_CREDENTIAL
INVALID_REQUEST
MODEL_NOT_FOUND
TOOL_EXECUTION_UNAVAILABLE
TRANSPORT
TIMEOUT
STREAM_CLOSED
PROTOCOL
NO_ADAPTER
ABORTED
UNKNOWN
```

OpenAI-compatible Adapter 必须保存 HTTP status、供应商 request id 和合法 Retry-After。

## 15. BlockAssembler

### 15.1 组装规则

1. 按 block index 首次出现顺序维护最终顺序，而不是按结束顺序。
2. 允许没有显式 block-start 的 delta 自动创建 block。
3. 允许没有显式 block-end 的完整 text/reasoning 在正常 finish 时收敛。
4. block-end 后的迟到 delta 忽略并记录协议诊断。
5. tool-call name 和 arguments delta 按原始顺序拼接。
6. 相同 index 不能在不兼容 block type 之间切换。
7. finish 后禁止 push。

### 15.2 正常完成

- `stop`：保留完整 text/reasoning 和完整 tool-call。
- `tool-calls`：Assembler 必须能无损产出只有 tool-call、没有文本的 message；但 Stage 2 Runner 因 `Tools=[]` 不将其写入 Surface，而是记录审计 chunk并以 `TOOL_EXECUTION_UNAVAILABLE` blocked 收敛。该组装能力作为 Stage 3 前置能力保留。
- `max-tokens`：保留安全 text/reasoning；删除不完整或参数不安全的 tool-call。

### 15.3 interruptedBlocks

正常 Cancel 时只返回：

- 非空 text；
- 非空 reasoning。

必须丢弃：

- 全部未执行 tool-call；
- 空 block；
- 已被协议判定无效的 block。

### 15.4 Attempt 隔离

每个 Attempt 使用独立 BlockAssembler 和独立 chunk seq 列表。Retry 时禁止复用上一个 Assembler。

## 16. assistant/chunk 与 assistant/message

### 16.1 Chunk 提交

不能先落库再让 Assembler 发现跨 chunk 冲突，否则非法 chunk 会被标记成“Runtime 已接受”。Assembler 使用事务式 staging：

```text
Assembler.PlanPush(chunk)
→ 在当前状态上完成全部单 chunk + 跨 chunk 校验
→ 返回不可变 Mutation/next state，但不修改 live Assembler
→ append assistant/chunk
→ committed 成功后 Mutation.Commit()
```

规则：

- `PlanPush` 失败：不写 `assistant/chunk`，追加 `model/error(PROTOCOL)` 并终止 attempt；可把脱敏诊断写入非 Surface error payload。
- Event Append 失败：丢弃 Mutation，不改变 Assembler；由 fencing/持久化错误路径收敛。
- Append 成功：安装 next state，不得再次执行可能失败的业务校验。
- Runner 单 goroutine 串行调用 Plan/Commit；Mutation 绑定当前 assembler revision，禁止重复 Commit。

Event 记录的是 Runtime 已接受且能够组装的规范 chunk，而不是供应商任意原始字节。

每个 chunk payload 必须含：

```text
turn_id
step_id
attempt
chunk_index（attempt 内单调递增）
规范 chunk
```

### 16.2 最终消息

正常 `stop/max-tokens`：

```text
finish
→ assembler.blocks()
→ 验证至少一个有效 block，或按 EMPTY_RESPONSE 处理
→ CommitAttemptTerminal 原子 append assistant/message + step/end
```

`tool-calls` 在 Stage 2 是特殊终态：完成组装与审计校验，但不写 Surface assistant/message；原子写 `model/error(TOOL_EXECUTION_UNAVAILABLE) + step/end(blocked)`，随后关闭 Turn。

`assistant/message` payload 至少包含：

```text
message
finish_kind
attempt
interrupted=false
usage summary
provider_request_id
sourceEventSeqs
```

### 16.3 失败 Attempt

失败 Attempt 的 chunk：

- 保留在 EventStore；
- 不生成 Surface message；
- 不被下一 Attempt 继承；
- UI 可按 attempt 分组显示为失败轨迹。

## 17. Cancel 设计

### 17.1 Cancel 请求

```go
type CancelOptions struct {
    KeepInbox bool
    Cause     CancelCause
}

// Agent Port 使用 Cancel(ctx, CancelCommand) (CancelResult, error)，
// RequestID/IdempotencyKey 属于命令信封，不塞进 CancelOptions。
```

Cause 至少包含：

```text
user
parent
lifecycle-disposed
timeout
system
```

### 17.2 顺序

活动时：

```text
1. RequestCancel(expected turn/step/attempt，可无 active Step)
2. Actor 校验当前 phase；若仍可取消，原子提交 agent/cancel-requested
3. keepInbox=false 时同批清空 next-step 和 next-turn
4. committed 后把 committed cause 交给 Driver 的 active cancel controller
5. 在 RuntimeEntry 锁内清除提交前已经存在的旧 wake latch；提交后到达的新输入可以重新置位
6. Driver 停止 Stream/backoff/hook
7a. 若有 active Step：CommitAttemptAborted 原子提交安全前缀（如有）+ step/end(aborted)
7b. 若无 active Step：跳过 Step closer
8. FinishCancelledTurn 原子复核 cancel cause + no active Step，提交 turn/end(aborted)，并重新检查提交后到达的 wake/Inbox
```

如果 cancel 事件提交失败：

- 本地仍要中止当前上游请求，避免 zombie 调用；
- API 返回持久化失败，不伪造已成功持久化的 cancel Receipt；
- RuntimeEntry 进入 `maintenance`，停止消费旧 Stream 的任何后续结果，不得宣告 idle；
- 使用同一个 cancel operation ID，在 Registry 自有 Context 下做最多 3 次短暂持久化重试；
- Append 返回 conflict、head advance 或其他“可能已提交但响应丢失”的结果时，必须先 Reload/查询 committed Event，并按 `cancel_operation_id` 对账；命中则视为原 cancel 已成功，不得追加第二个 cause；
- 任一次重试或对账确认成功后按 committed cancel 正常收敛；若仍失败或发生 fencing，则关闭并移除旧 Actor/Driver，等待后续同步 Load/Repair 以 `interrupted` 收敛未完成边界。

### 17.3 First cause wins

First cause 由 Actor 中第一个成功提交的 `agent/cancel-requested` 决定，而不是由 API goroutine 本地抢 `sync.Once` 决定。Driver 的 cancel controller 可以使用 `sync.Once`，但只能安装 Actor 返回的 committed cause；后续 Cancel 读取并返回既有 cause，不得覆盖。

如果成功 terminal 已先提交并关闭 Attempt，随后 Cancel 只能针对仍活动的 Turn 生效；如果 Turn 也已关闭，则是 idle no-op。反之 Cancel 先提交后，正常 success terminal Command 因 phase/cancel mismatch 被拒绝，只允许 aborted 收敛。

### 17.4 Idle Cancel

真正 idle 且没有 active Turn/maintenance 时：

- no-op；
- 不写 cancel Event；
- 默认不清理未来输入，因为没有“当前任务”可取消；
- 返回当前状态。

若 Inbox 非空但尚无 active Turn，不属于真正 idle。使用独立 Actor Command：

```text
CancelPendingInbox:
  前置：无 active Turn，Inbox 存在可取消输入
  同一 Append batch：
    1. agent/cancel-requested {
         scope: "pending-only",
         terminal: true,
         cancel_operation_id: ...
       }
    2. keepInbox=false 时清空 next-step/next-turn 的 splice
```

pending-only Cancel 用于 Receipt、幂等和清空事实，不安装 active Turn cancel cause，不生成 `turn/end`，也不阻止后续新输入。`keepInbox=true` 时没有可取消的 active 工作，因此等价于 no-op 并返回当前状态。它与新 FollowUp/Steer 的胜负仍由 Actor mailbox 顺序决定。

### 17.5 Cancel mid-stream

正常取消当前 Attempt：

1. `assembler.interruptedBlocks()` 取安全前缀。
2. 非空时写 `assistant/message {interrupted:true}`。
3. `sourceEventSeqs` 只引用当前 Attempt 中形成安全前缀的 chunk。
4. 不写未完成或未执行 tool-call。
5. 安全前缀为空时不写 assistant/message。

Retry backoff 或 error recovery 窗口 Cancel：

- 失败 Attempt 的 partial output 不进入 Surface；
- 取消可立即打断 timer；
- 不开始下一 Attempt。

## 18. Retry 设计

### 18.1 RetryPolicy

```go
type RetryPolicy interface {
    Decide(RetryContext) RetryDecision
}
```

输入至少包含：

```text
failure code
HTTP status
provider Retry-After
current attempt
已重试次数
本 Step 累计等待
模型和 Provider
```

输出：

```text
stop
retry-after(delay)
retry-always-after(delay)
```

### 18.2 默认规则

原版 DSH 当前默认值已经从 `packages/llm/llm/src/retry-policy.ts` 核对。Go 版 normal policy 对齐其主要参数，并额外增加累计等待上限：

```text
maxRetries    = 5（首次请求后的最大重试次数）
initialDelay  = 500ms
maxDelay      = 10s
jitterRatio   = 0.1（对称有界 jitter）
maxElapsed    = 60s（Go 版安全上限，不包含模型实际请求耗时）
```

Provider route 可以覆盖这些值；配置在 Generation 构建时校验并冻结。`always` 模式不受 maxRetries/maxElapsed 限制，但仍必须受 Turn Context、Cancel、Dispose 和单次 maxDelay 约束。测试默认注入固定随机源或将 jitter 设为 0，不能依赖真实随机时间。

默认可重试：

```text
RATE_LIMIT
TRANSPORT
TIMEOUT
EMPTY_RESPONSE
部分 5xx UNKNOWN
```

默认不可重试：

```text
INVALID_CREDENTIAL
INVALID_REQUEST
MODEL_NOT_FOUND
TOOL_EXECUTION_UNAVAILABLE
CONTEXT_WINDOW_EXCEEDED
QUOTA（除非 Provider 明确可恢复）
PROTOCOL
NO_ADAPTER
ABORTED
```

`STREAM_CLOSED` 默认不可重试，除非具体 Provider 策略明确允许；避免对已产生不可判定外部状态的流进行盲目恢复。

### 18.3 Delay

```text
providerRetryAfterMs 合法且 <= maxDelay
  → 优先使用
否则
  → exponential backoff + bounded jitter
```

要求：

- delay 非负且受 `maxDelay` 限制；
- 累计等待受 `maxElapsed` 限制，`always` 策略除外；
- timer 必须可被 Turn Context 取消；
- 测试注入 Clock/Sleeper，不使用真实 sleep。

### 18.4 Event 顺序

```text
assistant/chunk*（失败 attempt，可能为空）
→ CommitAttemptFailure：同批 model/error + llm/retry
→ cancellable backoff
→ StartRetryAttempt：同批 llm/retry-started + model/requested(attempt+1)
→ 新 Stream
```

如果策略决定不重试，则 `CommitAttemptFailure` 同批提交 `model/error + step/end(error)`。

Retry 数量由 `llm/retry` Event 计数，不依赖进程内变量。`StartRetryAttempt` 必须重新校验 turn/step、当前 retry_id、attempt 和 cancel 状态；Crash 落在 backoff 中时，恢复阶段只关闭边界，不自动继续等待或发起模型调用。

## 19. OpenAI-compatible Provider

### 19.1 请求映射

首版支持：

- `model`；
- `messages`；
- `stream=true`；
- `stream_options.include_usage=true`（供应商支持时）；
- `tools` 和 `tool_choice` 类型预留；
- temperature/max_tokens；
- 自定义 base URL；
- Authorization/Header 通过 CredentialRef 解析，不进入 Event。

### 19.2 响应映射

Reasoning 不做“看到任意相似字段就猜”的动态探测，而由 Provider route 选择显式 preset：

| Preset | 响应 reasoning 字段 | 历史 assistant reasoning 回传 | 说明 |
|---|---|---|---|
| `openai-chat` | 无 | 省略 | 标准 Chat Completions 文本/工具流，不臆造 reasoning |
| `deepseek-chat` | `choices[].delta.reasoning_content` | `messages[].reasoning_content` | 已从原版 `llm-deepseek` translate/serialize 核对 |
| `qwen-chat` | `choices[].delta.reasoning_content` | 按 route capability 决定是否回传同名字段 | 仅对明确支持该字段的阿里云/Qwen OpenAI-compatible route 启用 |
| `custom` | 配置的单一 `delta` 相对字段名 | 配置的单一 assistant 字段名 | 配置在 Generation 构建时校验，不允许运行期猜测 |

同一 chunk 若配置字段与其他已知 reasoning 字段同时出现且值冲突，归一为 `PROTOCOL`；空字符串不打开 reasoning block。未配置 reasoning capability 时，未知扩展字段忽略但可记录脱敏诊断，不得拼入普通 text。

Adapter 需要兼容：

- `delta.content` → text-delta；
- preset 指定字段 → reasoning-delta；
- `delta.tool_calls[index]` → tool-call-delta；
- finish_reason → FinishReason；
- usage → TokenUsage；
- SSE `[DONE]`；
- 非 2xx JSON error；
- 连接拒绝、读超时、半截 EOF 和非法 SSE。

### 19.3 安全

- base URL 必须通过配置和后续网络策略校验；本阶段至少拒绝缺失 scheme/host。
- 日志和 Event 不记录 API Key、Authorization 或完整敏感 Header。
- 错误体长度有上限并脱敏。
- SSE 单事件和累计响应大小有上限。
- Provider request ID 可记录，供应商原始响应默认不落库。

## 20. Crash Recovery

### 20.1 恢复原则

```text
保留已提交历史
→ 不猜测未提交副作用
→ 用 synthetic closer 收敛边界
→ 未领取 Inbox 继续保留
```

### 20.2 检测

`AgentProjector` Replay 后检测：

- 有 `turn/start`，无对应 `turn/end`；
- 有 `step/start`，无对应 `step/end`；
- 有 `model/requested`，但 Attempt 无 terminal；
- 已调度 `llm/retry`，但没有 `llm/retry-started`；
- 有 `llm/retry-started`，但没有下一次 `model/requested`。

### 20.3 Repair 与加载准入

Repair 必须是 `LoadActor` 初始化事务的一部分，不能在 Actor 已放入 Registry 后异步执行。冻结顺序：

```text
ClaimWriter
→ Replay + AgentProjection 检测不完整边界
→ 空 Append 再确认 epoch/head
→ 构造 admission=closed 的 candidate Actor/提交器
→ 同步提交 Repair batch
→ 应用 committed Repair Event 并得到最终 Snapshot
→ 启动 Actor goroutine
→ admission=accepting
→ Session Registry 发布 Actor
→ Agent RuntimeRegistry 才允许创建/唤醒 Driver
```

Repair batch：

```text
若 Step 未关闭：step/end(interrupted, synthetic=true)
若 Turn 未关闭：turn/end(interrupted, synthetic=true)
session/repaired（诊断摘要）
```

规则：

1. 已 Claim 输入不恢复 Inbox；`agent/input-claimed` 保留其归属证据。
2. 未 Claim 输入保持原队列。
3. 未完成 Attempt 不自动重发模型请求。
4. 未完成 tool call 绝不自动执行。
5. 已有 chunk 只保留审计，不自动生成 assistant/message。
6. 如果 crash 前已有 committed assistant/message，但缺 step/end，只追加 closer，不重复消息。
7. Repair Event 使用新 epoch 和当前 expectedSeq，仍受 fencing 保护。
8. Repair 失败或被 fencing：整个 Load 失败，candidate Actor 不进入 Registry、不启动 Driver。
9. Repair 必须幂等；第二次 Replay 看到 synthetic closer 后不得再次追加。
10. Stage 2 实现需重构当前 `LoadActor` 直接 `accepting=true` 的路径，新增 pre-publication initializer/repair hook；不得在 Publisher 回调中修复。

### 20.4 为什么不自动恢复 Attempt

无法证明崩溃前上游是否已接收请求、是否已计费、是否已产生供应商侧状态；未来 Tool 更可能有外部副作用。因此默认只关闭边界。用户或上层策略可以通过新 FollowUp 明确重试，而不是隐式重放。

## 21. 幂等与 Receipt

### 21.1 Receipt

```go
type Receipt struct {
    SessionID      string
    InputID        string
    RequestID      string
    CommandID      string
    IdempotencyKey string
    AcceptedSeq    uint64
    Target         InboxTarget
    Placement      string
    Duplicate      bool
    AcceptedAt     time.Time
}

type CancelCommand struct {
    RequestID      string
    IdempotencyKey string
    Options        CancelOptions
}

type CancelResult struct {
    Receipt *Receipt
    Status  Status
    NoOp    bool
}
```

Receipt 必须来自 committed Event，而不是预估 seq/time。普通输入的 `CommandID` 与 insertion command 关联，`InputID` 标识队列 item；Cancel 没有 InputID，但有独立 CommandID/cancel_operation_id。幂等命中时 `Duplicate=true`。idle 或 pending-only + keepInbox 的 Cancel 返回 `NoOp=true`、当前 Status 和 nil Receipt。

### 21.2 重复请求

幂等索引键为：

```text
(session_id, command_kind, idempotency_key)
```

- 历史 `payload_hash` 相同：返回原 Receipt，不重复 append；若 AgentProjection 仍有可唤醒工作，则幂等调用 `RequestWake`，否则不重复 wake。
- 历史 `payload_hash` 不同：返回 `IDEMPOTENCY_CONFLICT`。
- command_kind 不同：互不冲突，避免 FollowUp 与 Cancel 等命令意外共享命名空间。

空 IdempotencyKey 不去重，但仍要求 InputID 唯一。

Cancel 使用相同原则但不创建 InputID：`(session_id, command_kind=cancel, idempotency_key)` 命中且 payload hash 相同时返回原 cancel Receipt/cause，不重复清空 Inbox或再次触发本地取消；hash 不同返回 `IDEMPOTENCY_CONFLICT`。`cancel_operation_id` 持久化在 `agent/cancel-requested`，用于处理“数据库可能已提交但调用方未收到响应”的不确定结果。

### 21.3 Replay

AgentProjection 从接受输入的 insertion splice 重建：

```text
command_kind
idempotency_key
payload_hash
InputID
actual_target
AcceptedSeq（Event.Seq）
AcceptedAt（Event.CommittedAt）
```

该索引不随 item 被 Claim 或 Cancel 删除。首版允许全量保留；Stage 4/9 再设计可重建 checkpoint 和保留窗口。缺失任一必要字段的新版接受 Event 为 Replay 错误，不允许降级为“未幂等”。

## 22. Proto 与 Domain 对齐

### 22.1 当前问题

- `internal/agent.Agent` 与 `agent.proto` 的 request ID、idempotency key、结构化 ContentPart、Receipt 和 last error 不完全一致。
- 当前 `llm.Message.Content string` 和 `StreamEvent` 表达力不足。
- 当前 descriptor compatibility 仅做二进制文件比较，不能区分 additive 和 breaking change。

### 22.2 决策

1. Proto 是 Transport Contract，Go Domain 不直接使用生成类型，但语义必须一一映射。
2. Domain Port 先冻结，再做 additive Proto 修改。
3. 在修改 Proto baseline 前，先把兼容门改为语义检查：至少能允许字段追加、拒绝字段号复用/删除和类型破坏。
4. JSON/Event payload 单独做 fixture compatibility，不把 Proto descriptor 检查当成 Event Schema 检查。
5. 不在 Transport Handler 内补 Agent 状态机逻辑。

### 22.3 冻结的 Stage 2 additive 变更

Stage 2 不新增 `llm.proto`。原因是模型请求、StreamChunk、ContentBlock、FinishReason 和 LlmFailure 当前只在服务进程内部与 Session Event JSON payload 中使用，没有独立的外部 LLM RPC；此时提前建立 Transport Contract 会把尚未出现的 API 固化。Stage 3/8 出现真实跨进程或前端 typed contract 时再单独评审 `llm.proto`。

现有字段不删除、不改类型、不复用编号；本阶段追加字段冻结为可直接实现的 proto3 声明：

```proto
// common.proto
message Usage {
  // existing 1..4 unchanged
  optional uint64 reasoning_tokens = 5;
  bool cache_read_tokens_present = 6;
  bool cache_write_tokens_present = 7;
}

message Receipt {
  // existing 1..7 unchanged
  string input_id = 8;
  string target = 9;
  string idempotency_key = 10;
}

// agent.proto
message AgentStatus {
  // existing 1..6 unchanged
  uint32 active_attempt = 7;
  uint32 retry_count = 8;
  uint32 next_turn_messages = 9;
  uint32 next_step_messages = 10;
  Cancellation cancellation = 11;
  uint64 last_event_seq = 12;
}

message InjectRequest {
  // existing 1..5 unchanged
  string idempotency_key = 6;
}

message CancelAgentRequest {
  // existing 1..3 unchanged
  bool keep_inbox = 4;
  string idempotency_key = 5;
}

message SubmitPromptResponse {
  // existing receipt = 1 unchanged；现有 Agent RPC 共用该响应。
  AgentStatus status = 2;
  bool no_op = 3;
}
```

`Cancellation.source` 已是 typed cause，不重复新增另一套 CancelCause enum；Domain `CancelCause` 与该枚举显式映射。Receipt 现有 `accepted_seq=4`、`placement=5` 保持不变，新增 `target=9` 表示内部 next-turn/next-step。`command_id` 保留为命令追踪 ID，不能借用为 InputID。

Cancel 也使用 `(session_id, command_kind=cancel, idempotency_key)` 幂等命名空间；其 hash 覆盖 keep_inbox 和规范化 Cancellation，不包含 request_id/cancelled_at。空 key 不去重。既有 `Usage.cache_*_tokens` 不能在“只追加”规则下改为 optional，因此用 presence flag 区分“供应商未返回”和“明确为 0”：present=false 时对应 token 值必须为 0，present=true 时值为 0 也表示供应商明确返回 0；任何 `present=false + 非零值` 都是 Transport validation error。新 reasoning_tokens 可直接使用 proto3 optional。

Event 中 attempt、failure、usage 和 request hash 继续放在版本化 `EventEnvelope.data`，通过 JSON fixture compatibility 管理；不向 Event Envelope 顶层追加 attempt。完成这些变更前必须先把 descriptor 二进制 `cmp` 替换成语义兼容检查。

## 23. 并发与错误边界

### 23.1 并发不变量

1. 每 Session 最多一个 Driver loop。
2. 每 Turn 最多一个 active Attempt。
3. Actor 可以在 Attempt 运行时继续接受 Inbox 和 Cancel Command。
4. Runner 每次提交都携带预期活动 Turn/Step/Attempt/phase；过期 Stream 的 chunk 必须被拒绝。
5. Cancel、Provider terminal、Retry 和新输入的胜负只由 Actor mailbox 中 committed Event 顺序决定，不由 goroutine 到达时间或本地 `sync.Once` 单独决定。
6. 所有 Channel send 都可取消或非阻塞，禁止 goroutine 泄漏。
7. Driver 读取 Snapshot 后执行的任何“检查后提交”都必须在 Command 中重新校验，Snapshot 只用于提案，不是授权。

### 23.2 活动 fencing 与唯一终态 Command

除数据库 writer fencing 外，Agent Event Command 增加逻辑活动校验：

```text
expected_turn_id
expected_step_id
expected_attempt
expected_phase
```

冻结以下 Actor Command 语义：

| Command | 前置 phase | 原子结果 |
|---|---|---|
| `CommitAttemptChunk` | 当前 attempt=streaming，未 cancel | 一个 accepted assistant/chunk |
| `CommitAttemptTerminal` | 当前 attempt=streaming，未 cancel | usage? + assistant/message? + step/end |
| `CommitAttemptFailure` | 当前 attempt=streaming，未 cancel | model/error + llm/retry，或 model/error + step/end(error) |
| `StartRetryAttempt` | retry-scheduled，未 cancel | llm/retry-started + model/requested(attempt+1) |
| `RequestCancel` | Turn/maintenance 仍活动 | cancel-requested + 可选 Inbox clear |
| `CancelPendingInbox` | 无 active Turn 且 Inbox 非空 | terminal pending-only cancel-requested + 可选 Inbox clear，不生成 turn/end |
| `CommitAttemptAborted` | cancel-requested 且有 active Step | interrupted message? + step/end(aborted) |
| `FinishCancelledTurn` | cancel-requested 且无 active Step | turn/end(aborted)，随后按新 wake/Inbox 决定是否再启动 Turn |
| `FinishTurnIfQuiescent` | 无 active Step 且未 cancel | 条件满足时 turn/end；否则返回当前待处理工作 |

同一状态转换只有一个 Command 能成功：

- success terminal 先提交：后到 Cancel 按当前 Turn 状态判断，不能改写已关闭 Attempt。
- Cancel 先提交：后到 success/error terminal 被拒绝，只能提交 aborted terminal。
- model/error + retry 先提交：Cancel 可取消 retry-scheduled；`StartRetryAttempt` 必须再次检查 cancel。
- 新 next-step 输入先提交：`FinishTurnIfQuiescent` 不能关闭 Turn；Turn 先关闭时，后到的唤醒输入按 idle/abort-to-idle 规则进入下一 Turn。

当旧 Stream 在 Cancel 或新 Attempt 后迟到：

- Actor 拒绝追加为活动结果；
- Runner 关闭 Stream；
- 可记录本地诊断，不污染 Session 事实。

### 23.3 Panic

Runner 顶层恢复 panic：

1. 转换为内部 Failure；
2. 尝试关闭 active Step/Turn 为 error；
3. 释放 Stream 和 Lease；
4. 不吞掉无法持久化的错误；
5. 测试环境可重新 panic，以暴露编程缺陷。

## 24. 测试设计

### 24.1 Inbox 表驱动

- 多个 FollowUp 每条独占 Turn；
- next-step 全部先于 next-turn 第一条；
- Steer 唤醒，Inject 不唤醒；
- Claim 后 keepInbox Cancel 不恢复；
- 默认 Cancel 清空两队列；
- placement 映射正确；
- 幂等重放返回原 Receipt；
- 幂等键同 key 异 payload 冲突；
- abort-to-idle 窗口 wake 不丢失；
- disposed 不 latch；
- 接受后被 Claim/Cancel 删除，再 Replay 仍能幂等返回原 Receipt；
- `turn/start + Claim` 批次故障要么全有、要么全无；
- 后续 Claim 的 splice 与 `agent/input-claimed` 批次故障要么全有、要么全无；
- claim-delete splice 的 claim_id 必须一致，且 Replay 禁止 marker 中插入其他 Event；
- Inbox insertion 成功后进程重启、没有新请求时，WakeReconciler 仍能自动恢复 FollowUp/Steer；
- 幂等重复不重复 append，但待运行工作仍会幂等 RequestWake。

### 24.2 Turn/Step

- 正常无工具单 Step；
- 多个 next-step 形成多 Step；
- pre-step reject 无 step/start/user/message；
- pre-step 清空输入不调用 Provider；
- Assembler/Surface 能表达 tool-call-only assistant message；Stage 2 Runner 遇到该终态时 blocked 且不写 Surface；
- max-tokens 裁剪半截 tool call；
- turn-stopping 新增 steering 后继续 Step；
- stale attempt chunk 被拒绝；
- `FinishTurnIfQuiescent` 与新 Steer 竞争时由 mailbox 顺序得到唯一结果；
- Agent RuntimeRegistry single-flight 保证同 Session 单 Driver；
- Driver/Actor lazy load 失败保留 wake latch，按 fake clock 重试并暴露 last_start_error；
- session actor fencing/Remove/Dispose 会取消并等待 Driver，且释放 GenerationLease；
- root Dispose 顺序为 Agent RuntimeRegistry → GenerationManager → Session Registry，活动 GenerationLease 不造成死锁；
- pre-step/request/turn-stopping waterfall 的 reject/timeout/error/panic 都关闭正确边界，request Hook reject/失败不写 model/requested。

### 24.3 Stream/Assembler

- delta 无 start/end；
- 多 block 交错；
- tool name/arguments 分片；
- block-end 后迟到 delta；
- usage 在 finish 前；
- 双 finish；
- finish 后 chunk；
- terminal 前 EOF；
- empty response；
- partial EOF；
- stalled stream timeout；
- Context cancel；
- Close 幂等；
- Provider goroutine 无泄漏；
- provider terminal 后本地 lookahead 立即 EOF；
- provider terminal 后仍有数据归一为 PROTOCOL；
- 并发 Next、Close 后 Next、Close 超时均有确定结果；
- Assembler PlanPush 成功但 Event Append 失败时不安装 next state；
- 非法 chunk 不写 accepted assistant/chunk。

### 24.4 Retry

- TRANSPORT 恢复成功；
- EMPTY_RESPONSE 恢复成功；
- INVALID_CREDENTIAL 不重试；
- provider Retry-After 优先；
- maxRetries/maxElapsed，并验证总 attempt 数等于 `1 + maxRetries`；
- backoff 可取消；
- 失败 attempt chunk 不进入 Surface；
- retry Event 顺序可 Replay；
- `model/error + llm/retry` 原子批次；
- Cancel 与 `StartRetryAttempt` 竞争时只有一个状态转换成功；
- always 策略仍响应 disposed/cancel。

### 24.5 Cancel

- idle no-op；
- mid-stream 保存 text/reasoning 前缀；
- mid-tool-call 丢弃 tool call；
- cancel before first chunk 不写 message；
- cancel during backoff 不启动新 attempt；
- first committed cause wins；
- Cancel、finish、retry-started 三方竞态只有一个合法转换；
- Cancel 与新唤醒输入的两种 mailbox 顺序都得到确定 target/wake 结果；
- cancel Event 提交失败时仍关闭本地上游，RuntimeEntry 保持 maintenance，并通过同 operation ID 重试或关闭 Actor 等待 Repair；
- Cancel committed 后清除旧 wake signal，提交后新到达的 wake 不丢失；
- 无 active Step 的 pre-step/step-end 后窗口可直接 FinishCancelledTurn；
- 无 active Turn 但 Inbox 非空时 CancelPendingInbox 不生成 turn/end，并与新 FollowUp 的两种 mailbox 顺序都确定；
- idle Cancel 的 Domain/Proto 返回 `no_op=true + status`，pending-only Cancel 返回 committed Receipt。

### 24.6 Crash/Replay

- turn/start 后崩溃；
- step/start 后崩溃；
- partial chunks 后崩溃；
- assistant/message 后、step/end 前崩溃；
- llm/retry 后、timer 中崩溃；
- retry-started 后、request 前崩溃；
- Repair 幂等；
- Repair 受 writer fencing；
- Repair 完成前 Actor 不进入 Registry、Driver 不启动；
- Repair 失败的 candidate Actor 不可见；
- 未 Claim Inbox 保留；
- 已 Claim 输入不恢复；
- 未知 REQUIRED Agent Event 阻止 Replay；未知 IGNORABLE 诊断事件不影响控制流。

### 24.7 竞态与故障

必须运行：

```text
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

并使用：

- fake clock；
- scripted Mock Provider；
- blocking Provider；
- protocol-violating Provider；
- fault-injecting EventStore；
- fencing Actor；
- goroutine leak 检查。

## 25. Mock Provider

Scripted Provider 以确定性脚本驱动：

```go
type ScriptAction interface{ isScriptAction() }

type EmitChunk struct { Chunk StreamChunk }
type WaitGate struct { Name string }
type ReturnError struct { Err error }
type EndEOF struct{}
```

能力：

- 精确控制 chunk 顺序；
- 在 gate 处等待测试触发 Cancel/Steer；
- 模拟创建阶段错误；
- 模拟半截流断开；
- 记录收到的 ModelRequest；
- 校验 Retry 是否创建新 Attempt；
- 校验 Context 和 Close 是否传播。

测试不依赖真实模型和真实网络。

## 26. 实施顺序

设计评审通过后按以下顺序编码：

1. 修复 Proto compatibility 检查，冻结 additive 变更规则和 Stage 2 Event fixture。
2. 将 Stage 1 单 Projector 演进为 `SessionSchema + Composite Projection`，确保候选/committed/Replay 同规则可失败试算。
3. 为 `LoadActor` 增加 pre-publication initializer/repair hook，并验证失败 Actor 不进入 Registry。
4. 实现 Agent RuntimeRegistry、只读 SessionCatalog/WakeReconciler、single-flight Driver ownership、关闭顺序和 fencing 联动测试。
5. 重构 `internal/llm` 领域类型：Message、ContentBlock、Chunk、Finish、Failure、Usage。
6. 实现事务式 BlockAssembler 和完整单测。
7. 实现规范 LLMRuntime、ErrorNormalizer 和 Scripted Mock Provider。
8. 实现 AgentProjection、Inbox 接受/Claim Event、Replay、幂等和 placement。
9. 实现 Driver wake latch、Turn/Step/Attempt 状态机和唯一终态 Command。
10. 实现 Cancel、stale activity fencing 和 finish/retry/new-input 竞态测试。
11. 实现 RetryPolicy、fake clock 和 retry Event。
12. 实现 Agent Crash Repair，并接入第 3 步的 pre-publication hook。
13. 实现 OpenAI-compatible Provider 和本地假 HTTP/SSE Server 测试。
14. 对齐 Agent Proto/Domain/Transport。
15. 增加 `phase2-accept`，执行兼容、单测、race、vet、build 和故障测试。
16. 更新主计划和 CURRENT checkpoint。

任何一步发现协议或状态机需要变化，先回写本文并重新评审，不直接在代码中形成隐式新语义。

## 27. 预期文件影响

设计通过后预计修改：

```text
internal/llm/provider.go
internal/llm/*_test.go
internal/agent/agent.go
internal/agent/inbox.go
internal/agent/driver.go
internal/agent/runner.go
internal/agent/projector.go
internal/agent/retry.go
internal/agent/recovery.go
internal/agent/wake_reconciler.go
internal/session/replay.go
internal/session/event.go（增加只读 SessionCatalog 能力接口，不改变 Event Envelope）
internal/session/actor.go（仅 Projection/活动校验扩展，不放慢 I/O）
internal/session/surface.go（支持 tool-call-only assistant message）
api/proto/dsh/protocol/v1/agent.proto
api/proto/dsh/protocol/v1/common.proto（Stage 2 不新增 llm.proto）
internal/transport/http/*
internal/transport/grpc/*
Makefile
```

可能新增但本阶段暂不需要：

```text
migrations/*/000003_agent_loop.*
```

只有决定建立可重建 projection/checkpoint 表时才新增 Migration。

## 28. Stage 2 验收门 G2

Stage 2 只有同时满足以下条件才算完成：

1. FollowUp/Steer/Inject/Cancel 的持久语义、placement 和幂等通过测试。
2. 同 Session 并发输入下只有一个 Driver，Turn/Step/Attempt 轨迹确定。
3. Session Actor 在 LLM Stream 和 Retry backoff 期间仍可接受命令。
4. 结构化 Message 无损表达 text、reasoning 和 tool-call-only assistant message。
5. 每个规范 Stream 恰好一个 terminal finish，非法 Provider 被归一化。
6. 成功 Attempt 产生可 Replay 的 assistant/message 和 Usage。
7. 失败 Attempt chunk 保留审计但不进入 Surface。
8. Cancel 保存安全前缀、丢弃 tool call，并正确收敛 Step/Turn。
9. Retry 在同一 Step 内进行，Event 顺序和次数可 Replay。
10. Crash 后 synthetic closer 收敛不完整 Turn/Step，不盲目重放请求。
11. OpenAI-compatible Provider 通过本地假 HTTP/SSE Server 测试。
12. Proto/Event compatibility 检查能区分 additive 和 breaking change。
13. `go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./...` 全部通过。
14. 所有代码保持未提交、未推送，除非勇哥明确要求。

## 29. 已冻结决策摘要

| 主题 | 决策 |
|---|---|
| Inbox 存储 | 两队列：next-turn、next-step |
| UI placement | queued/steering/context 为投影，不是第三套状态 |
| Claim 顺序 | 全部 next-step，再取一个 next-turn |
| Claim 原子性 | 首 Turn：turn/start + splice + input-claimed 同批；后续：splice + input-claimed 同批 |
| 长流程位置 | Actor 外的每 Session 单 Driver |
| Driver 所有权 | `internal/agent.RuntimeRegistry` single-flight 管理，不把 Agent 反向塞入 Session Registry |
| Lease | ActorLease + GenerationLease 覆盖整个 Turn，固定顺序获取、反向释放 |
| Retry 边界 | 同一个 Step 内增加 Attempt |
| Stream 接口 | pull-based Stream；Adapter 先确认协议结束，Runtime 验证 pending terminal 后再发布 finish |
| Message | 结构化 ContentBlock，不再使用 string Content |
| Chunk | block index + text/reasoning/tool-call/usage/finish；Assembler 事务式 Plan/Commit |
| 失败输出 | chunk 留审计，不进入 Surface |
| Cancel 输出 | 仅安全 text/reasoning 前缀；丢弃 tool-call |
| Cancel Inbox | 默认清空；keepInbox 仅保留未 Claim 输入 |
| Wake | 容量 1 latch，解决 abort-to-idle 窗口 |
| Crash | LoadActor 对外可见前 synthetic close，不自动重放 Attempt/Tool |
| 状态扩展 | 不可拆分的 SessionSchema + Composite Projection，Agent 自有 Projector |
| 终态竞态 | Actor mailbox 中唯一终态 Command 和 committed Event 顺序裁决 |
| 数据库 | 暂不新增 000003；Event Replay 为真相 |
| Stage 3 边界 | Stage 2 审计 tool-call 但 blocked 且不进 Surface；Stage 3 才执行并配对 tool/result |

## 30. 独立设计评审结论

### 30.1 评审结果

2026-08-26 完成两轮编码前独立评审及修正后全文复核。第二轮曾追加发现 6 项 P1，已全部回写关闭。最终结论：**当前未发现仍未关闭的 P0/P1 设计问题，Stage 2 可以进入用户确认门；确认前仍不修改业务代码。**

首轮评审发现并已关闭的主要问题：

| 级别 | 问题 | 已冻结修正 |
|---|---|---|
| P0 | `turn/start`、Inbox Claim、输入归属之间存在 crash gap | `StartTurnAndClaim` / `ClaimForProposedStep` 原子批次 + REQUIRED `agent/input-claimed` |
| P0 | Session Registry 只拥有 Actor，无法保证单 Driver 与 fencing 后退出 | application Scope 中新增 Agent RuntimeRegistry，single-flight 管理 Driver，固定 Remove/Dispose 顺序 |
| P0 | 单 Projector、无错误返回，候选/committed/Replay 规则不一致 | 不可变 SessionSchema + Composite Projection + 显式 Clone/Apply + batch closure 校验 |
| P0 | Cancel、finish、retry-started 和新输入缺少唯一持久裁决 | expected activity fencing + 唯一终态 Command，由 Actor mailbox committed 顺序决定 |
| P0 | Runner 收到 finish 后停止读取，无法证明 terminal 后无数据 | Adapter pending terminal + 本地 EOF 保证 + Runtime lookahead 后发布唯一 finish |
| P0 | chunk 先落库再组装会把非法 chunk 标记为 accepted | BlockAssembler `PlanPush → Append → Commit` |
| P1 | Repair 在 Actor 对外可见后执行可能让 Driver 看见半恢复状态 | LoadActor admission closed，同步 Repair 后才发布 Actor/启动 Driver |
| P1 | Stage 2 无 Tool 执行却可能写入 tool-call-only Surface | 保留审计，`TOOL_EXECUTION_UNAVAILABLE` blocked，不写 assistant/message |
| P1 | Cancel cause 由本地竞态决定可能与 Event 不一致 | first committed `agent/cancel-requested` wins |
| P1 | Projection API 不能让 Agent 订阅 Core Event，也缺 batch/replay boundary | 所有 Projector 按稳定顺序消费全部 Event，增加 `ValidateBoundary`，未知 IGNORABLE 走 seq-only |
| P1 | EventStore 未保存 append batch ID，Replay 无法证明历史同批 | candidate/committed 验证真实 batch；Replay 验证连续 claim 结构和最终 marker 闭合，不作虚假同批承诺 |
| P1 | SessionSchema 被误放入可热切换 Generation，关闭顺序可能卡住 Lease | Schema 提升为 root 不可变能力；注册顺序固定为 Session Registry → GenerationManager → Agent RuntimeRegistry |
| P1 | 无 active Step 的 Cancel 和 cancel 持久化失败缺少收敛路径 | 增加 FinishCancelledTurn；失败进入 maintenance，以 operation ID 重试或移除 Actor 等待 Repair |
| P1 | request waterfall 失败会遗留 active Step | 纳入统一 Hook 契约，原子提交 hook-failed + step/turn closer |
| P1 | Proto 清单缺 idempotency/presence/type，无法一一映射 Domain | 冻结完整 additive proto3 声明；Stage 2 不新增 llm.proto |
| P1 | 持久 Inbox 在进程重启后可能失去内存 wake latch | root WakeReconciler 启动时只读扫描；幂等重放在仍有工作时重新 RequestWake |
| P1 | claim splice 缺 claim_id，无法验证同一 claim | claim-delete splice 强制携带同一 claim_id，insertion/cancel 禁止携带 |
| P1 | request Hook explicit reject 会越过 active Step | 原子提交 step/end(blocked) + turn/end(blocked)，且不写 model/requested |
| P1 | 无 Turn、只有待处理 Inbox 时 Cancel 无合法事件 | 新增 CancelPendingInbox terminal 事件批次，不生成 turn/end |
| P1 | Cancel Domain/Proto 的 Receipt/no-op/status 不闭合 | 补齐 CancelCommand/CancelResult、Receipt 字段和 SubmitPromptResponse additive 字段 |

### 30.2 已定稿的原“未决实现细节”

1. Projection 使用非泛型注册存储、显式 `New/Clone/Apply` 和泛型读取 helper；Core、Surface、Agent 都进入同一 SessionSchema。
2. `model/requested.request_hash` 使用 RFC 8785 JCS + SHA-256，只覆盖会影响模型 wire payload 的非敏感字段；attempt 单独记录。
3. Reasoning 由 route preset 显式映射：标准 OpenAI 不猜测，DeepSeek 使用 `reasoning_content`，Qwen route 仅在能力声明后启用同名字段。
4. normal Retry 默认对齐原版：5 次、500ms、10s、0.1 jitter；Go 版额外设置 60s 累计 backoff 上限。
5. Stage 2 不新增 `llm.proto`；现有 `common.proto`、`agent.proto` 的追加字段和编号已在 22.3 节冻结。
6. SessionSchema 是 root 不可变能力；Resource 注册顺序固定为 Session Registry → GenerationManager → Agent RuntimeRegistry，Driver 每 Turn 再获取 GenerationLease。
7. pre-step/request/turn-stopping Hook 默认单个 2s、Waterfall 总计 5s；reject/timeout/error/panic 使用确定性终态，不进入 LLM Retry。
8. 持久 Inbox 的进程重启唤醒由 root WakeReconciler 负责；相同幂等请求可重复触发 wake，但不重复 append。
9. Cancel 区分 active Turn、pending-only Inbox 和 true idle 三种状态，并以 CancelResult 在 Domain/Proto 中统一表达 Receipt、Status 和 no-op。

### 30.3 编码准入条件

进入编码前只需勇哥确认本文方案。确认后严格按第 26 节顺序实施，并遵守：

- 先修改 Proto compatibility 语义门和 SessionSchema 内核，再写 Agent Driver；
- 每个小步先补测试，再运行受影响包和最终 `phase2-accept`；
- 任何新发现的协议或状态机变化先回写本文重新评审；
- 保持改动未提交、未推送，除非勇哥明确要求。
