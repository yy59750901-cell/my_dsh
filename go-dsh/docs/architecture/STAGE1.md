# Stage 1：Runtime 与 Session 内核

## 1. 目标与范围

Stage 1 建立所有 Agent、Tool、MCP、Skill、Subagent 共用的运行时底座。本阶段实现 Scope、Generation、EventBus、Session Actor、Replay、Registry 和 Ordered Surface；不进入 LLM 调用与 Tool 执行闭环。

## 2. 原版语义对齐结论

| 语义 | 原版行为 | Go 版约束 |
|---|---|---|
| Scope | 子作用域可以覆盖父作用域同名服务 | 服务发现从当前 Scope 向父级逐层查找，最近定义胜出 |
| Agent Scope | setup 完成后才能公开 Session/Agent | Candidate Scope 未完成构建和校验前不可被外部发现 |
| 生命周期 | Agent 作用域内 Tool、Prompt、Listener 随 Agent 一起释放 | Scope 先释放全部子 Scope，再按注册逆序释放本 Scope 资源 |
| Generation | 新配置导入或应用失败时保留上一可用树 | Candidate 构建、校验、应用全部成功后才能原子切换 |
| 调用稳定性 | 已开始的调用不因热重载漂移 | 调用通过 Lease 锁定 generation，释放 Lease 后旧代才可销毁 |
| 配置事务 | 多项变更中途失败时回滚已应用变更 | Candidate 内完成全部变更；失败只销毁 Candidate，不修改 Current |
| broadcast | 观察者并行接收，单个失败不能撤销既成事实 | 返回观察者错误集合供记录，但不把它解释为提交回滚 |
| serial | 按注册顺序执行，失败停止后续处理 | 用于可 veto 的创建、校验和策略链 |
| waterfall | 前一处理器输出作为后一处理器输入 | 用于配置、请求、上下文等有序变换 |
| session/event | 事件提交后的 observe-only feed | EventStore 事务提交后广播；监听失败不能撤销 Append |
| session/flush | awaited parallel durability checkpoint | 并行等待全部 flush handler，任一失败都向调用方报告 |
| Replay | Header、事件结构和必要事件严格校验 | seq 必须连续；未知 REQUIRED 阻止恢复，IGNORABLE 保留但跳过投影 |
| Surface | append 与 positional replacement 并存 | Replacement 只改变模型可见投影，不删除原始审计事件 |

## 3. Runtime 设计

### 3.1 Scope

Scope 是层级能力容器，不是全局 Service Locator。每个 Scope 包含：

- `name`：诊断名称；
- `generation`：所属配置代；
- `parent` 与 `children`；
- 当前层注册的 `Binding`；
- 当前层资源的 `Lifecycle`。

不变量：

1. 同一 Scope 内同名能力禁止重复注册。
2. 子 Scope 可注册与父 Scope 同名能力，实现 nearest-scope shadowing。
3. 已 Dispose 的 Scope 不接受注册、Fork 和 Resolve。
4. Dispose 幂等；严格 child-first；本 Scope 资源按注册逆序释放。
5. Resolve 返回能力的 generation，供 Tool、Prompt 和 Provider 调用锁定版本。

### 3.2 Generation

Generation Manager 维护唯一 Current 和若干被 Lease 引用的 Retired generation：

```text
Build Candidate
→ Candidate 内完成注册、导入、校验和 apply
→ Commit 时校验 base generation 未变化
→ 原子切换 Current
→ 旧代进入 Retired
→ 最后一个 Lease 释放后销毁旧代
```

失败路径：

- Build/Validate/Apply 失败：销毁 Candidate，Current 不变。
- 并发 Commit 检测到 base generation 已变化：返回冲突，不覆盖较新的 Current。
- 新代切换后旧代 Dispose 失败：记录并上报清理错误，但不把 Current 切回旧代。
- Manager Dispose：拒绝新 Acquire/Begin，等待或取消现有清理，释放全部 generation。

### 3.3 EventBus

EventBus 保持每个 topic 的注册顺序，并返回可幂等调用的取消订阅函数。

- `Broadcast`：对同一输入并行调用快照中的全部 Handler，等待全部结束，按注册顺序返回错误槽位；调用方决定仅记录还是作为 checkpoint 失败。
- `Serial`：按注册顺序调用；第一个错误立即返回，后续 Handler 不运行。
- `Waterfall`：按注册顺序调用；每个输出成为下一 Handler 输入；第一个错误立即返回。

分发开始后使用订阅快照；并发订阅或取消只影响下一次分发。

## 4. Session Actor 设计

### 4.1 单写者模型

每个热 Session 对应一个 Actor 和一个有界 mailbox。外部只能提交 Command：

```text
Registry.GetOrLoad(sessionID)
→ single-flight ClaimWriter + Replay
→ Submit Command
→ Actor goroutine 串行 Decide
→ EventStore.Append(epoch, expectedSeq)
→ Apply 已提交事件
→ Broadcast session/event
→ 必要时等待 session/flush
```

Actor 内存状态至少包含：`sessionID`、`epoch`、`headSeq`、`status`、当前 Turn/Step、Ordered Surface 和最近活动时间。

### 4.2 Command 契约

首版 Actor 使用通用命令接口：

```go
type Command interface {
    Decide(context.Context, Snapshot) ([]NewEvent, error)
}
```

每个提交请求包含独立响应通道。调用方 Context 取消只取消等待；命令一旦进入持久化临界区，不允许形成“数据库已提交但 Actor 未 Apply”的分裂状态。

### 4.3 Replay

恢复顺序：

1. `ClaimWriter` 获取新 epoch 和当前 Head。
2. 从 seq 0 分页加载事件。
3. 校验 SessionID、连续 seq、Schema Major 和 ReplayPolicy。
4. 逐事件应用到 Actor Snapshot 和 Surface。
5. Replay 最终 head 必须与 ClaimWriter 返回的 head 一致。
6. 完成前 Actor 不进入 Registry，不接受命令。

### 4.4 Registry

Registry 负责：

- 同一 Session 并发加载 single-flight；
- 每个 Session 最多一个进程内 Actor；
- 加载失败不缓存半初始化 Actor；
- Actor Dispose 后从 Registry 移除；
- 空闲回收和服务关闭时有界、child-first 停止；
- 数据库 `actor_epoch + expectedSeq` 始终作为跨进程最终防线。

## 5. Ordered Surface

只有明确的模型可见事件进入 Surface。首版至少支持：

- `user/message`；
- `assistant/message`；
- `tool/result`。

Surface Node 保存稳定位置、内容和 `sourceEventSeqs`。Replacement 必须：

1. 引用存在且连续的目标位置；
2. 覆盖被替换节点的来源 seq；
3. 只修改投影，不删除或改写 EventStore；
4. Replay 后得到确定且相同的模型消息序列。

## 6. 实施顺序

1. Scope、Lifecycle child-first 与 nearest-scope shadowing。
2. Generation Candidate、Commit、Rollback 与 Lease。
3. EventBus 三种分发语义。
4. Replay、Actor mailbox 与提交后广播。
5. Registry single-flight、空闲回收和有界关闭。
6. Ordered Surface 与 replacement。
7. SQLite/PostgreSQL 集成、race、故障和兼容测试。

## 7. Stage 1 验收门

- 同一 Session 并发提交仍得到连续且唯一的 seq。
- Replay 后 Snapshot、Surface 和 Head 与停机前一致。
- 旧 epoch 写入返回 `ErrWriterFenced`。
- 未知 REQUIRED 事件阻止加载；IGNORABLE 事件不破坏后续 seq。
- Scope 严格 child-first，Dispose 幂等且并发安全。
- Candidate 失败或并发冲突不污染 Current generation。
- 已 Acquire 的调用在热切换后仍解析到原 generation。
- Broadcast、Serial、Waterfall 的顺序、错误和取消语义通过单测与 race。
- `go test ./...`、`go test -race ./...`、`go build ./...` 全部通过。

## 8. 暂不进入本阶段的内容

- LLM Provider、Turn/Step 推进和流式 chunk；
- Tool 参数校验、审批和真实执行；
- MCP、Skill、Sandbox、Subagent、Workflow；
- WebSocket 前端轨迹。

这些能力只能依赖本阶段 Runtime 与 Session Port，不得绕过 Scope、Generation、Actor 或 EventStore 自建生命周期和状态真相。

## 9. 最终实现补充

### 9.1 Surface Schema 与兼容

- 新 Surface 协议使用 `SurfaceSchemaMinor = 1`。
- minor 1 及以上的 Surface Event 必须显式携带 `SurfaceOp`。
- minor 0 历史事件 Replay 时通过 upcaster 补齐 append 操作。
- 旧 `assistant/message` 和 `tool/result` payload 会被包装为 `{"message": ...}`。
- 旧数据库中的默认空 `sourceEventSeqs` 会归一为 nil，保留“字段缺失”和“显式空数组”的语义差异。
- `assistant/message` 可显式使用空来源数组；其他 Surface Event 不允许显式空来源数组。

### 9.2 Candidate 与 Committed 校验

事件校验分为两层：

```go
type CandidateEventValidator interface {
    ValidateCandidate(Event) error
}

type EventValidator interface {
    Validate(Event) error
}
```

- Candidate Validator 在持久化前运行，可拒绝事件，但不得依赖数据库拥有字段。
- Committed Validator 在 Append 回读后和 Replay 时运行，两条路径观察同一形态的 Event。
- Actor 只使用真实 committed Event 更新 Snapshot、Surface 和提交后事件流。

### 9.3 EventStore 与 Migration

- GORM Append 在同一事务内更新 Head、插入事件并回读真实数据库行。
- JSON 数据按语义等价校验，避免 PostgreSQL JSONB 规范化造成误报。
- 时间写入前统一为 UTC 微秒精度，以匹配 PostgreSQL `TIMESTAMPTZ`。
- Ordered Surface 使用独立的 `000002_ordered_surface` up/down Migration，禁止回写已经发布的 `000001`。

### 9.4 Writer Fencing 与关闭顺序

- `LoadActor` 在 Replay 完成后执行空 Append，再次验证 epoch 和 head。
- Actor 写入收到 `ErrWriterFenced` 后标记失败并关闭。
- Registry 发现 Actor 不再 accepting 后执行 removal single-flight，再加载新 Actor。
- Shutdown 与 idle eviction 统一先在 `admissionMu` 下关闭准入，释放锁后再触发 `signalStop()`，避免与 `sync.Once` 形成锁序反转。

## 10. 最终验收结果

2026-08-26 执行：

```text
make phase1-accept              PASS
proto-compat                    PASS
go fmt ./...                    PASS
go test ./...                   PASS
go test -race ./...             PASS
go vet ./...                    PASS
go build ./...                  PASS
PostgreSQL integration          SKIP: DSH_TEST_POSTGRES_DSN 未设置
```

Stage 1 的代码基线已冻结。后续 Agent、LLM、Tool 和 UI 必须建立在本阶段 Runtime、Actor、EventStore 和 Surface 不变量之上。
