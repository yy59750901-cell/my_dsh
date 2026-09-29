# Stage 0：协议与工程基线

## 1. 目标

Stage 0 只冻结不可频繁变化的语义和工程边界，不实现完整 Agent Loop。完成后，项目应具备可生成的 Proto、可编译的核心接口、可运行的 Gin/gRPC 健康检查、可迁移的 SQLite/PostgreSQL Schema 和事件 Fixture 测试入口。

## 2. 已冻结决策

| ADR | 决策 | 约束 |
|---|---|---|
| ADR-001 | 首版采用模块化单体 | 按领域包隔离，达到规模后再拆服务 |
| ADR-002 | Gin 面向外部，gRPC 面向内部与远程能力 | Transport 不实现 Agent 状态机 |
| ADR-003 | Protobuf 是规范协议，JSON 是稳定映射 | JSON 使用 lowerCamelCase；事件类型使用字符串 |
| ADR-004 | Session Event Store 是会话唯一事实来源 | Projection 可删除重建，不能反写事实 |
| ADR-005 | 同一 Session 只有一个 Actor 串行修改状态 | 其他 goroutine 只能投递命令，不得直接写状态 |
| ADR-006 | Scope 采用 nearest-scope shadowing | 子 Scope 可以覆盖父 Scope 的同名能力 |
| ADR-007 | Generation 构建成功后原子切换 | 失败的新 Generation 不影响当前运行树 |
| ADR-008 | 资源注销幂等且 child-first | Dispose 有界等待，并传播取消 |
| ADR-009 | GORM 只存在于 Repository Adapter | Domain、Agent、Session 不依赖 GORM Model |
| ADR-010 | 统一错误与取消模型 | gRPC Status 只表达传输失败，业务错误进入规范 Error |
| ADR-011 | WebSocket 是主事件通道，SSE 是只读降级 | 命令走 HTTP/gRPC，不通过 WebSocket 修改状态 |
| ADR-012 | 能力按 Definition/Provider/Consumer 分层 | Tool、FS、Process、LLM、Sandbox、Subagent 统一遵守 |
| ADR-013 | Compaction 只替换模型可见 Surface | 原始审计事件永不因压缩删除 |
| ADR-014 | Tool、MCP、Skill、Subagent 复用统一生命周期 | 不允许各模块自建热重载和撤回机制 |
| ADR-015 | 默认拒绝、凭据引用、全链路脱敏 | 长期凭据不进入 Prompt、事件和普通日志 |
| ADR-016 | 事件按 major/minor 演进 | 未知 REQUIRED 事件停止重建；IGNORABLE 事件保留并跳过投影 |

## 3. 包依赖方向

```text
cmd
  → transport/httpapi, transport/grpcapi
  → application（后续阶段建立）
  → domain ports：session、agent、llm、tool、runtime

repository/gormrepo、LLM Provider、Remote Tool、MCP、Sandbox
  → 只实现 domain ports
```

禁止依赖：

1. `session` 依赖 Gin、gRPC、GORM、MCP 或 Skill。
2. `agent` 直接依赖具体模型 SDK、GORM Model 或 HTTP Request。
3. Repository/Transport 直接修改 Session Actor 内部状态。
4. Projection 成为权威数据源。
5. Tool、MCP、Skill 绕过 Runtime 生命周期注册资源。
6. 循环依赖和跨包全局可变状态。

## 4. 规范事件

每个事件都使用统一 Envelope：

```text
schema_version、event_type、event_id、session_id、seq
occurred_at、committed_at、replay_policy、data
turn_id、step_id、call_id、trace、causation_event_id、source_event_seqs
```

最小事件集：

```text
session/created
turn/start
user/message
step/start
assistant/chunk
assistant/message
model/error
tool/call
tool/result
approval/requested
approval/resolved
step/end
turn/end
session/cancelled
```

不变量：

1. 同 Session 的 `seq` 从 1 连续递增且唯一。
2. 事件只有在数据库事务提交后才能广播。
3. 一个 `tool/call` 最多有一个最终 `tool/result`。
4. Turn/Step 最终必须关闭，或存在明确可恢复状态。
5. 未知 REQUIRED 事件必须阻止 Session 继续运行。
6. 事件中的 `data` 必须可以保留未知字段，禁止 ProtoJSON 往返时静默丢失。

## 5. Session Actor 与 EventStore

Actor 串行处理：

```text
Command 入 mailbox
→ 校验 Actor epoch 与当前状态
→ 生成一批 NewEvent
→ EventStore.Append(expectedSeq, epoch, events)
→ 事务提交
→ Actor 应用事件更新内存状态
→ EventHub 广播已提交事件
```

EventStore 最小接口：

```go
type EventStore interface {
    Create(ctx context.Context, session NewSession) error
    ClaimWriter(ctx context.Context, sessionID string) (epoch uint64, head uint64, err error)
    Append(ctx context.Context, sessionID string, epoch uint64, expectedSeq uint64, events []NewEvent) ([]Event, error)
    Load(ctx context.Context, sessionID string, afterSeq uint64, limit int) ([]Event, error)
    Head(ctx context.Context, sessionID string) (uint64, error)
}
```

数据库必须以 `session_id + seq` 唯一约束、`actor_epoch` fencing 和 `expectedSeq` CAS 作为最终防线；进程内单 Actor 不能替代数据库约束。

## 6. 首批数据表

### sessions

- `id`、`tenant_id`、`workspace_id`
- `parent_id`、`fork_seq`
- `status`、`last_seq`、`actor_epoch`
- `created_at`、`updated_at`、`archived_at`

索引：`tenant_id + updated_at`、`workspace_id`、`parent_id`。

### session_events

- `session_id + seq` 联合主键
- `event_id` 全局唯一
- `type`、`schema_major`、`schema_minor`、`replay_policy`
- `occurred_at`、`committed_at`
- `turn_id`、`step_id`、`call_id`
- `trace_id`、`span_id`、`parent_span_id`
- `causation_event_id`、`source_event_seqs`
- `data`、`created_at`

索引：`session_id + type + seq`、`call_id`、`trace_id`。

### session_projections

- `session_id + name` 联合主键
- `projection_version`、`through_seq`、`data`、`updated_at`

Projection 必须支持全量删除后由事件重建。

## 7. 错误与取消

错误分类：

```text
VALIDATION、AUTHENTICATION、AUTHORIZATION、NOT_FOUND、CONFLICT
RATE_LIMIT、POLICY、APPROVAL、MODEL、TOOL、STORAGE、TRANSPORT
RESOURCE、INTERNAL、CANCELLATION
```

取消来源：

```text
USER、CLIENT_DISCONNECT、DEADLINE、PARENT、POLICY
SESSION_DISPOSE、SERVER_SHUTDOWN、TOOL_PROVIDER、MODEL_PROVIDER、SYSTEM
```

取消必须幂等，最终通过规范事件使 Turn/Step/Session 收敛；不能只依赖 `context.Canceled` 且不记事件。

## 8. Stage 0 验收

```bash
make proto
make fmt
make test
make test-race
make build
make phase0-accept
```

通过条件：

- Proto 可生成 Go 代码并通过兼容性检查。
- Gin 与 gRPC 健康检查可运行。
- SQLite 和 PostgreSQL Migration 具备成对 up/down 文件。
- 固定事件 Fixture 可验证连续 seq、未知事件策略和 Replay 基线。
- `go test ./...`、`go test -race ./...`、`go build ./...` 通过。
