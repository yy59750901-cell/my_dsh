# Go 版 DSH 完整实现规划

## 0. 文档定位与恢复入口

本文是将 DeepSeek Harness（下文简称原版 DSH）以 Go 重新实现为完整 Agent Harness 的长期实施基线。目标不是机械逐文件翻译 TypeScript，而是在保持关键语义、行为和可观测性的前提下，构建 Go-native 的 DSH。

**当前状态（2026-09-29）：已交付可独立启动的本地单用户 Go DSH：Agent 多轮、OpenAI-compatible/Demo、Tool/审批、Context 预算与版本化人工压缩、文件围栏、MCP/Skill 最小能力、持久任务与线性 Workflow、Gin/gRPC/SSE/中文工作台、Go SDK。全仓 race/vet/build 和本轮独立复核通过，浏览器与二进制重启 E2E 通过。原计划全量目标尚未完成：OS Sandbox/PTY/LSP、全扩展协议、全应用热重载与生产治理等仍有差距，详见 `docs/checkpoints/CURRENT.md`。真实模型及 PostgreSQL/外部服务验收未执行；没有提交/推送，未初始化 Git。**

### 0.1 实施角色与职责

本项目采用“一个主执行者 + 专项角色评审”的方式推进。角色是长期责任边界，不等同于必须拆成独立服务或长期并行开发。

| 角色 | 负责内容 | 阶段性产物 | 明确不负责 |
|---|---|---|---|
| 主执行与集成负责人 | 维护主计划、拆阶段、整合各专项设计、控制依赖和最终验收 | 主计划、阶段 Checkpoint、集成结果 | 不绕过专项评审直接改变关键语义 |
| 原版兼容研究员 | 对照原版 DSH 源码，提取 Session、Agent、Tool、Profile 和前端轨迹语义 | 语义映射、兼容 fixture、差异清单 | 不按 TypeScript 文件做机械翻译 |
| 协议与 API 负责人 | Proto、JSON 映射、Gin API、WebSocket/SSE、gRPC、版本兼容 | `common/session/agent/tool.proto`、API 契约 | 不在 Transport 层实现 Agent 状态机 |
| Runtime 内核负责人 | Scope、Generation、Lifecycle、Resource、Disposer、EventBus | Runtime 接口、生命周期测试、热切换 ADR | 不决定 LLM 推理策略 |
| Session/Event 负责人 | Session Actor、EventStore、Replay、Fork、Surface、Projection | 事件模型、Actor 状态机、存储实现 | 不让投影成为事实来源 |
| Agent/LLM/Context 负责人 | Inbox、Turn、Step、流式 LLM、Prompt、Token、Compaction | Agent Loop、Provider、上下文解释器 | 不直接执行工具和业务规则 |
| Tool 与扩展生态负责人 | Tool Registry、中间件、审批、MCP、Skill、Job、Schedule | Tool 闭环、MCP/Skill 生命周期 | 不在 Tool 内复制下游业务逻辑 |
| Workspace 与安全负责人 | FS、Process、Terminal、LSP、Sandbox、凭据和网络围栏 | Provider、安全策略、威胁测试 | 不把 Prompt 约束当安全边界 |
| Subagent/Workflow 负责人 | 父子 Session、取消传播、Roster、Workflow 恢复和补偿 | 多 Agent 与工作流编排 | 不绕过 Workspace 和 Tool 权限 |
| 轨迹 UI 与可观测负责人 | 会话/执行/诊断三层投影、重连回补、Trace、脱敏 | 调试台、回放器、错误证据链 | 不读取进程内临时状态作为事实 |
| 存储与质量负责人 | GORM、Migration、Fixture、竞态/兼容/故障/安全测试、CI | 数据库适配层、测试门禁、发布验收 | 不擅自修改已冻结协议 |

角色依赖顺序：

```text
主执行/兼容研究
→ 协议 + Runtime + Session/Event
→ Agent/LLM/Context
→ Tool/扩展 + Workspace/安全
→ Subagent/Workflow
→ 轨迹 UI
→ 全程由存储与质量角色设置验收门
```

### 0.2 执行与评审门

- **G0 契约门**：事件、Proto、错误模型、取消语义和包依赖冻结。
- **G1 内核门**：事件连续、Replay 确定、Generation 回滚和 child-first teardown 通过。
- **G2 Agent Loop 门**：Turn/Step、流式生成、取消和上下文解释通过。
- **G3 能力门**：Tool 审批、MCP/Skill 撤回、Sandbox 越权测试通过。
- **G4 编排与 UI 门**：Subagent 所有权、Workflow 恢复、WebSocket 断线回补通过。
- **G5 发布门**：Migration、兼容、负载、长稳、故障和安全审计通过。

每次会话优先读取本文，然后读取 `go-dsh/docs/checkpoints/CURRENT.md`。后者只记录当前阶段、已完成事项、验证结果、未解决问题和下一动作，作为最短断点恢复入口。

后续会话恢复时按以下顺序继续：

1. 阅读本文第 1～4 节，确认目标边界和总体架构。
2. 查看第 5 节阶段状态表与 `go-dsh/docs/checkpoints/CURRENT.md`，从明确断点继续。
3. 已确认批次内直接连续编码和目标包验证，不重复输出方案；只有协议或业务语义发生实质变化时再请求确认。
4. 未经明确授权不提交、不推送、不创建合并请求。
5. 每个合并批次结束时更新本文和 checkpoint，中间只保留必要断点，减少重复维护。

当前执行分为三个合并批次：**批次一 = Stage 2 + Stage 3（Agent Loop + LLM + Tool + Approval）；批次二 = Stage 4 + Stage 5 + Stage 6（Context + Workspace/Sandbox + MCP/Skill/Job）；批次三 = Stage 7 + Stage 8 + Stage 9（Subagent + Workflow + UI/Trace + SDK/生产化）。2026-09-29 改为跨批次连续推进最短可运行路径，不再逐小步询问。当前各批次均已有可用能力，完整范围差距与下一入口以 CURRENT.md 为准。**

## 1. 目标与完成定义

### 1.1 最终目标

实现一个可独立运行、可扩展、可排障的 Go 版 DSH，覆盖：

- Session 生命周期和事件溯源；
- Agent、Turn、Step、Inbox 和取消；
- 模型上下文派生、系统提示词、Token 预算和上下文压缩；
- 流式 LLM Provider、模型目录、重试和错误归一化；
- Tool 注册、参数校验、权限、审批、执行、结果和审计；
- MCP Client、Skill 发现与渐进式注入；
- Workspace、文件系统、子进程、终端、沙箱和 LSP；
- Job、Schedule、Subagent、Workflow；
- Profile、Bundle、配置覆盖、凭据和热重载；
- Gin HTTP API、WebSocket/SSE 事件流、gRPC 内部协议和 SDK；
- 前端会话页、执行轨迹、工具卡片、审批交互和问题定位；
- 测试、遥测、回放、安全治理和生产部署。

### 1.2 “完全实现”的判定

“完全实现”不以源码行数相等为标准，而以以下三类兼容为标准：

1. **语义兼容**：Session、Turn、Step、Tool、Compaction 等关键语义一致。
2. **行为兼容**：相同输入、模型返回和工具结果能形成等价事件轨迹与最终状态。
3. **协议兼容**：关键 API、事件类型、MCP/ACP 等外部协议可兼容或提供明确适配层。

不追求 TypeScript 包名、Cordis API 或前端实现方式的源码兼容。

### 1.3 非目标

- 不逐文件、逐函数翻译约 64 万行 TypeScript/TSX。
- 不在首版拆成大量微服务。
- 不使用 Go 原生 `plugin` 动态加载 `.so` 作为主要扩展机制。
- 不把核心业务规则复制进 Agent Tool；业务规则仍由现有 Go/Java 服务负责。
- 不在 Agent Runtime 内保存明文长期凭据。

## 2. 原版 DSH 必须保留的核心原则

### 2.1 事件溯源

Session 是连续序号的 append-only 事件日志。模型历史、前端消息、统计和状态都是事件投影，而不是各自维护的独立真相。

必须维持：

```text
模型可见内容 = 可从 Session Event 日志重建的内容
```

核心事件至少包含：

```text
turn/start
user/message
step/start
assistant/chunk
assistant/message
tool/call
tool/result
step/end
turn/end
```

每个事件必须具备：

```text
session_id、seq、event_id、type、time、schema_version、data
```

与轨迹相关的事件还应包含：

```text
turn、step、call_id、trace_id、span_id、parent_span_id、source_event_seqs
```

### 2.2 单 Session 串行状态机

同一 Session 的 Inbox 认领、Turn/Step 推进、事件追加和压缩必须串行。Go 中采用“一 Session 一 Actor/单 goroutine mailbox”，禁止多个 goroutine 直接并发修改 Session 状态。

### 2.3 能力与实现解耦

文件、进程、终端、沙箱、LLM、Subagent 都采用 Definition/Provider/Consumer 分层。业务工具依赖能力接口，不绑定本机实现。

### 2.4 可撤销的扩展生命周期

原版 Cordis 的重要价值不是普通依赖注入，而是 Scope、Generation、Effect 和可撤销注册。Go 版必须自行补齐：

- 作用域与 nearest-scope shadowing；
- 配置热重载的新旧 generation 隔离；
- 注册资源的幂等注销；
- child-first teardown；
- 等待请求、进程、Agent 和子任务完全停止；
- 配置重组失败时回滚，不留下半挂载状态。

## 3. 总体技术架构

### 3.1 部署形态

首版采用模块化单体，内部保持清晰接口，达到规模后再拆服务：

```text
Browser / Client
    ↓ HTTP + SSE/WebSocket
Gin API Gateway
    ↓
Session Service ── Agent Runtime ── LLM Provider
    │                    │
    │                    └── Tool Registry + Policy Pipeline
    │                                      ↓
    │                             Local Tool / gRPC Tool / MCP
    ↓
Event Store + Projection + GORM
```

内部远程工具、Worker 和业务系统使用 gRPC。外部浏览器接口使用 Gin；流式轨迹首选 WebSocket，纯文本流可同时提供 SSE。

### 3.2 Go 包结构

```text
cmd/dsh-server/                 # 主进程
cmd/dsh-worker/                 # 可选远程执行 Worker
api/proto/                      # gRPC 与事件 Proto
internal/runtime/               # Scope、Generation、Lifecycle
internal/session/               # Session、EventStore、Surface、Projection
internal/agent/                 # Agent、Turn、Step、Inbox、状态机
internal/context/               # 消息派生、Token 预算、Compaction
internal/prompt/                # System Prompt Contributor
internal/llm/                   # Provider、流事件、错误归一化
internal/tool/                  # Registry、Schema、Executor、Middleware
internal/approval/              # 审批和用户提问
internal/workspace/             # Workspace 和文件作用域
internal/fs/                    # 文件能力 Definition/Provider
internal/process/               # Subprocess、Terminal、LSP
internal/sandbox/               # 本地、容器、远程沙箱
internal/mcp/                   # MCP Client 与生命周期
internal/skill/                 # Skill 发现、解析、作用域和注入
internal/job/                   # 后台任务与输出
internal/schedule/              # 定时任务
internal/subagent/              # 父子 Agent 和 Provider
internal/workflow/              # 工作流编排
internal/profile/               # Profile、Bundle、Patch、热重载
internal/credential/            # 凭据引用和短期票据
internal/repository/            # GORM Repository
internal/transport/http/        # Gin API
internal/transport/grpc/        # gRPC Server/Client
internal/observability/         # Trace、Metric、Audit、Redaction
web/                            # 前端工程
sdk/go/                         # Go SDK
sdk/typescript/                 # 可选 TypeScript SDK
```

### 3.3 技术边界

| 技术 | 使用位置 | 不负责的内容 |
|---|---|---|
| Gin | 外部 HTTP、管理接口、SSE/WebSocket 升级 | Agent 状态机 |
| gRPC | 内部服务、远程 Tool、Worker、Subagent Provider | 浏览器通用事件流 |
| GORM | Session/Event/Approval/Job/Profile 元数据持久化 | Agent Loop 控制流 |
| Go Channel | Session Mailbox、LLM Stream、事件订阅 | 持久化真相 |
| `context.Context` | 取消、超时、调用树传播 | 长期状态保存 |
| PostgreSQL | 生产事件存储和投影 | 高频临时流缓冲 |
| SQLite | 本地单机开发模式 | 分布式部署 |
| Redis | 可选分布式租约、Pub/Sub、限流 | Session 权威日志 |

## 4. 核心数据与协议

### 4.1 核心表

首批核心表建议为：

- `sessions`：会话元数据、工作区、父会话、当前状态；
- `session_events`：`session_id + seq` 唯一，保存规范事件；
- `session_projections`：会话摘要、模型选择、Todo、Goal 等可重建投影；
- `workspaces`：工作目录、沙箱提供方和所有权；
- `approvals`：审批请求、结果、过期时间和调用身份；
- `jobs`：后台任务、状态、输出游标和取消状态；
- `schedules`：计划任务和下一次触发时间；
- `profiles`：Profile 元数据和当前 generation；
- `credential_refs`：只保存凭据引用和元数据，不保存前端可见明文。

`turns`、`steps`、`tool_calls` 可以作为查询加速投影，但 Session Event 才是事实来源。

### 4.2 Agent 核心接口

```go
type Agent interface {
    FollowUp(ctx context.Context, msg UserMessage) (Receipt, error)
    Steer(ctx context.Context, msg UserMessage) (Receipt, error)
    Cancel(ctx context.Context, reason string) error
    Inject(ctx context.Context, input InjectedContext) error
    Status() AgentStatus
    Dispose(ctx context.Context) error
}

type LLMProvider interface {
    Stream(ctx context.Context, req ModelRequest) (<-chan StreamEvent, error)
}

type Tool interface {
    Definition() ToolDefinition
    Execute(ctx context.Context, call ToolCall) (JSONValue, error)
}
```

### 4.3 工具执行流水线

```text
模型生成 tool-call
→ 锁定 Tool Definition 与 generation
→ JSON Schema 参数校验
→ pre-execute：允许 / 拒绝 / 询问
→ execute：超时、重试、指标、实际执行
→ post-execute：脱敏、结果转换、附加上下文
→ result：只读审计和遥测
→ 追加 tool/result
→ Agent 进入下一 Step
```

### 4.4 前端轨迹协议

上行命令与下行事件分离：

```text
POST /api/sessions/:id/prompts
POST /api/sessions/:id/cancel
POST /api/approvals/:id/resolve
GET  /api/sessions/:id/history
WS   /api/events/mux
WS   /api/events/host
```

Mux 至少包含：

```text
session/event
session/subscribed
session/queue
session/jobs
session/projection
approval/requested
approval/resolved
question/requested
question/resolved
stream/error
```

Host 至少包含：

```text
host/session-added
host/session-removed
host/session-status
host/agent-error
host/workspace-changed
```

## 5. 分阶段实施路线

### 状态总览

| 阶段 | 名称 | 当前状态 | 核心产物 |
|---|---|---|---|
| 0 | 规格冻结与工程骨架 | 已完成 | ADR、Proto、目录、CI；`make phase0-accept` PASS |
| 1 | Runtime 与 Session 内核 | 已完成 | 生命周期、事件日志、Actor、Replay、Ordered Surface；`make phase1-accept` PASS |
| 2 | Agent Loop 与 LLM | 本地闭环可用 | Driver、OpenAI-compatible、Retry/Cancel；真实模型与完整 Hook 待验收 |
| 3 | Tool、审批与首个业务闭环 | 本地闭环可用 | Registry、Schema子集、审批、echo、多Step；远程gRPC仅客户端 |
| 4 | 上下文与长期会话 | 部分完成 | 来源预览、预算、版本化人工压缩；自动总结/精确tokenizer未完成 |
| 5 | Workspace、执行环境与安全 | 部分完成 | 文件围栏；OS Sandbox、Process/PTY/LSP未完成 |
| 6 | MCP、Skill、Job 与 Schedule | 部分完成 | HTTP MCP工具、库级stdio、Skill渐进披露、持久Job/固定间隔计划 |
| 7 | Subagent 与 Workflow | 部分完成 | 隔离历史子Session、线性持久Workflow；细粒度权限/DAG/ACP未完成 |
| 8 | 前端与可观测轨迹 | 本地版可用 | 中文UI、SSE回补、审批/上下文/任务；原版WebSocket兼容未完成 |
| 9 | Profile、SDK、兼容与生产化 | 部分完成 | 启动Profile/库级事务版本、Go SDK；全应用热切换/多租户/生产门禁未完成 |

### 阶段 0：规格冻结与工程骨架

**目标**：先冻结不能频繁变化的协议和设计原则。

交付物：

- Go module、目录结构、配置加载和本地启动命令；
- Session Event Proto/JSON Schema v1；
- LLM、Tool、Agent、EventStore 核心接口；
- ADR：单 Session Actor、事件溯源、扩展生命周期、错误模型；
- SQLite/PostgreSQL 数据库迁移框架；
- 单测、集成测试、Lint、生成代码和 CI 骨架；
- 与原版 DSH 的语义映射表。

验收：

- Proto 可以生成 Go 代码；
- 数据库迁移可正向和回滚；
- 空服务可通过 Gin 和 gRPC 健康检查；
- 事件兼容测试框架可以读取固定 JSON fixture；
- 尚不要求模型调用。

### 阶段 1：Runtime 与 Session 内核

**目标**：建立后续所有能力的可靠底座。

实现：

- Scope、Generation、Resource、Disposer；
- EventBus 的 broadcast、serial、waterfall 三种语义；
- Session Actor、连续 `seq`、Append、Replay、Fork；
- Ordered Surface 与 `deriveMessages()`；
- EventStore、Projection、Checkpoint；
- 冷 Session 恢复与 single-flight；
- 子资源优先的有界退出。

验收：

- 同一 Session 并发写入仍保持连续且无重复的 `seq`；
- 进程重启后能够从事件完整恢复；
- 未识别的必要事件拒绝重建，标记为 ignorable 的事件可跳过；
- Fork 可在指定事件边界生成子 Session；
- 失败 generation 不影响当前运行树。

实际完成结果（2026-08-26）：

- 完成 Scope、Lifecycle、Generation、EventBus、Session Actor、Replay、Registry、Actor Lease 和空闲回收；
- 完成 `actor_epoch + expectedSeq` writer fencing，以及 Actor 被 fencing 后的关闭和 Registry 自愈重载；
- 完成 Ordered Surface、positional replacement、`sourceEventSeqs` 校验和确定性 `deriveMessages()`；
- 完成 Surface schema minor 1、minor 0 legacy upcaster、独立 `000002_ordered_surface` 增量迁移；
- 完成 Candidate Validator 与 committed Validator 分层、GORM 事务内 committed readback；
- `make phase1-accept` 全部通过：Proto compatibility、fmt、unit、race、vet、build；
- 真实 PostgreSQL 集成测试因未设置 `DSH_TEST_POSTGRES_DSN` 而跳过，继续作为独立验证项保留。

### 阶段 2：Agent Loop 与 LLM

**目标**：跑通无工具和有模型流的完整 Turn。

实现：

- Inbox 的 queued、steering、context 三种 placement；
- Agent 状态机、Turn、Step、FollowUp、Steer、Cancel；
- System Prompt 基础装配；
- OpenAI-compatible Provider；
- 流式 text/reasoning/tool-call/usage 事件；
- 模型选择、错误归一化、重试和超时；
- `assistant/chunk` 与 `assistant/message` 组装。

验收：

- 可使用 DeepSeek、Qwen 等 OpenAI-compatible Endpoint；
- 流式 chunk 能实时下发并最终组装完整消息；
- 取消后网络请求、Turn 和状态正确收敛；
- 每次成功模型调用都有可回放的消息和 Usage；
- Turn/Step 状态机通过表驱动和竞态测试。

### 阶段 3：Tool、审批与首个业务闭环

**目标**：实现最关键的 Agent 工具闭环。

实现：

- Tool Registry、JSON Schema、作用域和 generation；
- pre/execute/post/result 中间件；
- 本地函数 Tool 与 gRPC Remote Tool；
- Approval、AskUserQuestion；
- Tool Call/Result 卡片元数据；
- 超时、取消、幂等、审计和脱敏；
- 首个只读业务 Tool。

验收场景：

```text
用户提问
→ 模型选择业务工具
→ 权限策略通过
→ gRPC 调用现有后端
→ tool/result 入日志
→ 模型继续推理并回答
```

写操作必须能够触发审批；审批前不得执行。工具热撤回后，已锁定 generation 的调用可安全结算，新调用不可继续发现它。

### 阶段 4：上下文与长期会话

**目标**：让长会话可控、可重建且模型输入可解释。

实现：

- Prompt Contributor 的有序装配和作用域覆盖；
- 模型可见 Surface 与来源事件；
- Token Meter 和预算策略；
- Tool Result Pruning；
- Summary Compaction 和 Surface Replacement；
- 附件、项目上下文、记忆和动态 Inject；
- 上下文预览与来源说明。

验收：

- 任意模型请求都能列出“为什么这些消息进入上下文”；
- 压缩后模型历史变短，原始审计事件不丢失；
- 重放后得到相同的 Model Request；
- 上下文溢出能够压缩并在新 Turn 重试，不形成无限循环。

### 阶段 5：Workspace、执行环境与安全

**目标**：支持代码 Agent 所需的文件和进程能力。

实现：

- Workspace 资源归属；
- FS Provider、路径围栏、读写策略；
- Subprocess、Bash/Pwsh、PTY Terminal；
- 本地 Sandbox 与容器/远程 Sandbox Provider；
- LSP Provider；
- 输出截断、spill、后台进程清理；
- 网络出口策略和 SSRF 防护。

验收：

- Agent 不能越出 Workspace；
- 任务结束后无残留进程；
- CPU、内存、进程数、磁盘、超时可被外部限制；
- 默认不能访问 localhost、私网和云元数据地址；
- Bash、Terminal、LSP 可替换为远程 Provider，而 Tool 无需修改。

### 阶段 6：MCP、Skill、Job 与 Schedule

**目标**：形成可复用的扩展生态。

MCP：

- 支持 stdio 与 Streamable HTTP；
- 初始化、能力协商、Tools/Resources/Prompts 发现；
- MCP Tool 映射到 Tool Registry；
- 服务重连、超时、撤回和错误隔离。

Skill：

- 从用户级和项目级目录发现 `SKILL.md`；
- 解析 Frontmatter、触发条件和依赖；
- 渐进披露：先展示摘要，调用时再注入正文；
- Skill 可贡献 Prompt、Tool 和资源；
- 作用域、版本、安全检查和热重载。

Job/Schedule：

- 后台任务启动、输出读取、取消和 Owner 清理；
- Cron/一次性任务持久化；
- Agent 空闲时 FollowUp，忙碌时 Inject。

验收：

- MCP Server 上下线时工具集合同步变化；
- Skill 修改后新 generation 生效，旧 Turn 不漂移；
- 重启后计划任务恢复且不会重复触发；
- Job 可在 Agent Turn 结束后继续运行并受 Owner 管理。

### 阶段 7：Subagent 与 Workflow

**目标**：支持多 Agent 委派与确定性工作流。

实现：

- In-process、fork、gRPC/ACP Subagent Provider；
- 父子 Session、所有权围栏、取消传播和结果归一化；
- Roster、任务板和 Mailbox；
- Workflow DSL、节点状态、暂停、恢复、重试和补偿；
- 结构化输出和终结策略。

验收：

- 父 Agent 可创建、等待、恢复和取消子 Agent；
- 子 Agent 不能访问超出授权的 Workspace、凭据和 Tool；
- Workflow 在进程重启后可从持久节点恢复；
- 子 Agent 内部错误归一化为结果，不造成父 Agent Runtime 崩溃。

### 阶段 8：前端与可观测轨迹

**目标**：让用户能够看懂 Agent 为什么这么执行，并快速定位错误。

页面至少包含：

1. 会话列表、归档、Fork 和恢复；
2. 消息流与增量输出；
3. Turn/Step 时间轴；
4. LLM 请求节点：Provider、Model、耗时、Token、停止原因、重试；
5. Tool 节点：工具名、参数、权限决策、耗时、结果、错误；
6. Approval 与 Question 交互；
7. Queue、Steering、Context Inject、Job 状态；
8. Context Inspector：本次请求包含哪些 Prompt Section 和历史消息；
9. 原始事件查看器和按 `seq` 回放；
10. Trace 导出、敏感信息脱敏和错误定位建议。

轨迹页采用三层视图：

```text
会话视图：用户和助手最终看到什么
执行视图：Turn → Step → LLM → Tool 的时序
诊断视图：原始事件、上下文、错误、Token、延迟和 Trace
```

关键诊断字段：

```text
session_id、seq、turn、step、call_id、rpc_id
trace_id、span_id、parent_span_id
provider、model、attempt、latency_ms
input_tokens、output_tokens、cache_tokens、stop_reason
tool_name、policy_decision、approval_id、is_error
error_category、upstream_status、retryable、cancel_source
```

验收：

- WebSocket 断线后根据 `last_seq` 补齐事件且不重不漏；
- 点击任一回答可以追溯到对应模型调用、工具调用和来源事件；
- 能区分模型鉴权错误、模型权限错误、工具失败、参数错误、策略拒绝和用户取消；
- Session 重放能恢复相同轨迹；
- 默认不展示 API Key、Authorization、Cookie 和敏感 Tool 结果。

### 阶段 9：Profile、SDK、兼容与生产化

**目标**：形成可安装、可升级、可运营的完整产品。

实现：

- Bundle、Profile、Home Patch、命令行 Overlay 的分层组合；
- Patch 按 ID 整段替换配置，并提供冲突诊断；
- 配置热重载、事务式 generation 切换和失败回滚；
- Credentials、Settings、Profile 管理接口；
- Go SDK、TypeScript SDK、JSON-RPC/ACP 适配层；
- 多租户身份、配额、费用、审计和速率限制；
- PostgreSQL、Redis、对象存储、远程 Sandbox 部署；
- 原版 DSH 行为对照测试和迁移工具。

最终验收：

- 核心事件、Agent Loop、工具闭环、上下文压缩和恢复通过兼容测试；
- Profile 可组合 LLM、Tool、MCP、Skill、Sandbox 和 UI 扩展；
- 灰度升级失败可回滚到旧 generation；
- 单机模式和服务端模式都能运行；
- 完成安全、故障恢复、负载和长稳测试。

## 6. 前端“轨迹”专项设计

前端轨迹不是把日志堆在页面上，而是对 Session Event 做可解释投影。

### 6.1 数据流

```text
Agent 追加规范事件
→ EventStore 事务提交
→ EventHub 广播
→ WebSocket Mux 下发
→ 前端按 session_id + seq 去重排序
→ Conversation / Timeline / Diagnostics 三类投影
```

### 6.2 故障定位示例

当模型调用失败时，页面应形成证据链：

```text
Step 2
├── Provider：aliyun-openai-compatible
├── Model：qwen3.7-plus
├── HTTP 状态：403
├── 归一化错误：MODEL_ACCESS_DENIED
├── 上游错误：AccessDenied.Unpurchased
├── 输入 Token：0
├── 是否重试：否
└── 建议：检查 Workspace 模型调用资格
```

当工具失败时应显示：

```text
Tool Call
├── 工具及参数摘要
├── 参数校验结果
├── 权限/审批结果
├── 实际执行 Provider
├── 耗时和取消来源
├── 规范化错误
└── 已脱敏的原始结果
```

### 6.3 轨迹数据原则

- 用户界面读取规范事件，不读取进程内临时对象；
- 原始模型请求和工具结果按安全策略存储，默认脱敏；
- `assistant/chunk` 可按批次压缩存储，但必须能恢复流式顺序；
- 展示投影可以重建，不能成为唯一事实；
- 所有异步动作都必须携带 Session、Turn、Step 和 Trace 关联信息。

## 7. 测试策略

### 7.1 测试金字塔

- 单元测试：状态机、Schema、投影、Patch、错误归一化；
- 属性测试：事件序号连续、回放确定性、取消幂等；
- 集成测试：真实 SQLite/PostgreSQL、Mock LLM、Mock MCP、gRPC Tool；
- 轨迹金丝雀：固定输入生成固定事件序列；
- 兼容测试：Go 版与原版 DSH 消费相同 fixture，比较语义结果；
- E2E：Web 发起 Prompt、审批 Tool、重连、回放、Fork；
- 故障测试：网络断开、模型限流、进程退出、数据库切换、重复消息；
- 安全测试：越权、路径穿越、SSRF、命令注入、凭据泄露。

### 7.2 必须长期保持的不变量

1. 同一 Session 的 `seq` 连续且唯一。
2. 模型可见内容都能从事件重建。
3. 一个 `tool/call` 最多对应一个最终 `tool/result`。
4. Turn 和 Step 最终必须关闭或明确恢复。
5. 取消和 Dispose 幂等。
6. Profile 重组失败不污染当前 generation。
7. UI 重连后事件不重不漏。
8. 凭据不进入 Prompt、事件和普通日志。

## 8. 风险与关键决策

### 8.1 最大风险

- 低估 Cordis 的动态作用域、生命周期和热撤回语义；
- 只实现聊天循环，却缺少事件可重建性；
- Tool 执行和 Session 写事件出现竞态；
- Compaction 删除原始审计事实；
- MCP、Skill、Subagent 各自实现一套生命周期；
- 前端轨迹依赖临时内存，重连后丢失；
- 为追求“完全一致”过早复制不需要的边缘功能。

### 8.2 已确定的技术决策

- 采用 Go-native 重构，不逐文件翻译；
- 首版模块化单体；
- 单 Session Actor 串行推进；
- Event Store 是会话唯一事实来源；
- Gin 面向外部，gRPC 面向内部和远程能力；
- GORM 只做持久化；
- 扩展采用接口注册 + Profile 配置，不以 Go `.so` 为主；
- 业务 Tool 是现有后端的受控适配层；
- 前端轨迹由规范事件投影生成。

### 8.3 已冻结与后续待决策事项

已冻结：

- 事件规范以 Protobuf 为权威协议，JSON 作为稳定映射；
- 会话轨迹使用 WebSocket，纯文本流可额外提供 SSE；
- SQLite 用于本地开发，PostgreSQL 用于生产和真实集成验证；
- Session Event Store 是唯一事实来源，模型上下文和前端轨迹必须可由事件重建。

后续阶段再决策：

- Profile Patch 采用 YAML 整段替换还是额外支持 Merge Patch；
- 前端采用 React 复刻还是先做轻量诊断台；
- 是否首期兼容原版 `/api/session.*` RPC Envelope。

## 9. 里程碑管理与下一步

2026-09-29 起沿已授权目标连续实现，不再每个小进展重复确认：

```text
读取最短 checkpoint → 并行实现独立模块 → 集成与测试
→ 独立复核实质缺陷 → 可运行结果与剩余差距
```

只有外部授权/凭据、部署或重大协议语义变化时再询问。

阶段 2 详细设计已确认并持续实施。已完成 Proto 语义兼容、Composite Projection、LoadActor Repair、RuntimeRegistry/WakeReconciler，以及 Agent Inbox/Claim 原子闭环。以下是已完成的批次一主链路（历史实施顺序，不是当前待办；新断点以 CURRENT.md 为准）：

1. 实现 Turn/Step/Attempt Driver，并以持久 Claim 作为模型输入所有权边界；
2. 实现 LLM 领域类型、pull-based Stream、Adapter terminal contract 和 BlockAssembler；
3. 实现错误归一化、Retry、Cancel 与崩溃恢复；
4. 连续实现 Tool Registry/Execution、Approval 和首个只读业务闭环；
5. 批次末统一执行兼容、Replay、故障、race、build 和独立终审。

真实 PostgreSQL 集成验证作为独立待办保留，不阻塞批次一纯内核开发。
