# Go DSH 当前断点

更新时间：2026-09-29

## 结论与协作约定

已从内核骨架推进到可启动的本地单用户版本，主要运行链路及 UI 已贯通；**不等于原计划 Stage 0–9 全量功能或生产验收完成**。用户本次授权最短路径连续推进，不再每个小进展询问。保留原目标，不用 MVP 替代全量定义。未经授权仍不提交、不推送、不建分支/MR；当前工程没有 `.git`。

## 已交付能力

- Session/EventStore、Actor、Claim/Inbox、Surface、Replay、Writer fencing、Repair 底座保留。
- 完整 Harness/Driver：FollowUp/Steer/Inject、Turn/Step、同 Step Retry、取消、模型流、工具后下一 Step。默认首次请求加五次重试，有界 backoff。
- OpenAI-compatible HTTP/SSE Provider：text/reasoning/tool-call 分片、usage、错误归一化、取消/Close、重定向拒绝；单 choice。Demo 明确标注“非真实 LLM”，不需要凭据。
- Tool Registry：Schema 受支持子集（不支持的关键字拒绝注册）、固定 generation handle、校验、超时、panic/输出限制、持久审批。取消保留安全文本前缀；未决或已启动副作用崩溃后不自动重放。
- 内部 ToolCallID 按 Turn/Step/Provider ID 隔离；模型侧保留 Provider ID。工具结果完整保留正文、structured 和错误码；gRPC remote client 映射有界 wire ID。
- Context：来源可解释、保守字节估算（不是精确 tokenizer）、128000 默认预算/4096 输出预留、超限持久终态。人工摘要压缩需 `expected_head`，原子全 Surface 替换且保留审计；拒绝迟到摘要，冷预览不夺 writer。
- Workspace：OpenRoot 文件围栏，list/read/write；写需审批；拒绝敏感路径、绝对路径和符号链接逃逸。**不是 OS 沙箱，无 shell/PTY/LSP**。
- MCP HTTP：initialize、tools/list/call、JSON/SSE、固定配置 endpoint、所有远端工具需审批。stdio client 为库级，显式 trusted-operator opt-in、无沙箱、进程组退出；默认应用不启用。
- Skill：仅显式 workspace/skills 路径、frontmatter、list/read 渐进披露、变更拒绝、不会执行脚本或自动读取个人技能。
- Orchestration：持久 Job、隔离历史的子 Session、一次/固定间隔 Schedule、线性 Workflow、暂停/恢复/取消和节点结果回放；同进程同 Harness 的 Manager 共享投递/控制门。普通 Turn cancel 不永久关闭编排；`cancel-tree` 是明确的永久关闭入口。
- Profile：严格 JSON Bundle/Patch 按 ID 替换；凭据为 ENV 引用；库级候选/lease/回滚。启动支持一个模型和至多一个 HTTP MCP，其余明确拒绝；尚无全应用热重载。
- 应用：嵌入不可变 SQL 迁移和 checksum；SQLite 默认、Postgres 配置可选；数据库必须在模型 Workspace 外，文件身份判断覆盖大小写别名；关闭 Manager → Harness → Tools → DB。
- Gin HTTP、gRPC、Go SDK、SSE 游标回补、中文浅色工作台：对话、工具/审批、事件、上下文、人工压缩、子任务/顺序工作流。Bearer 可选、同源/Host 校验、脱敏、DOM 文本渲染。监听仅数字 loopback 地址。

## 主要代码入口

| 模块 | 入口 |
|---|---|
| 启动和配置 | `cmd/dsh-server/main.go`、`internal/app/app.go`、`internal/app/profile_config.go` |
| 迁移 | `db/migrate.go`；原 SQL 不变 |
| Agent | `internal/agent/harness.go`、`driver.go`、`control_commands.go`、`control_projection.go` |
| Context | `internal/agent/context.go`：`CompactAt`、`ContextPreview` |
| LLM | `internal/llm/openai_provider.go`、`openai_sse.go`、`demo_provider.go` |
| Tools | `internal/tool/registry.go`、`schema.go`、`grpc_remote.go` |
| 扩展 | `internal/workspace`、`internal/mcp`、`internal/skill`、`internal/profile` |
| 编排 | `internal/orchestration` |
| API/UI/SDK | `internal/transport/httpapi`、`grpcapi`、`sdk/go` |

## 运行方式

2026-09-29 本地配置更新：从仓库运行 `go run ./cmd/dsh-server`，默认读取当前目录 `config.local.json`。支持 `-config /绝对路径/config.local.json` 和 `-check-config`（纯本地检查，不创建数据库/恢复任务/调用模型）。默认模式改为 openai，缺必需项报错，只有显式 demo 才使用模拟模型。

用户授权的火山方舟配置已写入私有 `config.local.json`（0600、Git忽略，禁止读入评审上下文、复制入outputs/日志/源码包）；无密钥模板 `config.example.json`。model=`glm-5-2-260617`，base_url=`https://ark.cn-beijing.volces.com/api/v3`，HTTP18081/gRPC19091；实际请求自动追加 `/chat/completions`。db_path=`../.workbuddy/dsh/real.db`，workspace=`../.workbuddy/dsh/real-workspace`，相对路径以配置目录为准。密钥值不记录在此。文件与ENV冲突会报变量名，不能与 `DSH_PROFILE_*` 混用；旧ENV/profile方式仅默认本地文件不存在时可用。

新二进制 `outputs/go-dsh-local-config` 已构建；全仓 race/vet/build通过，独立复核P0/P1=0，私有配置 `-check-config` 通过。没有启动真实实例、没有发起远端付费请求或验证模型权限。生成源码包必须显式排除 `*.local.json`、`.env*` 和数据库/工作区，不依赖gitignore自动生效。

本次本机预览使用 `http://127.0.0.1:18080`，gRPC 19090；下次先确认是否仍在运行，不能假设后台进程跨会话存活。二进制位于工作区 `outputs/go-dsh`，数据位于工作区 `.workbuddy/dsh`，不要清理该目录。

## 验证证据

- 最新全仓 `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go test -mod=readonly -race -count=1 ./...` PASS。
- 全仓 `go vet -mod=readonly ./...`、`go build -mod=readonly ./...` PASS；独立 darwin/arm64 服务构建成功。
- 独立终审：本轮发现的问题均已修复，最终 P0=0/P1=0/P2=0（仅审查已实现范围，不代表原版全量等价）。
- SQLite 覆盖重试、审批允许/拒绝、取消安全前缀、重复 ToolCallID、durable finish/append uncertainty、重启不重放副作用、压缩版本/完整来源、编排 gate/时钟前跳；关键回归多轮 race 通过。
- 浏览器实际通过：新建会话、echo 工具两 Step 回答（28 事件）、刷新回放、人工摘要（增加2事件）、预算查看、2/2 Workflow 节点完成；控制台零 error。
- `outputs/smoke_test.py` 黑盒启动真实二进制：工具闭环、版本压缩幂等、SIGTERM 优雅退出、重启后 30 个事件完全一致 PASS；结果 `outputs/go-dsh-verification.json`。
- 没有调用真实付费模型或外部 MCP/gRPC 服务，没有创建实际定时计划。真实 PostgreSQL、远端服务联调未执行。未运行会安装工具的 `make phase1-accept`，本次直接执行现有依赖门禁；不宣称重新生成了 Proto。

## 与原始全量目标的差距（必须保留）

1. 真正 OS/容器/远端 Sandbox、资源与网络围栏、Process/PTY/Terminal/LSP 未实现；现有文件围栏不能替代这些能力。
2. 自动 LLM 总结、精确 tokenizer、附件/记忆贡献器、完整 Hook/中间件扩展、完整 Cancel options 尚未完成。
3. MCP resources/prompts、OAuth、重连续传及完整协议兼容；stdio/gRPC remote 尚未接启动 Profile；Skill hot reload/generation 全链路不完整。
4. 子 Agent 目前为用户/API 启动、同 workspace 同单用户权限，尚无 LLM 自动 spawn、权限子集/凭据隔离、gRPC/ACP provider、Roster/协作邮箱。Workflow 为线性 prompt 节点，非完整 DAG/DSL/补偿。Schedule 为一次/固定间隔而非完整 Cron。
5. API Fork/归档和原版 RPC/WebSocket mux/host 未完成；gRPC 直接 Execute 为防审批绕过明确 Unimplemented；UI Workflow 列表按 jobs 聚合为 partial。现有 SSE 不等于原版 WebSocket 协议兼容。
6. 全应用 Profile 热切换、远端安全部署、多租户/配额/费用/审计生产治理、负载/长稳/故障、安全生产验收、原版 golden fixture/迁移兼容尚未完成。
7. 真实模型 Endpoint/权限、PostgreSQL JSONB/时间精度/并发、外部 MCP/gRPC 环境验收仍缺。

## 下一入口

无需重复实现以上已完成模块。优先沿未完成项推进真正执行环境/完整扩展接线，再做原版行为对照和生产门禁；如用户只是体验本地版本，先运行现有二进制。仅外部凭据/授权、部署或重大语义变化时询问，不再每个小进展暂停。
