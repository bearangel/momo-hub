# momo-hub 整体设计（v1）

- **日期**：2026-10-23
- **状态**：设计已批准（三轮分节呈现逐节确认）；M0 实施未开始
- **关联**：momo-studio `docs/specs/2026-10-20-remove-p2p-lsp-design.md`（P2P 移除，momo-hub 为钦定联网继任）；momo-studio AGENTS.md「后续联网走 momo-hub」

## 1. 背景与定位

momo-studio 于 2026-10-20 整体移除 P2P 子系统（mDNS 发现 / TCP 直连 / Ed25519 信任 / 资源分享 / 任务只读镜像，2520 行、耦合 15 个外部文件）——直连 mesh 的发现/NAT/信任链复杂度压垮收益。momo-hub 是产品方向上的修正：**中心化常驻服务**，承载 momo-studio 的跨设备能力。

两大功能：

1. **资源分发**（M1）：插件 / skill / MCP / 智能体四类资源的私有 marketplace
2. **分布式任务节点**（M2）：工作组、工作空间配置镜像、任务委派、远程查看

成功标准：一台机器上的工作空间与资源配置可在另一台机器上跑起来；任务可分散到多台机器执行且进度实时可见；全链路不出现密钥集中风险。

## 2. 核心裁定（会话已批准）

| 裁定点 | 结论 |
|---|---|
| 部署形态 | 独立常驻中心服务（NAS/VPS/常开机器），所有 studio 为客户端；「主节点」是工作组内管理角色，不是服务器 |
| 网络基准 | 跨互联网为设计基准；TLS + 每实例 token 认证第一天即硬性要求；局域网是特例自动覆盖 |
| 资源分发语义 | 私有 marketplace——任意 studio 可发布、任意 studio 可安装；hub 成为资源库第五种 source |
| 技术栈 | Node 24 + TypeScript strict + Fastify + WebSocket + SQLite（better-sqlite3），与 momo-studio 同栈同纪律 |
| 协议 | JSON 消息信封 over WS；共享 TS 协议包单源（momo-hub `packages/protocol`），studio 经 git 依赖单向引用 |
| 里程碑拆分 | M0 连接基建 → M1 资源分发 → M2 分布式工作组，严格顺序；每期独立 spec → 实施计划 → 实施 → 验收 |

## 3. 总体架构

```
┌────────────────┐  WSS 长连接（控制面：RPC + 推送事件）  ┌──────────────────┐
│ momo-studio A  │ ◄─────────────────────────────────► │                  │
│ （工作组·主节点）│  HTTPS（数据面：资源包/镜像 payload）  │     momo-hub     │
└────────────────┘                                    │  常驻中心服务      │
┌────────────────┐ ◄─────────────────────────────────► │  Node24/TS/SQLite │
│ momo-studio B  │                                     │                  │
│ （工作组·子节点） │                                     └──────────────────┘
└────────────────┘
```

- **控制面（WS）**：RPC 请求/响应 + 服务端推送。实时能力全部在此：委派任务进度流、节点上下线、资源更新通知、远程查看数据流。
- **数据面（HTTP）**：资源包上传/下载、工作空间配置镜像 payload。大文件不进 WS 帧（并发、超时、断点语义更自然）。
- **hub 持久化（SQLite + 落盘目录）**：节点注册表、工作组与角色、资源仓元数据 + content-addressed blobs、任务委派账本、审计日志。hub 重启不丢账。

## 4. 仓库边界与协议纪律

**momo-hub 仓库**：

```
packages/
  hub/        — hub server（Fastify + ws + better-sqlite3）
  protocol/   — 协议与类型单源：消息信封、全部 RPC 方法签名、事件类型、
                资源 manifest 格式、工作组/节点模型。零运行时依赖。
```

**momo-studio 仓库**（改造点）：

- `electron/src/main/hub/` — 连接层（重连/心跳/凭证保管，token 入 keychain）
- 资源域：`ResourceSource += 'hub'`，新增 `HubProvider`（挂现有 `resource/hub/` 抽象，与 smithery 并列）
- IPC + UI：设置页 hub 配置、资源库 hub 源页签、（M2）工作组面板与远程查看入口

**纪律**：

- 引用方向单向：momo-studio →（pnpm git 依赖）→ `momo-hub/packages/protocol`；hub 永不依赖 studio
- 协议变更固定流程：momo-hub 先改 protocol 包并 bump 版本，studio 跟进消费；不搞手工双份类型对齐（momo-studio renderer/electron 的「仅结构对齐」模式不跨仓库复制——跨仓库契约漂移是 8 个 P0 教训中的重灾区）

## 5. 安全模型

- **信任根**：hub 初始化生成加入凭证（enrollment token，可吊销/可设有效期）；studio 首次连接凭它换取每实例长期 device token，此后一律 token 认证。**无开放注册**。
- **传输**：TLS 强制。主路径反代 Caddy（自动证书）；LAN/快速路径允许内置自签。
- **权限矩阵**：工作组内 leader（主节点）持有委派/镜像/查看权，**hub 侧强制校验**（非 studio 侧自律）；子节点默认只能操作自己；leader 角色可转移。
- **密钥红线**：hub 与镜像 payload 中永不出现 provider API key（key 留在各 studio 本机 keychain）。委派任务在子节点执行时用子节点本机 provider 配置；委派前提是该节点已配好同类 provider。hub 被攻破 ≠ 全部节点密钥失守。
- **审计**：hub 全量操作日志（谁/何时/委派/查看/发布了什么）；studio 侧沿用现有工具调用审计体系。

## 6. M0 · 连接基建

**目标**：hub 能跑起来、studio 能可靠连上、协议有版本纪律。

| 组件 | 内容 |
|---|---|
| hub 服务骨架 | Fastify + `/ws`；启动初始化 SQLite（内联 SQL 迁移）；生成 hub 身份与加入凭证（输出到启动日志/文件） |
| protocol 包 | 信封 `{ v, id, kind: req/res/evt, method, payload }`；M0 RPC 集：`hello`（版本/能力协商）、`enroll`（凭证换 device token）、`ping/pong`；事件集：`node.online` / `node.offline`。协议 semver，不兼容变更 bump major，hub 明确拒绝不匹配版本 |
| studio 连接层 | 状态机 disconnected→connecting→online，指数退避自动重连；心跳超时判定；device token 存 keychain；设置页 hub 配置（地址/节点名/连接状态指示） |

**错误处理**：断线重连对上层透明；hub 重启 studio 自动恢复；版本不匹配为显式错误（界面明示 studio/hub 谁太旧）。

**验收形态**：两台设备 + 一个 hub，设置页看到彼此上线/离线。

## 7. M1 · 资源分发（私有 marketplace）

**核心原则**：最大化复用 momo-studio 现有 marketplace 机制，hub 源为第五种 ResourceSource，零新概念。

**数据流**：

```
发布：studio 资源库（custom 资源）→「发布到 hub」→ manifest + zip payload
      → HTTP 上传 → hub 安全校验（slug 白名单/版本/checksum，移植
      momo-studio marketplace/types.ts 的 S1 规则）→ 入仓（sha256 内容寻址）
浏览：资源库「hub」页签 → HubProvider.list() → 安装走 HTTP 下载 +
      checksum 校验 → 复用现有 marketplace 安装落地链路
更新：hub 侧版本目录；resource.published/updated 事件推送在线 studio →
      资源库角标提示可更新
```

**内嵌裁定**：

1. 资源仓为 **hub 级全局**，不按工作组隔离（个人多设备场景单一私有 marketplace 足够；工作组隔离等真实需求出现再加）
2. manifest 格式对齐现有 `MarketplaceItem`，`type` 扩为四类 `agent|mcp|skill|plugin`（现有 marketplace 类型仅三类，hub 协议从第一天含 plugin）
3. hub 源离线表现复用 smithery 的 degraded 退避模式（置灰不隐藏）

**hub 存储**：`resources`（manifest）+ `resource_versions`（版本/checksum/size）两张表 + content-addressed blobs 目录；上传中断重试天然幂等。

**验收形态**：机器 A 发布自定义 agent → 机器 B 资源库 hub 页签可见 → 安装 → spawn 成功。

## 8. M2 · 分布式工作组

### 8.1 工作组模型

- `groups` + `group_members`（node_id, role: `leader` | `member`）；主节点 = leader 所在 studio；创建组者默认 leader，可转移
- 表结构允许一节点多组，UI 首版按单组操作

### 8.2 镜像（工作空间配置 → 任意节点）

- **范围**：agent 定义与成员、团队、MCP 启用清单、skill、插件启用清单、provider 引用清单（不含 key）、工具上限等偏好。**不含聊天历史/文件工作区**（「配置镜像」，非数据同步）
- **语义**：整体替换式，不做增量合并；目标工作空间不存在则创建，存在则覆盖配置，覆盖前弹差异摘要确认
- **传输**：快照经 hub 中转落盘（content-addressed 存档 = 快照历史与回滚）；**不做节点直连传输**
- **衔接 M1**：快照引用资源目标节点缺失时，自动从 hub 资源仓补装

### 8.3 委派（任务 → 任意节点执行）

- **语义**：主节点发任务书（自然语言 + 目标工作空间上下文）→ 子节点本地 AgentRunner 执行（本机 provider key + 已镜像配置）→ 进度流实时回传 → 结果落入主节点会话
- **链路**：master →（`task.delegate`）→ hub 账本落档（pending→running→done/failed/cancelled）→ 子节点（`task.assigned`）→ `task.progress` 事件流经 hub 中继 → `task.completed/failed`
- **hub 只中继不执行**（headless runner 是远期路线图，不进 M2）
- **断线语义**：子节点执行中掉线 → hub 标记 interrupted；重连后续传（复用 studio 现有断点续跑能力）或主节点取消
- 子节点有「接受委派」开关 + 并发上限

### 8.4 远程查看/管理

- **首版四视图**：会话列表与消息流（只读回放 + 实时追加）、任务列表与状态、资源与成员配置、工作空间概览。机制红利：`message_events.seq` 天然全序，跨节点回放 = 按 seq 拉取 + 订阅尾部增量
- **不进 M2**：远端代发消息等交互写操作（委派已覆盖「让远端做事」核心诉求）；文件树远程浏览为 M2 后快速跟进项；远端写操作不做
- **双层同意**：hub 校验 leader 角色 + 子节点「允许本组查看」开关（默认开，可关）

**验收形态**：主节点镜像工作空间到子节点（缺失资源自动补装）→ 委派任务在子节点执行、进度实时回传 → 主节点回放子节点会话消息流。

## 9. 测试策略

- **hub 单测**：鉴权（无 token 拒/错 token 拒/enroll 流程）、任务状态机、S1 校验拒绝路径（坏 slug/坏 checksum/坏版本）、账本；vitest，纪律同 momo-studio
- **protocol**：编解码 round-trip、版本协商拒绝
- **跨仓库集成**：进程级起 hub + headless studio 主进程，覆盖发布→安装 round-trip、委派 round-trip
- **studio 回归**：资源库四源 → 五源 UI 回归锁

## 10. 里程碑与研发流程

**顺序严格 M0 → M1 → M2**（镜像补装依赖 M1 资源仓；委派中继依赖 M0 事件通道）。

每期独立走：spec 细化 → 实施计划（writing-plans）→ 实施 → 验收（momo-studio 侧变更遵守其 skills 规则：boundary/test/feature-scope/ui-preview）。M1/M2 详细 spec 在各自期初再细化，避免过早细化返工。

## 11. 明确不做（YAGNI 清单）

- 开放注册 / 多用户 SaaS 化（个人多设备为唯一场景）
- headless agent runner（hub 不执行任务，仅中继）
- 节点直连传输 / NAT 打洞（P2P 教训；全部走 hub 中转）
- 资源仓按工作组隔离
- 远端代发消息等交互写操作
- 移动端
