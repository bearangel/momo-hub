# momo-hub M3 立项建议书 · 分期、依赖与验收面

- **状态**：已批准（2026-10-28，用户全系列预授权 + 过夜自主模式；详见 §6）
- **日期**：2026-10-28
- **上游**：M2 系列冻结（alpha.35 / hub v0.1.5）+ M2.5 B1–B13 关闭决议 + 两仓能力事实盘点（本文件 §1）
- **定位**：M3 不做「大而全的一期」，按**协议面**与**可独立交付性**切 5 期；每期独立 spec → plan → 实施 → 验收 → 冻结。

## 1. 事实基线（代码级，纠正三处既有认知）

| # | 事项 | 事实 | 影响 |
|---|---|---|---|
| F1 | **多组 UI 其实已完成** | hub schema 支持同节点多组（`groups.leader_node_id` 无 UNIQUE、`group_members` 主键 `(group_id,node_id)`）；studio 侧栏已渲染全部组并可切换（`hub-group.store.ts:20-54`、`WorkgroupSidebar.tsx:73-98`）；调度/委派均按 `groupId` 建模 | **B3「多组 UI」可直接关闭为已交付**，不占 M3 期 |
| F2 | 多组唯一真缺口 | `view.*` 六方法的 leader-only 守卫靠 `commonGroupId` 任意取一组（`groups.ts:92-104`），载荷无 `groupId` | 并入 M3.2（远端可见性） |
| F3 | HITL 等待链路已在 | worker 侧未决表 `pendingWriteBlocked` + `sessionWaiting` 信号；leader 侧「等待节点确认」角标与等待摘要卡（M2.1 T11/T12）；`kinds='waiting-input'` 已透传 | **L2 缺的只有「应答通道」四件**：RPC / 事件 / `waiting` 态 / 决策列 + 代答权 |
| F4 | 远端可见性 | `view.*` 仅 6 方法（workspace/sessions/messages/tasks/subscribe/unsubscribe）；**无 `view.files`**、**无目标节点空间枚举** | M3.2 需两个新方法（minor 扩展） |
| F5 | 快照版本 | `SnapshotManifest.version` 在顶层（建仓代），`SnapshotResourceRef` 无 `version`；未知字段**剥除不拒收** | B13 可用「可选字段」扩展，向后兼容 |
| F6 | 资源生命周期 | 无 DELETE/PUT/PATCH 端点、`resources` 无 deleted/deprecated 列，仅版本不可变 409 | B12 需新增撤回语义（含缓存失效） |
| F7 | 协议兼容 | `PROTOCOL_VERSION='1.2.0'`，`isCompatible` **只比 major**，无最低 minor 门槛、无能力协商；老客户端过 major 检查后在方法级撞 `unknown_method` | **M3 新增协议语义前必须先补能力协商/最低 minor 门槛**，否则旧节点静默降级 |
| F8 | 任务模型 | 9 态状态机就绪；**无 `parent_id`/`depends_on`/`blocked_by`**（唯一 parent 语义是循环链）；批量创建仅 renderer 多行顺序提交，主进程无批量 IPC | M3.3 编排是从零建模型 |
| F9 | 文件读取可复用 | `WorkspaceFS.assertInWorkspace`（多根 + 符号链接逃逸 + .git 保护）+ `readFile`/`listDir`/`searchNames` 现成 | M3.2 远端文件树**不必重写路径防御** |

## 2. 分期建议（5 期，按依赖排序）

| 期 | 主题 | 协议影响 | 依赖 | 工作量 | 核心交付 |
|---|---|---|---|---|---|
| 期 | 主题 | 协议影响 | 依赖 | 工作量 | 核心交付 |
|---|---|---|---|---|---|
| **M3.0** ✅ 已交付（hub v0.1.6 / studio alpha.36 / main `48f3b4e1`，oracle MERGE READY，CI 三连绿） | 收官清理与协议前置 | hub minor（能力协商字段，可选） | 无 | **S**（1–2 天） | 6 项债务 + **能力协商/最低 minor 门槛**（F7，M3.1/M3.2 的前置） |
| **M3.1** ✅ 已交付（hub v0.1.7 协议 1.3.0 / studio alpha.37 / main `ac756a32`，oracle MERGE READY，UC 6/6） | HITL L2 远程代答 | **minor 1.3.0**（`task.answer` RPC + `task.waiting` 事件 + `waiting` 态 + 决策列） | M3.0 | **M**（4–6 天） | leader 卡片代答 → 直达 worker 授权处置；超时/幂等/审计；代答权 = 同组 leader |
| **M3.2** | 远端可见性补全 | **minor**（`view.workspaces` + `view.files`；`view.*` 载荷加 `groupId`） | M3.0；F9 复用 | **M**（4–6 天） | 远端空间枚举 + 文件树浏览（只读）；多组归属歧义消除 |
| **M3.3** | 任务编排与依赖 | studio 为主（可选 minor：依赖元数据透传） | M3.1（等待/失败语义复用） | **L**（7–10 天） | `parent_id`/`depends_on` + DAG 视图 + 批量 IPC + 阻塞态 |
| **M3.4** | 资源生命周期与版本对齐 | **minor**（manifest `resources[].version` 可选 + 撤回语义） | M3.2（远端可见性） | **M/L**（5–8 天） | B13 镜像按版本对齐 + B12 撤回/下架 + 目录缓存失效链 |

### 2.1 M3.0 明细（唯一可立即开工的一期）

| 项 | 内容 | 来源 |
|---|---|---|
| 0-1 | **协议能力协商/最低 minor 门槛**（hello 增 `caps` 或 `minMinor`），保证 M3.1/M3.2 新语义对旧节点**显式降级而非静默 unknown_method** | F7 |
| 0-2 | 非 2xx 下载人话化（`下载失败: HTTP 404` → 「资源包不存在或已下架」等） | M2.5 D1 残留 |
| 0-3 | monaco 全量并行 hook 超时固化（`hookTimeout` 进 vitest 配置） | M2.5 oracle 建议 |
| 0-4 | hub `MOMO_HUB_HOST` 支持 + 默认收紧 `127.0.0.1` | M2.5 C 组 |
| 0-5 | keychain 服务名按 profile 作用域化（隔离 profile 假 key 孤儿键） | M2.5 C 组 |
| 0-6 | readonly 即时拒标记持久化（M2.5 曾关闭为设计边界——**重开需批准**；不批准则维持关闭） | A5 |
| 0-7 | CI 两条 e2e 解禁（browser/kanban-drag 输入适配）——**可选**，若不解决则长期留在 `testIgnore` | M2.4/2.5 |

## 3. 推荐顺序与理由

**推荐：M3.0 → M3.1 → M3.2 → M3.3 → M3.4**

1. **M3.0 先行**是不可协商的：0-1 是 M3.1/M3.2 的**兼容性前置**——现有只比 major 的策略下，新增 `waiting` 语义会让旧 studio 收到不认识的载荷/事件而静默失效，这正是 M2.3/M2.4 反复出现的「静默」类缺陷。
2. **M3.1 优先于 M3.2**：HITL 是**当前唯一会让用户「卡住干等」的链路**（worker 停在写授权等待，leader 只有取消/重试两个选择）；而远端文件树是「更好用」，非「卡住」。价值密度 M3.1 更高。
3. **M3.2 次之**：两个新方法共用一套权限/consent 面，一批做省重复；顺带修 F2 的多组归属歧义。
4. **M3.3 放后**：从零建 DAG 模型（F8），且依赖 M3.1 的等待/失败语义，前置未稳时做会返工。
5. **M3.4 最后**：B13 要 manifest 扩展、B12 要撤回语义 + 缓存失效，两者都触协议与目录链，放最后一期让前几期的协议演进先稳定。

## 4. 每期冻结判据（沿用 M2 纪律）

- spec 批准 → plan + Momus → 分批 TDD → 真机验收 → oracle 全分支终审 → MERGE READY → 合并推送 → 台账冻结
- 每期必带：失败路径用例 ≥1/链路、缺陷年龄 ≤2 期、消费面矩阵（新能力需建）
- 协议变更期必带：**双仓契约测试** + 旧版本节点兼容用例（≤上一个小版本的 payload 形态）

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 协议新增语义让旧节点静默失效 | M3.0-1 能力协商前置；新方法/字段一律「可选 + 缺省走旧行为」 |
| HITL 代答引入越权（非 leader 代答 / 跨组代答） | 代答权=同组 leader + taskId 反查 groupId，双校验；审计日志逐次落 hub |
| 远端文件树泄漏工作空间外文件 | 直接复用 `WorkspaceFS.assertInWorkspace`（多根+符号链接逃逸+.git）+ 只读 + 分页上限 |
| DAG 引入调度死锁（循环依赖） | 提交时环检测；运行时 blocked_by 未满足不进入 queued |
| 撤回/下架破坏已装节点 | 撤回只影响「目录可见性」，已装保留；下载端点保留历史版本 |
| 期数膨胀（M2 教训：5 期变 8 期） | 每期只做本表交付，新增缺口一律记下期票桶；**M3 系列硬上限 5 期**，超出则重排而非加期 |

## 6. 批准记录（2026-10-28）

用户指令：「开始M3 任务，我准备休息。所有需要我审核的文档，我允许你直接通过直到所有M3任务完成」。

1. 分期与顺序：**采纳**推荐（M3.0 → M3.1 → M3.2 → M3.3 → M3.4，硬上限 5 期）。
2. B3 多组 UI：**关闭为已交付**（F1 取证成立）。
3. M3.0 可选项：0-6 readonly 持久化**不纳入**（维持 M2.5「设计边界」裁决）；0-7 CI e2e 解禁**纳入**。
4. M3.0 **立即开工**；后续各期独立出 spec，用户级审核自动通过。

**质量门不变**：spec → plan → Momus（技术门）→ TDD 分批 → 真机验收 → oracle 终审 → MERGE READY → 合并推送。用户门预授权 ≠ 跳过 agent 门。
