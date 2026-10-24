# momo-hub M2（分布式任务节点·工作组）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 工作组内核 + 镜像（含 M1 资源补装）+ 委派（委派会话/断线续传）+ 远程查看（seq 回放+尾部订阅），hub 定向投递能力，真机三进程全链验收。

**Architecture:** hub 增 registry.sendTo 定向投递 + 迁移 v3 五表（groups/members/invites/snapshots_meta/delegations）+ leader-only RPC 校验层；镜像=HTTP 快照存档（C6 纪律）+ ws 两段式 mirror + 目标节点应用器；委派=账本状态机 + 子节点独立委派会话（MC1）+ 500ms 节流进度流；查看=拉取+订阅（MC3）+ 双层同意（MC4）。

**Tech Stack:** 同 M0/M1。派发纪律 RM1-13：仅 quick/deep 档（MiniMax 不可用）。

**Spec:** `docs/specs/2026-10-24-momo-hub-m2-distributed-workgroups.md`（MC1-MC8 + §3-§9 契约——冲突以 spec 为准）

## Global Constraints

- 两仓分支：momo-hub `feat/m2-workgroups`、momo-studio `feat/hub-m2`（各自 main 切出）；每任务 commit；Conventional Commits
- TS strict 禁 any/@ts-ignore；注释中文；vitest forks（hub）/--pool=forks（studio 单目录）；裸 run 全量；ABI 流程照旧；孤儿 worker 清理
- 协议纪律：protocol 变更 hub 先行；1.2.0 / tag v0.1.2 / studio pin 升级（MC7）
- 红线：镜像 payload 无 provider key/无 url-MCP 凭证头/无聊天历史（构建器单测锁）；查看面零写入口（UI 测试锁）；委派任务书与快照过 hub 审计
- studio 侧任务加载 skills：momo-test-rules / momo-boundary-rules（IPC 面）/ momo-ui-preview-rules（T8/T12/T14 三处 P1 门禁——实现前静态预览获批）/ momo-feature-scope-rules（消费面矩阵）
- MC8 前置：T1 先行，不并行后续依赖它的批次
- mock 纪律（M1 D1 教训）：hub mock 仿真鉴权/角色/定向路由语义；信封走真 protocol

## Review Focus

1. **定向投递的可达性与积压**：sendTo 目标离线→node_offline 错误码（不留静默积压）；订阅转发无泄漏（unsubscribe 后零事件）→ T3/T13 用例
2. **两段式 conflict 竞态**：confirm 重发期间目标工作空间被他人镜像→第二次仍校验（版本对账）→ T6 用例
3. **账本状态机非法迁移**：cancel 对 done、interrupted 后 double-complete 等迁移全拒（hub 单点校验）→ T9 矩阵用例
4. **镜像红线渗漏**：构建器产物逐字段断言无 key/凭证头/历史（变异验证：塞入即红）→ T7 用例
5. **进度节流与序完整性**：500ms 合并批内 seq 单调不丢（子节点批量转发 vs 单事件乱序）→ T11 用例

---

## M2a · 前置小票（Task 1）

### Task 1: D2 keychain 实例作用域 + D7 角标锚已装版

**Files:** Modify `electron/src/main/hub/creds.ts`（作用域派生）、keychain 调用链；Modify `renderer/`（角标锚定已装版）+ stores；Test 对应

**Interfaces:**
- Produces: `hub.deviceToken` 键加**实例作用域后缀**（由 userData 隔离目录稳定派生——如目录名 hash 前 8 位；单默认实例后缀为空保持向后兼容：**迁移逻辑**——启动时若旧键存在且本实例为默认实例则迁移，非默认实例不读旧键）；D7：已装 hub 项角标改锚 `installed_packages.version`（主进程透出 installedVersion 字段——join 或二次查询，T14 遗漏的 M2 首票）+ 更新判定改「installedVersion ≠ versions[0]」

- [ ] Step 1: 失败测试：双实例作用域键隔离（mock keychain 键集合断言）；默认实例迁移幂等；角标锚 installedVersion（目录先行+事件迟到场景角标正确显示）
- [ ] Step 2: 红 → 实现 → 绿（tests/hub + tests/resource + renderer 相关套件）
- [ ] Step 3: typecheck/lint → Commit `fix(hub): keychain 实例作用域（D2）与角标锚已装版（D7）——M2 前置小票`

---

## M2b · 定向路由 + 工作组内核（Task 2-4）

### Task 2: protocol 1.2.0——组/镜像/委派/查看契约

**Files:** Create `src/groups.ts`、`src/delegation.ts`、`src/remote-view.ts`；Modify `src/events.ts`/`error.ts`/`version.ts`(1.2.0)/`index.ts`；Test `packages/hub/tests/protocol-m2.test.ts`

**Interfaces（全部类型与目录常量，后续任务消费）:**
- RPC 目录常量：`RPC.group = {create:'group.create', list:'group.list', invite:'group.invite', join:'group.join', leave:'group.leave', transfer:'group.transfer'}`；`RPC.workspace = {mirror:'workspace.mirror', snapshots:'workspace.snapshots'}`（build 走 HTTP）；`RPC.task = {delegate:'task.delegate', cancel:'task.cancel', list:'task.list'}`；`RPC.view = {workspace:'view.workspace', sessions:'view.sessions', messages:'view.messages', tasks:'view.tasks', subscribe:'view.subscribe', unsubscribe:'view.unsubscribe'}`
- 类型：GroupInfo/GroupMember/InviteInfo/WorkspaceSnapshotMeta/MirrorRequest{confirm?}/MirrorResult{appliedResources}/DelegationTask{title,prompt,agentSlug?}/TaskRecord{status: 'pending'|'running'|'done'|'failed'|'interrupted'|'cancelled'}/ViewOverview/ViewSessionPage/ViewMessagePage/ViewSubscribe 等载荷与响应形状（spec §3.1 逐项）
- EVT 新增：groupChanged/taskAssigned/taskProgress/taskCompleted/taskFailed/taskInterrupted/taskCancelled/viewAppended/workspaceMirrored（§3.2 载荷逐字段）
- RpcErrorCode += 'forbidden'|'node_offline'|'not_found'|'conflict'
- `validateSnapshotManifest`（快照 manifest 结构校验——workspaceName/version/resources 引用清单/配置段，S1 同款纪律：字段类型/长度/未知容忍）

- [ ] TDD（round-trip + 目录常量 + 错误码 + manifest 好-坏矩阵）→ 红 → 实现 → 绿 → Commit `feat(protocol): M2 契约——组/镜像/委派/查看 RPC 与事件、错误码、快照 manifest 校验（1.2.0）`

### Task 3: hub 迁移 v3 组表 + 定向投递 + 组管理 RPC

**Files:** Modify `packages/hub/src/{db.ts(迁移),registry.ts(sendTo),server.ts(校验层)}`；Create `packages/hub/src/groups.ts`（hub 域）；Test `packages/hub/tests/groups.test.ts`

**Interfaces:**
- `NodeRegistry.sendTo(nodeId, env): 'sent'|'offline'`（在线判定 + ws.send try/catch）
- 组管理五 RPC 全实现（spec §3.1）：create/list/invite（码生成+可撤销）/join（码→组，码撤销或不存在→not_found）/leave/transfer（leader-only）
- **leader-only 校验层**：RPC 派发前按方法表校验组角色（mirror/delegate/cancel/view.*/transfer→leader；非成员→forbidden）；实现为 dispatcher 外围守卫（方法→所需角色映射表）
- group.changed 组内广播；迁移 v3（groups/group_members/group_invites 三表，spec §4 SQL 逐字）

- [ ] TDD 矩阵（组生命周期/邀请码撤销/角色矩阵——非 leader 调 leader-only 方法 forbidden/非成员/离线定向 node_offline/sendTo 在线投递）→ 红 → 实现 → 绿（M0/M1 全量回归）→ Commit `feat(hub): 工作组内核——迁移 v3/定向投递/组管理 RPC/leader-only 校验层`

### Task 4: studio 组管理接入

**Files:** Create `electron/src/main/hub/groups.ts`（客户端：组 RPC 调用 + presence 融合）；Modify hub client（RPC 请求-响应关联器：id→pending promise，超时 10s）+ ipc.handlers + preload/types；Create `renderer/src/components/settings/GroupPanel.tsx` + store；Test 贴源/镜像

**Interfaces:**
- hub client 增 `request(method, payload): Promise<响应>`（信封 id 关联——M0 单向 onEvent 扩为双向 RPC 语义；重连时 pending 全部 reject 人话）
- IPC：`hub:groupList/create/invite/join/leave/transfer` + 推送 `hub:groupChanged`
- 组面板：组列表（成员+在线态复用 presence）/凭码入组/邀请码展示复制/退组/转移（简单表单，非 P1 新形态——复用既有表单模式可豁免预览，若实现中构成新布局仍出预览）

- [ ] TDD（RPC 关联器单测/mock hub 组矩阵/面板渲染与操作）→ 绿 → Commit `feat(hub): 工作组管理接入——组面板/RPC 关联器/presence 融合`

---

## M2c · 镜像（Task 5-8）

### Task 5: hub 快照存档端点 + workspace.snapshots

**Files:** Create `packages/hub/src/snapshots.ts`；Modify `server.ts`/`resources.ts`（Bearer 鉴权复用）；Test `packages/hub/tests/snapshots.test.ts`

**Interfaces:**
- `POST /workspaces/snapshots`（Bearer + multipart：manifest + payload tar.gz ≤200MB 流式，**服务端自算 checksum**——C6/M1 blobs 纪律全复用）；manifest 过 protocol validateSnapshotManifest；落 `ws-snapshots/{sha256}.tar.gz` + workspace_snapshots_meta 表
- `workspace.snapshots` RPC（列表，byNode/at/version）
- 无组校验（任何已注册节点可存档自己的快照——镜像动作才校验 leader）

- [ ] TDD（上传/校验矩阵/超限/重复版本 409 同款/列表）→ 绿 → Commit `feat(hub): 工作空间快照存档端点——C6 纪律复用/manifest 校验/元数据目录`

### Task 6: hub workspace.mirror 两段式路由

**Files:** Modify `packages/hub/src/snapshots.ts` 或新 `mirror.ts`；Test `packages/hub/tests/mirror.test.ts`

**Interfaces:**
- `workspace.mirror` RPC（leader-only）：leader→hub→**定向 sendTo(targetNodeId)** 转发镜像应用请求（含 snapshotId/strategy/confirm）→ 目标节点应用（T8）→ 回执经 hub 汇总返 leader（异步：hub 先返 `{accepted:true, mirrorId}`，结果经 `workspace.mirrored` 事件回 leader——**两段式镜像也两段式响应**）
- conflict 语义：目标节点工作空间已存在且 `confirm!==true` → 目标节点返差异摘要（新增/移除/变更计数）→ hub 透传 leader（`conflict` 错误码 + summary payload）；confirm=true 直落
- **竞态防线（Review Focus #2）**：目标节点应用时校验 mirrorId 对账（同工作空间并发镜像后到拒——conflict + 'mirror_in_progress'）
- 目标离线：node_offline 即返
- workspace.mirrored 组内广播（appliedResources 汇总）

- [ ] TDD（转达/回执/conflict 两段/并发对账/离线/leader 校验）→ 绿 → Commit `feat(hub): 镜像路由——两段式 conflict/定向转发/回执汇总/并发对账`

### Task 7: studio 快照构建器（leader 侧）

**Files:** Create `electron/src/main/hub/snapshot-builder.ts`；Test `electron/tests/hub/snapshot-builder.test.ts`

**Interfaces:**
- `buildWorkspaceSnapshot(workspaceId): Promise<{manifest, archivePath, checksum}>`——快照内容=spec §5.2.1 清单（agent 定义〔custom 内嵌 / 资源 slug 引用〕/成员/团队/MCP 启用〔仅 stdio〕/skill slug/插件启用〔id+hub slug〕/provider 引用〔类型+型号偏好〕/工具上限偏好）
- **红线锁（Review Focus #4）**：payload 序列化后扫描断言零 key 材料（provider key/url-MCP 凭证头/聊天历史表导出）——测试变异验证（人为塞入即红）
- 复用：M1 publish 的 tar 构造/manifest 形态；agent 域定义读取；teams/mcp/skills/plugins 启用面查询（grep 实际查询入口）

- [ ] TDD（内容清单逐项/红线变异四连/引用 vs 内嵌分流）→ 绿 → Commit `feat(hub): 工作空间快照构建器——清单全量/红线零渗漏/资源引用分流`

### Task 8: studio 应用器 + 镜像向导（P1 预览门禁）

**Files:** Create `electron/src/main/hub/mirror-applier.ts`（目标节点侧）；Create 镜像向导 UI（工作组面板内，P1）；Modify 组面板 store；Test 贴源/镜像

**Interfaces:**
- 应用器：收镜像请求（hub client onEvent 新增 mirror.apply 私有方法名——**hub 转发用专用 RPC 目标端语义：目标节点收到 `mirror.apply` 请求信封**）→ 工作空间不存在创建/存在且无 confirm 返差异摘要 → **资源补装**（快照引用 slug 缺失→M1 安装链，先装后配）→ 配置整体替换落库 → 回执 appliedResources → `workspace.mirrored` 由 hub 广播
- leader 向导：选工作空间→构建（进度）→上传→选目标节点→（conflict 时差异摘要确认 UI）→执行→结果
- **P1 预览门禁**：向导三步布局+差异摘要卡先出预览获批再实现

- [ ] Step 1 静态预览 → **暂停等用户批准**；Step 2-5 TDD（应用器矩阵：新建/替换/补装顺序/缺资源回滚语义〔补装失败整体中止不落半配〕/confirm 分流/并发对账）→ 绿 → Commit `feat(hub): 镜像应用器与向导——资源补装/两段确认/原子落库`

---

## M2d · 委派（Task 9-12）

### Task 9: hub 委派账本与 task.* RPC

**Files:** Create `packages/hub/src/delegations.ts`；Modify `server.ts`；Test `packages/hub/tests/delegations.test.ts`

**Interfaces:**
- delegations 表 + 状态机（pending→running→done/failed；interrupted 中间态〔目标离线时 hub 主动迁移 pending/running→interrupted〕；cancelled 仅从 pending/running/interrupted）；**非法迁移全拒矩阵**（Review Focus #3——hub 单点校验，目标节点的 completed/failed 上报须匹配当前态）
- `task.delegate`（leader-only，定向 task.assigned）/`task.cancel`（定向取消 + 账本）/`task.list`（过滤）
- task.progress/completed/failed 转发（目标→leader 定向）+ 账本同步更新；审计日志

- [ ] TDD（状态机全迁移矩阵×非法拒/定向转发/leader 校验/离线 interrupted）→ 绿 → Commit `feat(hub): 委派账本——状态机单点校验/定向转发/中断语义`

### Task 10: studio 子节点委派执行器

**Files:** Create `electron/src/main/hub/delegation-runner.ts`；Modify hub client（task.assigned 事件→执行器）；Test `electron/tests/hub/delegation-runner.test.ts`

**Interfaces:**
- 收 task.assigned → 前置检查（接受开关/并发上限/工作空间存在——拒因经 hub 回 task.failed{reason:'declined',detail}）→ **建委派会话**（MC1：`delegated` 元数据 + 任务书为首条 user 消息）→ AgentRunner 执行（既有会话执行入口 grep 复用——spawn 语义同本地会话）→ 完成态上报
- 进度：会话 message_events 尾部**500ms 合并批量转发**（seq 单调，Review Focus #5）——复用 M0 事件链
- 并发计数器（上限可配，默认 2）

- [ ] TDD（前置三拒/委派会话标记隔离/进度批单调/完成上报/开关即时生效）→ 绿 → Commit `feat(hub): 子节点委派执行器——委派会话/前置检查/节流进度批`

### Task 11: studio 断线续传与取消落地

**Files:** Modify `delegation-runner.ts` + hub client（重连恢复）；Test 扩展

**Interfaces:**
- 子节点重连→查 hub `task.list{status:interrupted,target:自己}` → 本地委派会话若存在（会话标记匹配 taskId）→ **复用会话断点续跑**（既有续跑入口）→ 继续 progress；本地无此会话（如重启丢档）→ 上报 failed{reason:'local_session_lost'}
- `task.cancel` 到达：运行中→停跑（既有取消入口）→ 账本 cancelled；pending/interrupted→直接 cancelled

- [ ] TDD（interrupted 恢复/本地丢失上报/cancel 三态）→ 绿 → Commit `feat(hub): 委派断线续传与取消——重连恢复/丢失上报/三态取消`

### Task 12: leader 委派面板（P1 预览门禁）

**Files:** Create 委派 UI（任务列表+进度流+取消+新建向导，P1）；Modify 组面板/工作空间面板入口；Test 贴源

**Interfaces:** 委派列表（状态徽标 task-status 语义色）/进度流（节流合并展示——MC1 会话式）/取消按钮/新建（选节点+agent 可选+任务书）；入口=工作空间面板「委派任务」

- [ ] Step 1 预览（列表+进度流+新建三块）→ **暂停获批**；TDD（三块渲染/事件驱动更新/取消接线）→ 绿 → Commit `feat(hub): 委派面板——状态徽标/进度流/取消/新建向导`

---

## M2e · 远程查看（Task 13-15）

### Task 13: hub view.* 转发层

**Files:** Create `packages/hub/src/remote-view.ts`；Modify `server.ts`；Test `packages/hub/tests/remote-view.test.ts`

**Interfaces:**
- view.* 六 RPC（leader-only + **目标节点同意开关校验**：节点上线/变更时上报 `view.consent {allowed}` 存 hub 内存态；allowed=false → forbidden）→ 定向转发目标节点→响应透传 leader
- 订阅登记：`view.subscribe` 建 (leaderConn, targetNodeId, sessionId) 三元组；目标节点尾部事件（`view.tail` 私有上行）→ hub 组装 view.appended 定向推订阅端；unsubscribe 注销；**连接断开自动注销**（Review Focus #1 无泄漏）

- [ ] TDD（六方法矩阵/consent 校验/订阅生命周期含断开注销/离线）→ 绿 → Commit `feat(hub): 远程查看转发层——consent 校验/订阅登记/尾部定向`

### Task 14: studio 查看服务端 + 四视图 UI（P1 预览门禁）

**Files:** Create `electron/src/main/hub/view-server.ts`（目标节点组装器：workspace/sessions/messages 分页/tasks + 尾部转发——复用 sessions 查询入口 grep）；Create 四视图 UI（P1）；Modify 工作空间面板「查看 X 的工作空间」入口；Test 贴源/镜像

**Interfaces:** 组装器四方法 + view.subscribe 后 message_events 尾部转发（进程内事件订阅→定向上行）；UI=概览/会话列表（点击进回放视图：seq 分页向上加载+底部实时追加）/任务/配置四页签；**只读零写入口**（UI 测试断言无任何写操作按钮——红线）

- [ ] Step 1 预览（四页签+回放交互形态）→ **暂停获批**；TDD（组装器矩阵/尾部单调/UI 只读断言/分页）→ 绿 → Commit `feat(hub): 远程查看服务端与四视图——seq 回放/尾部订阅/只读红线`

### Task 15: 双层同意开关 + M2 集成测试

**Files:** Modify 子节点设置（开关 UI+上报链）；Create `electron/tests/hub/m2-integration.test.ts`（进程内三路：mock hub 全仿真 + 两 studio 实例 headless——镜像→委派→查看全链）

**Interfaces:** 开关默认开/变更即时上报/离线上报兜底（重连时补报）；集成测试覆盖 spec §1 验收形态三段（资源补装断言/进度实时断言/回放断言）

- [ ] TDD → 绿 → Commit `feat(hub): 查看同意开关与 M2 三段集成测试`

---

## M2f · 验收与收尾（Task 16-17）

### Task 16: 真机三进程验收（momo-acceptance）

hub 真进程 + 双隔离实例（leader A / worker B）：组创建入组→镜像（含 conflict 摘要与补装断言）→委派（进度实时/取消/interrupted 模拟——kill B 重连续传）→查看（回放+尾部+consent 关闭 403）→M0/M1 回归（presence/资源链）。缺陷分级 P0-P3 修复轮预留。产出 `.omo/qa-reports/`。

### Task 17: 收尾——版本账本 + 终审准备

- 协议/hub 版本 0.1.2 + tag v0.1.2 + studio pin 升级（若 T2 后协议有补改，tag 打最终协议态）
- studio 三处 alpha.30 + CHANGELOG M2 条目（已知限制：UI 单组/委派单任务无编排/结果以消息流消费/文件树远程不在本版）
- hub CHANGELOG 0.1.2；全量验证阶梯双仓
- oracle 全分支终审 → 合并选项呈报

---

## 附录：SDD 派发批量（RM1-13：quick/deep 档）

| 批 | 任务 | 档 | 门禁点 |
|---|---|---|---|
| D1 | T1 | deep | — |
| D2 | T2 | deep | — |
| D3 | T3+T4 | deep | — |
| D4 | T5+T6 | deep | — |
| D5 | T7 | deep | 红线变异 |
| D6 | T8 | deep | **P1 预览暂停** |
| D7 | T9 | deep | — |
| D8 | T10+T11 | deep | — |
| D9 | T12 | deep | **P1 预览暂停** |
| D10 | T13 | deep | — |
| D11 | T14+T15 | deep | **P1 预览暂停** |
| D12 | T16+T17+终审 | deep/oracle | 验收+终审 |

接口速查：T2 契约→全部；T3 sendTo/角色层→T6/T9/T13；T5 存档→T6/T7；T7 构建器→T8；T9 账本→T10/T11/T12；T13 订阅→T14；T4 组面板→T8/T12/T14 入口承载。
