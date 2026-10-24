# momo-hub M2（分布式任务节点·工作组）设计 spec

- **日期**：2026-10-24
- **状态**：已批准（2026-10-24，八项裁定随文确认）
- **上游**：`2026-10-23-momo-hub-overall-design.md` §8（M2 范围，随整体设计批准）；M0/M1 已交付（连接基建 + 私有 marketplace，tag v0.1.1 / studio 2.1.0-alpha.29）
- **本文职责**：把 §8 细化为可实施契约——协议/存储/路由/镜像/委派/远程查看/安全/UI；并折入 M1 终审 M2 票（D2 keychain 实例作用域为 M2 首波前置小票）

## 1. 目标与验收形态

**目标**：通过 hub 创建分布式工作组（组内每台 momo-studio 是节点，创建者为 leader/主节点）；主节点可**镜像**工作空间配置到任意节点、**委派**任务给任意节点执行、**远程查看**任意节点某工作空间的会话/任务/配置。

**验收形态**（spec §8 原文落地）：主节点镜像工作空间到子节点（缺失资源自动从 hub 资源仓补装）→ 委派任务在子节点执行、进度实时回传 → 主节点回放子节点会话消息流。

## 2. 关键裁定（本文新增，供审阅否决）

| # | 裁定 | 理由 | 若错的代价 |
|---|---|---|---|
| MC1 | **委派任务在子节点落位为独立「委派会话」**：新建会话（会话名=任务书摘要），带元数据 `delegated: {byNode, byUser, parentId, taskId}`，不侵入目标工作空间既有会话；进度回传=该会话 message_events 尾部流 | 复用 sessions/message_events 全部既有语义（seq 全序/断点续跑/工具审计），委派=「远端 spawn 一次会话」 | 若做「主节点会话投影」需跨节点会话合成，复杂度暴涨 |
| MC2 | **镜像单向：主→子推送**；快照经 hub content-addressed 存档（快照历史可回放旧版）；子节点不可反向推送配置 | spec §8.2 原文语义；快照即回滚点 | 反向同步是「数据同步」非「配置镜像」，超出 M2 |
| MC3 | **远程查看=拉取+尾部订阅**：会话列表/历史消息按 seq 拉取（分页），实时增量=子节点向 hub 转发尾部事件、hub 定向推给查看中的主节点；无轮询 | §8.4 机制红利（seq 天然全序） | 轮询实现简单但延迟+带宽差，且事件链基建已备 |
| MC4 | **权限双层执行**：hub RPC 层强制 leader-only（groups 表角色校验）；子节点「允许本组查看」开关（默认开，关掉后 hub 对其远程查看请求返 403 语义错误） | §8.4 双层同意原文 | 仅 hub 层校验则子节点无自主权；仅节点层则契约散 |
| MC5 | **委派断线语义**：子节点执行中掉线 → hub 账本标 `interrupted`；重连后子节点续跑（复用既有断点续跑）并继续推流；主节点可 `task.cancel`（子节点收到后停跑、账本 `cancelled`） | §8.3 原文 | 悬挂任务无回收即「幽灵委派」 |
| MC6 | **多组**：表结构与协议支持多组/节点多组；UI 首版单工作组操作（组列表可见、切换后操作） | §8.1 原文 | 无 |
| MC7 | 协议 **1.2.0**（新增 RPC/事件均向后兼容——未知 method 容忍已验）；hub 包版本与 tag **v0.1.2**；studio pin 升 `#v0.1.2` | 沿用 C7 纪律线 | 无 |
| MC8 | **M2 首波前置小票**：D2（keychain `hub.deviceToken` 加实例作用域——以 userData 隔离目录派生 scope 后缀）先于工作组功能落地，否则同机多实例（验收/开发常态）节点身份串味（M1 验收 D5 根因） | 验收环境阻断级教训 | 验收与日常双开即复现串味 |

## 3. 协议（hub 根 `src/`，1.2.0 新增）

### 3.1 RPC（ws，node→hub→node 定向路由）

hub 新增**定向投递能力**：`registry.sendTo(nodeId, envelope)`（现仅 broadcast）。RPC 分两类：

**工作组管理（node→hub 直答）**

| 方法 | 请求 | 响应 |
|---|---|---|
| `group.create` | `{groupName}` | `{groupId, nodeId(自己)}` —— 创建者即 leader |
| `group.list` | — | `{groups: [{groupId, groupName, leaderNodeId, members: [{nodeId, name, online}]}]}` |
| `group.invite` | `{groupId, enrollmentToken?}` | `{inviteCode}` —— 组邀请码（hub 生成，入组凭证；**不复用** hub enrollment token——组凭证独立可撤销） |
| `group.join` | `{inviteCode}` | `{groupId}` |
| `group.leave` | `{groupId}` | `{ok}` |
| `group.transfer` | `{groupId, toNodeId}` | `{ok}`（leader 转移，仅 leader 可调） |

**镜像（leader→hub→目标节点）**

| 方法 | 请求 | 响应 |
|---|---|---|
| `workspace.snapshot.build`（leader 本地构建后上传） | multipart HTTP `POST /workspaces/snapshots`（数据面，同 M1 发布形态）：manifest part + payload part（tar.gz ≤200MB） | `{snapshotId, checksum}`（服务端自算，C6 同款纪律） |
| `workspace.mirror` | `{groupId, targetNodeId, workspaceName, snapshotId, strategy:'replace'}` | `{ok, appliedResources: [...]}`（目标节点应用后经 hub 回执汇总）——**应用在目标节点执行**，hub 中转回执 |
| `workspace.snapshots` | `{workspaceName?}` | `{snapshots: [{snapshotId, workspaceName, version, byNode, at}]}`（hub 存档目录，可回放旧版=`workspace.mirror` 引旧 snapshotId） |

**委派（leader→hub→目标节点）**

| 方法 | 请求 | 响应 |
|---|---|---|
| `task.delegate` | `{groupId, targetNodeId, workspaceName, task: {title, prompt, agentSlug?}}` | `{taskId}`（hub 账本落档 pending）——目标节点收到 `task.assigned` 事件后本地建委派会话执行 |
| `task.cancel` | `{taskId}` | `{ok}` |
| `task.list` | `{groupId?, status?}` | `{tasks: [{taskId, workspaceName, targetNodeId, status, title, createdAt, updatedAt}]}` |

**远程查看（leader→hub→目标节点）**

| 方法 | 请求 | 响应 |
|---|---|---|
| `view.workspace` | `{targetNodeId, workspaceName}` | `{overview, agentMembers, teams, mcpEnabled, skills, pluginsEnabled, resourcesSummary}`（目标节点实时组装） |
| `view.sessions` | `{targetNodeId, workspaceName, beforeSeq?, limit}` | `{sessions: [{sessionId, title, lastSeq, updatedAt}]}` |
| `view.messages` | `{targetNodeId, sessionId, fromSeq, limit}` | `{messages: [...], hasMore}`（按 seq 分页回放） |
| `view.tasks` | `{targetNodeId, workspaceName}` | `{tasks: [...]}`（本地任务列表） |
| `view.subscribe` | `{targetNodeId, sessionId}` | `{ok}`（尾部增量：目标节点持续转发该会话新 message_events → hub 定向推 → 订阅端收 `view.appended` 事件） |
| `view.unsubscribe` | `{targetNodeId, sessionId}` | `{ok}` |

### 3.2 事件（EVT 新增）

- `group.changed` `{groupId, kind: 'joined'|'left'|'leaderChanged', nodeId}`（组内广播）
- `task.assigned` `{taskId, workspaceName, task, byNode}`（定向→目标节点）
- `task.progress` `{taskId, sessionId, lastSeq, excerpt}`（定向→leader；节流：目标节点 500ms 合并批量转发）
- `task.completed` / `task.failed` / `task.interrupted` / `task.cancelled` `{taskId, reason?}`（定向→leader + 账本状态机迁移）
- `view.appended` `{sessionId, events: [...]}`（定向→订阅端）
- `workspace.mirrored` `{groupId, targetNodeId, workspaceName, snapshotId, appliedResources}`（组内广播——组内皆可见谁被镜像到什么版本）

### 3.3 错误码（RpcErrorCode 扩充，只增）

`forbidden`（非 leader / 组查看被关 / 非本组成员）、`node_offline`（定向投递目标离线）、`not_found`（taskId/snapshotId/sessionId 无效）、`conflict`（镜像覆盖时目标工作空间存在且 leader 未确认——配合差异摘要两段式，见 §5.2）

## 4. hub 存储（迁移 v3）

```sql
CREATE TABLE IF NOT EXISTS groups (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, leader_node_id TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS group_members (
  group_id TEXT NOT NULL, node_id TEXT NOT NULL, joined_at INTEGER NOT NULL,
  PRIMARY KEY (group_id, node_id)
);
CREATE TABLE IF NOT EXISTS group_invites (
  code TEXT PRIMARY KEY, group_id TEXT NOT NULL, created_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workspace_snapshots_meta (
  snapshot_id TEXT PRIMARY KEY, workspace_name TEXT NOT NULL, version TEXT NOT NULL,
  by_node TEXT NOT NULL, checksum TEXT NOT NULL, size_bytes INTEGER NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS delegations (
  task_id TEXT PRIMARY KEY, group_id TEXT NOT NULL, workspace_name TEXT NOT NULL,
  target_node_id TEXT NOT NULL, by_node_id TEXT NOT NULL, title TEXT NOT NULL,
  status TEXT NOT NULL,            -- pending|running|done|failed|interrupted|cancelled
  session_id TEXT,                 -- 目标节点回填
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
```

- 快照 blob：`{dataDir}/ws-snapshots/{sha256}.tar.gz`（内容寻址，复用 blobs 纪律）
- 委派账本状态机迁移全部经 hub 单点（定向事件与账本更新同事务边界尽力——账本先行，事件丢失可由 `task.list` 对账恢复）

## 5. studio 侧落地

### 5.1 工作组面板（设置「节点互联」扩容 + 独立工作组页）

- 组管理：创建组/邀请码生成与分享/凭码入组/退组/转移 leader；组内节点列表（在线态复用 M0 presence）
- 委派受控开关 + 并发上限（子节点侧设置项）

### 5.2 镜像流（leader 侧）

1. 选工作空间 → 本地构建配置快照：**agent 定义**（引用 M1 资源 slug 或内嵌 custom 定义）、agent 成员清单、团队、MCP 启用清单（仅 stdio 引用，不含 url 凭证头——红线延续）、skill 清单（slug）、插件启用清单（插件 id + hub slug 引用）、provider **引用**（类型+型号偏好，不含 key——红线）、工具上限等偏好
2. 上传 hub 存档 → 选目标节点 → `workspace.mirror`
3. **两段式确认**：目标节点工作空间已存在 → hub 返 `conflict` → leader 面板展示差异摘要（目标节点实况 vs 快照：新增/移除/变更计数）→ 确认后带 `confirm: true` 重发
4. 目标节点应用：整体替换式落库（工作空间不存在则创建）→ **资源补装**：快照引用的 hub slug 缺失 → 自动从 M1 资源仓安装（顺序：资源先装后配置落）→ 回执 `appliedResources`
5. 应用全程进度回传 leader（复用定向事件）

### 5.3 委派流（leader 侧）

- 工作空间面板「委派任务」：选目标节点+agent（可选）+任务书（自然语言）→ `task.delegate`
- 任务面板：组内委派列表（状态徽标复用 task-status 语义色）、进度实时流（`task.progress` → 会话式展示，节流合并显示）、取消
- 子节点：收到 `task.assigned` → 若并发上限未满且开关开 → 建委派会话（MC1）执行；拒绝条件（关/满/工作空间不存在）→ hub 回 `task.failed {reason:'declined'}`

### 5.4 远程查看（leader 侧）

- 工作空间面板「查看 X 的工作空间」→ 四视图页（概览/会话列表/任务/资源配置）
- 会话视图=seq 分页回放 + 进入时自动 `view.subscribe` 尾部订阅（离开 unsubscribe）
- 只读红线：查看面无任何写操作入口（§8.4 不进 M2 项）

### 5.5 双层同意（子节点侧）

- 「允许本组查看」开关（默认开）——hub 收到 view.* RPC 时校验目标节点开关态（节点上线时上报 + 变更即时上报 hub）

## 6. 安全模型增量

- leader-only 强制点：`workspace.mirror` / `task.delegate` / `task.cancel` / `view.*` / `group.transfer` 全部在 hub RPC 层先验组角色（§8 权限矩阵）
- 组邀请码独立于 hub enrollment token（作用域=单组，可撤销，不复用信任根）
- 镜像 payload 红线复检：无 provider key / 无 url-MCP 凭证头 / 无聊天历史（构建器侧单元锁）
- 委派任务书经 hub 中转落档（审计链：谁在何时对谁委派/镜像/查看——hub 日志复用 M0 审计形态）

## 7. 里程碑拆分（每块独立可验收）

- **M2a 前置小票**：D2 keychain 实例作用域（MC8）+ M1 终审 M2 小票中与 M2 交集者（D7 角标锚已装版——委派/查看面板会复用已装语义，顺手闭）
- **M2b hub 定向路由 + 工作组内核**：registry.sendTo / 组表与 RPC 五方法 / 邀请码 / group.changed / 迁移 v3（组部分）/ leader 校验层
- **M2c 镜像**：快照 HTTP 存档端点 / 两段式 conflict / 目标节点应用器（含 M1 资源补装）/ workspace.mirrored / studio leader 构建器（快照内容红线锁）
- **M2d 委派**：delegations 账本 / task.* RPC+事件 / 子节点委派会话执行（MC1）/ 断线 interrupted+续传（MC5）/ 进度节流转发 / leader 任务面板
- **M2e 远程查看**：view.* RPC / 尾部订阅转发 / 四视图 UI / 双层同意开关上报
- **M2f 工作组 UI 汇总 + 验收**：组管理面板/镜像向导（P1 预览门禁）/委派向导（P1）/查看面板（P1）/ 真机验收（spec §1 形态全走查）/ alpha.30 / CHANGELOG 两仓

## 8. 测试策略

- hub：组角色校验矩阵（非 leader 各 RPC 403 语义/非成员/撤销码）、账本状态机全迁移（含 interrupted 续传窗口）、定向投递离线（node_offline）、conflict 两段式、快照校验（S1/C6 同款：服务端自算 checksum/200MB/manifest 校验）
- studio：构建器红线单测（payload 无 key/无凭证头/无历史）、应用器资源补装顺序、委派会话标记与隔离、查看只读面零写入口、开关上报
- 集成：真三进程 e2e（hub+leader+worker）——镜像→委派→查看全链（momo-acceptance 真机验收收口）
- 回归：M0 presence / M1 资源链全量不回归

## 9. 明确不做（M2 YAGNI）

- 远端代发消息/远程写操作（§8.4 不进项）
- 文件树远程浏览（M2 后跟进项）
- 子节点反向配置推送（MC2）
- 委派任务编排/依赖图（单任务委派，非流水线）
- 跨组路由/组间可见性
- 镜像增量合并（只做整体替换 + 差异摘要确认）
- 委派结果自动回收为本地产物（结果以会话消息流形态在查看面消费；「拉取结果文件」M3+ 议）
