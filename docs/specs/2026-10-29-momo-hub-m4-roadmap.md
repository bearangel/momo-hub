# momo-hub M4 立项建议书 · 分期、依赖与验收面（起草中）

- **状态**：已批准（2026-10-29 用户「没有问题」——分期顺序/C4 延后到该期再裁/keychain 清理执行/M4.0 立即开工；判定槽探查回填中）
- **日期**：2026-10-29
- **上游**：M3 系列五期收官（协议 1.2.0→1.5.0，hub v0.1.9，studio alpha.40）；候选池 = M3 台账延后项 + 能力面残余
- **定位**：M3 是「节点互联能力补全」，M4 建议聚焦「**契约统一与治理沉淀**」——把 M3 快速铺开的能力面收拢为一致契约。硬上限沿用 5 期纪律。

## 1. 候选池（六项，探查定案中）

| # | 候选 | 探查定案（file:line） | 判定 / 工作量 |
|---|---|---|---|
| C1 | **多组终态** | commonGroupId 残余四点：def（groups.ts:94-106）/ viewTargetGroup 回落（:111，8 个 view.* 缺省路径）/ handleView 回落（remote-view.ts:158）/ **handleTail 订阅复检永远 commonGroupId（remote-view.ts:216）——订阅记录 (leader,target,sessionId) 不存 groupId，显式化也无法消除歧义，需扩订阅记录**。另有无组作用域语义面：presence 全局（hello onlineNodes + node.online/offline 全广播）、resource HTTP 面全局、**TaskRecord 无 groupId——委派板跨组聚合无法按组归属**（studio hub-delegation.store.ts:120 taskList() 无过滤）；WorkgroupView 无选中组时 groupId=null 静默回落 | **M4.2 做**，M：①订阅记录+协议可选字段（minor 1.6.0，M3.2 模式）②TaskRecord+groupId 可选字段（委派卡组归属）③presence 组过滤（组内可见性裁定时再定，可能裁关闭）。④⑤回落路径保留至调用方全显式化 |
| C2 | **错误契约统一** | 仅 hub:taskAnswer 有 `[code]` 前缀（ipc.handlers.ts:115-119）；其余全部通道（group.*×6 / mirror / taskList/Delegate/Cancel / view.*×8 / submitRoutedTask / retryDispatch）裸抛 RpcRemoteError——Electron IPC 只保 message（#24427），code 全丢失。消费端脆匹配：humanizeViewError 按「已关闭本组查看/不在线/离线」子串（hub-view.store.ts:145-150，对 hub 措辞脆弱）；DelegatedTaskCard regex 前缀解析（:418-436）。统一面：全通道包裹 + renderer 共享解析 lib（error-prefix 双侧契约模式）+ humanizeViewError 改 code 驱动；**协议零改** | **M4.1 做**，S/M：纯 studio 侧；迁移逐通道 + 既有文案锁对齐 + 双侧契约测试 |
| C3 | **委派依赖透传** | 插入点已定：协议 DelegationTask（hub src/delegation.ts:20-27）+ hub parse 白名单（delegations.ts:109-126，brief 不持久化只随 task.assigned 事件过 :274-277）+ studio dispatch-submit.ts:53-58/:139-148 + hub:taskDelegate 直通道（ipc.handlers.ts:91-103）+ retryDispatch。**语义裁定要点：dependsOn 是 leader 本地任务 id，跨节点无意义——应为 leader 侧派发闸（blocked 不 delegate）而非 worker 侧消费**；brief 字段仅作展示/审计透传 | **M4.3 做**，S/M：leader 侧闸（复用 M3.3 四闸模式）+ 协议可选字段（1.6.0 与 C1 同车）；worker 侧不消费 |
| C4 | **撤回治理** | resources 无 owner 列（仅 resource_versions.published_by 存节点名，name 唯一可解析）；**resources 无组概念**——leader-only 需先建 resource↔group 关联（publish 带 groupId 或撤回时按发布节点所属组）+ 端点守卫；真实多租场景尚不存在 | **延后到 M4.4 开工前再裁**（默认倾向裁关闭：无真实场景，先做则需先定 resource 组归属模型——超出治理票据范围） |
| C5 | **e2e 回归沉淀** | 8 spec 清单；hub-distribution.spec.ts **已有多实例原语**（launchApp×2 + mock hub WS + mock LLM + sendToNode/waitForNodeConn）——但 mock hub 无 task.answer/view.*/unpublished 处理；M3.1/3.3/3.4 零 e2e 覆盖确认；无共享 harness 模块。**裁定：沉淀两件**——①M3.3 依赖链 e2e（单实例零 hub 依赖，最高性价比）②M3.1 HITL 代答 e2e（mock hub 扩 task.answer）；M3.4 撤回 e2e **不做**（mock HTTP 面失真，单测+真机验收已覆盖，记裁定理由） | **M4.0 做**，M：两个 spec 文件（一新一扩） |
| C6 | **票桶 P3** | D1 已修（`fb9ca5ea`：发布事件对称化重拉列表——根因 onResourcePublished 只记角标不 load 与撤回事件不对称）；缩进已修（`3b1ff257`）；pullNodes 只增不删 = M2.5 A6 设计边界非缺陷（关闭） | ✅ **已完成**（2026-10-29，分支 m4.0） |

**待您裁定的非代码项（不入期，单独确认）**：验收残留 keychain 假键清理（`Momo Studio-<hash8>` 共 8 条，`security delete-generic-password` 一次性脚本）。

## 2. 分期草案（探查后定稿）

※ 初步倾向（待证据定序）：**M4.0 沉淀清偿批**（C5 e2e 沉淀 + C6 P3 + keychain 裁定）→ **M4.1 错误契约统一**（C2，studio 侧为主协议零改）→ **M4.2 多组终态**（C1，协议可选字段沿用 M3.2 模式）→ **M4.3 委派依赖透传**（C3，协议 1.6.0）→ **M4.4 撤回治理**（C4，视真实场景必要性可裁关闭）。

每期沿用 M3 全流程门禁：探查实证 → spec+plan → Momus → 实施 → 真机验收 → oracle → PR/CI → 冻结。

## 3. 风险（预置）

| 风险 | 缓解 |
|---|---|
| C2 统一改错链破坏既有文案锁 | 前缀编码双侧测试锁（M3.1 已立模式）逐通道迁移，行为锁逐条对齐 |
| C1 多组终态再动守卫层 | M3.2 的 groupId 可选字段模式复用；缺省回落保留至全部调用方显式化 |
| C3 依赖透传重开协议 minor | 能力门槛实战 3 次零事故，模式成熟 |
| M4 膨胀（M2 教训） | 硬上限 5 期；C4 可裁关闭；票桶不占期 |

## 4. 批准记录（2026-10-29）

用户指令：「没有问题」。

1. 分期与顺序：**采纳**（M4.0 沉淀清偿 → M4.1 错误契约统一 → M4.2 多组终态 → M4.3 委派依赖透传 → M4.4 撤回治理，硬上限 5 期）。
2. C4 撤回治理：**延后到该期开工前再裁**（默认保留为 M4.4，若届时无真实多租场景即裁关闭）。
3. keychain 清理：**执行**（仅删验收残留分域键——按创建日期 2026-10-28/29 判别，勿动个人隔离 profile 真实键与裸名既有条目）。
4. M4.0 **立即开工**（探查回报后回填判定定稿 M4.0 票据）。

**质量门不变**：沿用 M3 全流程（Momus/oracle 技术门保留）。
