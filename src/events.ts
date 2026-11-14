// 服务端推送事件目录（ws 控制面，evt 信封）。
import type { DelegationTask, InterventionAnswer, InterventionRequest } from './delegation';
import type { HubResourceType } from './resources';
import type { ViewMessage } from './remote-view';

export const EVT = {
  nodeOnline: 'node.online',
  nodeOffline: 'node.offline',
  resourcePublished: 'resource.published',
  // M3.4（1.5.0）新增：资源撤回（目录不可见，历史版本下载保留）
  resourceUnpublished: 'resource.unpublished',
  // M2（1.2.0）新增九事件（spec 2026-10-24 M2 §3.2）
  groupChanged: 'group.changed',
  taskAssigned: 'task.assigned',
  taskProgress: 'task.progress',
  taskCompleted: 'task.completed',
  taskFailed: 'task.failed',
  taskInterrupted: 'task.interrupted',
  taskCancelled: 'task.cancelled',
  // M3.1（1.3.0）新增：代答复核回执（定向→委派 leader）。刻意不做 task.waiting
  // 双信号——leader 等待展示走 task.progress kind 通道（spec §3.4 裁定）。
  taskAnswered: 'task.answered',
  // 1.11.0（任务生命周期 v2）新增五事件（spec 2026-11-13 §8.2）：干预请求/答复
  // 回执/提交/验收通过/打回——定向语义见各载荷接口；peer 组仅转发提交/验收
  // 三事件，干预两事件不发（hub 只发中性进度，spec §8.6）。
  taskIntervention: 'task.intervention',
  taskInterventionAnswered: 'task.interventionAnswered',
  taskSubmitted: 'task.submitted',
  taskAccepted: 'task.accepted',
  taskRejected: 'task.rejected',
  viewAppended: 'view.appended',
  workspaceMirrored: 'workspace.mirrored',
} as const;
export type EventMethodName = (typeof EVT)[keyof typeof EVT];

export interface NodeOnlineEvent { nodeId: string; nodeName: string; at: number }
export interface NodeOfflineEvent { nodeId: string; at: number }

/** resource.published：发布成功后广播全部在线节点（spec M1 §3.3/§5）。 */
export interface ResourcePublishedEvent {
  slug: string;
  type: HubResourceType;
  name: string;
  version: string;
  author: string;
  at: number;
  /** 归属节点 id（1.8.0 M5.0）：发布即认领/放行的 owner——studio 目录增量合并保留
   *  该字段（徽标/按钮收敛），缺省不带键（旧 hub 事件零变化）。 */
  ownerNodeId?: string;
}

/** resource.unpublished（1.5.0 M3.4 B12）：撤回成功后广播全部在线节点——
 *  目录过滤墓碑项、已装节点不受影响、历史版本下载端点保留。
 *  by = 执行撤回的鉴权节点名（信任根在 hub，与 publish 的 author 同口径）。 */
export interface ResourceUnpublishedEvent {
  slug: string;
  by: string;
  at: number;
}

// ────────────────────────────────────────────────────────────────────────────
// M2 事件载荷（§3.2 逐字段）
// ────────────────────────────────────────────────────────────────────────────

/** group.changed：组内广播（成员变动/leader 转移/绑定变化）。
 *  'bound'（1.10.0）：绑定/换绑/解绑/镜像开关翻转——消费方据此重拉 group.list
 *  （GroupMember.bound/boundWorkspaceName/acceptsMirror 投影刷新）。 */
export interface GroupChangedEvent {
  groupId: string;
  kind: 'joined' | 'left' | 'leaderChanged' | 'bound';
  nodeId: string;
}

/** task.assigned：定向→目标节点；收到后本地建委派会话执行（MC1）。
 *  groupId?（1.10.0）：组归属透传——peer 草稿接收端据此解析组级绑定空间
 *  （多 peer 组绑定的判别键）；hub 恒置（旧 worker 未知字段容忍零消费）。 */
export interface TaskAssignedEvent {
  taskId: string;
  workspaceName: string;
  task: DelegationTask;
  byNode: string;
  groupId?: string;
}

/** task.progress：定向→leader（目标节点 500ms 合并批量转发；seq 单调）。 */
export interface TaskProgressEvent {
  taskId: string;
  sessionId: string;
  lastSeq: number;
  excerpt: string;
  /** 等待标注（M2.1 T11b，0.1.4）：'waiting-input' = 目标侧等待用户确认——随
   *  task.report 进度段透传；缺省 = 正常进度（0.1.3 载荷向后兼容）。
   *  1.10.0 平行模式扩 'received'|'arranged'（草稿已接收/已安排——随 status:'pending'
   *  生命周期拍透传，发起方看板行状态展示）。 */
  kind?: 'waiting-input' | 'received' | 'arranged';
}

/** 终态四件（completed/failed/interrupted/cancelled）共用形状：定向→leader + 账本状态机迁移。 */
export interface TaskTerminalEvent {
  taskId: string;
  reason?: string;
}

/** view.appended：定向→订阅端（尾部增量）。 */
export interface ViewAppendedEvent {
  sessionId: string;
  events: ViewMessage[];
  /** 组锚定（M4.2，1.6.0）：显式 groupId 订阅的透传记录——订阅时锚定、推送时
   *  带出（订阅端据此判定该增量归属哪个组的查看面板）；缺省不带键（旧订阅
   *  形状载荷逐字节零变化）。 */
  groupId?: string;
}

/** workspace.mirrored：组内广播——组内皆可见谁被镜像到什么版本。 */
export interface WorkspaceMirroredEvent {
  groupId: string;
  targetNodeId: string;
  workspaceName: string;
  snapshotId: string;
  appliedResources: string[];
  /** 目标无法解析的引用（软失败——T8b 应用器语义；缺省 = 全部解析）。 */
  unresolvedRefs?: string[];
}

// ────────────────────────────────────────────────────────────────────────────
// 1.11.0 任务生命周期 v2 事件载荷（spec 2026-11-13 §8.2 五事件逐字段）
// ────────────────────────────────────────────────────────────────────────────

/** task.intervention：定向→发起方（lf 组）——worker 人工请求镜像源（发起方据此建
 *  镜像请求行并代答）。peer 组不转发本事件（hub 只发中性进度，spec §8.6）。 */
export interface TaskInterventionEvent {
  taskId: string;
  intervention: InterventionRequest;
}

/** task.interventionAnswered：定向→另一侧（答方=发起方则发 worker；答方=worker
 *  则发发起方）——first_wins 收敛信号，对端据此关闭本地镜像行；answeredBy 为
 *  答方节点 id（身份取自信封鉴权，不信任载荷自报）。 */
export interface TaskInterventionAnsweredEvent {
  taskId: string;
  interventionId: string;
  answer: InterventionAnswer;
  answeredBy: string;
}

/** task.submitted：定向→发起方——run 提交待验收（账本 submitted）；发起方按
 *  (taskId, runNo) 幂等懒建镜像 run 行。deliverable 为提交载荷摘要（摘要/文件/
 *  产物/测试结果，自由形状），缺省不带键。 */
export interface TaskSubmittedEvent {
  taskId: string;
  runNo: number;
  deliverable?: Record<string, unknown>;
}

/** task.accepted：定向→worker——验收通过（账本 submitted→done，accepted_at 落账）。 */
export interface TaskAcceptedEvent {
  taskId: string;
  runNo: number;
}

/** task.rejected：定向→worker——打回（账本 submitted→running，run_no+1 起新 run）；
 *  runNo 为新 run 号；reason 必带（打回理由，进 agent 上下文）。 */
export interface TaskRejectedEvent {
  taskId: string;
  runNo: number;
  reason: string;
}
