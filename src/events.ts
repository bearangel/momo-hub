// 服务端推送事件目录（ws 控制面，evt 信封）。
import type { DelegationTask } from './delegation';
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

/** group.changed：组内广播（成员变动/leader 转移）。 */
export interface GroupChangedEvent {
  groupId: string;
  kind: 'joined' | 'left' | 'leaderChanged';
  nodeId: string;
}

/** task.assigned：定向→目标节点；收到后本地建委派会话执行（MC1）。 */
export interface TaskAssignedEvent {
  taskId: string;
  workspaceName: string;
  task: DelegationTask;
  byNode: string;
}

/** task.progress：定向→leader（目标节点 500ms 合并批量转发；seq 单调）。 */
export interface TaskProgressEvent {
  taskId: string;
  sessionId: string;
  lastSeq: number;
  excerpt: string;
  /** 等待标注（M2.1 T11b，0.1.4）：'waiting-input' = 目标侧等待用户确认——随
   *  task.report 进度段透传；缺省 = 正常进度（0.1.3 载荷向后兼容）。 */
  kind?: 'waiting-input';
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
