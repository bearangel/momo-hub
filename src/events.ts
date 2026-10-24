// 服务端推送事件目录（ws 控制面，evt 信封）。
import type { DelegationTask } from './delegation';
import type { HubResourceType } from './resources';
import type { ViewMessage } from './remote-view';

export const EVT = {
  nodeOnline: 'node.online',
  nodeOffline: 'node.offline',
  resourcePublished: 'resource.published',
  // M2（1.2.0）新增九事件（spec 2026-10-24 M2 §3.2）
  groupChanged: 'group.changed',
  taskAssigned: 'task.assigned',
  taskProgress: 'task.progress',
  taskCompleted: 'task.completed',
  taskFailed: 'task.failed',
  taskInterrupted: 'task.interrupted',
  taskCancelled: 'task.cancelled',
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
}

/** workspace.mirrored：组内广播——组内皆可见谁被镜像到什么版本。 */
export interface WorkspaceMirroredEvent {
  groupId: string;
  targetNodeId: string;
  workspaceName: string;
  snapshotId: string;
  appliedResources: string[];
}
