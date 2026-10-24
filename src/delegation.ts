// 委派契约（spec 2026-10-24 M2 §3.1「委派」表 + §3.2 事件 + 裁定 MC1/MC5）——
// leader→hub→目标节点定向路由；账本状态机六态；hub 与 studio 双端消费。
export const TASK_RPC = {
  delegate: 'task.delegate',
  cancel: 'task.cancel',
  list: 'task.list',
} as const;

/** 委派账本状态机（§4 delegations.status）：
 *  pending→running→done/failed；interrupted=目标离线中间态（重连续跑）；
 *  cancelled 仅自 pending/running/interrupted（迁移合法性由 hub 单点校验）。 */
export type DelegationStatus = 'pending' | 'running' | 'done' | 'failed' | 'interrupted' | 'cancelled';

/** 任务书（MC1：目标节点据此建独立委派会话执行）。 */
export interface DelegationTask {
  title: string;
  prompt: string;
  agentSlug?: string;
}

export interface TaskDelegateReq {
  groupId: string;
  targetNodeId: string;
  workspaceName: string;
  task: DelegationTask;
}
/** hub 账本落档 pending 后返 taskId。 */
export interface TaskDelegateRes { taskId: string }

export interface TaskCancelReq { taskId: string }
export interface TaskCancelRes { ok: boolean }

export interface TaskListReq { groupId?: string; status?: DelegationStatus }
export interface TaskListRes { tasks: TaskRecord[] }

/** 委派账本条目（§3.1 task.list 响应项——七字段）。 */
export interface TaskRecord {
  taskId: string;
  workspaceName: string;
  targetNodeId: string;
  status: DelegationStatus;
  title: string;
  createdAt: number;
  updatedAt: number;
}
