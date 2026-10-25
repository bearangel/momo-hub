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

// ────────────────────────────────────────────────────────────────────────────
// 委派私有 wire（spec §3.1 裁量 + hub T9 与 studio T10/T11 的对账约定）——
// 不进 RPC 目录（ws 控制面公开方法），仅供 hub 与 studio 目标端/leader 端消费。
// ────────────────────────────────────────────────────────────────────────────

/** 委派链路私有方法名（信封 method 字段值）。 */
export const TASK_WIRE = {
  /**
   * 目标节点 → hub：状态上报（res 信封，id=taskId——id 即对账键，ok=true，
   * 载荷 TaskReportPayload）。running 态重复上报 = 进度拍（500ms 合并节流在
   * 目标侧完成，hub 只做直转，不缓存不合并）。interrupted / cancelled 不经
   * 上报：前者由 hub 在节点离线时主动迁移，后者由 leader 经 task.cancel 触发。
   */
  report: 'task.report',
} as const;

/** task.report 载荷：status 三值；hub 按 id=taskId 对账、canTransition 单点
 *  校验后更新账本，并按 status 转发对应 EVT 定向给 leader——
 *  running→task.progress、done→task.completed、failed→task.failed{reason}。 */
export interface TaskReportPayload {
  status: 'running' | 'done' | 'failed';
  /** 委派会话 id（MC1 独立委派会话——首次 running 上报建立，hub 回填账本 session_id）。 */
  sessionId?: string;
  /** failed 附因（约定值：'declined'=目标拒派；'local_session_lost'=续传时本地
   *  会话已丢失；其余自由文本）；done 不携带。 */
  reason?: string;
  /** 进度段（running 上报可携带；终态转发不消费该段）。 */
  progress?: TaskReportProgress;
}

/** 进度拍：委派会话 message_events 尾部摘要（excerpt 为截断文本）。 */
export interface TaskReportProgress {
  lastSeq: number;
  excerpt: string;
}
