// 委派契约（spec 2026-10-24 M2 §3.1「委派」表 + §3.2 事件 + 裁定 MC1/MC5；
// M3.1 增 waiting 态与远程代答，spec 2026-10-28 §3.1）——
// leader→hub→目标节点定向路由；账本状态机七态；hub 与 studio 双端消费。
export const TASK_RPC = {
  delegate: 'task.delegate',
  cancel: 'task.cancel',
  list: 'task.list',
  answer: 'task.answer',
} as const;

/** 委派账本状态机（§4 delegations.status）：
 *  pending→running→done/failed；interrupted=目标离线中间态（重连续跑）；
 *  cancelled 仅自 pending/running/waiting/interrupted（迁移合法性由 hub 单点校验）；
 *  waiting=running 中 agent 等待用户确认的中间态（M3.1）——由目标侧
 *  kind='waiting-input' 上报驱动进出，hub 代答不直接迁移（状态单源）。 */
export type DelegationStatus =
  | 'pending' | 'running' | 'waiting' | 'done' | 'failed' | 'interrupted' | 'cancelled';

/** 任务书（MC1：目标节点据此建独立委派会话执行）。 */
export interface DelegationTask {
  title: string;
  prompt: string;
  agentSlug?: string;
  /** HITL 权限档位（M2.1 T10b，0.1.3）：readonly/sandbox/confirm 三值枚举，缺省合法
   *  （0.1.2 载荷向后兼容）；hub 侧非法值剥除透传（未知容忍先例 modelBinding）。 */
  permissionTier?: 'readonly' | 'sandbox' | 'confirm';
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

/** 委派账本条目（§3.1 task.list 响应项——七字段 + M3.1 代答留痕四字段 +
 *  M4.2 组归属字段）。 */
export interface TaskRecord {
  taskId: string;
  workspaceName: string;
  targetNodeId: string;
  /** 组归属（M4.2，1.6.0）：hub 从 delegations.group_id 既有列直接投影（零迁移）
   *  ——leader 委派板据此按组过滤/聚合；可选字段（1.5 消费方未知容忍剥除）。 */
  groupId?: string;
  status: DelegationStatus;
  title: string;
  createdAt: number;
  updatedAt: number;
  /** 代答留痕四列（M3.1，db v6）：NULL = 未代答；重复代答记最新一次（worker 自报
   *  stale 时决策列同样覆盖）。 */
  answeredBy?: string | null;
  answerDecision?: 'approve' | 'deny' | null;
  answerMessage?: string | null;
  answeredAt?: number | null;
}

/** task.answer 请求（M3.1 HITL L2 远程代答，leader-only，门槛 minor 3）。
 *  message 仅审计与 leader 侧展示（≤512 字符，hub 校验），不注入子进程——
 *  approve 走 covered 轮询原地重执行、deny 走标准拒绝文本，两条既有路径
 *  均无注入点（一义一名不扩义）。 */
export interface TaskAnswerReq {
  taskId: string;
  decision: 'approve' | 'deny';
  message?: string;
}

/** task.answer 响应：granted=已批准 / denied=已拒绝 / stale=worker 侧已无等待
 *  （重复代答幂等口径——决策列仍记最新）。 */
export interface TaskAnswerRes { disposition: 'granted' | 'denied' | 'stale' }

/** task.answered 事件（M3.1）：代答落账后定向→委派 leader（by_node_id），
 *  与 task.progress/terminal 同向——leader UI 即时反馈。 */
export interface TaskAnsweredEvent {
  taskId: string;
  decision: 'approve' | 'deny';
  /** 代答执行者节点 id（同组 leader）。 */
  by: string;
  disposition: 'granted' | 'denied' | 'stale';
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

/** 进度拍：委派会话 message_events 尾部摘要（excerpt 为截断文本）。
 *  kind 可选标注（M2.1 T11b，0.1.4）：'waiting-input' = 目标侧 agent 正等待
 *  用户确认（写授权卡等）——leader 看板据此切「等待节点确认」态。hub 侧白名单
 *  + 枚举校验，非法值剥除不抛（先例 permissionTier/modelBinding）。 */
export interface TaskReportProgress {
  lastSeq: number;
  excerpt: string;
  kind?: 'waiting-input';
}
