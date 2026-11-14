// 委派契约（spec 2026-10-24 M2 §3.1「委派」表 + §3.2 事件 + 裁定 MC1/MC5；
// M3.1 增 waiting 态与远程代答，spec 2026-10-28 §3.1；M5.3 增 team 段，
// spec 2026-10-30 §5.1；1.11.0 任务生命周期 v2 增 submitted 态/干预类型族/
// 干预四 RPC/结构化答复，spec 2026-11-13 §8.1/§8.2）——
// leader→hub→目标节点定向路由；账本状态机；hub 与 studio 双端消费。
export const TASK_RPC = {
  delegate: 'task.delegate',
  cancel: 'task.cancel',
  list: 'task.list',
  answer: 'task.answer',
  // 1.11.0（任务生命周期 v2，门槛 minor 11）：干预上报 / 验收通过 / 打回 /
  // 重连对账拉取（载荷与 hub 行为见 spec 2026-11-13 §8.2）。
  intervene: 'task.intervene',
  accept: 'task.accept',
  reject: 'task.reject',
  interventions: 'task.interventions',
} as const;

/** 委派账本状态机（§4 delegations.status）：
 *  pending→running→done/failed；interrupted=目标离线中间态（重连续跑）；
 *  cancelled 仅自 pending/running/waiting/interrupted（迁移合法性由 hub 单点校验）；
 *  waiting=running 中 agent 等待用户确认的中间态（M3.1）——由目标侧
 *  kind='waiting-input' 上报驱动进出，hub 代答不直接迁移（状态单源）；
 *  submitted=run 完成待验收的中间态（1.11.0）——worker task.report 上报驱动进入，
 *  发起方 task.accept/task.reject 决定终局（done / 打回 running 新 run）。 */
export type DelegationStatus =
  | 'pending' | 'running' | 'waiting' | 'submitted' | 'done' | 'failed' | 'interrupted' | 'cancelled';

/** 任务书（MC1：目标节点据此建独立委派会话执行）。 */
export interface DelegationTask {
  title: string;
  prompt: string;
  agentSlug?: string;
  /** HITL 权限档位（M2.1 T10b，0.1.3）：readonly/sandbox/confirm 三值枚举，缺省合法
   *  （0.1.2 载荷向后兼容）；hub 侧非法值剥除透传（未知容忍先例 modelBinding）。 */
  permissionTier?: 'readonly' | 'sandbox' | 'confirm';
  /** 依赖元数据（M4.3，1.7.0）：leader 本地任务 id 清单（如 T-x 形态）——语义裁定
   *  （spec 2026-10-29 §2.1）：纯透传，仅展示/审计用途；hub 不校验存在性（leader
   *  本地 id 对 hub 无意义——跨节点不可解析）；worker 零消费（依赖闸在 leader 侧
   *  派发前置，blocked 不 delegate）。hub 只做形状校验（string 数组 ≤50 项、每项
   *  ≤128 字符，非法整包拒 400——沿 manifest version? 三格模式）；随 task.assigned
   *  事件过不持久化（同 brief 口径）；缺省不带键（1.6 载荷向后兼容）。 */
  dependsOn?: string[];
  /** 外派团队段（M5.3，1.9.0）：成员名单快照随任务走（A 方案裁定——不依赖两边
   *  团队表一致，派发时团队展开为名单）；接收端按名解析成员建多成员执行会话
   *  （leader 接待 + dispatch 注入），任一解析失败 decline 人话名单（裁定 3——
   *  不静默降级单 agent）。与 agentSlug 互斥（同场整包拒）；随 task.assigned 事件
   *  透传，hub 不持久化（与 agentSlug/dependsOn 同生命周期——账本只存
   *  taskId/title/prompt）；缺省不带键（1.8 载荷向后兼容）。 */
  team?: DelegationTeam;
  /** 平行组草稿标记（1.10.0，spec 2026-11-05 §3.2）：peer 组唯一合法形态——任务书
   *  仅承载 title/prompt/priority（人接手自行安排），携带 agentSlug/team/dependsOn/
   *  permissionTier 任一键整包拒（深度防御：防异构客户端把完整编排 smuggle 进平行组
   *  ——静默换班禁令）；lf 组携带 draft 同样整包拒（模式纯净）。缺省不带键
   *  （1.9 载荷向后兼容）。 */
  draft?: true;
  /** 草稿优先级（1.10.0，仅 draft 在场时合法）：0-10 整数，落接收方任务行 priority 列。 */
  priority?: number;
  /** 执行尝试号（1.11.0，任务生命周期 v2）：1 起整数，打回（task.reject）后 +1 起
   *  新 run；缺省 = 1。纯透传展示/对账用途（发起方看板按 (taskId, runNo) 幂等建
   *  镜像 run 行）；缺省不带键（1.10 载荷向后兼容）。 */
  runNo?: number;
}

/** 团队成员条目：name = agent 定义名（与快照名对齐口径一致），1..64 字符。 */
export interface DelegationTeamMember {
  name: string;
  isLeader: boolean;
}

/** 外派团队段形状：members 2..8 项、恰好 1 项 isLeader=true（hub 侧校验锚点）。 */
export interface DelegationTeam {
  members: DelegationTeamMember[];
}

/**
 * 团队派发所需的最低协议 minor（M5.3 §5.1 版本门，1.9.0）：team 段是载荷能力
 * （非新 RPC 方法，不入 METHOD_REQUIRED_MINOR）——目标节点 protoVer minor 低于
 * 该值的旧 studio 会忽略未知 team 字段按单 agent 跑（静默换班，裁定禁止）。hub
 * handleDelegate 按目标连接 peerProtoVer 权威门禁；studio 派发闸（提交时早失败）
 * 消费同一常量作预检提示。1.9.0 = minor 9。
 */
export const TEAM_DELEGATION_REQUIRED_MINOR = 9;

/**
 * 平行组草稿委派所需的最低协议 minor（1.10.0 §3.5）：draft 是载荷能力（非新 RPC
 * 方法，不入 METHOD_REQUIRED_MINOR）——peer 组建组/入组节点 minor 低于该值直接拒
 * （组内全员能力对齐），task.delegate 载荷带 draft 时 hub 按目标连接 peerProtoVer
 * 权威门禁（同 team 门机制）。1.10.0 = minor 10。
 */
export const PEER_DELEGATION_REQUIRED_MINOR = 10;

/**
 * 任务 HITL 生命周期 v2 所需的最低协议 minor（1.11.0 spec §8.5）：submitted 账本态/
 * report submitted 上报/干预类型族/TaskAnswerReq 结构化答复（interventionId+answer）均为
 * 载荷能力（非新 RPC 方法，不入 METHOD_REQUIRED_MINOR——四新方法各自登记 11）——低于该值
 * 的旧节点不识别 submitted 态与干预行语义，单方启用会留半套账本。hub 按对端连接
 * peerProtoVer 权威门禁；studio 派发闸（提交时早失败）消费同一常量作预检提示，任一侧
 * < 11 降级为旧行为（单方完成、仅写授权）。1.11.0 = minor 11。
 */
export const TASK_HITL_REQUIRED_MINOR = 11;

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
  /** 执行尝试号（1.11.0）：delegations.run_no 列投影（1 起，打回 +1）；缺省不带键
   *  （1.10 投影零变化——旧行 run_no 缺省即 1，消费方 ?? 1 兜底）。 */
  runNo?: number;
  /** 最近一次提交时刻（1.11.0）：worker report submitted 驱动落账；未提交不带键。 */
  submittedAt?: number;
  /** 验收通过时刻（1.11.0）：发起方 task.accept 驱动落账（终态 done 伴随）；未验收不带键。 */
  acceptedAt?: number;
}

// ────────────────────────────────────────────────────────────────────────────
// 干预类型族（1.11.0 任务生命周期 v2，spec 2026-11-13 §8.1/§8.2）——
// worker 侧人工请求（工具闸/提问/手动动作）经 task.intervene 上报 hub 落
// delegation_interventions 行，镜像给发起方代答；answer 走 task.answer 结构化
// 载荷按 interventionId 定位行（first_wins）。
// ────────────────────────────────────────────────────────────────────────────

/** 干预种类：tool_gate=工具调用闸（写/bash 等授权卡）、question=多选/自由文本提问、
 *  manual_action=人工线下动作确认（如重启服务后点完成）。验收（acceptance）为发起方
 *  本地请求，不经本类型族上报 hub（spec §8.1 表注）。 */
export type InterventionKind = 'tool_gate' | 'question' | 'manual_action';

/** 干预行生命周期：pending=待答；answered=已答（answer 段定格）；expired=超时；
 *  cancelled=任务取消级联；superseded=被同 run 后续请求取代/打回作废。 */
export type InterventionStatus = 'pending' | 'answered' | 'expired' | 'cancelled' | 'superseded';

/** 结构化答复（task.answer.answer / task.interventionAnswered.answer）：
 *  decision=tool_gate 批准/拒绝；text=自由文本附言；selected=options[].id 多选；
 *  completed=manual_action 完成确认。全可选——按 kind 取用，hub 不校验组合
 *  （消费端 agent 按 kind 解释，宽松容忍）。 */
export interface InterventionAnswer {
  decision?: 'approve' | 'deny';
  text?: string;
  selected?: string[];
  completed?: boolean;
}

/** 干预请求（worker → hub task.intervene 载荷 / task.intervention 事件透传）：
 *  interventionId 由 worker 单点生成（本地 human_requests.id）；runNo 锚定执行
 *  尝试（打回后旧请求作废）；payload 为 kind 专属上下文（如工具名/目标路径）；
 *  options 为 question 可选项清单；deadlineAt 为超时时刻（缺省不超时）。 */
export interface InterventionRequest {
  interventionId: string;
  runNo: number;
  kind: InterventionKind;
  title: string;
  prompt: string;
  payload?: Record<string, unknown>;
  options?: Array<{ id: string; label: string; description?: string }>;
  deadlineAt?: number;
}

/** 干预账本行（delegation_interventions 投影 / task.interventions 对账响应项）：
 *  request 段 + 状态与答复留痕；answer/answeredBy/answeredAt 仅 status=answered
 *  时在场（pending 行不带键）。 */
export interface InterventionRecord extends InterventionRequest {
  status: InterventionStatus;
  answer?: InterventionAnswer;
  answeredBy?: string;
  answeredAt?: number;
  createdAt: number;
}

/** task.intervene 请求（worker → hub，门槛 minor 11）：载荷 intervention 为 worker
 *  本地 human_requests 行镜像（interventionId 单点生成）；hub 校验来源 = 目标节点、
 *  账本非终态后幂等落 delegation_interventions 行（id=interventionId 主键）并按组
 *  模式定向通知发起方（lf → task.intervention 事件；peer / 发起方低版本 → 中性
 *  进度，内容零泄漏，spec 2026-11-13 §8.6）。 */
export interface TaskInterveneReq {
  taskId: string;
  intervention: InterventionRequest;
}

/** task.intervene 响应：幂等落行（同 interventionId 重发返回既有行，不重复建行）
 *  后返 interventionId。 */
export interface TaskInterveneRes { interventionId: string }

/** task.answer 请求（M3.1 HITL L2 远程代答，leader-only，门槛 minor 3；1.11.0 扩
 *  为双向结构化答复，门槛 minor 11——TASK_HITL_REQUIRED_MINOR）。
 *  旧形态（interventionId 缺席）：decision 写授权语义原样保留（1.10 兼容，不触
 *  干预行）——message 仅审计与 leader 侧展示（≤512 字符，hub 校验），不注入子进程；
 *  approve 走 covered 轮询原地重执行、deny 走标准拒绝文本，两条既有路径均无注入点。
 *  新形态（interventionId 在场）：定位 delegation_interventions 行（不存在 →
 *  intervention_not_found；非 pending → intervention_stale），answer 按 kind 解释，
 *  权威校验与 first_wins 收敛由 hub 把守（spec 2026-11-13 §8.6）。 */
export interface TaskAnswerReq {
  taskId: string;
  decision: 'approve' | 'deny';
  message?: string;
  /** 定位干预行（1.11.0）：缺席 = 旧写授权语义；在场 = 结构化答复。 */
  interventionId?: string;
  /** 结构化答复体（1.11.0）：仅 interventionId 在场时消费；与 answer 段内 decision
   *  同名不同层——顶层 decision 保留旧兼容（approve/deny），段内按 kind 取值。 */
  answer?: InterventionAnswer;
}

/** task.answer 响应：granted=已批准 / denied=已拒绝 / stale=worker 侧已无等待
 *  （重复代答幂等口径——决策列仍记最新）；answered=结构化答复落行成功（1.11.0，
 *  interventionId 在场路径——行 answered + 对端 interventionAnswered 事件）。 */
export interface TaskAnswerRes { disposition: 'granted' | 'denied' | 'stale' | 'answered' }

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

/** task.report 载荷：status 五值；hub 按 id=taskId 对账、canTransition 单点
 *  校验后更新账本，并按 status 转发对应 EVT 定向给 leader——
 *  running→task.progress、done→task.completed、failed→task.failed{reason}。
 *  'pending'（1.10.0 平行模式）= 生命周期拍专用值：不改账本状态、不回填
 *  sessionId，仅转发 progress 给委派发起方（承载草稿 received/arranged 拍）；
 *  终态账本收到 pending 拍静默忽略。
 *  'submitted'（1.11.0）= run 完成待验收：账本 running|waiting→submitted 迁移 +
 *  submitted_at 落账，转发 task.submitted 给发起方；runNo 透传自载荷（缺省沿用
 *  账本行值，1.10 上报形态零变化）。interrupted / cancelled 不经上报（同既有口径）。 */
export interface TaskReportPayload {
  status: 'running' | 'done' | 'failed' | 'pending' | 'submitted';
  /** 委派会话 id（MC1 独立委派会话——首次 running 上报建立，hub 回填账本 session_id）。 */
  sessionId?: string;
  /** failed 附因（约定值：'declined'=目标拒派；'local_session_lost'=续传时本地
   *  会话已丢失；其余自由文本）；done 不携带。 */
  reason?: string;
  /** 进度段（running 上报可携带；终态转发不消费该段）。 */
  progress?: TaskReportProgress;
  /** 执行尝试号（1.11.0，仅 submitted 上报携带）：打回后新 run 提交时必带；
   *  缺省沿用账本行 run_no（1.10 上报零变化）。 */
  runNo?: number;
}

/** 进度拍：委派会话 message_events 尾部摘要（excerpt 为截断文本）。
 *  kind 可选标注（M2.1 T11b，0.1.4）：'waiting-input' = 目标侧 agent 正等待
 *  用户确认（写授权卡等）——leader 看板据此切「等待节点确认」态。hub 侧白名单
 *  + 枚举校验，非法值剥除不抛（先例 permissionTier/modelBinding）。
 *  1.10.0 平行模式扩两值：'received' = 草稿已落接收方看板；'arranged' = 接收方
 *  已指派 agent/团队——随 status:'pending' 拍携带，发起方看板行生命周期展示。 */
export interface TaskReportProgress {
  lastSeq: number;
  excerpt: string;
  kind?: 'waiting-input' | 'received' | 'arranged';
}
