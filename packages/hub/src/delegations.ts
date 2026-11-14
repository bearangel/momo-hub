// hub 委派账本与 task.* RPC（M2d spec 2026-10-24 §3.1「委派」/§3.2 事件/§4
// delegations 表/§6 + T9；M3.1 waiting 态与 task.answer 远程代答，spec
// 2026-10-28 §3.1/§3.2）——状态机单点校验（Review Focus #3）/定向转发/
// 中断语义（MC5）/审计日志（§6 审计链）/远程代答（leader→worker 定向 RPC）。
//
// ▲ wire 约定（单源在协议包 src/delegation.ts 的 TASK_WIRE/TaskReportPayload，
// 本注释为 hub 侧执行语义）：目标节点收到 task.assigned 事件后，状态上报统一用
// res 信封——method='task.report'，id=taskId（对账键），ok=true，payload=
// {status:'running'|'done'|'failed', sessionId?, reason?, progress?{lastSeq, excerpt}}。
// hub 按 id=taskId 对账：载荷校验 → 来源必须是该任务的目标节点 → canTransition
// 单点校验 → 账本更新（status + session_id 回填）→ 按 status 转发对应 EVT 定向
// 给 leader：running→task.progress（直转——500ms 合并节流由目标侧负责，hub 不
// 缓存）、done→task.completed、failed→task.failed{reason}。同 running 态的重复
// 上报视为进度拍（非状态迁移）：账本 status 不动、sessionId 照常回填、
// task.progress 照转。非法迁移 / 未知 taskId / 非目标节点来源 / 异形载荷 /
// ok=false → 拒绝该上报（账本不动 + 日志），不炸连接（res 信封无请求方，本就是
// 单向上报，无错误回传通道）。interrupted / cancelled 不经 task.report：前者由
// hub 在节点离线时主动迁移（onNodeOffline），后者由 leader 经 task.cancel 触发。
//
// ▲ 两裁定（task-9 brief）：① 离线拒派——目标离线时 task.delegate 即返
// node_offline 且不落档（leader 即时改派，不留幽灵 pending）；② 终态取消返
// conflict——done/failed/cancelled 态的 task.cancel 返 conflict 错误（leader UI
// 显真实态），不做幂等 ok。
import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  EVT, METHOD_REQUIRED_MINOR, PEER_DELEGATION_REQUIRED_MINOR, RPC, RPC_ERROR_CODES,
  TASK_HITL_REQUIRED_MINOR, TEAM_DELEGATION_REQUIRED_MINOR, minorOf,
  type DelegationStatus, type DelegationTask, type DelegationTeamMember, type EventEnvelope,
  type InterventionAnswer, type InterventionKind, type InterventionRequest, type InterventionStatus,
  type ResponseEnvelope, type RpcErrorCode, type RpcErrorPayload, type TaskAnsweredEvent,
  type TaskAssignedEvent, type TaskAcceptedEvent, type TaskInterventionAnsweredEvent,
  type TaskInterventionEvent, type TaskProgressEvent, type TaskRecord,
  type TaskRejectedEvent, type TaskReportPayload, type TaskReportProgress, type TaskTerminalEvent,
} from 'momo-hub-protocol';
import type { NodeRegistry } from './registry';
import type { GroupHandlerContext, RpcOutcome } from './groups';
import { logger } from './logger';

// ────────────────────────────────────────────────────────────────────────────
// 状态机（hub 单点校验——Review Focus #3）
// ────────────────────────────────────────────────────────────────────────────

/**
 * 合法迁移表（§4 状态机 + MC5 + brief 矩阵 + M3.1 waiting + 1.11.0 submitted，
 * spec 2026-11-13 §8.1）：
 * - pending → running / failed('declined' 目标拒派) / cancelled / interrupted（hub 离线迁移）
 * - running → waiting（M3.1，kind='waiting-input' 上报触发）/ submitted（1.11.0
 *   run 完成待验收）/ done / failed / cancelled / interrupted
 * - waiting → running（无 kind 上报=等待解除）/ submitted / done / failed / cancelled / interrupted
 * - submitted → done（task.accept 验收）/ running（task.reject 打回，新 run）/
 *   cancelled / failed（run 失败结算，§8.3 映射）
 * - interrupted → running（重连续传）/ failed('local_session_lost') / cancelled
 * - done / failed / cancelled 终态，拒一切。
 * 全部迁移（task.cancel / task.report 上报 / onNodeOffline 自动迁移）必经
 * canTransition 单点——目标节点的上报必须匹配账本当前态。waiting↔running 由
 * acceptReport 特殊分支处理（独立审计行），不走通用迁移路径；task.answer 刻意
 * 不迁移状态（状态单源：worker 上报驱动，代答只写决策列）。submitted 的进入由
 * worker task.report(status='submitted') 驱动（解析器白名单 1.11.0 扩列后生效），
 * 出边由 task.accept / task.reject RPC 结算——两条 RPC 本 task（H3）只接线状态机，
 * RPC 端点由干预任务接入。
 */
const DELEGATION_TRANSITIONS: Record<DelegationStatus, readonly DelegationStatus[]> = {
  pending: ['running', 'failed', 'cancelled', 'interrupted'],
  running: ['waiting', 'submitted', 'done', 'failed', 'cancelled', 'interrupted'],
  waiting: ['running', 'submitted', 'done', 'failed', 'cancelled', 'interrupted'],
  submitted: ['done', 'running', 'cancelled', 'failed'],
  interrupted: ['running', 'failed', 'cancelled'],
  done: [],
  failed: [],
  cancelled: [],
};

/** 状态机单点校验（纯函数）：合法迁移对查表；自迁移一律 false——running 进度拍
 *  由 acceptReport 单独处理，不伪装成迁移。 */
export function canTransition(from: DelegationStatus, to: DelegationStatus): boolean {
  return DELEGATION_TRANSITIONS[from].includes(to);
}

/** 状态枚举全集（task.list 过滤校验 + 矩阵测试遍历用；1.11.0 增 submitted）。 */
export const DELEGATION_STATUSES: readonly DelegationStatus[] = [
  'pending', 'running', 'waiting', 'submitted', 'done', 'failed', 'interrupted', 'cancelled',
];

// ────────────────────────────────────────────────────────────────────────────
// 载荷校验（S1 同款纪律：字段类型/长度/DoS 下限；未知字段容忍）
// ────────────────────────────────────────────────────────────────────────────

const WORKSPACE_NAME_MAX = 128;   // S1 NAME_MAX（mirror 同款）
const TITLE_MAX = 128;            // STR_LIST_ITEM_MAX 同量级
const PROMPT_MAX_BYTES = 16 * 1024; // 任务书正文（自然语言，给足但封顶防灌库）
const SESSION_ID_MAX = 128;
const REASON_MAX = 512;
const EXCERPT_MAX = 512;
const SLUG_MAX = 64;              // S1 SLUG_MAX
const ANSWER_MESSAGE_MAX = 512;   // 代答备注（TaskAnswerReq.message 同界）
const DEPENDS_ON_MAX = 50;        // 依赖 id 清单上限（M4.3，DoS 下限）
const DEPENDS_ON_ITEM_MAX = 128;  // 依赖 id 单项上限（STR_LIST_ITEM_MAX 同量级）
const TEAM_MEMBERS_MIN = 2;       // 团队成员下限（M5.3 spec §5.1：单 agent 走 agentSlug/缺省）
const TEAM_MEMBERS_MAX = 8;       // 团队成员上限（DoS 下限 + 快照团队形态同量级）
const TEAM_MEMBER_NAME_MAX = 64;  // 成员名上限（节点名/组名同款约束，name=agent 定义名）

/** permissionTier 合法档位（M2.1 T10b，与协议包 DelegationTask.permissionTier 枚举同源）。 */
const PERMISSION_TIERS: readonly DelegationTask['permissionTier'][] = ['readonly', 'sandbox', 'confirm'];

/** progress.kind 合法标注（M2.1 T11b + 1.10.0 平行模式两值，与协议包 TaskReportProgress.kind 同源）。 */
const PROGRESS_KINDS: readonly NonNullable<TaskReportProgress['kind']>[] = ['waiting-input', 'received', 'arranged'];

/** 草稿优先级界（1.10.0）：与 studio tasks.priority 同量纲 0..10。 */
const DRAFT_PRIORITY_MIN = 0;
const DRAFT_PRIORITY_MAX = 10;

const err = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });
const textEncoder = new TextEncoder();

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function reqStr(payload: unknown, key: string): string | null {
  const v = isRecord(payload) ? payload[key] : undefined;
  return typeof v === 'string' && v.length > 0 ? v : null;
}

function optBoundedStr(raw: unknown, max: number): string | null {
  return typeof raw === 'string' && raw.length > 0 && raw.length <= max ? raw : null;
}

/** team 段校验（M5.3 spec §5.1）：members 2..8 项、每项 {name 1..64, isLeader:
 *  boolean}、恰好 1 项 isLeader=true。白名单重建（未知字段剥除）；任何已知约束
 *  不满足返回 null（由 parseDelegationTask 整包拒——沿 dependsOn 语义）。 */
function parseTeamSection(raw: unknown): DelegationTask['team'] | null {
  if (!isRecord(raw) || !Array.isArray(raw.members)) return null;
  const members = raw.members;
  if (members.length < TEAM_MEMBERS_MIN || members.length > TEAM_MEMBERS_MAX) return null;
  const out: DelegationTeamMember[] = [];
  let leaders = 0;
  for (const m of members) {
    if (!isRecord(m)) return null;
    const name = optBoundedStr(m.name, TEAM_MEMBER_NAME_MAX);
    if (name === null) return null;
    if (typeof m.isLeader !== 'boolean') return null;
    if (m.isLeader) leaders++;
    out.push({ name, isLeader: m.isLeader });
  }
  if (leaders !== 1) return null;
  return { members: out };
}

/** 任务书校验：title 1..128、prompt 非空且 ≤16KB（字节）、agentSlug 可选 ≤64、
 *  dependsOn 可选 string 数组 ≤50 项且每项 ≤128 字符（M4.3，非法整包拒）；
 *  team 可选（M5.3：与 agentSlug 互斥，非法整包拒——team 是派发核心语义，静默
 *  剥除会让接收端误解为单 agent）。 */
function parseDelegationTask(raw: unknown): DelegationTask | null {
  if (!isRecord(raw)) return null;
  const title = optBoundedStr(raw.title, TITLE_MAX);
  if (title === null) return null;
  if (typeof raw.prompt !== 'string' || raw.prompt.length === 0
    || textEncoder.encode(raw.prompt).length > PROMPT_MAX_BYTES) return null;
  const task: DelegationTask = { title, prompt: raw.prompt };
  if (raw.agentSlug !== undefined) {
    const slug = optBoundedStr(raw.agentSlug, SLUG_MAX);
    if (slug === null) return null;
    task.agentSlug = slug;
  }
  // permissionTier（M2.1 T10b）：三值枚举校验，非法值剥除不抛（未知容忍先例
  // modelBinding——档位缺省语义在目标侧，hub 不因坏档位拒整张任务书）
  if (raw.permissionTier !== undefined && (PERMISSION_TIERS as readonly string[]).includes(raw.permissionTier as string)) {
    task.permissionTier = raw.permissionTier as DelegationTask['permissionTier'];
  }
  // dependsOn（M4.3）：形状校验 string 数组 + 上限，非法整包拒（沿 manifest
  // version? 三格模式：缺省不带键 / 在场保留 / 非法拒收——与 permissionTier
  // 的剥除语义不同：依赖清单是 leader 侧闸的数据依据，静默剥除会让 leader 误判
  // 透传成功）。透传不校验存在性——leader 本地任务 id 对 hub 无意义（语义裁定
  // spec 2026-10-29 §2.1，双仓注释锚定）；随 task.assigned 事件过不持久化。
  if (raw.dependsOn !== undefined) {
    if (!Array.isArray(raw.dependsOn) || raw.dependsOn.length > DEPENDS_ON_MAX
      || !raw.dependsOn.every((s) => typeof s === 'string' && s.length <= DEPENDS_ON_ITEM_MAX)) return null;
    task.dependsOn = [...raw.dependsOn];
  }
  // team（M5.3）：非法整包拒（沿 dependsOn 三格模式：缺省不带键 / 在场保留 /
  //  非法拒收——静默剥除会让接收端误解为单 agent 跑，正是版本门要防的静默换班）；
  //  与 agentSlug 互斥（单 agent 形态与团队形态二选一）；随 task.assigned 事件过
  //  不持久化（与 agentSlug/dependsOn 同生命周期——账本只存 taskId/title/prompt）。
  if (raw.team !== undefined) {
    if (task.agentSlug !== undefined) return null;
    const team = parseTeamSection(raw.team);
    if (team === null) return null;
    task.team = team;
  }
  // draft（1.10.0 平行模式）：字面 true 才合法（防 truthy 泛化）；在场时执行编排
  // 四键白名单互斥——任一键在场整包拒（深度防御：防异构客户端 smuggle 完整编排
  // 进平行组，静默换班禁令的 hub 侧锚点）；priority 仅 draft 在场时合法（0..10 整数）。
  if (raw.draft !== undefined) {
    if (raw.draft !== true) return null;
    if (task.agentSlug !== undefined || task.team !== undefined
      || task.dependsOn !== undefined || task.permissionTier !== undefined) return null;
    task.draft = true;
    if (raw.priority !== undefined) {
      if (typeof raw.priority !== 'number' || !Number.isInteger(raw.priority)
        || raw.priority < DRAFT_PRIORITY_MIN || raw.priority > DRAFT_PRIORITY_MAX) return null;
      task.priority = raw.priority;
    }
  } else if (raw.priority !== undefined) {
    return null;
  }
  return task;
}

/** task.report 载荷校验：status 四值枚举（pending=1.10.0 生命周期拍值——仅转
 *  progress 不迁移状态；interrupted/cancelled 不是上报值）、sessionId/reason 有界串、
 *  progress 形态（lastSeq 非负整数 + excerpt 有界串——RM2-7：excerpt="" 视同缺省
 *  受理，转发时以占位呈现，lastSeq 不丢）。 */
function parseReportPayload(raw: unknown): TaskReportPayload | null {
  if (!isRecord(raw)) return null;
  if (raw.status !== 'running' && raw.status !== 'done' && raw.status !== 'failed' && raw.status !== 'pending') return null;
  const out: TaskReportPayload = { status: raw.status };
  if (raw.sessionId !== undefined) {
    const sessionId = optBoundedStr(raw.sessionId, SESSION_ID_MAX);
    if (sessionId === null) return null;
    out.sessionId = sessionId;
  }
  if (raw.reason !== undefined) {
    const reason = optBoundedStr(raw.reason, REASON_MAX);
    if (reason === null) return null;
    out.reason = reason;
  }
  if (raw.progress !== undefined) {
    const p = raw.progress;
    if (!isRecord(p)) return null;
    const lastSeq = p.lastSeq;
    if (typeof lastSeq !== 'number' || !Number.isInteger(lastSeq) || lastSeq < 0) return null;
    const excerpt = typeof p.excerpt === 'string' && p.excerpt.length <= EXCERPT_MAX ? p.excerpt : null;
    if (excerpt === null) return null;
    out.progress = { lastSeq, excerpt };
    // kind（M2.1 T11b）：单值枚举校验，非法值剥除不抛（先例 permissionTier——
    // 等待标注的消费语义在 leader 侧，hub 不因坏标注拒整张进度拍）
    if (p.kind !== undefined && (PROGRESS_KINDS as readonly string[]).includes(p.kind as string)) {
      out.progress.kind = p.kind as TaskReportProgress['kind'];
    }
  }
  return out;
}

// ────────────────────────────────────────────────────────────────────────────
// 干预载荷校验（1.11.0 task.intervene——沿 S1 字段类型/长度纪律，白名单重建）
// ────────────────────────────────────────────────────────────────────────────

const INTERVENTION_ID_MAX = 128;    // interventionId（worker 本地 human_requests.id，UUID 量级）
const OPTION_STR_MAX = 128;         // options[].id/label（STR_LIST_ITEM_MAX 同量级）
const OPTION_DESC_MAX = 512;        // options[].description（EXCERPT_MAX 同量级）

/** kind 合法三枚举（与协议包 InterventionKind 同源）。 */
const INTERVENTION_KINDS: readonly InterventionKind[] = ['tool_gate', 'question', 'manual_action'];

/** 干预请求校验（spec 2026-11-13 §8.2 载荷表）：interventionId 1..128、runNo 正整数、
 *  kind 三枚举、title 1..128、prompt 字符串 ≤16KB（字节）、payload 纯对象可选、
 *  options 数组可选（每项 id/label 1..128、description? ≤512）、deadlineAt 正整数
 *  可选；未知字段剥除（白名单重建），任何已知约束不满足返回 null（整包拒）。 */
function parseIntervention(raw: unknown): InterventionRequest | null {
  if (!isRecord(raw)) return null;
  const interventionId = optBoundedStr(raw.interventionId, INTERVENTION_ID_MAX);
  if (interventionId === null) return null;
  if (typeof raw.runNo !== 'number' || !Number.isInteger(raw.runNo) || raw.runNo < 1) return null;
  if (!(INTERVENTION_KINDS as readonly string[]).includes(raw.kind as string)) return null;
  const title = optBoundedStr(raw.title, TITLE_MAX);
  if (title === null) return null;
  if (typeof raw.prompt !== 'string' || textEncoder.encode(raw.prompt).length > PROMPT_MAX_BYTES) return null;
  const out: InterventionRequest = {
    interventionId,
    runNo: raw.runNo,
    kind: raw.kind as InterventionKind,
    title,
    prompt: raw.prompt,
  };
  if (raw.payload !== undefined) {
    if (!isRecord(raw.payload)) return null;
    out.payload = raw.payload;
  }
  if (raw.options !== undefined) {
    if (!Array.isArray(raw.options)) return null;
    const options: NonNullable<InterventionRequest['options']> = [];
    for (const o of raw.options) {
      if (!isRecord(o)) return null;
      const id = optBoundedStr(o.id, OPTION_STR_MAX);
      const label = optBoundedStr(o.label, OPTION_STR_MAX);
      if (id === null || label === null) return null;
      const item: NonNullable<InterventionRequest['options']>[number] = { id, label };
      if (o.description !== undefined) {
        const description = optBoundedStr(o.description, OPTION_DESC_MAX);
        if (description === null) return null;
        item.description = description;
      }
      options.push(item);
    }
    out.options = options;
  }
  if (raw.deadlineAt !== undefined) {
    if (typeof raw.deadlineAt !== 'number' || !Number.isInteger(raw.deadlineAt) || raw.deadlineAt < 1) return null;
    out.deadlineAt = raw.deadlineAt;
  }
  return out;
}

/** 干预答复校验（1.11.0 task.answer 干预分支，spec §8.2）：answer 段白名单重建
 *  （decision 二枚举 / text 1..512 / selected 各项 1..128 / completed 布尔），未知
 *  字段剥除；顶层 decision 兼容映射（旧客户端只懂顶层字段）——answer.decision
 *  缺失且顶层在场时映射进段内。映射后至少一项在场，全空返 null（空答复对任何
 *  kind 都无意义 → bad_request）。组合不校验（协议口径：消费端 agent 按 kind 解释，
 *  宽松容忍）。 */
function parseInterventionAnswer(payload: Record<string, unknown>): InterventionAnswer | null {
  let answer: InterventionAnswer = {};
  if (payload.answer !== undefined) {
    if (!isRecord(payload.answer)) return null;
    const a = payload.answer;
    if (a.decision !== undefined) {
      if (a.decision !== 'approve' && a.decision !== 'deny') return null;
      answer.decision = a.decision;
    }
    if (a.text !== undefined) {
      const text = optBoundedStr(a.text, ANSWER_MESSAGE_MAX);
      if (text === null) return null;
      answer.text = text;
    }
    if (a.selected !== undefined) {
      if (!Array.isArray(a.selected)
        || !a.selected.every((s) => typeof s === 'string' && s.length > 0 && s.length <= OPTION_STR_MAX)) return null;
      answer.selected = [...a.selected];
    }
    if (a.completed !== undefined) {
      if (typeof a.completed !== 'boolean') return null;
      answer.completed = a.completed;
    }
  }
  if (answer.decision === undefined && payload.decision !== undefined) {
    if (payload.decision !== 'approve' && payload.decision !== 'deny') return null;
    answer = { ...answer, decision: payload.decision };
  }
  if (answer.decision === undefined && answer.text === undefined
    && answer.selected === undefined && answer.completed === undefined) return null;
  return answer;
}

// ────────────────────────────────────────────────────────────────────────────
// 账本行
// ────────────────────────────────────────────────────────────────────────────

interface DelegationRow {
  task_id: string;
  group_id: string;
  workspace_name: string;
  target_node_id: string;
  by_node_id: string;
  title: string;
  status: DelegationStatus;
  session_id: string | null;
  created_at: number;
  updated_at: number;
  // 决策四列（M3.1 db v6）：NULL = 未代答；answer_decision 只由 handleAnswer 写入
  // （'approve'|'deny' 两值，库层 TEXT 不设约束与 status 同取舍）。
  answered_by: string | null;
  answer_decision: string | null;
  answer_message: string | null;
  answered_at: number | null;
  // 生命周期三列（1.11.0 db v10）：run_no NULL = 第 1 次执行（投影 ?? 1）；
  // submitted_at/accepted_at NULL = 未提交/未验收。
  run_no: number | null;
  submitted_at: number | null;
  accepted_at: number | null;
}

function toRecord(r: DelegationRow): TaskRecord {
  return {
    taskId: r.task_id,
    // 组归属（M4.2）：group_id 既有列直接投影——零迁移，leader 委派板按组聚合的依据
    groupId: r.group_id,
    workspaceName: r.workspace_name,
    targetNodeId: r.target_node_id,
    status: r.status,
    title: r.title,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    answeredBy: r.answered_by ?? null,
    answerDecision: (r.answer_decision ?? null) as TaskRecord['answerDecision'],
    answerMessage: r.answer_message ?? null,
    answeredAt: r.answered_at ?? null,
    // 执行尝试号（1.11.0）：NULL = 第 1 次执行 → 1；提交/验收时刻可空直传
    // （?? undefined——wire 序列化丢 undefined 键，未提交/未验收不带键）
    runNo: r.run_no ?? 1,
    submittedAt: r.submitted_at ?? undefined,
    acceptedAt: r.accepted_at ?? undefined,
  };
}

// ────────────────────────────────────────────────────────────────────────────
// 委派路由器
// ────────────────────────────────────────────────────────────────────────────

/** 代答转发在途超时（worker 挂起不答的兜底结算——不设则 leader 请求永久悬挂）。 */
const ANSWER_FORWARD_TIMEOUT_MS = 10_000;

/** 构造选项（代答转发超时测试注入——server.ts answerForwardTimeoutMs 透传）。 */
export interface DelegationRouterOpts {
  answerTimeoutMs?: number;
}

interface PendingAnswer {
  id: string;
  taskId: string;
  targetNodeId: string;
  byNodeId: string;
  settle: (outcome: RpcOutcome) => void;
  timer: NodeJS.Timeout;
}

export class DelegationRouter {
  /** 代答转发挂起表：转发 id → 在途请求（worker res 按 id 对账，view 回执同模式）。 */
  private answerPendingById = new Map<string, PendingAnswer>();
  private readonly answerTimeoutMs: number;

  constructor(
    private readonly db: Database.Database,
    private readonly registry: NodeRegistry,
    opts: DelegationRouterOpts = {},
  ) {
    this.answerTimeoutMs = opts.answerTimeoutMs ?? ANSWER_FORWARD_TIMEOUT_MS;
  }

  private getTask(taskId: string): DelegationRow | undefined {
    return this.db.prepare('SELECT * FROM delegations WHERE task_id = ?').get(taskId) as DelegationRow | undefined;
  }

  /**
   * task.delegate RPC（leader-only 已由 dispatcher 外围守卫校验——提取器取 groupId）。
   * 校验序：字段形态 → 组存在（守卫已验 leader，此处兜底）→ target 同组 →
   * 账本先行落档 pending（§4 账本先行纪律）→ 定向 task.assigned →
   * 离线即回滚落档并返 node_offline（裁定①：离线拒派，不留幽灵 pending）。
   */
  async handleDelegate(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const groupId = reqStr(payload, 'groupId');
    const targetNodeId = reqStr(payload, 'targetNodeId');
    const workspaceName = reqStr(payload, 'workspaceName');
    if (!groupId || !targetNodeId || !workspaceName) {
      return { ok: false, error: err('bad_request', 'groupId/targetNodeId/workspaceName 缺失') };
    }
    if (workspaceName.length > WORKSPACE_NAME_MAX) {
      return { ok: false, error: err('bad_request', `workspaceName 超长（≤${WORKSPACE_NAME_MAX}）`) };
    }
    const task = parseDelegationTask(isRecord(payload) ? payload.task : undefined);
    if (!task) return { ok: false, error: err('bad_request', 'task 非法（title 1-128 / prompt 非空 ≤16KB / agentSlug? ≤64 / dependsOn? ≤50 项且每项 ≤128 / team? members 2-8 项〔name 1-64、isLeader 布尔且恰 1 个 true、与 agentSlug 互斥〕）') };

    if (!this.db.prepare('SELECT 1 FROM groups WHERE id = ?').get(groupId)) {
      return { ok: false, error: err('not_found', '工作组不存在') };
    }
    if (!this.db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?').get(groupId, targetNodeId)) {
      return { ok: false, error: err('forbidden', '目标节点不是本组成员') };
    }
    // 1.10.0 组模式分流（spec 2026-11-05 §3.2——守卫层已按模式放行 peer 调用）：
    //  peer 组：调用方须为成员（lf 组守卫已验 leader）+ 任务书必须 draft + 目标
    //  已绑定承接空间 + 目标 minor ≥ 10（draft 能力门）。
    //  lf 组：draft 任务书整包拒（模式纯净——编排委派与草稿委派不混流）。
    const groupMode = (this.db.prepare('SELECT mode FROM groups WHERE id = ?').get(groupId) as
      | { mode: 'leader-follower' | 'peer' }
      | undefined)?.mode ?? 'leader-follower';
    if (groupMode === 'peer') {
      if (!this.db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?').get(groupId, ctx.nodeId)) {
        return { ok: false, error: err('forbidden', '仅本组成员可委派任务') };
      }
      if (task.draft !== true) {
        return { ok: false, error: err('bad_request', '平行组仅接受草稿委派（task.draft=true，不含 agent/团队编排）') };
      }
      const binding = this.db.prepare('SELECT 1 FROM group_bindings WHERE group_id = ? AND node_id = ?')
        .get(groupId, targetNodeId);
      if (!binding) {
        return { ok: false, error: err('forbidden', '目标节点未绑定承接空间，无法接收草稿') };
      }
    } else if (task.draft === true) {
      return { ok: false, error: err('bad_request', '主从组不接受草稿委派（draft 仅平行组合法）') };
    }
    // M5.3 团队派发版本门（防旧 studio 静默降级——旧节点忽略未知 team 字段按单
    // agent 跑，正是裁定禁止的静默换班）：任务带 team 时目标连接 minor < 9 即拒
    // （不落档）。指定节点与组内调度在 hub 侧同门——组内 least-busy 选点在 leader
    // studio（pickWorker），到达 hub 的载荷一律是显式 targetNodeId，本门即两种
    // 路由形态的权威兜底。目标离线（registry 查无）不在本门处理——走下方既有
    // node_offline 路径（离线语义优先，先落档再回滚不留档）。
    if (task.team !== undefined) {
      const conn = this.registry.get(targetNodeId);
      if (conn) {
        const peerMinor = minorOf(conn.peerProtoVer);
        if (peerMinor === null || peerMinor < TEAM_DELEGATION_REQUIRED_MINOR) {
          logger.info('委派被拒：目标节点协议过旧，不支持团队派发', {
            targetNodeId, peerProtoVer: conn.peerProtoVer, byNode: ctx.nodeId,
          });
          return {
            ok: false,
            error: err('node_unsupported', `目标节点 ${conn.nodeName} 协议过旧（${conn.peerProtoVer}，需 1.${TEAM_DELEGATION_REQUIRED_MINOR}.0 及以上），不支持团队派发`),
          };
        }
      }
    }
    // 1.10.0 草稿版本门（同 team 门机制——旧节点收到 draft 字段会按普通任务书自动
    // 执行，正是平行模式要防的「静默换班」：草稿必须由懂 draft 语义的节点落板）。
    if (task.draft === true) {
      const conn = this.registry.get(targetNodeId);
      if (conn) {
        const peerMinor = minorOf(conn.peerProtoVer);
        if (peerMinor === null || peerMinor < PEER_DELEGATION_REQUIRED_MINOR) {
          logger.info('委派被拒：目标节点协议过旧，不支持草稿委派', {
            targetNodeId, peerProtoVer: conn.peerProtoVer, byNode: ctx.nodeId,
          });
          return {
            ok: false,
            error: err('node_unsupported', `目标节点 ${conn.nodeName} 协议过旧（${conn.peerProtoVer}，需 1.${PEER_DELEGATION_REQUIRED_MINOR}.0 及以上），不支持草稿委派`),
          };
        }
      }
    }

    const taskId = randomUUID();
    const now = Date.now();
    // 账本先行：INSERT → sendTo；离线则 DELETE 回滚（同 mirror 在途登记回滚纪律）
    this.db.prepare(`INSERT INTO delegations
      (task_id, group_id, workspace_name, target_node_id, by_node_id, title, status, session_id, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)`)
      .run(taskId, groupId, workspaceName, targetNodeId, ctx.nodeId, task.title, now, now);
    const evt: EventEnvelope = {
      v: 1, kind: 'evt', method: EVT.taskAssigned,
      payload: { taskId, workspaceName, task, byNode: ctx.nodeId, groupId } satisfies TaskAssignedEvent,
    };
    const sent = await this.registry.sendTo(targetNodeId, evt);
    if (sent === 'offline') {
      this.db.prepare('DELETE FROM delegations WHERE task_id = ?').run(taskId);
      logger.info('委派被拒：目标离线', { taskId, groupId, targetNodeId, byNode: ctx.nodeId });
      return { ok: false, error: err('node_offline', '目标节点不在线，委派被拒（离线拒派）') };
    }
    logger.info('委派已受理', { taskId, groupId, targetNodeId, workspaceName, byNode: ctx.nodeId });
    return { ok: true, payload: { taskId } };
  }

  /**
   * task.cancel RPC（leader-only——守卫提取器按 taskId 查 delegations 回填 groupId）。
   * 当前态可取消（pending/running/interrupted，经 canTransition 单点判定）→
   * 账本 cancelled + 定向 task.cancelled 通知目标停跑（T11 接；目标离线时通知
   * 静默丢弃——账本已是事实，目标重连后对账）。终态 → conflict（裁定②：leader
   * UI 显真实态，不做幂等 ok）。未知 taskId → not_found（提取器返 null 放行至此）。
   */
  async handleCancel(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const taskId = reqStr(payload, 'taskId');
    if (!taskId) return { ok: false, error: err('bad_request', 'taskId 缺失') };
    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    // 1.10.0 平行组分流（守卫层已按模式放行 peer 调用）：发起者（by_node_id）可
    // 取消自己发出的草稿；lf 组守卫层已验 leader（发起者恒为 leader，语义等价）。
    const groupMode = (this.db.prepare('SELECT mode FROM groups WHERE id = ?').get(row.group_id) as
      | { mode: 'leader-follower' | 'peer' }
      | undefined)?.mode ?? 'leader-follower';
    if (groupMode === 'peer' && row.by_node_id !== ctx.nodeId) {
      return { ok: false, error: err('forbidden', '平行组仅委派发起者可取消该任务') };
    }
    if (!canTransition(row.status, 'cancelled')) {
      return { ok: false, error: err('conflict', `任务已处于终态 ${row.status}，不可取消`) };
    }
    const from = row.status;
    this.db.prepare("UPDATE delegations SET status = 'cancelled', updated_at = ? WHERE task_id = ?")
      .run(Date.now(), taskId);
    logger.info('委派已取消', { taskId, from, byNode: ctx.nodeId });
    const sent = await this.registry.sendTo(row.target_node_id, {
      v: 1, kind: 'evt', method: EVT.taskCancelled,
      payload: { taskId } satisfies TaskTerminalEvent,
    });
    if (sent === 'offline') logger.info('取消通知未送达（目标离线）', { taskId });
    return { ok: true, payload: { ok: true } };
  }

  /**
   * task.answer RPC（M3.1 HITL L2 远程代答，leader-only——守卫提取器按 taskId 反查
   * groupId，校验一）。流程（spec §3.2 六步）：载荷校验 → getTask/not_found →
   * 非 waiting/not_waiting → handler 内重查组与 leader 复核（remote-view 双校验
   * 先例，校验二）→ worker 在线/node_offline → worker minor 门槛/node_unsupported →
   * 定向转发 task.answer req（per-forward 超时可注入）→ worker res ok 则写决策四列
   * （状态不动——waiting↔running 单由 worker 上报驱动）+ 审计 + task.answered 定向
   * leader。重复 answer 不拒：worker 无 pending 自报 stale，决策列记最新。
   *
   * 1.11.0 结构化答复分流（spec §8.2/§8.6）：载荷带 interventionId → 走
   * answerIntervention 干预分支（守卫层对干预载荷放行——见 groups.ts 提取器注），
   * 不进本链；interventionId 在场但非法同样在分支内拒 bad_request——不静默回落
   * 旧路径（decision 语义两分支不同，半套受理比报错更糟）。缺席 → 下方旧
   * approve/deny 写授权链逐行不动（1.10 兼容，回归锁）。
   */
  async handleAnswer(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const taskId = reqStr(payload, 'taskId');
    if (!taskId) return { ok: false, error: err('bad_request', 'taskId 缺失') };
    if (isRecord(payload) && payload.interventionId !== undefined) {
      return this.answerIntervention(ctx, taskId, payload);
    }
    const decision = isRecord(payload) ? payload.decision : undefined;
    if (decision !== 'approve' && decision !== 'deny') {
      return { ok: false, error: err('bad_request', 'decision 非法（approve|deny）') };
    }
    let message: string | null = null;
    if (isRecord(payload) && payload.message !== undefined) {
      message = optBoundedStr(payload.message, ANSWER_MESSAGE_MAX);
      if (message === null) {
        return { ok: false, error: err('bad_request', `message 非法（1-${ANSWER_MESSAGE_MAX} 字符）`) };
      }
    }

    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    if (row.status !== 'waiting') {
      return { ok: false, error: err('not_waiting', `任务当前状态为 ${row.status}，非等待态不可代答`) };
    }
    // 校验二：守卫层通过后组关系可能已变（transfer 竞态），以账本 group_id 重查
    const group = this.db.prepare('SELECT leader_node_id FROM groups WHERE id = ?')
      .get(row.group_id) as { leader_node_id: string } | undefined;
    if (!group || group.leader_node_id !== ctx.nodeId) {
      return { ok: false, error: err('forbidden', '仅组长（leader）可代答') };
    }

    const conn = this.registry.get(row.target_node_id);
    if (!conn) return { ok: false, error: err('node_offline', '目标节点不在线，无法代答') };
    const requiredMinor = METHOD_REQUIRED_MINOR[RPC.task.answer] ?? 2;
    const workerMinor = minorOf(conn.peerProtoVer);
    if (workerMinor === null || workerMinor < requiredMinor) {
      return {
        ok: false,
        error: err('node_unsupported', `目标节点协议版本 ${conn.peerProtoVer} 过低（需 1.${requiredMinor} 及以上），无法远程代答`),
      };
    }

    const forwardPayload: Record<string, unknown> = { taskId, decision };
    if (message !== null) forwardPayload.message = message;
    const outcome = await this.forwardAnswer(row, ctx.nodeId, forwardPayload);
    if (!outcome.ok) return outcome;

    const disposition = (outcome.payload as { disposition: 'granted' | 'denied' | 'stale' }).disposition;
    const now = Date.now();
    this.db.prepare(`UPDATE delegations
      SET answered_by = ?, answer_decision = ?, answer_message = ?, answered_at = ?, updated_at = ?
      WHERE task_id = ?`)
      .run(ctx.nodeId, decision, message, now, now, taskId);
    logger.info('委派已代答', { taskId, decision, byNode: ctx.nodeId, disposition });
    void this.registry.sendTo(row.by_node_id, {
      v: 1, kind: 'evt', method: EVT.taskAnswered,
      payload: { taskId, decision, by: ctx.nodeId, disposition } satisfies TaskAnsweredEvent,
    });
    return { ok: true, payload: { disposition } };
  }

  /**
   * task.answer 干预分支（1.11.0 结构化答复，spec 2026-11-13 §8.2/§8.6——载荷带
   * interventionId 时 handleAnswer 分流至此，守卫层放行、权威校验在本方法）。
   * 答复权双通道（hub 是授权权威，身份取自信封鉴权 ctx.nodeId 不信任载荷自报）：
   * 发起方代答（by_node_id）仅 lf 组——peer 组仅执行节点所有者可答 →
   * forbidden_responder；target worker 本地答回传两模式均放行；其余节点 forbidden。
   * 流程：载荷解析（answer 白名单重建 + 顶层 decision 兼容映射，映射后全空 →
   * bad_request）→ 任务存在 + 账本非终态（pending/running/waiting/interrupted 允许，
   * 终态与 submitted → conflict——run 已收口无在途人工请求，同 intervene 口径）→
   * 干预行定位（不存在 intervention_not_found / 跨任务归属 conflict / 非 pending
   * intervention_stale 预检）→ 权威校验 → first_wins 原子落行（UPDATE 带
   * status='pending' 条件，changes=0 即竞态落败 → intervention_stale——双答竞态的
   * 正解，预检与写入之间的真竞态窗口在此收敛）→ 账本 waiting→running
   * （canTransition 守卫；非 waiting 不动——peer 草稿 pending、在途 running 均原地）
   * → 对端事件 task.interventionAnswered 定向「另一侧」（答方=发起方 → 发执行
   * 节点；答方=执行节点 → 发发起方）；peer 组不发（§8.6 平行组两干预事件均不
   * 转发，answer 载荷含 interventionId 与答复内容——泄漏存在性即泄漏请求本身）。
   * 设计差异点（与旧代答链刻意相反）：不做 worker 在线检查与转发挂起——干预
   * 答复落 hub 行即事实，对端离线容忍，重连经 task.interventions 拉取对账收敛
   * （H7）；旧链 node_offline/answer_timeout 语义不适用。res：{ disposition:
   * 'answered' }。
   */
  private async answerIntervention(
    ctx: GroupHandlerContext,
    taskId: string,
    payload: Record<string, unknown>,
  ): Promise<RpcOutcome> {
    const interventionId = optBoundedStr(payload.interventionId, INTERVENTION_ID_MAX);
    if (interventionId === null) {
      return { ok: false, error: err('bad_request', `interventionId 非法（1-${INTERVENTION_ID_MAX} 字符）`) };
    }
    const answer = parseInterventionAnswer(payload);
    if (answer === null) {
      return { ok: false, error: err('bad_request', 'answer 非法（decision approve|deny / text 1-512 / selected 各项 1-128 / completed 布尔；顶层 decision 可映射，至少一项非空）') };
    }

    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    if (row.status !== 'pending' && row.status !== 'running' && row.status !== 'waiting'
      && row.status !== 'interrupted') {
      return { ok: false, error: err('conflict', `任务当前状态为 ${row.status}，不可答复干预`) };
    }

    const iv = this.db.prepare('SELECT delegation_id, status FROM delegation_interventions WHERE id = ?')
      .get(interventionId) as { delegation_id: string; status: InterventionStatus } | undefined;
    if (iv === undefined) {
      return { ok: false, error: err('intervention_not_found', '干预请求不存在') };
    }
    if (iv.delegation_id !== taskId) {
      return { ok: false, error: err('conflict', 'interventionId 已归属其他任务，不可跨任务答复') };
    }
    if (iv.status !== 'pending') {
      return { ok: false, error: err('intervention_stale', `干预请求已处于 ${iv.status} 态，不可重复答复`) };
    }

    const groupMode = (this.db.prepare('SELECT mode FROM groups WHERE id = ?').get(row.group_id) as
      | { mode: 'leader-follower' | 'peer' }
      | undefined)?.mode ?? 'leader-follower';
    if (ctx.nodeId === row.by_node_id) {
      if (groupMode === 'peer') {
        return { ok: false, error: err('forbidden_responder', '平行组仅执行节点所有者可答复干预') };
      }
    } else if (ctx.nodeId !== row.target_node_id) {
      return { ok: false, error: err('forbidden', '仅委派发起方或执行节点可答复干预') };
    }

    const now = Date.now();
    let first = false;
    this.db.transaction(() => {
      const res = this.db.prepare(`UPDATE delegation_interventions
        SET status = 'answered', answer_json = ?, answered_by = ?, answered_at = ?, updated_at = ?
        WHERE id = ? AND status = 'pending'`)
        .run(JSON.stringify(answer), ctx.nodeId, now, now, interventionId);
      first = res.changes === 1;
      if (first && row.status === 'waiting' && canTransition(row.status, 'running')) {
        this.db.prepare("UPDATE delegations SET status = 'running', updated_at = ? WHERE task_id = ?")
          .run(now, taskId);
      }
    })();
    if (!first) {
      return { ok: false, error: err('intervention_stale', '干预请求已被先答（first_wins），本次答复未生效') };
    }
    logger.info('干预已答复', { taskId, interventionId, byNode: ctx.nodeId, ledgerFrom: row.status });

    if (groupMode === 'leader-follower') {
      const peerNodeId = ctx.nodeId === row.by_node_id ? row.target_node_id : row.by_node_id;
      const sent = await this.registry.sendTo(peerNodeId, {
        v: 1, kind: 'evt', method: EVT.taskInterventionAnswered,
        payload: { taskId, interventionId, answer, answeredBy: ctx.nodeId } satisfies TaskInterventionAnsweredEvent,
      });
      if (sent === 'offline') logger.info('干预答复通知未送达（对端离线）', { taskId, interventionId });
    }
    return { ok: true, payload: { disposition: 'answered' } };
  }

  /** 定向转发代答并挂起等待 worker res：req 信封 task.answer + 新 id；sendTo 判
   *  离线即回 node_offline（不占超时窗）；超时 answer_timeout（账本不动，可重试）。 */
  private forwardAnswer(row: DelegationRow, byNodeId: string, payload: unknown): Promise<RpcOutcome> {
    const id = randomUUID();
    return new Promise<RpcOutcome>((resolve) => {
      const timer = setTimeout(() => {
        this.answerPendingById.delete(id);
        logger.warn('代答转发超时', { taskId: row.task_id, targetNodeId: row.target_node_id, byNode: byNodeId });
        resolve({ ok: false, error: err('answer_timeout', '目标节点代答应答超时（账本未动，可重试）') });
      }, this.answerTimeoutMs);
      timer.unref?.();
      this.answerPendingById.set(id, { id, taskId: row.task_id, targetNodeId: row.target_node_id, byNodeId, settle: resolve, timer });
      void this.registry.sendTo(row.target_node_id, { v: 1, kind: 'req', id, method: RPC.task.answer, payload })
        .then((sent) => {
          if (sent === 'offline') {
            const p = this.answerPendingById.get(id);
            if (p) this.settleAnswer(p, { ok: false, error: err('node_offline', '目标节点不在线') });
          }
        });
    });
  }

  /** worker res 对账（上行 res 信封，id=转发 id）：未匹配 / 来源非目标 → 忽略；
   *  ok 载荷 disposition 三枚举校验（异形回 internal）；错误码保序透传（view 模板）。 */
  acceptAnswerRes(fromNodeId: string, res: ResponseEnvelope): void {
    const pending = this.answerPendingById.get(res.id);
    if (!pending) return;
    if (fromNodeId !== pending.targetNodeId) {
      logger.warn('代答应答被拒：来源非目标节点', { taskId: pending.taskId, from: fromNodeId });
      return;
    }
    if (!res.ok) {
      const raw = (res.payload ?? {}) as { code?: unknown; message?: unknown };
      const known = typeof raw.code === 'string' && (RPC_ERROR_CODES as readonly string[]).includes(raw.code);
      this.settleAnswer(pending, {
        ok: false,
        error: err(
          known ? raw.code as RpcErrorCode : 'internal',
          typeof raw.message === 'string' && raw.message.length > 0 ? raw.message : '目标节点处理失败',
        ),
      });
      return;
    }
    const disposition = (res.payload as { disposition?: unknown } | null)?.disposition;
    if (disposition !== 'granted' && disposition !== 'denied' && disposition !== 'stale') {
      this.settleAnswer(pending, { ok: false, error: err('internal', '目标节点代答应答载荷异形') });
      return;
    }
    this.settleAnswer(pending, { ok: true, payload: { disposition } });
  }

  private settleAnswer(p: PendingAnswer, outcome: RpcOutcome): void {
    clearTimeout(p.timer);
    this.answerPendingById.delete(p.id);
    p.settle(outcome);
  }

  /**
   * task.intervene RPC（1.11.0 HITL 结构化干预上报，worker → hub——刻意不入
   * LEADER_ONLY_METHODS：发起者是执行节点 target 而非 leader〔task.answer 入表是
   * 因代答为发起方/授权方特权，intervene 方向恰好相反〕，checkGroupRole 对未登记
   * 方法默认放行，权限锚在本 handler 的来源校验 ctx.nodeId === target_node_id）。
   * 流程（spec 2026-11-13 §8.2/§8.6）：载荷校验 → getTask/not_found → 来源
   * forbidden → 账本非终态（pending/running/waiting/interrupted 允许；终态与
   * submitted → conflict——submitted 是待验收态，run 已收口不存在在途人工请求）
   * → 幂等落行（id=interventionId 主键：已存在非 pending → intervention_stale；
   * 已存在 pending → 幂等重发）→ 账本 →waiting（canTransition 守卫：running 合迁、
   * pending/interrupted 无此边不动、已在 waiting 不动）→ 组模式分流通知发起方。
   * 裁定⑤（幂等重发也重发事件）：发起方重连后可能丢失首次事件，重发帮助收敛；
   * 事件侧幂等性由 studio 镜像行 UNIQUE(remote) 保证，hub 不去重。
   * res：{ interventionId }。
   */
  async handleIntervene(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const taskId = reqStr(payload, 'taskId');
    if (!taskId) return { ok: false, error: err('bad_request', 'taskId 缺失') };
    const intervention = parseIntervention(isRecord(payload) ? payload.intervention : undefined);
    if (!intervention) {
      return { ok: false, error: err('bad_request', 'intervention 非法（interventionId 1-128 / runNo 正整数 / kind tool_gate|question|manual_action / title 1-128 / prompt 字符串 ≤16KB / payload? 纯对象 / options? 数组〔id/label 1-128、description? ≤512〕/ deadlineAt? 正整数）') };
    }

    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    if (ctx.nodeId !== row.target_node_id) {
      return { ok: false, error: err('forbidden', '仅任务执行节点可上报干预') };
    }
    if (row.status !== 'pending' && row.status !== 'running' && row.status !== 'waiting'
      && row.status !== 'interrupted') {
      return { ok: false, error: err('conflict', `任务当前状态为 ${row.status}，不可上报干预`) };
    }

    // 幂等归属双查（评审轮 1）：status 判幂等重发/stale 之外，delegation_id 必须
    // 与本次 taskId 一致——跨任务复用/撞车同 interventionId 时若只查 status 会
    // 返回 ok 并把 B 任务账本推 waiting、给 B 发起方发事件，但行仍挂 A 任务
    // （B 干预无行，H7 对账查不到）→ mismatch 直接 conflict 拒绝
    const existing = this.db.prepare('SELECT delegation_id, status FROM delegation_interventions WHERE id = ?')
      .get(intervention.interventionId) as { delegation_id: string; status: InterventionStatus } | undefined;
    if (existing !== undefined && existing.delegation_id !== taskId) {
      return { ok: false, error: err('conflict', 'interventionId 已归属其他任务，不可跨任务复用') };
    }
    if (existing !== undefined && existing.status !== 'pending') {
      return { ok: false, error: err('intervention_stale', `干预请求已处于 ${existing.status} 态，不可重复上报`) };
    }
    const fresh = existing === undefined;
    const now = Date.now();
    // payload_json 按 spec §8.1 注释口径存内容字段（title/prompt/payload/options/
    // deadline）——id/run_no/kind 已有专属列，投影侧合并还原
    const contentJson = JSON.stringify({
      title: intervention.title,
      prompt: intervention.prompt,
      ...(intervention.payload !== undefined ? { payload: intervention.payload } : {}),
      ...(intervention.options !== undefined ? { options: intervention.options } : {}),
      ...(intervention.deadlineAt !== undefined ? { deadlineAt: intervention.deadlineAt } : {}),
    });
    this.db.transaction(() => {
      if (fresh) {
        this.db.prepare(`INSERT INTO delegation_interventions
          (id, delegation_id, run_no, kind, status, payload_json, answer_json, answered_by, answered_at, created_at, updated_at)
          VALUES (?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL, ?, ?)`)
          .run(intervention.interventionId, taskId, intervention.runNo, intervention.kind, contentJson, now, now);
      }
      if (row.status !== 'waiting' && canTransition(row.status, 'waiting')) {
        this.db.prepare("UPDATE delegations SET status = 'waiting', updated_at = ? WHERE task_id = ?")
          .run(now, taskId);
      }
    })();
    if (fresh) {
      logger.info('干预已落行', { taskId, interventionId: intervention.interventionId, kind: intervention.kind, byNode: ctx.nodeId });
    }

    // 组模式分流 + 发起方版本门（防半开，spec §8.5）：lf 且发起方 minor ≥ 11 →
    // task.intervention 定向；peer（spec §8.6 平行组不转发请求内容）或发起方低版本
    // （老发起方读不懂新事件）→ 中性进度，内容零泄漏（title/prompt/options 一律不带）。
    const groupMode = (this.db.prepare('SELECT mode FROM groups WHERE id = ?').get(row.group_id) as
      | { mode: 'leader-follower' | 'peer' }
      | undefined)?.mode ?? 'leader-follower';
    const originConn = this.registry.get(row.by_node_id);
    const originMinor = originConn !== undefined ? minorOf(originConn.peerProtoVer) : null;
    const originKnowsIntervention = originMinor !== null && originMinor >= TASK_HITL_REQUIRED_MINOR;
    // 发起方离线两分支均容忍：账本已 waiting、行已 pending，重连走 task.interventions
    // 对账（H7），不回滚（与 handleDelegate 离线回滚语义相反——intervene 落的是
    // 事实不是在途请求）
    if (groupMode === 'leader-follower' && originKnowsIntervention) {
      const sent = await this.registry.sendTo(row.by_node_id, {
        v: 1, kind: 'evt', method: EVT.taskIntervention,
        payload: { taskId, intervention } satisfies TaskInterventionEvent,
      });
      if (sent === 'offline') logger.info('干预通知未送达（发起方离线）', { taskId, interventionId: intervention.interventionId });
    } else {
      // 中性进度 lastSeq 定约（controller，评审轮 1）：恒 0——真实 worker seq 是
      // 小整数，用毫秒时间戳冒充会让消费侧按 lastSeq 单调去重把恢复后的合法进度
      // 当陈旧丢弃（任务冻结）；0 不污染序列空间，任何后续真实 seq 恒大于它。
      // 消费侧不得用 waiting-input 中性拍做 seq 追踪。
      const sent = await this.registry.sendTo(row.by_node_id, {
        v: 1, kind: 'evt', method: EVT.taskProgress,
        payload: {
          taskId,
          sessionId: row.session_id ?? '',
          lastSeq: 0,
          excerpt: '执行方等待人工处理',
          kind: 'waiting-input',
        } satisfies TaskProgressEvent,
      });
      if (sent === 'offline') logger.info('干预中性进度未送达（发起方离线）', { taskId, interventionId: intervention.interventionId });
    }
    return { ok: true, payload: { interventionId: intervention.interventionId } };
  }

  /**
   * task.accept RPC（1.11.0 发起方验收通过，spec 2026-11-13 §8.2——刻意不入
   * LEADER_ONLY_METHODS，沿 handleIntervene 先例：权威关系是「委派发起方」而非
   * 「现任组长」——peer 组发起方=by_node_id 未必是 leader_node_id，入表会被
   * checkGroupRole 的 leader 判定拦死；lf 组若入表则 transfer 后旧发起方（仍
   * by_node_id）被守卫拦死、submitted 任务无人可验收。权限锚在本 handler 的
   * 来源校验 ctx.nodeId === by_node_id）。
   * 流程：载荷校验 → getTask/not_found → 来源 forbidden → 非 submitted →
   * not_submitted（幂等拒绝：已 done 再 accept 同报此错，leader UI 显真实态，
   * 与 task.cancel 终态 conflict 同取向——不做幂等 ok）→ 账本 submitted→done
   * （accepted_at 落账；submitted→done 边在 DELEGATION_TRANSITIONS 由 H3 矩阵
   * 锁定，submitted-only 状态门即迁移单点的等价收敛）→ 定向 task.accepted 给
   * worker（runNo 取账本投影，离线容忍——账本已是事实，worker 重连经对账收敛）。
   * delegation_interventions 无 acceptance 类行（验收为发起方本地请求，spec
   * §8.1 表注）——无级联操作。res：{ runNo }。
   */
  async handleAccept(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const taskId = reqStr(payload, 'taskId');
    if (!taskId) return { ok: false, error: err('bad_request', 'taskId 缺失') };

    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    if (ctx.nodeId !== row.by_node_id) {
      return { ok: false, error: err('forbidden', '仅委派发起方可验收') };
    }
    if (row.status !== 'submitted') {
      return { ok: false, error: err('not_submitted', `任务当前状态为 ${row.status}，非待验收态不可验收（已验收不可重复受理）`) };
    }

    const runNo = row.run_no ?? 1;
    const now = Date.now();
    this.db.prepare("UPDATE delegations SET status = 'done', accepted_at = ?, updated_at = ? WHERE task_id = ?")
      .run(now, now, taskId);
    logger.info('委派已验收', { taskId, runNo, byNode: ctx.nodeId });
    const sent = await this.registry.sendTo(row.target_node_id, {
      v: 1, kind: 'evt', method: EVT.taskAccepted,
      payload: { taskId, runNo } satisfies TaskAcceptedEvent,
    });
    if (sent === 'offline') logger.info('验收通知未送达（目标离线，重连对账收敛）', { taskId });
    return { ok: true, payload: { runNo } };
  }

  /**
   * task.reject RPC（1.11.0 发起方打回，spec 2026-11-13 §8.2——与 handleAccept
   * 同款不入 LEADER_ONLY_METHODS、by_node_id 单点权威）。与 accept 的差异：
   * reason 必填非空白（trim 后 1..512，打回理由随事件进 worker agent 上下文）；
   * 账本 submitted→running + run_no+1（打回起新 run——run_no 单源在 hub，worker
   * 下次 submitted 上报携带同号对账）；submitted_at 保留历史不清（打回前的提交
   * 时刻是审计事实），accepted_at 恒 NULL（未验收过）。定向 task.rejected
   * { taskId, runNo: 新, reason } 给 worker（离线容忍同 accept）。res：{ runNo: 新 }。
   */
  async handleReject(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const taskId = reqStr(payload, 'taskId');
    if (!taskId) return { ok: false, error: err('bad_request', 'taskId 缺失') };
    const reasonRaw = isRecord(payload) ? payload.reason : undefined;
    const reason = typeof reasonRaw === 'string' && reasonRaw.trim().length >= 1 && reasonRaw.length <= REASON_MAX
      ? reasonRaw
      : null;
    if (reason === null) {
      return { ok: false, error: err('bad_request', `reason 非法（非空白 1-${REASON_MAX} 字符）`) };
    }

    const row = this.getTask(taskId);
    if (!row) return { ok: false, error: err('not_found', '任务不存在') };
    if (ctx.nodeId !== row.by_node_id) {
      return { ok: false, error: err('forbidden', '仅委派发起方可打回') };
    }
    if (row.status !== 'submitted') {
      return { ok: false, error: err('not_submitted', `任务当前状态为 ${row.status}，非待验收态不可打回`) };
    }

    const newRunNo = (row.run_no ?? 1) + 1;
    const now = Date.now();
    this.db.prepare("UPDATE delegations SET status = 'running', run_no = ?, updated_at = ? WHERE task_id = ?")
      .run(newRunNo, now, taskId);
    logger.info('委派已打回', { taskId, newRunNo, byNode: ctx.nodeId });
    const sent = await this.registry.sendTo(row.target_node_id, {
      v: 1, kind: 'evt', method: EVT.taskRejected,
      payload: { taskId, runNo: newRunNo, reason } satisfies TaskRejectedEvent,
    });
    if (sent === 'offline') logger.info('打回通知未送达（目标离线，重连对账收敛）', { taskId });
    return { ok: true, payload: { runNo: newRunNo } };
  }

  /**
   * task.list RPC（组过滤 + status 过滤；leader 或成员均可——不在 §6 leader-only
   * 清单）。指定 groupId 时要求本组成员身份（§9 组间可见性不做）；缺省 groupId
   * 返回调用方所在全部组的任务。
   */
  handleList(ctx: GroupHandlerContext, payload: unknown): RpcOutcome {
    const groupId = reqStr(payload, 'groupId');
    const statusRaw = isRecord(payload) ? payload.status : undefined;
    let status: DelegationStatus | null = null;
    if (statusRaw !== undefined) {
      if (typeof statusRaw !== 'string' || !(DELEGATION_STATUSES as readonly string[]).includes(statusRaw)) {
        return { ok: false, error: err('bad_request', 'status 非法（八态枚举外）') };
      }
      status = statusRaw as DelegationStatus;
    }

    let groupIds: string[];
    if (groupId !== null) {
      if (!this.db.prepare('SELECT 1 FROM groups WHERE id = ?').get(groupId)) {
        return { ok: false, error: err('not_found', '工作组不存在') };
      }
      if (!this.db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?').get(groupId, ctx.nodeId)) {
        return { ok: false, error: err('forbidden', '仅本组成员可查看该组任务') };
      }
      groupIds = [groupId];
    } else {
      groupIds = (this.db.prepare('SELECT group_id FROM group_members WHERE node_id = ?').all(ctx.nodeId) as Array<{ group_id: string }>)
        .map((r) => r.group_id);
      if (groupIds.length === 0) return { ok: true, payload: { tasks: [] } };
    }

    const placeholders = groupIds.map(() => '?').join(', ');
    const args: unknown[] = [...groupIds];
    let sql = `SELECT * FROM delegations WHERE group_id IN (${placeholders})`;
    if (status !== null) { sql += ' AND status = ?'; args.push(status); }
    sql += ' ORDER BY created_at ASC, task_id ASC';
    const rows = this.db.prepare(sql).all(...args) as DelegationRow[];
    return { ok: true, payload: { tasks: rows.map(toRecord) } };
  }

  /**
   * 目标节点上行 task.report（res 信封——id=taskId 对账）。拒绝路径全部静默
   * （日志 + 账本不动，不炸连接，见文件头 wire 约定）。合法迁移 → 账本更新 +
   * 定向转发对应 EVT 到 leader；running→running = 进度拍（status 不动，sessionId
   * 回填，task.progress 直转）。
   */
  acceptReport(fromNodeId: string, res: ResponseEnvelope): void {
    if (!res.ok) {
      logger.warn('委派上报被拒：res.ok=false', { taskId: res.id, from: fromNodeId });
      return;
    }
    const report = parseReportPayload(res.payload);
    if (!report) {
      logger.warn('委派上报被拒：载荷异形', { taskId: res.id, from: fromNodeId });
      return;
    }
    const row = this.getTask(res.id);
    if (!row) {
      logger.warn('委派上报被拒：未知任务', { taskId: res.id, from: fromNodeId });
      return;
    }
    if (fromNodeId !== row.target_node_id) {
      logger.warn('委派上报被拒：来源非目标节点', { taskId: row.task_id, from: fromNodeId });
      return;
    }

    const to = report.status;
    const kind = report.progress?.kind;

    // 1.10.0 生命周期拍（peer 草稿 received/arranged）：账本状态不动、sessionId
    // 不回填，仅透传 progress 给委派发起方（by_node）；终态/未知态收到 pending 拍
    // 静默忽略（对账键合法即不炸连接——拍是通知面不是账本）。
    if (to === 'pending') {
      if (row.status === 'done' || row.status === 'failed' || row.status === 'cancelled') {
        logger.info('委派生命周期拍被忽略：账本已终态', { taskId: row.task_id, status: row.status, from: fromNodeId });
        return;
      }
      this.forwardProgress(row, report);
      return;
    }

    // M3.1 waiting 触发规则（spec §3.1）：kind 只随 running 上报影响账本——
    // running+kind → 迁入 waiting；waiting+kind → 进度拍（账本不动）；
    // waiting 无 kind → 解除回 running。均独立于通用迁移路径（审计行分开）。
    if (to === 'running' && row.status === 'running' && kind === 'waiting-input') {
      const sessionId = report.sessionId ?? row.session_id;
      this.db.prepare("UPDATE delegations SET status = 'waiting', session_id = ?, updated_at = ? WHERE task_id = ?")
        .run(sessionId, Date.now(), row.task_id);
      logger.info('委派进入等待', { taskId: row.task_id, byNode: fromNodeId });
      this.forwardProgress({ ...row, status: 'waiting', session_id: sessionId }, report);
      return;
    }
    if (to === 'running' && row.status === 'waiting') {
      const sessionId = report.sessionId && report.sessionId !== row.session_id
        ? report.sessionId
        : null;
      if (kind === 'waiting-input') {
        // 等待期间同类上报 = 进度拍（账本不动，进度照转）
        if (sessionId !== null) {
          this.db.prepare('UPDATE delegations SET session_id = ?, updated_at = ? WHERE task_id = ?')
            .run(sessionId, Date.now(), row.task_id);
        }
        this.forwardProgress({ ...row, session_id: sessionId ?? row.session_id }, report);
        return;
      }
      // 等待解除：状态单源铁律——hub 不在 task.answer 时迁移，approve/deny 后
      // worker 下一拍无 kind 即回 running
      this.db.prepare("UPDATE delegations SET status = 'running', session_id = ?, updated_at = ? WHERE task_id = ?")
        .run(sessionId ?? row.session_id, Date.now(), row.task_id);
      logger.info('委派等待解除', { taskId: row.task_id, byNode: fromNodeId });
      this.forwardProgress({ ...row, status: 'running', session_id: sessionId ?? row.session_id }, report);
      return;
    }

    if (to === row.status) {
      if (to === 'running') {
        const sessionId = report.sessionId && report.sessionId !== row.session_id
          ? report.sessionId
          : null;
        if (sessionId !== null) {
          this.db.prepare('UPDATE delegations SET session_id = ?, updated_at = ? WHERE task_id = ?')
            .run(sessionId, Date.now(), row.task_id);
        }
        this.forwardProgress({ ...row, session_id: sessionId ?? row.session_id }, report);
      } else {
        logger.info('委派上报重复终态，忽略', { taskId: row.task_id, status: to, from: fromNodeId });
      }
      return;
    }
    if (!canTransition(row.status, to)) {
      logger.warn('委派上报被拒：非法状态迁移', { taskId: row.task_id, from: row.status, to, by: fromNodeId });
      return;
    }

    const sessionId = report.sessionId ?? row.session_id;
    this.db.prepare('UPDATE delegations SET status = ?, session_id = ?, updated_at = ? WHERE task_id = ?')
      .run(to, sessionId, Date.now(), row.task_id);
    logger.info('委派状态迁移', { taskId: row.task_id, from: row.status, to, byNode: fromNodeId });

    if (to === 'running') {
      this.forwardProgress({ ...row, status: 'running', session_id: sessionId }, report);
      return;
    }
    const terminal: TaskTerminalEvent = { taskId: row.task_id };
    if (to === 'failed' && report.reason !== undefined) terminal.reason = report.reason;
    void this.registry.sendTo(row.by_node_id, {
      v: 1, kind: 'evt', method: to === 'done' ? EVT.taskCompleted : EVT.taskFailed,
      payload: terminal,
    });
  }

  /**
   * 节点离线（registry remove 后调用，与 mirror.onNodeOffline 同门）：该节点非终态
   * 委派 pending/running/waiting → interrupted（MC5——悬挂任务回收；waiting 含入
   * 是 M3.1 卡死回收口径，spec §1 非目标：不做周期 sweeper，断线即收），并定向
   * task.interrupted 通知 leader（leader 离线则 sendTo 静默丢弃）。在途代答转发
   * 即时结算不占超时窗（view 模板）。
   */
  onNodeOffline(nodeId: string): void {
    for (const p of [...this.answerPendingById.values()]) {
      if (p.targetNodeId === nodeId) {
        this.settleAnswer(p, { ok: false, error: err('node_offline', '目标节点在处理期间离线') });
      } else if (p.byNodeId === nodeId) {
        this.settleAnswer(p, { ok: false, error: err('internal', '请求方已断开') });
      }
    }
    const rows = this.db.prepare(
      "SELECT * FROM delegations WHERE target_node_id = ? AND status IN ('pending', 'running', 'waiting')",
    ).all(nodeId) as DelegationRow[];
    for (const row of rows) {
      if (!canTransition(row.status, 'interrupted')) continue; // 单点校验（防御——过滤条件已限定）
      this.db.prepare("UPDATE delegations SET status = 'interrupted', updated_at = ? WHERE task_id = ?")
        .run(Date.now(), row.task_id);
      logger.info('委派状态迁移', { taskId: row.task_id, from: row.status, to: 'interrupted', byNode: 'hub' });
      void this.registry.sendTo(row.by_node_id, {
        v: 1, kind: 'evt', method: EVT.taskInterrupted,
        payload: { taskId: row.task_id } satisfies TaskTerminalEvent,
      });
    }
  }

  /** task.progress 直转（节流合并由目标侧负责——hub 不缓存不合并）。sessionId
   *  缺省（上报与账本均无）时转发空串，保 TaskProgressEvent 形态恒定。
   *  RM2-7：excerpt 空/缺省 → 占位「（无文本增量）」转发（leader 恒见非空摘要——
   *  T10 目标侧恒非空是第一道保险，hub 兜底是第二道）。
   *  kind（M2.1 T11b）：目标侧等待标注原样透传 leader 推送面（缺省不带键——
   *  0.1.3 载荷兼容）。 */
  private forwardProgress(row: DelegationRow, report: TaskReportPayload): void {
    const evt: EventEnvelope = {
      v: 1, kind: 'evt', method: EVT.taskProgress,
      payload: {
        taskId: row.task_id,
        sessionId: row.session_id ?? '',
        lastSeq: report.progress?.lastSeq ?? 0,
        excerpt: report.progress?.excerpt || '（无文本增量）',
        ...(report.progress?.kind !== undefined ? { kind: report.progress.kind } : {}),
      } satisfies TaskProgressEvent,
    };
    void this.registry.sendTo(row.by_node_id, evt);
  }
}
