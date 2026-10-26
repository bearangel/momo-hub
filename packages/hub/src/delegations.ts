// hub 委派账本与 task.* RPC（M2d spec 2026-10-24 §3.1「委派」/§3.2 事件/§4
// delegations 表/§6 + T9）——状态机单点校验（Review Focus #3）/定向转发/
// 中断语义（MC5）/审计日志（§6 审计链）。
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
  EVT,
  type DelegationStatus, type DelegationTask, type EventEnvelope, type ResponseEnvelope,
  type RpcErrorPayload, type TaskAssignedEvent, type TaskProgressEvent, type TaskRecord,
  type TaskReportPayload, type TaskTerminalEvent,
} from 'momo-hub-protocol';
import type { NodeRegistry } from './registry';
import type { GroupHandlerContext, RpcOutcome } from './groups';
import { logger } from './logger';

// ────────────────────────────────────────────────────────────────────────────
// 状态机（hub 单点校验——Review Focus #3）
// ────────────────────────────────────────────────────────────────────────────

/**
 * 合法迁移表（§4 状态机 + MC5 + brief 矩阵）：
 * - pending → running / failed('declined' 目标拒派) / cancelled / interrupted（hub 离线迁移）
 * - running → done / failed / cancelled / interrupted
 * - interrupted → running（重连续传）/ failed('local_session_lost') / cancelled
 * - done / failed / cancelled 终态，拒一切。
 * 全部迁移（task.cancel / task.report 上报 / onNodeOffline 自动迁移）必经
 * canTransition 单点——目标节点的上报必须匹配账本当前态。
 */
const DELEGATION_TRANSITIONS: Record<DelegationStatus, readonly DelegationStatus[]> = {
  pending: ['running', 'failed', 'cancelled', 'interrupted'],
  running: ['done', 'failed', 'cancelled', 'interrupted'],
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

/** 状态枚举全集（task.list 过滤校验 + 矩阵测试遍历用）。 */
export const DELEGATION_STATUSES: readonly DelegationStatus[] = [
  'pending', 'running', 'done', 'failed', 'interrupted', 'cancelled',
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

/** 任务书校验：title 1..128、prompt 非空且 ≤16KB（字节）、agentSlug 可选 ≤64。 */
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
  return task;
}

/** task.report 载荷校验：status 三值枚举（interrupted/cancelled 不是上报值）、
 *  sessionId/reason 有界串、progress 形态（lastSeq 非负整数 + excerpt 有界串——
 *  RM2-7：excerpt="" 视同缺省受理，转发时以占位呈现，lastSeq 不丢）。 */
function parseReportPayload(raw: unknown): TaskReportPayload | null {
  if (!isRecord(raw)) return null;
  if (raw.status !== 'running' && raw.status !== 'done' && raw.status !== 'failed') return null;
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
  }
  return out;
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
}

function toRecord(r: DelegationRow): TaskRecord {
  return {
    taskId: r.task_id,
    workspaceName: r.workspace_name,
    targetNodeId: r.target_node_id,
    status: r.status,
    title: r.title,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

// ────────────────────────────────────────────────────────────────────────────
// 委派路由器
// ────────────────────────────────────────────────────────────────────────────

export class DelegationRouter {
  constructor(private readonly db: Database.Database, private readonly registry: NodeRegistry) {}

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
    if (!task) return { ok: false, error: err('bad_request', 'task 非法（title 1-128 / prompt 非空 ≤16KB / agentSlug? ≤64）') };

    if (!this.db.prepare('SELECT 1 FROM groups WHERE id = ?').get(groupId)) {
      return { ok: false, error: err('not_found', '工作组不存在') };
    }
    if (!this.db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?').get(groupId, targetNodeId)) {
      return { ok: false, error: err('forbidden', '目标节点不是本组成员') };
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
      payload: { taskId, workspaceName, task, byNode: ctx.nodeId } satisfies TaskAssignedEvent,
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
        return { ok: false, error: err('bad_request', 'status 非法（六态枚举外）') };
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
   * 委派 pending/running → interrupted（MC5——悬挂任务回收，canTransition 单点
   * 判定），并定向 task.interrupted 通知 leader（leader 离线则 sendTo 静默丢弃）。
   */
  onNodeOffline(nodeId: string): void {
    const rows = this.db.prepare(
      "SELECT * FROM delegations WHERE target_node_id = ? AND status IN ('pending', 'running')",
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
   *  T10 目标侧恒非空是第一道保险，hub 兜底是第二道）。 */
  private forwardProgress(row: DelegationRow, report: TaskReportPayload): void {
    const evt: EventEnvelope = {
      v: 1, kind: 'evt', method: EVT.taskProgress,
      payload: {
        taskId: row.task_id,
        sessionId: row.session_id ?? '',
        lastSeq: report.progress?.lastSeq ?? 0,
        excerpt: report.progress?.excerpt || '（无文本增量）',
      } satisfies TaskProgressEvent,
    };
    void this.registry.sendTo(row.by_node_id, evt);
  }
}
