// hub 远程查看转发层（M2e spec 2026-10-24 §3.1「远程查看」/§3.2 view.appended/
// §5.5 双层同意/§8 RF#1 + T13；M3.2 增 workspaces/files 两方法与 groupId 路径）——
// leader-only 八方法定向转发 + consent 内存态 + 尾部订阅登记 + 断开双侧注销。
//
// ▲ wire 约定（本任务定义；协议单源在协议包 src/remote-view.ts 的 VIEW_WIRE）：
// 1) view.consent（节点 → hub，req 信封，hub 直接 res {ok:true} 确认）：
//    payload={allowed:boolean}；节点 hello 后与开关变更时上行；hub 存内存态
//    per-nodeId，默认 true（spec §5.5 默认开）；断线清除回默认 true——
//    裁定 RM2-9：不残留陈旧 false，重连由节点重报覆盖。
// 2) view.* 八方法（leader → hub → 目标节点；M3.2 增 workspaces/files）：hub 以
//    req 信封「原 method +
//    新 id」转发目标，leader 的原始请求挂起；目标以 res 信封回（id=转发 id
//    关联）——hub 按挂起请求表把结果透传 leader（ok 载荷等值 / 错误码保序——
//    目标错误码在 RpcErrorCode 枚举内原码原文透传，未知码回 internal 兜底）。
//    在途窗口内目标断开 → 立即结算 node_offline（不悬到超时）；leader 断开 →
//    结算释放（res 无处投递，静默丢弃）。
// 3) view.subscribe 特殊：目标接受（res ok）后 hub 登记 (leaderNodeId,
//    targetNodeId, sessionId) 三元组；此后目标以 req 信封 method=view.tail
//    （payload={sessionId, events:[{seq,at,role,content}...]}）主动上行尾部
//    增量——hub 组装 view.appended evt（§3.2）定向推全部订阅该会话的 leader
//    （多 leader 扇出）；无订阅到达即丢弃 + 日志，对目标仍回 ok（不炸上行）。
// 4) 豁免约定：view.unsubscribe 为清理路径——不受 consent 校验阻断（目标关
//    开关后 leader 恒可退订，防 RF#1 卡死）；本地注销先行，目标在线则转发并
//    透传其应答，离线/超时仍回 ok（幂等清理语义）。
// 5) 订阅注销三入口（RF#1 零泄漏）：unsubscribe 显式注销 / leader 断开自动
//    注销 / 目标断开自动注销；同 leader 同会话重复 subscribe 幂等（登记去重，
//    每次 tail 恒投递一份）。
import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  EVT, RPC, RPC_ERROR_CODES,
  type EventEnvelope, type ResponseEnvelope, type RpcErrorCode, type RpcErrorPayload,
  type ViewAppendedEvent, type ViewMessage, type ViewTailPayload,
} from 'momo-hub-protocol';
import type { NodeRegistry } from './registry';
import { commonGroupId, isMember, type GroupHandlerContext, type RpcOutcome } from './groups';
import { logger } from './logger';

const err = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

/** 转发在途超时（目标挂起不答的兜底结算——不设则 leader 请求永久悬挂）。 */
const FORWARD_TIMEOUT_MS = 10_000;

const NODE_ID_MAX = 128;
const SESSION_ID_MAX = 128;
const TAIL_EVENTS_MAX = 200;
const TAIL_ROLE_MAX = 128;
const TAIL_CONTENT_MAX = 256 * 1024;

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

/** view.tail 载荷校验：sessionId 有界 + events 数组（≤200 条，每条 seq 非负整数 /
 *  at 非负数 / role 有界 / content 封顶——中转方 DoS 下限，深语义由订阅端消费方定）。 */
function parseTailPayload(raw: unknown): ViewTailPayload | null {
  if (!isRecord(raw)) return null;
  const sessionId = optBoundedStr(raw.sessionId, SESSION_ID_MAX);
  if (!sessionId) return null;
  if (!Array.isArray(raw.events) || raw.events.length > TAIL_EVENTS_MAX) return null;
  const events: ViewMessage[] = [];
  for (const e of raw.events) {
    if (!isRecord(e)) return null;
    if (typeof e.seq !== 'number' || !Number.isInteger(e.seq) || e.seq < 0) return null;
    if (typeof e.at !== 'number' || !Number.isFinite(e.at) || e.at < 0) return null;
    if (typeof e.role !== 'string' || e.role.length === 0 || e.role.length > TAIL_ROLE_MAX) return null;
    if (typeof e.content !== 'string' || e.content.length > TAIL_CONTENT_MAX) return null;
    events.push({ seq: e.seq, at: e.at, role: e.role, content: e.content });
  }
  return { sessionId, events };
}

/** 目标 res → leader 透传结果（错误码保序：枚举内原码原文，未知/异形回 internal）。 */
function resToOutcome(res: ResponseEnvelope): RpcOutcome {
  if (res.ok) return { ok: true, payload: res.payload };
  const raw = (res.payload ?? {}) as { code?: unknown; message?: unknown };
  const known = typeof raw.code === 'string' && (RPC_ERROR_CODES as readonly string[]).includes(raw.code);
  return {
    ok: false,
    error: err(
      known ? raw.code as RpcErrorCode : 'internal',
      typeof raw.message === 'string' && raw.message.length > 0 ? raw.message : '目标节点处理失败',
    ),
  };
}

function subKey(leaderNodeId: string, targetNodeId: string, sessionId: string): string {
  return `${leaderNodeId}\u0000${targetNodeId}\u0000${sessionId}`;
}

interface PendingForward {
  id: string;
  method: string;
  leaderNodeId: string;
  targetNodeId: string;
  settle: (outcome: RpcOutcome) => void;
  timer: NodeJS.Timeout;
}

interface SubEntry { leaderNodeId: string; targetNodeId: string; sessionId: string }

/** 构造选项（转发超时测试注入——server.ts viewForwardTimeoutMs 透传）。 */
export interface RemoteViewRouterOpts {
  forwardTimeoutMs?: number;
}

export class RemoteViewRouter {
  /** consent 内存态 per-nodeId（缺省 true——spec §5.5；断线清除回默认，RM2-9）。 */
  private consents = new Map<string, boolean>();
  /** 挂起转发表：转发 id → 在途请求（目标 res 按 id 对账，同 mirror 回执模式）。 */
  private pendingById = new Map<string, PendingForward>();
  /** 订阅登记：leader\0target\0session 三元组去重（同键重复 subscribe 幂等）。 */
  private subs = new Map<string, SubEntry>();
  private readonly forwardTimeoutMs: number;

  constructor(
    private readonly db: Database.Database,
    private readonly registry: NodeRegistry,
    opts: RemoteViewRouterOpts = {},
  ) {
    this.forwardTimeoutMs = opts.forwardTimeoutMs ?? FORWARD_TIMEOUT_MS;
  }

  /**
   * view.* 八方法统一入口（leader-only 与「leader 和 target 同组」由外围守卫提取器
   * 预校验；此处同款查询兜底覆盖提取器返 null 的全部情形）。校验序：载荷形态 →
   * 同组（forbidden；M3.2 起带 groupId 走 isMember 精确路径）→ consent（forbidden，
   * unsubscribe 豁免）→ 定向转发（离线 node_offline）→ 目标 res 透传。subscribe
   * 成功后登记订阅；unsubscribe 本地注销先行 + 尽力转发。
   */
  async handleView(ctx: GroupHandlerContext, method: string, payload: unknown): Promise<RpcOutcome> {
    const targetNodeId = optBoundedStr(reqStr(payload, 'targetNodeId'), NODE_ID_MAX);
    if (!targetNodeId) return { ok: false, error: err('bad_request', 'targetNodeId 缺失或超长') };

    // 登记侧 sessionId 与 tail 上行同界（SESSION_ID_MAX——T13 评审 Minor③：
    // 订阅键长度对称约束，防超长键灌入 subs 表）
    const sessionId = isRecord(payload) ? optBoundedStr(payload.sessionId, SESSION_ID_MAX) : null;
    if ((method === RPC.view.subscribe || method === RPC.view.unsubscribe) && !sessionId) {
      return { ok: false, error: err('bad_request', 'sessionId 缺失或超长') };
    }

    // 同组校验双路径（M3.2 §2.2）：载荷带 groupId 时按该组精确判定 isMember(groupId,
    // target)——组存在性与 leader 身份已由 checkGroupRole 提取器先行（groupId 优先
    // 回填）；缺省回落 commonGroupId（「caller 为 leader 且 target 为成员」公共组，
    // MC6 多组任意取旧——1.2/1.3 调用者零变化，歧义由显式 groupId 消除）。
    const groupId = reqStr(payload, 'groupId');
    const sameGroup = groupId !== null
      ? isMember(this.db, groupId, targetNodeId)
      : commonGroupId(this.db, ctx.nodeId, targetNodeId) !== undefined;
    if (!sameGroup) {
      return { ok: false, error: err('forbidden', '仅组长可远程查看目标节点（需与其同组）') };
    }

    if (method === RPC.view.unsubscribe) {
      // 清理路径（豁免 consent）：本地注销先行；离线/无目标应答仍 ok（幂等语义）
      if (sessionId !== null) {
        this.subs.delete(subKey(ctx.nodeId, targetNodeId, sessionId));
        logger.info('订阅已注销', { leaderNodeId: ctx.nodeId, targetNodeId, sessionId, by: 'unsubscribe' });
      }
      if (!this.registry.get(targetNodeId)) return { ok: true, payload: { ok: true } };
      return this.forward(ctx.nodeId, targetNodeId, method, payload);
    }

    if (this.consents.get(targetNodeId) === false) {
      return { ok: false, error: err('forbidden', '目标节点已关闭本组查看') };
    }

    const outcome = await this.forward(ctx.nodeId, targetNodeId, method, payload);
    if (outcome.ok && method === RPC.view.subscribe) {
      if (sessionId !== null) {
        this.subs.set(subKey(ctx.nodeId, targetNodeId, sessionId), { leaderNodeId: ctx.nodeId, targetNodeId, sessionId });
        logger.info('订阅已登记', { leaderNodeId: ctx.nodeId, targetNodeId, sessionId });
      }
    }
    return outcome;
  }

  /** view.consent 上行：形状校验（allowed 布尔）+ 上报者须为在线注册节点（未知拒）。 */
  handleConsent(ctx: GroupHandlerContext, payload: unknown): RpcOutcome {
    if (!isRecord(payload) || typeof payload.allowed !== 'boolean') {
      return { ok: false, error: err('bad_request', 'allowed 必须为布尔') };
    }
    if (!this.registry.get(ctx.nodeId)) {
      return { ok: false, error: err('forbidden', '仅在线节点可上报查看开关') };
    }
    this.consents.set(ctx.nodeId, payload.allowed);
    logger.info('查看开关上报', { nodeId: ctx.nodeId, allowed: payload.allowed });
    return { ok: true, payload: { ok: true } };
  }

  /** view.tail 上行：载荷校验 → 匹配订阅（targetNodeId=上报者 + sessionId）→
   *  组装 view.appended 定向逐 leader；无订阅丢弃 + 日志（对目标恒回 ok）。 */
  handleTail(ctx: GroupHandlerContext, payload: unknown): RpcOutcome {
    const tail = parseTailPayload(payload);
    if (!tail) {
      return { ok: false, error: err('bad_request', `tail 载荷异形（sessionId + events[{seq,at,role,content}] ≤${TAIL_EVENTS_MAX} 条）`) };
    }
    if (tail.events.length === 0) {
      // 空批零信息量——不扇出（防订阅端噪音），对目标仍 ok
      logger.info('尾部事件为空批，忽略', { from: ctx.nodeId, sessionId: tail.sessionId });
      return { ok: true, payload: { ok: true } };
    }
    const subscribers = [...this.subs.values()]
      .filter((s) => s.targetNodeId === ctx.nodeId && s.sessionId === tail.sessionId
        // 订阅存续期复检同组（T13 评审 Important①）：hub 不主动杀登记，退组后
        // 靠复检收敛——同组关系已解除的订阅不再扇出
        && commonGroupId(this.db, s.leaderNodeId, s.targetNodeId) !== undefined);
    if (subscribers.length === 0) {
      logger.info('尾部事件无订阅，丢弃', { from: ctx.nodeId, sessionId: tail.sessionId, count: tail.events.length });
      return { ok: true, payload: { ok: true } };
    }
    const evt: EventEnvelope = {
      v: 1, kind: 'evt', method: EVT.viewAppended,
      payload: { sessionId: tail.sessionId, events: tail.events } satisfies ViewAppendedEvent,
    };
    for (const s of subscribers) void this.registry.sendTo(s.leaderNodeId, evt);
    logger.info('尾部事件定向推送', { from: ctx.nodeId, sessionId: tail.sessionId, count: tail.events.length, subscribers: subscribers.length });
    return { ok: true, payload: { ok: true } };
  }

  /** 目标 res 对账（上行 res 信封，id=转发 id）：未匹配 / 来源非该转发目标 → 忽略。 */
  acceptViewRes(fromNodeId: string, res: ResponseEnvelope): void {
    const pending = this.pendingById.get(res.id);
    if (!pending) return;
    if (fromNodeId !== pending.targetNodeId) {
      logger.warn('查看应答被拒：来源非目标节点', { method: pending.method, from: fromNodeId });
      return;
    }
    this.settlePending(pending, resToOutcome(res));
  }

  /** 节点断开清理（RM2-9 + RF#1）：consent 回默认 / 订阅双侧注销 / 在途结算释放。 */
  onNodeOffline(nodeId: string): void {
    this.consents.delete(nodeId);
    for (const [key, s] of [...this.subs]) {
      if (s.leaderNodeId === nodeId || s.targetNodeId === nodeId) this.subs.delete(key);
    }
    for (const p of [...this.pendingById.values()]) {
      if (p.targetNodeId === nodeId) {
        this.settlePending(p, { ok: false, error: err('node_offline', '目标节点在处理期间离线') });
      } else if (p.leaderNodeId === nodeId) {
        this.settlePending(p, { ok: false, error: err('internal', '请求方已断开') });
      }
    }
  }

  /** 定向转发并挂起等待目标 res：req 信封原 method + 新 id；sendTo 判离线即回
   *  node_offline（不占超时窗）；超时 internal 兜底结算。 */
  private forward(leaderNodeId: string, targetNodeId: string, method: string, payload: unknown): Promise<RpcOutcome> {
    const id = randomUUID();
    return new Promise<RpcOutcome>((resolve) => {
      const timer = setTimeout(() => {
        this.pendingById.delete(id);
        logger.warn('查看转发超时', { method, targetNodeId, leaderNodeId });
        // unsubscribe 超时按幂等清理语义回 ok（本地注销已先行，T13 评审 Important②
        // 对齐报告约定：目标不应答不得卡死 leader 的退订请求）
        resolve(method === RPC.view.unsubscribe
          ? { ok: true, payload: { ok: true } }
          : { ok: false, error: err('internal', '目标节点响应超时') });
      }, this.forwardTimeoutMs);
      timer.unref?.();
      this.pendingById.set(id, { id, method, leaderNodeId, targetNodeId, settle: resolve, timer });
      void this.registry.sendTo(targetNodeId, { v: 1, kind: 'req', id, method, payload }).then((sent) => {
        if (sent === 'offline') {
          const p = this.pendingById.get(id);
          if (p) this.settlePending(p, { ok: false, error: err('node_offline', '目标节点不在线') });
        }
      });
    });
  }

  private settlePending(p: PendingForward, outcome: RpcOutcome): void {
    clearTimeout(p.timer);
    this.pendingById.delete(p.id);
    p.settle(outcome);
  }
}

/** 上行 res 分流判定：method 为 view.* 八方法（协议公开目录）即归本路由对账。 */
export function isViewRpcMethod(method: string): boolean {
  return (Object.values(RPC.view) as readonly string[]).includes(method);
}
