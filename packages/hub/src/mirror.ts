// hub 镜像路由（M2c spec 2026-10-24 §3.1/§3.2/§5.2 + T6）——两段式定向路由：
// leader → hub（workspace.mirror RPC）→ 定向 sendTo(targetNodeId) 转发 mirror.apply
// 请求信封（私有目标端语义，id=mirrorId 即对账键）→ hub 先返 {accepted:true, mirrorId}
// → 目标节点应用后回执 mirror.receipt（res 信封，按 id 对账）→ hub 汇总：
// 成功 = workspace.mirrored 组内广播 + mirror.result（res 信封）定向回 leader；
// conflict = 错误 + diffSummary 原样透传 leader（confirm=true 重发直落由 leader 侧发起）。
// 并发对账（Review Focus #2）：内存登记 mirrorId → {workspaceName, targetNodeId}，
// 同 workspaceName+targetNodeId 在途 → 新请求 conflict 'mirror_in_progress'；
// 回执/断连清登记。目标节点侧应用器（含差异摘要计算与资源补装）= studio T8，不在本域。
import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  EVT, MIRROR_WIRE,
  type EventEnvelope, type MirrorAcceptRes, type MirrorApplyPayload, type MirrorDiffSummary,
  type MirrorResultPayload, type ResponseEnvelope, type RpcErrorPayload, type WorkspaceMirroredEvent,
} from 'momo-hub-protocol';
import type { NodeRegistry } from './registry';
import type { GroupHandlerContext, RpcOutcome } from './groups';

/** 在途镜像登记项（并发对账 + 回执对账的单一状态源）。 */
interface PendingMirror {
  mirrorId: string;
  groupId: string;
  leaderNodeId: string;
  targetNodeId: string;
  workspaceName: string;
  snapshotId: string;
}

const err = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

/** workspaceName 与 manifest 同款上限（S1 NAME_MAX）——登记键有界。 */
const WORKSPACE_NAME_MAX = 128;

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function reqStr(payload: unknown, key: string): string | null {
  const v = isRecord(payload) ? payload[key] : undefined;
  return typeof v === 'string' && v.length > 0 ? v : null;
}

function isDiffSummary(x: unknown): x is MirrorDiffSummary {
  if (!isRecord(x)) return false;
  const { added, removed, changed } = x;
  return [added, removed, changed].every((n) => typeof n === 'number' && Number.isFinite(n));
}

function inFlightKey(workspaceName: string, targetNodeId: string): string {
  return `${workspaceName}\u0000${targetNodeId}`;
}

export class MirrorRouter {
  private byId = new Map<string, PendingMirror>();
  /** workspaceName\0targetNodeId → mirrorId（同键在途去重——后到拒）。 */
  private inFlight = new Map<string, string>();

  constructor(private readonly db: Database.Database, private readonly registry: NodeRegistry) {}

  /**
   * workspace.mirror RPC（leader-only 已由 dispatcher 外围守卫校验——提取器回填 groupId）。
   * 校验序：字段形态 → 组存在（守卫已验 leader，此处兜底）→ target 同组 → 快照存在 →
   * 在途对账 → 定向转发（ws 回调判定）→ 登记并受理。
   */
  async handleMirror(ctx: GroupHandlerContext, payload: unknown): Promise<RpcOutcome> {
    const groupId = reqStr(payload, 'groupId');
    const targetNodeId = reqStr(payload, 'targetNodeId');
    const workspaceName = reqStr(payload, 'workspaceName');
    const snapshotId = reqStr(payload, 'snapshotId');
    if (!groupId || !targetNodeId || !workspaceName || !snapshotId) {
      return { ok: false, error: err('bad_request', 'groupId/targetNodeId/workspaceName/snapshotId 缺失') };
    }
    if (workspaceName.length > WORKSPACE_NAME_MAX) {
      return { ok: false, error: err('bad_request', `workspaceName 超长（≤${WORKSPACE_NAME_MAX}）`) };
    }
    const strategy = isRecord(payload) ? payload.strategy : undefined;
    if (strategy !== 'replace') {
      return { ok: false, error: err('bad_request', "strategy 仅支持 'replace'") };
    }
    const confirmRaw = isRecord(payload) ? payload.confirm : undefined;
    if (confirmRaw !== undefined && typeof confirmRaw !== 'boolean') {
      return { ok: false, error: err('bad_request', 'confirm 必须为布尔') };
    }

    const group = this.db.prepare('SELECT 1 FROM groups WHERE id = ?').get(groupId);
    if (!group) return { ok: false, error: err('not_found', '工作组不存在') };
    const member = this.db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?')
      .get(groupId, targetNodeId);
    if (!member) return { ok: false, error: err('forbidden', '目标节点不是本组成员') };
    const snap = this.db.prepare('SELECT 1 FROM workspace_snapshots_meta WHERE snapshot_id = ?').get(snapshotId);
    if (!snap) return { ok: false, error: err('not_found', '快照不存在') };

    // 并发对账：预占在 await 之前（单线程同步段原子——两发并发不得双双过检）
    const mirrorId = randomUUID();
    const key = inFlightKey(workspaceName, targetNodeId);
    if (this.inFlight.has(key)) {
      return { ok: false, error: err('conflict', 'mirror_in_progress：同工作空间对该目标已有在途镜像') };
    }
    this.inFlight.set(key, mirrorId);

    // 定向转发（私有目标端语义）：req 信封 id=mirrorId——目标端回执按 id 对账
    const applyPayload: MirrorApplyPayload = { mirrorId, snapshotId, workspaceName, strategy };
    if (confirmRaw === true) applyPayload.confirm = true;
    const sent = await this.registry.sendTo(targetNodeId, {
      v: 1, kind: 'req', id: mirrorId, method: MIRROR_WIRE.apply, payload: applyPayload,
    });
    if (sent === 'offline') {
      this.inFlight.delete(key); // 未受理：登记回滚（不得卡死后续重试）
      return { ok: false, error: err('node_offline', '目标节点不在线') };
    }

    this.byId.set(mirrorId, {
      mirrorId, groupId, leaderNodeId: ctx.nodeId, targetNodeId, workspaceName, snapshotId,
    });
    return { ok: true, payload: { accepted: true, mirrorId } satisfies MirrorAcceptRes };
  }

  /**
   * 目标节点回执（上行 res 信封——按 id 对账 pending）。未匹配（迟到/重复/未知 id）、
   * 或来源不是该 pending 的目标节点（伪造）→ 静默忽略。成功：清登记 + 组内广播 +
   * 定向 result；失败（conflict 等）：清登记 + 错误与 diffSummary 原样透传 leader。
   */
  acceptReceipt(fromNodeId: string, res: ResponseEnvelope): void {
    const pending = this.byId.get(res.id);
    if (!pending || fromNodeId !== pending.targetNodeId) return;
    this.settle(pending);

    if (res.ok) {
      const raw = res.payload as { appliedResources?: unknown; unresolvedRefs?: unknown } | null;
      const appliedResources = Array.isArray(raw?.appliedResources) && raw.appliedResources.every((s) => typeof s === 'string')
        ? (raw.appliedResources as string[])
        : [];
      // unresolvedRefs（T8b 软失败清单）同款校验透传——缺省/异形 = 视为全部解析
      const unresolvedRefs = Array.isArray(raw?.unresolvedRefs) && raw.unresolvedRefs.every((s) => typeof s === 'string')
        ? (raw.unresolvedRefs as string[])
        : undefined;
      this.broadcastMirrored(pending, appliedResources, unresolvedRefs);
      this.notifyResult(pending, true, { mirrorId: pending.mirrorId, appliedResources, ...(unresolvedRefs ? { unresolvedRefs } : {}) });
      return;
    }
    const raw = (res.payload ?? {}) as { code?: unknown; message?: unknown; diffSummary?: unknown };
    const result: MirrorResultPayload = {
      mirrorId: pending.mirrorId,
      code: typeof raw.code === 'string' ? raw.code : 'internal',
      message: typeof raw.message === 'string' ? raw.message : '目标节点应用失败',
    };
    if (isDiffSummary(raw.diffSummary)) result.diffSummary = raw.diffSummary;
    this.notifyResult(pending, false, result);
  }

  /**
   * 节点离线清理：受理后目标断连的 pending 逐个结算——登记释放（同工作空间+目标
   * 不被卡死）+ 定向告知 leader node_offline（应用不可能再回执）。leader 自身离线
   * 无需处理——sendTo 自然丢弃，pending 随目标回执/离线正常结算。
   */
  onNodeOffline(nodeId: string): void {
    for (const pending of [...this.byId.values()]) {
      if (pending.targetNodeId !== nodeId) continue;
      this.settle(pending);
      this.notifyResult(pending, false, { mirrorId: pending.mirrorId, code: 'node_offline', message: '目标节点在镜像应用期间离线' });
    }
  }

  private settle(pending: PendingMirror): void {
    this.byId.delete(pending.mirrorId);
    this.inFlight.delete(inFlightKey(pending.workspaceName, pending.targetNodeId));
  }

  /** mirror.result 第二段（res 信封，id=mirrorId）定向回 leader。 */
  private notifyResult(pending: PendingMirror, ok: boolean, payload: MirrorResultPayload): void {
    void this.registry.sendTo(pending.leaderNodeId, {
      v: 1, kind: 'res', id: pending.mirrorId, method: MIRROR_WIRE.result, ok, payload,
    });
  }

  /** workspace.mirrored 组内广播（§3.2——组内皆可见谁被镜像到什么版本）。 */
  private broadcastMirrored(pending: PendingMirror, appliedResources: string[], unresolvedRefs?: string[]): void {
    const evt: EventEnvelope = {
      v: 1, kind: 'evt', method: EVT.workspaceMirrored,
      payload: {
        groupId: pending.groupId,
        targetNodeId: pending.targetNodeId,
        workspaceName: pending.workspaceName,
        snapshotId: pending.snapshotId,
        appliedResources,
        ...(unresolvedRefs ? { unresolvedRefs } : {}),
      } satisfies WorkspaceMirroredEvent,
    };
    const members = this.db.prepare('SELECT node_id FROM group_members WHERE group_id = ?')
      .all(pending.groupId) as Array<{ node_id: string }>;
    for (const m of members) void this.registry.sendTo(m.node_id, evt);
  }
}
