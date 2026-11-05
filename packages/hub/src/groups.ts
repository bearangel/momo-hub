// 工作组域（M2b spec 2026-10-24 §3.1/§3.2/§4/§6）——组管理六 RPC + leader-only
// 校验层（dispatcher 外围守卫）+ group.changed 组内广播。
// 角色模型：groups.leader_node_id 单点判定 leader（§8.1 表结构无 role 列的落地），
// group_members 含 leader 本人；组凭证独立于 hub enrollment token（可撤销，不复用信任根）。
import { randomBytes, randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  EVT, RPC, VIEW_RPC, PEER_DELEGATION_REQUIRED_MINOR, minorOf,
  type EventEnvelope, type GroupChangedEvent, type GroupMode, type RpcErrorPayload,
} from 'momo-hub-protocol';
import type { NodeRegistry } from './registry';

export type RpcOutcome = { ok: true; payload: unknown } | { ok: false; error: RpcErrorPayload };

export interface GroupHandlerContext {
  db: Database.Database;
  registry: NodeRegistry;
  /** 调用方节点 id（鉴权身份，非载荷自称） */
  nodeId: string;
}

/** ws RPC handler 签名：同步或异步皆可（异步首例 = M2c workspace.mirror——
 *  定向 sendTo 走 ws 回调判定后才受理）。 */
export type GroupHandler = (ctx: GroupHandlerContext, payload: unknown) => RpcOutcome | Promise<RpcOutcome>;

const err = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function reqStr(payload: unknown, key: string): string | null {
  const v = isRecord(payload) ? payload[key] : undefined;
  return typeof v === 'string' && v.length > 0 ? v : null;
}

/** 组名规范：trim 后 1..64 字符（与节点名同款约束）。 */
function validGroupName(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const n = raw.trim();
  return n.length >= 1 && n.length <= 64 ? n : null;
}

function groupRow(db: Database.Database, groupId: string): { id: string; name: string; leader_node_id: string; mode: GroupMode } | undefined {
  return db.prepare('SELECT id, name, leader_node_id, mode FROM groups WHERE id = ?').get(groupId) as
    | { id: string; name: string; leader_node_id: string; mode: GroupMode }
    | undefined;
}

/** 组成员判定（M3.2 起导出：remote-view handleView 的 groupId 路径消费）。 */
export function isMember(db: Database.Database, groupId: string, nodeId: string): boolean {
  return db.prepare('SELECT 1 FROM group_members WHERE group_id = ? AND node_id = ?').get(groupId, nodeId) !== undefined;
}

/** 组长判定（M4.2 起导出：remote-view handleTail 订阅复检的记录组路径消费——
 *  与 isMember 分工同 M3.2 双路径：复检验 leader 身份 + target 成员两半）。 */
export function isLeaderOf(db: Database.Database, groupId: string, nodeId: string): boolean {
  return db.prepare('SELECT 1 FROM groups WHERE id = ? AND leader_node_id = ?').get(groupId, nodeId) !== undefined;
}

/**
 * group.changed 组内广播（§3.2）：状态变迁落库成功后调用；逐成员定向投递，
 * 离线成员静默跳过（sendTo 判 'offline'）——事件是通知面不是账本，不积压不重放。
 */
function broadcastGroupChanged(
  ctx: GroupHandlerContext,
  groupId: string,
  kind: GroupChangedEvent['kind'],
  nodeId: string,
): void {
  const evt: EventEnvelope = {
    v: 1, kind: 'evt', method: EVT.groupChanged,
    payload: { groupId, kind, nodeId } satisfies GroupChangedEvent,
  };
  const members = ctx.db.prepare('SELECT node_id FROM group_members WHERE group_id = ?')
    .all(groupId) as Array<{ node_id: string }>;
  for (const m of members) void ctx.registry.sendTo(m.node_id, evt);
}

// ────────────────────────────────────────────────────────────────────────────
// leader-only 校验层（spec §6 权限矩阵——dispatcher 外围守卫）
// ────────────────────────────────────────────────────────────────────────────

/** leader-only 方法 → 载荷 groupId 提取器（db 供需查表回填的方法用）。映射先建全：
 *  task.cancel 按 taskId 查 delegations 回填（M2d T9）：未知 taskId 返 null 放行，
 *  由 handler 给 not_found。view 域八方法（M2e T13 回填 + M3.2 两新方法）：载荷
 *  可选 groupId 在场则优先（viewGroupExtractor——group.transfer 同模式），缺省按
 *  targetNodeId 归组——查「caller 为 leader 且 target 为成员」的组；查无
 *  （非 leader / 不同组 / 载荷坏）返 null 放行，由 remote-view handler 兜底
 *  forbidden（同码同义，§3.3 防探测不细分）。
 */
export type LeaderOnlyExtractor = (
  payload: unknown,
  db: Database.Database,
  callerNodeId: string,
) => string | null;

/** 「caller 为 leader 且 target 为成员」的公共组查询（view 域守卫与 handler 共用；
 *  MC6 多组下命中任一即可——created_at 最早者稳定取一）。 */
export function commonGroupId(
  db: Database.Database,
  callerNodeId: string,
  targetNodeId: string,
): string | undefined {
  const row = db.prepare(`
    SELECT g.id AS id FROM groups g
    JOIN group_members m ON m.group_id = g.id
    WHERE g.leader_node_id = ? AND m.node_id = ?
    ORDER BY g.created_at ASC, g.id ASC LIMIT 1
  `).get(callerNodeId, targetNodeId) as { id: string } | undefined;
  return row?.id;
}

const viewTargetGroup: LeaderOnlyExtractor = (p, db, callerNodeId) => {
  const targetNodeId = reqStr(p, 'targetNodeId');
  if (!targetNodeId) return null;
  return commonGroupId(db, callerNodeId, targetNodeId) ?? null;
};

/** view 域提取器（M3.2 §2.2）：载荷可选 groupId 在场则优先（组存在性与 leader
 *  身份由 checkGroupRole 后续校验——ghost groupId → not_found），缺省回落
 *  viewTargetGroup 归组（commonGroupId 旧路径——1.2/1.3 调用者零变化）。 */
const viewGroupExtractor: LeaderOnlyExtractor = (p, db, callerNodeId) =>
  reqStr(p, 'groupId') ?? viewTargetGroup(p, db, callerNodeId);

const LEADER_ONLY_METHODS: Record<string, LeaderOnlyExtractor> = {
  [RPC.group.transfer]: (p) => reqStr(p, 'groupId'),
  [RPC.workspace.mirror]: (p) => reqStr(p, 'groupId'),
  [RPC.task.delegate]: (p) => reqStr(p, 'groupId'),
  [RPC.task.cancel]: (p, db) => {
    const taskId = reqStr(p, 'taskId');
    if (!taskId) return null;
    const row = db.prepare('SELECT group_id FROM delegations WHERE task_id = ?').get(taskId) as
      | { group_id: string }
      | undefined;
    return row?.group_id ?? null;
  },
  // task.answer 同款 taskId 反查（M3.1）：代答权锚 delegations.group_id →
  // groups.leader_node_id（与 cancel 一致）；未知 taskId 返 null 放行 → handler not_found。
  [RPC.task.answer]: (p, db) => {
    const taskId = reqStr(p, 'taskId');
    if (!taskId) return null;
    const row = db.prepare('SELECT group_id FROM delegations WHERE task_id = ?').get(taskId) as
      | { group_id: string }
      | undefined;
    return row?.group_id ?? null;
  },
  [RPC.view.workspace]: viewGroupExtractor,
  [RPC.view.sessions]: viewGroupExtractor,
  [RPC.view.messages]: viewGroupExtractor,
  [RPC.view.tasks]: viewGroupExtractor,
  [RPC.view.subscribe]: viewGroupExtractor,
  [RPC.view.unsubscribe]: viewGroupExtractor,
  [RPC.view.workspaces]: viewGroupExtractor,
  [RPC.view.files]: viewGroupExtractor,
};

/**
 * 组角色守卫：非 leader-only 方法直通；leader-only 方法先验组存在（not_found）
 * 再验 leader 身份（非 leader / 非成员一律 forbidden，§3.3——不区分两者防探测）。
 * 1.10.0 平行组分流：peer 组 view.* 全拒（平行组无节点查看——spec §2 隐私裁定）；
 * task.delegate / task.cancel 放行为 peer 分支——delegate 的「任意成员 + 草稿载荷
 * 白名单 + 目标已绑定」与 cancel 的「发起者可取消」校验由 delegations handler
 * 承接（守卫层只做模式分流，不重复业务规则）。
 */
export function checkGroupRole(
  db: Database.Database,
  nodeId: string,
  method: string,
  payload: unknown,
): { ok: true } | { ok: false; error: RpcErrorPayload } {
  const extractor = LEADER_ONLY_METHODS[method];
  if (!extractor) return { ok: true };
  const groupId = extractor(payload, db, nodeId);
  if (groupId === null) return { ok: true };
  const group = groupRow(db, groupId);
  if (!group) return { ok: false, error: err('not_found', '工作组不存在') };
  if (group.mode === 'peer') {
    if ((Object.values(VIEW_RPC) as readonly string[]).includes(method)) {
      return { ok: false, error: err('forbidden', '平行组不提供节点查看') };
    }
    if (method === RPC.task.delegate || method === RPC.task.cancel) return { ok: true };
  }
  if (group.leader_node_id !== nodeId) {
    return { ok: false, error: err('forbidden', '仅组长（leader）可执行该操作') };
  }
  return { ok: true };
}

// ────────────────────────────────────────────────────────────────────────────
// 组管理六 RPC（spec §3.1「工作组管理」表逐行）
// ────────────────────────────────────────────────────────────────────────────

/** 绑定空间名上限（S1 NAME_MAX 同款——与 workspaceName 全线对齐）。 */
const BIND_WORKSPACE_NAME_MAX = 128;

const handleCreate: GroupHandler = (ctx, payload) => {
  const groupName = validGroupName(isRecord(payload) ? payload.groupName : undefined);
  if (!groupName) return { ok: false, error: err('bad_request', '组名非法（1-64 个字符）') };
  const modeRaw = isRecord(payload) ? payload.mode : undefined;
  const mode: GroupMode = modeRaw === 'peer' ? 'peer' : 'leader-follower';
  let bindWorkspaceName: string | null = null;
  if (mode === 'peer') {
    // 建组闸：调用方 minor ≥ 10（旧客户端不识 peer 语义，半懂建组=组内能力错位）
    const conn = ctx.registry.get(ctx.nodeId);
    const callerMinor = conn ? minorOf(conn.peerProtoVer) : null;
    if (callerMinor === null || callerMinor < PEER_DELEGATION_REQUIRED_MINOR) {
      return {
        ok: false,
        error: err('node_unsupported', `本节点协议过旧（${conn?.peerProtoVer ?? '未知'}，需 1.${PEER_DELEGATION_REQUIRED_MINOR}.0 及以上），不能创建平行组`),
      };
    }
    bindWorkspaceName = validBindName(isRecord(payload) ? payload.bindWorkspaceName : undefined);
    if (!bindWorkspaceName) {
      return { ok: false, error: err('bad_request', `平行组建组必须携带组长绑定空间（bindWorkspaceName 1-${BIND_WORKSPACE_NAME_MAX} 字符）`) };
    }
  }
  const groupId = randomUUID();
  const now = Date.now();
  const tx = ctx.db.transaction(() => {
    ctx.db.prepare('INSERT INTO groups (id, name, leader_node_id, mode, created_at) VALUES (?, ?, ?, ?, ?)')
      .run(groupId, groupName, ctx.nodeId, mode, now);
    ctx.db.prepare('INSERT INTO group_members (group_id, node_id, joined_at) VALUES (?, ?, ?)')
      .run(groupId, ctx.nodeId, now);
    if (mode === 'peer' && bindWorkspaceName !== null) {
      ctx.db.prepare(
        'INSERT INTO group_bindings (group_id, node_id, workspace_name, accepts_mirror, updated_at) VALUES (?, ?, ?, 0, ?)',
      ).run(groupId, ctx.nodeId, bindWorkspaceName, now);
    }
  });
  tx();
  broadcastGroupChanged(ctx, groupId, 'joined', ctx.nodeId);
  return { ok: true, payload: { groupId, nodeId: ctx.nodeId } };
};

/** 绑定空间名合法化：trim 后 1..128（与组名/工作空间名同款有界约束）。 */
function validBindName(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const n = raw.trim();
  return n.length >= 1 && n.length <= BIND_WORKSPACE_NAME_MAX ? n : null;
}

/** peer 组成员闸：调用方 minor ≥ 10（入组节点能力对齐——draft 链路两侧都要懂）。 */
function peerJoinGate(ctx: GroupHandlerContext): RpcOutcome | null {
  const conn = ctx.registry.get(ctx.nodeId);
  const callerMinor = conn ? minorOf(conn.peerProtoVer) : null;
  if (callerMinor === null || callerMinor < PEER_DELEGATION_REQUIRED_MINOR) {
    return {
      ok: false,
      error: err('node_unsupported', `本节点协议过旧（${conn?.peerProtoVer ?? '未知'}，需 1.${PEER_DELEGATION_REQUIRED_MINOR}.0 及以上），不能加入平行组`),
    };
  }
  return null;
}

const handleList: GroupHandler = (ctx) => {
  // 只返回调用方所在的组（§9 组间可见性不做——非成员不得枚举他人组）
  const rows = ctx.db.prepare(`
    SELECT g.id, g.name, g.leader_node_id, g.mode
    FROM groups g JOIN group_members m ON m.group_id = g.id
    WHERE m.node_id = ?
    ORDER BY g.created_at ASC, g.id ASC
  `).all(ctx.nodeId) as Array<{ id: string; name: string; leader_node_id: string; mode: GroupMode }>;
  const memberStmt = ctx.db.prepare(`
    SELECT m.node_id, COALESCE(n.name, m.node_id) AS name
    FROM group_members m LEFT JOIN nodes n ON n.id = m.node_id
    WHERE m.group_id = ?
    ORDER BY m.joined_at ASC, m.node_id ASC
  `);
  // 绑定投影（1.10.0，仅 peer 组消费——lf 组旧形状零变化）
  const bindingStmt = ctx.db.prepare(
    'SELECT workspace_name, accepts_mirror FROM group_bindings WHERE group_id = ? AND node_id = ?',
  );
  const groups = rows.map((g) => ({
    groupId: g.id,
    groupName: g.name,
    leaderNodeId: g.leader_node_id,
    mode: g.mode,
    members: (memberStmt.all(g.id) as Array<{ node_id: string; name: string }>).map((m) => {
      // protoVer（M5.3 nodeCaps 数据源）：在线成员携带 hello 上报的协议版本
      // （registry 会话内存态）；离线成员不带键——旧形状零变化。
      const conn = ctx.registry.get(m.node_id);
      const member: Record<string, unknown> = {
        nodeId: m.node_id,
        name: m.name,
        online: conn !== undefined,
        ...(conn !== undefined ? { protoVer: conn.peerProtoVer } : {}),
      };
      if (g.mode === 'peer') {
        const binding = bindingStmt.get(g.id, m.node_id) as
          | { workspace_name: string; accepts_mirror: number }
          | undefined;
        member.bound = binding !== undefined;
        if (binding !== undefined) {
          member.boundWorkspaceName = binding.workspace_name;
          member.acceptsMirror = binding.accepts_mirror === 1;
        }
      }
      return member;
    }),
  }));
  return { ok: true, payload: { groups } };
};

const handleInvite: GroupHandler = (ctx, payload) => {
  const groupId = reqStr(payload, 'groupId');
  if (!groupId) return { ok: false, error: err('bad_request', 'groupId 缺失') };
  if (!groupRow(ctx.db, groupId)) return { ok: false, error: err('not_found', '工作组不存在') };
  // invite 非 leader-only（§6 强制点清单不含它），但要求成员身份——非成员 forbidden
  if (!isMember(ctx.db, groupId, ctx.nodeId)) {
    return { ok: false, error: err('forbidden', '仅本组成员可生成邀请码') };
  }
  const code = randomBytes(24).toString('base64url');
  const tx = ctx.db.transaction(() => {
    // 再生成即撤销旧码：单组单有效码（撤销语义的落地——无独立 revoke RPC）
    ctx.db.prepare('UPDATE group_invites SET revoked = 1 WHERE group_id = ? AND revoked = 0').run(groupId);
    ctx.db.prepare('INSERT INTO group_invites (code, group_id, created_at, revoked) VALUES (?, ?, ?, 0)')
      .run(code, groupId, Date.now());
  });
  tx();
  return { ok: true, payload: { inviteCode: code } };
};

const handleJoin: GroupHandler = (ctx, payload) => {
  const inviteCode = reqStr(payload, 'inviteCode');
  if (!inviteCode) return { ok: false, error: err('bad_request', 'inviteCode 缺失') };
  const invite = ctx.db.prepare('SELECT group_id, revoked FROM group_invites WHERE code = ?')
    .get(inviteCode) as { group_id: string; revoked: number } | undefined;
  // 不存在与已撤销同款 not_found——不向组外探测者区分两者
  if (!invite || invite.revoked !== 0 || !groupRow(ctx.db, invite.group_id)) {
    return { ok: false, error: err('not_found', '邀请码无效或已被撤销') };
  }
  const groupId = invite.group_id;
  // 幂等：已是成员直接回组 id（重试安全），无状态变迁不广播
  if (isMember(ctx.db, groupId, ctx.nodeId)) return { ok: true, payload: { groupId } };
  const group = groupRow(ctx.db, groupId);
  if (group?.mode === 'peer') {
    const gate = peerJoinGate(ctx);
    if (gate !== null) return gate;
  }
  ctx.db.prepare('INSERT INTO group_members (group_id, node_id, joined_at) VALUES (?, ?, ?)')
    .run(groupId, ctx.nodeId, Date.now());
  broadcastGroupChanged(ctx, groupId, 'joined', ctx.nodeId);
  return { ok: true, payload: { groupId } };
};

const handleLeave: GroupHandler = (ctx, payload) => {
  const groupId = reqStr(payload, 'groupId');
  if (!groupId) return { ok: false, error: err('bad_request', 'groupId 缺失') };
  const group = groupRow(ctx.db, groupId);
  if (!group) return { ok: false, error: err('not_found', '工作组不存在') };
  if (!isMember(ctx.db, groupId, ctx.nodeId)) {
    return { ok: false, error: err('forbidden', '仅本组成员可退组') };
  }
  // leader 不得直接退组（防无主组）——先 transfer 再退是唯一路径
  if (group.leader_node_id === ctx.nodeId) {
    return { ok: false, error: err('forbidden', '组长需先转移组长身份再退组') };
  }
  broadcastGroupChanged(ctx, groupId, 'left', ctx.nodeId);
  const tx = ctx.db.transaction(() => {
    ctx.db.prepare('DELETE FROM group_members WHERE group_id = ? AND node_id = ?').run(groupId, ctx.nodeId);
    // 退组同步清绑定（防幽灵绑定——离组成员不再可被委派/镜像，登记随之失效）
    ctx.db.prepare('DELETE FROM group_bindings WHERE group_id = ? AND node_id = ?').run(groupId, ctx.nodeId);
  });
  tx();
  return { ok: true, payload: { ok: true } };
};

const handleTransfer: GroupHandler = (ctx, payload) => {
  // leader 身份已由外围守卫校验（transfer 在 LEADER_ONLY_METHODS）
  const groupId = reqStr(payload, 'groupId');
  const toNodeId = reqStr(payload, 'toNodeId');
  if (!groupId || !toNodeId) return { ok: false, error: err('bad_request', 'groupId/toNodeId 缺失') };
  if (!isMember(ctx.db, groupId, toNodeId)) {
    return { ok: false, error: err('forbidden', '目标节点不是本组成员') };
  }
  // 自转移幂等：无状态变迁不落库不广播
  if (toNodeId === ctx.nodeId) return { ok: true, payload: { ok: true } };
  ctx.db.prepare('UPDATE groups SET leader_node_id = ? WHERE id = ?').run(toNodeId, groupId);
  broadcastGroupChanged(ctx, groupId, 'leaderChanged', toNodeId);
  return { ok: true, payload: { ok: true } };
};

// ── 绑定三 RPC（1.10.0 平行模式——minor 10 门槛由分发层 METHOD_REQUIRED_MINOR 守卫）──

/** peer 组绑定前置校验：组存在 ∧ peer 模式 ∧ 调用方是成员。 */
function peerMemberCtx(
  ctx: GroupHandlerContext,
  groupId: string,
): { ok: true; group: { id: string; name: string; leader_node_id: string; mode: GroupMode } } | { ok: false; error: RpcErrorPayload } {
  const group = groupRow(ctx.db, groupId);
  if (!group) return { ok: false, error: err('not_found', '工作组不存在') };
  if (group.mode !== 'peer') {
    return { ok: false, error: err('bad_request', '仅平行组支持承接空间绑定（主从组走按名镜像）') };
  }
  if (!isMember(ctx.db, groupId, ctx.nodeId)) {
    return { ok: false, error: err('forbidden', '仅本组成员可绑定承接空间') };
  }
  return { ok: true, group };
}

const handleBind: GroupHandler = (ctx, payload) => {
  const groupId = reqStr(payload, 'groupId');
  const workspaceName = validBindName(isRecord(payload) ? payload.workspaceName : undefined);
  if (!groupId) return { ok: false, error: err('bad_request', 'groupId 缺失') };
  if (!workspaceName) {
    return { ok: false, error: err('bad_request', `workspaceName 非法（1-${BIND_WORKSPACE_NAME_MAX} 字符）`) };
  }
  const guard = peerMemberCtx(ctx, groupId);
  if (!guard.ok) return guard;
  // 幂等换绑：最后一次生效；accepts_mirror 保留既有值（换空间不重置开关语义）
  ctx.db.prepare(`
    INSERT INTO group_bindings (group_id, node_id, workspace_name, accepts_mirror, updated_at)
    VALUES (?, ?, ?, COALESCE((SELECT accepts_mirror FROM group_bindings WHERE group_id = ? AND node_id = ?), 0), ?)
    ON CONFLICT(group_id, node_id) DO UPDATE SET
      workspace_name = excluded.workspace_name,
      accepts_mirror = excluded.accepts_mirror,
      updated_at = excluded.updated_at
  `).run(groupId, ctx.nodeId, workspaceName, groupId, ctx.nodeId, Date.now());
  broadcastGroupChanged(ctx, groupId, 'bound', ctx.nodeId);
  return { ok: true, payload: { ok: true } };
};

const handleUnbind: GroupHandler = (ctx, payload) => {
  const groupId = reqStr(payload, 'groupId');
  if (!groupId) return { ok: false, error: err('bad_request', 'groupId 缺失') };
  const guard = peerMemberCtx(ctx, groupId);
  if (!guard.ok) return guard;
  ctx.db.prepare('DELETE FROM group_bindings WHERE group_id = ? AND node_id = ?').run(groupId, ctx.nodeId);
  broadcastGroupChanged(ctx, groupId, 'bound', ctx.nodeId);
  return { ok: true, payload: { ok: true } };
};

const handleSetMirrorAccept: GroupHandler = (ctx, payload) => {
  const groupId = reqStr(payload, 'groupId');
  const accepts = isRecord(payload) ? payload.accepts : undefined;
  if (!groupId) return { ok: false, error: err('bad_request', 'groupId 缺失') };
  if (typeof accepts !== 'boolean') return { ok: false, error: err('bad_request', 'accepts 必须为布尔') };
  const guard = peerMemberCtx(ctx, groupId);
  if (!guard.ok) return guard;
  const bound = ctx.db.prepare('SELECT 1 FROM group_bindings WHERE group_id = ? AND node_id = ?')
    .get(groupId, ctx.nodeId);
  if (!bound) {
    return { ok: false, error: err('bad_request', '未绑定承接空间，无法设置镜像开关') };
  }
  ctx.db.prepare('UPDATE group_bindings SET accepts_mirror = ?, updated_at = ? WHERE group_id = ? AND node_id = ?')
    .run(accepts ? 1 : 0, Date.now(), groupId, ctx.nodeId);
  broadcastGroupChanged(ctx, groupId, 'bound', ctx.nodeId);
  return { ok: true, payload: { ok: true } };
};

/** 组管理 + 绑定 RPC 分发表（wire 方法名 → handler）。 */
export const GROUP_HANDLERS: Record<string, GroupHandler> = {
  [RPC.group.create]: handleCreate,
  [RPC.group.list]: handleList,
  [RPC.group.invite]: handleInvite,
  [RPC.group.join]: handleJoin,
  [RPC.group.leave]: handleLeave,
  [RPC.group.transfer]: handleTransfer,
  [RPC.group.bind]: handleBind,
  [RPC.group.unbind]: handleUnbind,
  [RPC.group.setMirrorAccept]: handleSetMirrorAccept,
};
