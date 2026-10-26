// hub 委派账本与 task.* RPC 契约锁（M2d spec §3.1/§3.2/§4/§6 + T9 + MC1/MC5）——
// 真 socket 双客户端（leader + 目标节点）全真 wire 语义：
// canTransition 状态机全迁移矩阵（36 对，Review Focus #3）/ delegate 全链
// （task.assigned 六字段定向 + 账本 pending）/ 离线拒派（裁定①）/ cancel 三态
// + 终态 conflict（裁定②）/ task.report 状态机 wire 矩阵（合法迁移 + 全非法拒
// ——账本不动、连接不炸）/ interrupted 自动迁移（断连）与续传（MC5）/ declined
// 与 local_session_lost 附因 / task.list 过滤 / leader+同组守卫矩阵 / 上报
// 健壮性（未知 id / 伪造来源 / 异形载荷 / ok=false）/ 审计日志。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import { canTransition, DELEGATION_STATUSES } from '../src/delegations';
import { logger } from '../src/logger';
import {
  EVT, PROTOCOL_VERSION, RPC, TASK_WIRE, encodeEnvelope, makeRequest, parseEnvelope,
  type DelegationStatus, type Envelope,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-dele-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let wsUrl: string;

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  const baseUrl = await server.listen(0, '127.0.0.1');
  wsUrl = baseUrl.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => {
  // close() 内置 close 处理器排空（M2d 起 onNodeOffline 查库——晚到事件不得越界）
  await server.close();
  db.close();
});

// ── 工具（mirror.test.ts 同款客户端骨架）────────────────────────────────────

function connect(name: string) {
  return new Promise<{ ws: WebSocket; nodeId: string; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enrollNode(db, identity.enrollmentToken, name);
    if ('error' in cred) throw new Error('enroll 失败');
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const inbox: Envelope[] = [];
    const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
    ws.on('message', (data) => {
      const e = parseEnvelope(String(data));
      if (!e) return;
      inbox.push(e);
      const i = waiters.findIndex((w) => w.match(e));
      if (i >= 0) waiters.splice(i, 1)[0]!.resolve(e);
    });
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: name }))));
    ws.on('error', reject);
    const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
      new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
    void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
      if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
      else resolve({ ws, nodeId: cred.nodeId, inbox, next });
    });
  });
}

type Client = Awaited<ReturnType<typeof connect>>;

function callRpc(client: Client, method: string, payload: unknown): Promise<Envelope> {
  const env = makeRequest(method, payload);
  client.ws.send(encodeEnvelope(env));
  return client.next((e) => e.kind === 'res' && e.id === env.id);
}

function resPayload(e: Envelope): Record<string, unknown> {
  if (e.kind !== 'res') throw new Error('非 res 信封');
  return (e.payload ?? {}) as Record<string, unknown>;
}

function errOf(e: Envelope): { code: string; message: string } {
  if (e.kind !== 'res' || e.ok) throw new Error('预期错误响应');
  return e.payload as { code: string; message: string };
}

/** 建组：leader（创建者）+ target（凭码入组）+ outsider（组外）。 */
async function setupGroup(pref: string): Promise<{ leader: Client; target: Client; outsider: Client; groupId: string }> {
  const leader = await connect(`${pref}-leader`);
  const target = await connect(`${pref}-target`);
  const outsider = await connect(`${pref}-outsider`);
  const created = await callRpc(leader, RPC.group.create, { groupName: `Team-${pref}` });
  const { groupId } = resPayload(created) as { groupId: string };
  const invited = await callRpc(leader, RPC.group.invite, { groupId });
  const { inviteCode } = resPayload(invited) as { inviteCode: string };
  await callRpc(target, RPC.group.join, { inviteCode });
  await leader.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
    && (e.payload as { nodeId: string; kind: string }).nodeId === target.nodeId);
  return { leader, target, outsider, groupId };
}

interface LedgerRow {
  task_id: string; group_id: string; workspace_name: string; target_node_id: string;
  by_node_id: string; title: string; status: DelegationStatus;
  session_id: string | null; created_at: number; updated_at: number;
}

function ledgerRow(taskId: string): LedgerRow | undefined {
  return db.prepare('SELECT * FROM delegations WHERE task_id = ?').get(taskId) as LedgerRow | undefined;
}

function delegateReq(groupId: string, targetNodeId: string, opts: { workspaceName?: string; title?: string; prompt?: string; agentSlug?: string; permissionTier?: unknown } = {}): Record<string, unknown> {
  const task: Record<string, unknown> = { title: opts.title ?? '修登录 bug', prompt: opts.prompt ?? '请修复登录失败问题' };
  if (opts.agentSlug !== undefined) task.agentSlug = opts.agentSlug;
  if (opts.permissionTier !== undefined) task.permissionTier = opts.permissionTier;
  return { groupId, targetNodeId, workspaceName: opts.workspaceName ?? 'ws-t', task };
}

async function delegateOk(leader: Client, req: Record<string, unknown>): Promise<string> {
  const res = await callRpc(leader, RPC.task.delegate, req);
  if (!(res.kind === 'res' && res.ok)) throw new Error(`delegate 失败：${JSON.stringify(res.payload)}`);
  return (resPayload(res) as { taskId: string }).taskId;
}

/** 目标节点发 task.report（res 信封 id=taskId——wire 约定）。 */
function report(ws: WebSocket, taskId: string, payload: Record<string, unknown>, ok = true): void {
  ws.send(encodeEnvelope({ v: 1, kind: 'res', id: taskId, method: TASK_WIRE.report, ok, payload }));
}

const settle = (ms = 200): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ────────────────────────────────────────────────────────────────────────────
// 状态机纯函数矩阵（Review Focus #3——36 对全覆盖）
// ────────────────────────────────────────────────────────────────────────────

describe('canTransition 全迁移矩阵', () => {
  const LEGAL: Array<[DelegationStatus, DelegationStatus]> = [
    ['pending', 'running'], ['pending', 'failed'], ['pending', 'cancelled'], ['pending', 'interrupted'],
    ['running', 'done'], ['running', 'failed'], ['running', 'cancelled'], ['running', 'interrupted'],
    ['interrupted', 'running'], ['interrupted', 'failed'], ['interrupted', 'cancelled'],
  ];

  it('11 个合法对放行', () => {
    for (const [from, to] of LEGAL) {
      expect(canTransition(from, to), `${from}→${to}`).toBe(true);
    }
    expect(LEGAL).toHaveLength(11);
  });

  it('25 个非法对全拒（含自迁移与终态出场）', () => {
    for (const from of DELEGATION_STATUSES) {
      for (const to of DELEGATION_STATUSES) {
        const legal = LEGAL.some(([f, t]) => f === from && t === to);
        if (legal) continue;
        expect(canTransition(from, to), `${from}→${to}`).toBe(false);
      }
    }
  });
});

// ────────────────────────────────────────────────────────────────────────────
// task.delegate 全链
// ────────────────────────────────────────────────────────────────────────────

describe('task.delegate 全链', () => {
  it('受理返 {taskId}；task.assigned 定向目标（六字段精确）；账本 pending 落档全字段', async () => {
    const { leader, target, outsider, groupId } = await setupGroup('del');
    const res = await callRpc(leader, RPC.task.delegate,
      delegateReq(groupId, target.nodeId, { workspaceName: 'ws-del', agentSlug: 'coder-pro' }));
    expect(res.kind === 'res' && res.ok).toBe(true);
    const taskId = (resPayload(res) as { taskId: string }).taskId;
    expect(taskId).toMatch(/^[0-9a-f-]{36}$/);

    const assigned = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned);
    expect(assigned.payload).toEqual({
      taskId,
      workspaceName: 'ws-del',
      task: { title: '修登录 bug', prompt: '请修复登录失败问题', agentSlug: 'coder-pro' },
      byNode: leader.nodeId,
    });
    // 定向：leader/outsider 不收 task.assigned
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskAssigned)).toHaveLength(0);
    expect(outsider.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskAssigned)).toHaveLength(0);

    const row = ledgerRow(taskId);
    expect(row).toMatchObject({
      group_id: groupId, workspace_name: 'ws-del', target_node_id: target.nodeId,
      by_node_id: leader.nodeId, title: '修登录 bug', status: 'pending', session_id: null,
    });
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('无 agentSlug → assigned 载荷不含该键（wire 精确性）', async () => {
    const { leader, target, groupId } = await setupGroup('delns');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-ns' }));
    const assigned = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned);
    expect((assigned.payload as { task: Record<string, unknown> }).task).toEqual({
      title: '修登录 bug', prompt: '请修复登录失败问题',
    });
    leader.ws.close(); target.ws.close();
  });

  // ── permissionTier 透传（M2.1 T10b：HITL 权限档位随任务书定向到目标节点）──

  it('permissionTier 合法三值（readonly/sandbox/confirm）均透传——assigned 载荷 task 含原值', async () => {
    const { leader, target, groupId } = await setupGroup('deltier');
    for (const tier of ['readonly', 'sandbox', 'confirm'] as const) {
      const res = await callRpc(leader, RPC.task.delegate,
        delegateReq(groupId, target.nodeId, { workspaceName: `ws-tier-${tier}`, permissionTier: tier }));
      expect(res.kind === 'res' && res.ok, `档位 ${tier} 应受理`).toBe(true);
      const assigned = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned
        && (e.payload as { workspaceName: string }).workspaceName === `ws-tier-${tier}`);
      expect((assigned.payload as { task: Record<string, unknown> }).task).toEqual({
        title: '修登录 bug', prompt: '请修复登录失败问题', permissionTier: tier,
      });
    }
    leader.ws.close(); target.ws.close();
  });

  it('permissionTier 非法值丢弃不抛（未知容忍，先例 modelBinding）：assigned 载荷不含该键，title/prompt/agentSlug 完好', async () => {
    const { leader, target, groupId } = await setupGroup('deltbad');
    for (const bad of ['foo', 123] as const) {
      const res = await callRpc(leader, RPC.task.delegate,
        delegateReq(groupId, target.nodeId, {
          workspaceName: 'ws-tier-bad', agentSlug: 'coder-pro',
          title: '排查崩溃', prompt: '分析 dump 定位根因', permissionTier: bad,
        }));
      // 丢弃 ≠ 拒绝：委派照常受理（零授权缺省语义在目标侧，hub 只剥非法档位）
      expect(res.kind === 'res' && res.ok, `非法值 ${JSON.stringify(bad)} 应丢弃而非拒绝`).toBe(true);
      const taskId = (resPayload(res) as { taskId: string }).taskId;
      const assigned = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned
        && (e.payload as { taskId: string }).taskId === taskId);
      expect((assigned.payload as { task: Record<string, unknown> }).task).toEqual({
        title: '排查崩溃', prompt: '分析 dump 定位根因', agentSlug: 'coder-pro',
      });
      expect((assigned.payload as { task: Record<string, unknown> }).task).not.toHaveProperty('permissionTier');
    }
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 离线拒派（裁定①）
// ────────────────────────────────────────────────────────────────────────────

describe('离线拒派', () => {
  it('目标离线 → node_offline 且不落档；重发仍 node_offline（无幽灵 pending）；重连后可派', async () => {
    const { leader, target, groupId } = await setupGroup('doff');
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === target.nodeId);

    const res = await callRpc(leader, RPC.task.delegate, delegateReq(groupId, target.nodeId));
    expect(errOf(res).code).toBe('node_offline');
    const again = await callRpc(leader, RPC.task.delegate, delegateReq(groupId, target.nodeId));
    expect(errOf(again).code).toBe('node_offline');
    expect((db.prepare("SELECT COUNT(*) AS n FROM delegations WHERE target_node_id = ?").get(target.nodeId) as { n: number }).n).toBe(0);

    const back = await connect('doff-target');
    const ok = await callRpc(leader, RPC.task.delegate, delegateReq(groupId, target.nodeId));
    expect(ok.kind === 'res' && ok.ok).toBe(true);
    leader.ws.close(); back.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// task.report wire 状态机矩阵（合法迁移 + 全非法拒——账本不动 + 连接不炸）
// ────────────────────────────────────────────────────────────────────────────

describe('task.report wire 矩阵', () => {
  it('pending：done 拒（账本不动、无事件）→ running 收（task.progress + session_id 回填）', async () => {
    const { leader, target, groupId } = await setupGroup('rpend');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));

    report(target.ws, taskId, { status: 'done' });
    await settle();
    expect(ledgerRow(taskId)?.status).toBe('pending');
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskCompleted)).toHaveLength(0);

    report(target.ws, taskId, { status: 'running', sessionId: 'sess-1', progress: { lastSeq: 3, excerpt: '开始分析' } });
    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    expect(prog.payload).toEqual({ taskId, sessionId: 'sess-1', lastSeq: 3, excerpt: '开始分析' });
    expect(ledgerRow(taskId)).toMatchObject({ status: 'running', session_id: 'sess-1' });
    leader.ws.close(); target.ws.close();
  });

  it('running：进度拍（同态上报 status 不动、progress 直转）→ done 收（task.completed + 账本 done）', async () => {
    const { leader, target, groupId } = await setupGroup('rrun');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    report(target.ws, taskId, { status: 'running', sessionId: 'sess-2', progress: { lastSeq: 1, excerpt: 'a' } });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);

    report(target.ws, taskId, { status: 'running', progress: { lastSeq: 9, excerpt: ' nearing' } });
    const beat = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { lastSeq: number }).lastSeq === 9);
    expect(beat.payload).toEqual({ taskId, sessionId: 'sess-2', lastSeq: 9, excerpt: ' nearing' });
    expect(ledgerRow(taskId)?.status).toBe('running');

    report(target.ws, taskId, { status: 'done' });
    const done = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted);
    expect(done.payload).toEqual({ taskId });
    expect(ledgerRow(taskId)?.status).toBe('done');
    leader.ws.close(); target.ws.close();
  });

  it('interrupted：done 拒 → running 收（续传窗口，MC5）', async () => {
    const { leader, target, groupId } = await setupGroup('rint');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    report(target.ws, taskId, { status: 'running', sessionId: 'sess-3' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterrupted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(ledgerRow(taskId)?.status).toBe('interrupted');

    const back = await connect('rint-target');
    report(back.ws, taskId, { status: 'done' });
    await settle();
    expect(ledgerRow(taskId)?.status).toBe('interrupted');
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskCompleted)).toHaveLength(0);

    report(back.ws, taskId, { status: 'running', sessionId: 'sess-3', progress: { lastSeq: 20, excerpt: '续传' } });
    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { lastSeq: number }).lastSeq === 20);
    expect(prog.payload).toEqual({ taskId, sessionId: 'sess-3', lastSeq: 20, excerpt: '续传' });
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); back.ws.close();
  });

  it('done/failed/cancelled 终态：running/failed/done 上报全拒（账本不动、无事件）', async () => {
    const { leader, target, groupId } = await setupGroup('rterm');
    // done 态
    const tDone = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-d1' }));
    report(target.ws, tDone, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === tDone);
    report(target.ws, tDone, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted
      && (e.payload as { taskId: string }).taskId === tDone);
    // failed 态（pending→failed 合法）
    const tFailed = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-d2' }));
    report(target.ws, tFailed, { status: 'failed', reason: 'declined' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed
      && (e.payload as { taskId: string }).taskId === tFailed);
    // cancelled 态
    const tCancel = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-d3' }));
    await callRpc(leader, RPC.task.cancel, { taskId: tCancel });

    for (const [name, taskId] of [['done', tDone], ['failed', tFailed], ['cancelled', tCancel]] as const) {
      report(target.ws, taskId, { status: 'running' });
      report(target.ws, taskId, { status: 'done' });
      report(target.ws, taskId, { status: 'failed', reason: 'x' });
    }
    await settle();
    expect(ledgerRow(tDone)?.status, 'done 态').toBe('done');
    expect(ledgerRow(tFailed)?.status, 'failed 态').toBe('failed');
    expect(ledgerRow(tCancel)?.status, 'cancelled 态').toBe('cancelled');
    // 拒绝的上报不产生任何新事件：inbox 只增不减，settle 后计数不得增长
    const evtCount = (): number => leader.inbox.filter((e) => e.kind === 'evt'
      && [EVT.taskProgress, EVT.taskCompleted, EVT.taskFailed].includes(e.method)
      && [tDone, tFailed, tCancel].includes((e.payload as { taskId: string }).taskId)).length;
    const beforeCount = evtCount();
    await settle(150);
    expect(evtCount()).toBe(beforeCount);
    leader.ws.close(); target.ws.close();
  });

  it('failed 附因透传：declined（目标拒派，§5.3）与 local_session_lost（续传丢失）', async () => {
    const { leader, target, groupId } = await setupGroup('rfail');
    const tDeclined = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-f1' }));
    report(target.ws, tDeclined, { status: 'failed', reason: 'declined' });
    const failed1 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed
      && (e.payload as { taskId: string }).taskId === tDeclined);
    expect(failed1.payload).toEqual({ taskId: tDeclined, reason: 'declined' });
    expect(ledgerRow(tDeclined)).toMatchObject({ status: 'failed' });

    // interrupted → failed('local_session_lost')：断连 → 重连上报失败
    const tLost = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-f2' }));
    report(target.ws, tLost, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === tLost);
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterrupted
      && (e.payload as { taskId: string }).taskId === tLost);
    const back = await connect('rfail-target');
    report(back.ws, tLost, { status: 'failed', reason: 'local_session_lost' });
    const failed2 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed
      && (e.payload as { taskId: string }).taskId === tLost);
    expect(failed2.payload).toEqual({ taskId: tLost, reason: 'local_session_lost' });
    expect(ledgerRow(tLost)?.status).toBe('failed');
    leader.ws.close(); back.ws.close();
  });

  it('重复终态上报：静默忽略（不重复转发事件）', async () => {
    const { leader, target, groupId } = await setupGroup('rdup');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    report(target.ws, taskId, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    report(target.ws, taskId, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted);
    report(target.ws, taskId, { status: 'done' });
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskCompleted)).toHaveLength(1);
    expect(ledgerRow(taskId)?.status).toBe('done');
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// interrupted 自动迁移（节点断连，MC5）
// ────────────────────────────────────────────────────────────────────────────

describe('interrupted 自动迁移', () => {
  it('目标断连 → 该节点 pending 与 running 委派均迁 interrupted + 各自定向 task.interrupted；终态不动', async () => {
    const { leader, target, groupId } = await setupGroup('iauto');
    const tRun = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-i1' }));
    report(target.ws, tRun, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    const tPend = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-i2' }));
    const tDone = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-i3' }));
    report(target.ws, tDone, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === tDone);
    report(target.ws, tDone, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted
      && (e.payload as { taskId: string }).taskId === tDone);

    target.ws.close();
    const e1 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterrupted);
    const e2 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterrupted
      && e1 !== e && (e.payload as { taskId: string }).taskId !== (e1.payload as { taskId: string }).taskId);
    const interruptedIds = new Set([e1, e2].map((e) => (e.payload as { taskId: string }).taskId));
    expect(interruptedIds).toEqual(new Set([tRun, tPend]));
    expect(ledgerRow(tRun)?.status).toBe('interrupted');
    expect(ledgerRow(tPend)?.status).toBe('interrupted');
    expect(ledgerRow(tDone)?.status).toBe('done'); // 终态不动
    leader.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// task.cancel（裁定②：终态 conflict）
// ────────────────────────────────────────────────────────────────────────────

describe('task.cancel', () => {
  it('pending/running 可取消：账本 cancelled + 目标定向收 task.cancelled；interrupted 态（目标离线）取消照样成立', async () => {
    const { leader, target, groupId } = await setupGroup('cancel');
    const tPend = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-c1' }));
    const r1 = await callRpc(leader, RPC.task.cancel, { taskId: tPend });
    expect(r1.kind === 'res' && r1.ok).toBe(true);
    expect(resPayload(r1)).toEqual({ ok: true });
    expect(ledgerRow(tPend)?.status).toBe('cancelled');
    const evt1 = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskCancelled
      && (e.payload as { taskId: string }).taskId === tPend);
    expect(evt1.payload).toEqual({ taskId: tPend });

    const tRun = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-c2' }));
    report(target.ws, tRun, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === tRun);
    const r2 = await callRpc(leader, RPC.task.cancel, { taskId: tRun });
    expect(r2.kind === 'res' && r2.ok).toBe(true);
    expect(ledgerRow(tRun)?.status).toBe('cancelled');
    await target.next((e) => e.kind === 'evt' && e.method === EVT.taskCancelled
      && (e.payload as { taskId: string }).taskId === tRun);

    // interrupted：目标已断连——取消仍成立（通知静默丢弃，账本是事实）
    const tInt = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-c3' }));
    report(target.ws, tInt, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === tInt);
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterrupted
      && (e.payload as { taskId: string }).taskId === tInt);
    const r3 = await callRpc(leader, RPC.task.cancel, { taskId: tInt });
    expect(r3.kind === 'res' && r3.ok).toBe(true);
    expect(ledgerRow(tInt)?.status).toBe('cancelled');
    leader.ws.close();
  });

  it('终态取消 → conflict（显真实态，裁定②）；未知 taskId → not_found；缺 taskId → bad_request', async () => {
    const { leader, target, groupId } = await setupGroup('cconf');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    report(target.ws, taskId, { status: 'failed', reason: 'declined' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed);

    const conf = await callRpc(leader, RPC.task.cancel, { taskId });
    const confErr = errOf(conf);
    expect(confErr.code).toBe('conflict');
    expect(confErr.message).toContain('failed');

    const ghost = await callRpc(leader, RPC.task.cancel, { taskId: 'no-such-task' });
    expect(errOf(ghost).code).toBe('not_found');
    const bad = await callRpc(leader, RPC.task.cancel, {});
    expect(errOf(bad).code).toBe('bad_request');
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// task.list 过滤
// ────────────────────────────────────────────────────────────────────────────

describe('task.list', () => {
  it('组过滤 + 状态过滤 + 成员可查（非 leader）；缺省 groupId=自己所在组；非本组 forbidden；ghost not_found；坏 status bad_request', async () => {
    const { leader, target, groupId } = await setupGroup('list');
    const other = await connect('list-other');
    const otherCreated = await callRpc(other, RPC.group.create, { groupName: 'Team-list-other' });
    const otherGroupId = (resPayload(otherCreated) as { groupId: string }).groupId;

    const t1 = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-l1' })); // pending
    const t2 = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-l2' })); // → running
    report(target.ws, t2, { status: 'running', sessionId: 's' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    const t3 = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-l3' })); // → failed
    report(target.ws, t3, { status: 'failed', reason: 'declined' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed);
    await delegateOk(other, delegateReq(otherGroupId, other.nodeId, { workspaceName: 'ws-lo' })); // 别组的

    // 组过滤（成员视角——target 是成员非 leader）
    const byGroup = await callRpc(target, RPC.task.list, { groupId });
    expect(byGroup.kind === 'res' && byGroup.ok).toBe(true);
    const tasks = (resPayload(byGroup) as { tasks: Array<Record<string, unknown>> }).tasks;
    expect(tasks.map((t) => t.taskId).sort()).toEqual([t1, t2, t3].sort());
    const rec = tasks.find((t) => t.taskId === t2)!;
    expect(rec).toMatchObject({ workspaceName: 'ws-l2', targetNodeId: target.nodeId, status: 'running', title: '修登录 bug' });
    expect(Object.keys(rec).sort()).toEqual(['createdAt', 'status', 'targetNodeId', 'taskId', 'title', 'updatedAt', 'workspaceName']);

    // 状态过滤
    const runningOnly = await callRpc(leader, RPC.task.list, { groupId, status: 'running' });
    expect((resPayload(runningOnly) as { tasks: Array<{ taskId: string }> }).tasks.map((t) => t.taskId)).toEqual([t2]);
    const pendingOnly = await callRpc(target, RPC.task.list, { groupId, status: 'pending' });
    expect((resPayload(pendingOnly) as { tasks: Array<{ taskId: string }> }).tasks.map((t) => t.taskId)).toEqual([t1]);

    // 缺省 groupId：leader 自己所在组（本组三条；不在别组）
    const mine = await callRpc(leader, RPC.task.list, {});
    expect((resPayload(mine) as { tasks: Array<{ taskId: string }> }).tasks.map((t) => t.taskId).sort()).toEqual([t1, t2, t3].sort());

    // 非本组成员查他人组 → forbidden；ghost 组 → not_found；坏 status → bad_request
    const byTargetOther = await callRpc(target, RPC.task.list, { groupId: otherGroupId });
    expect(errOf(byTargetOther).code).toBe('forbidden');
    const ghost = await callRpc(leader, RPC.task.list, { groupId: 'no-such-group' });
    expect(errOf(ghost).code).toBe('not_found');
    const badStatus = await callRpc(leader, RPC.task.list, { groupId, status: 'finished' });
    expect(errOf(badStatus).code).toBe('bad_request');

    // 无组节点 → 空列表（非错）
    const loner = await connect('list-loner');
    const empty = await callRpc(loner, RPC.task.list, {});
    expect(resPayload(empty)).toEqual({ tasks: [] });
    leader.ws.close(); target.ws.close(); other.ws.close(); loner.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// leader + 同组守卫矩阵
// ────────────────────────────────────────────────────────────────────────────

describe('leader + 同组守卫矩阵', () => {
  it('delegate：非 leader 成员/非成员调用 → forbidden；ghost 组 → not_found；目标非本组成员 → forbidden；坏载荷 → bad_request', async () => {
    const { leader, target, outsider, groupId } = await setupGroup('guard');
    const byTarget = await callRpc(target, RPC.task.delegate, delegateReq(groupId, target.nodeId));
    expect(errOf(byTarget).code).toBe('forbidden');
    const byOutsider = await callRpc(outsider, RPC.task.delegate, delegateReq(groupId, target.nodeId));
    expect(errOf(byOutsider).code).toBe('forbidden');
    const ghost = await callRpc(leader, RPC.task.delegate, delegateReq('no-such-group', target.nodeId));
    expect(errOf(ghost).code).toBe('not_found');
    const toOutsider = await callRpc(leader, RPC.task.delegate, delegateReq(groupId, outsider.nodeId));
    expect(errOf(toOutsider).code).toBe('forbidden');

    const bads: Array<{ name: string; payload: Record<string, unknown> }> = [
      { name: '缺 groupId', payload: delegateReq('', target.nodeId) },
      { name: '缺 targetNodeId', payload: delegateReq(groupId, '') },
      { name: '缺 workspaceName', payload: delegateReq(groupId, target.nodeId, { workspaceName: '' }) },
      { name: '超长 workspaceName（>128）', payload: delegateReq(groupId, target.nodeId, { workspaceName: 'w'.repeat(129) }) },
      { name: '缺 task', payload: { groupId, targetNodeId: target.nodeId, workspaceName: 'ws' } },
      { name: 'task 缺 title', payload: { groupId, targetNodeId: target.nodeId, workspaceName: 'ws', task: { prompt: 'p' } } },
      { name: 'title 超长（>128）', payload: delegateReq(groupId, target.nodeId, { title: 't'.repeat(129) }) },
      { name: 'prompt 为空', payload: delegateReq(groupId, target.nodeId, { prompt: '' }) },
      { name: 'prompt 超长（>16KB）', payload: delegateReq(groupId, target.nodeId, { prompt: 'p'.repeat(16 * 1024 + 1) }) },
      { name: 'agentSlug 超长（>64）', payload: delegateReq(groupId, target.nodeId, { agentSlug: 'a'.repeat(65) }) },
    ];
    for (const c of bads) {
      const res = await callRpc(leader, RPC.task.delegate, c.payload);
      expect(errOf(res).code, `用例：${c.name}`).toBe('bad_request');
    }
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('cancel：非 leader（成员/组外）→ forbidden；leader 可取消他人委派的任务（角色以当前 groups.leader 为准）', async () => {
    const { leader, target, outsider, groupId } = await setupGroup('cguard');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    const byTarget = await callRpc(target, RPC.task.cancel, { taskId });
    expect(errOf(byTarget).code).toBe('forbidden');
    const byOutsider = await callRpc(outsider, RPC.task.cancel, { taskId });
    expect(errOf(byOutsider).code).toBe('forbidden');
    expect(ledgerRow(taskId)?.status).toBe('pending');

    // 转移 leader 后新 leader 可取消（守卫以 groups.leader_node_id 单点判定）
    const joined = await callRpc(leader, RPC.group.invite, { groupId });
    await callRpc(outsider, RPC.group.join, { inviteCode: (resPayload(joined) as { inviteCode: string }).inviteCode });
    const transfer = await callRpc(leader, RPC.group.transfer, { groupId, toNodeId: outsider.nodeId });
    expect(transfer.kind === 'res' && transfer.ok).toBe(true);
    const byNewLeader = await callRpc(outsider, RPC.task.cancel, { taskId });
    expect(byNewLeader.kind === 'res' && byNewLeader.ok).toBe(true);
    expect(ledgerRow(taskId)?.status).toBe('cancelled');
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 上报健壮性（未知 id / 伪造来源 / 异形载荷 / ok=false——账本不动、连接不炸）
// ────────────────────────────────────────────────────────────────────────────

describe('上报健壮性', () => {
  it('未知 taskId / 非目标节点伪造 / 异形载荷 / ok=false → 忽略；随后合法上报照常工作（连接未炸）', async () => {
    const { leader, target, outsider, groupId } = await setupGroup('rob');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));

    report(target.ws, 'bogus-task-id', { status: 'running' });
    report(outsider.ws, taskId, { status: 'done' }); // 伪造：outsider 非目标节点
    report(target.ws, taskId, { status: 'cancelled' }); // status 非上报枚举
    report(target.ws, taskId, {}); // 缺 status
    report(target.ws, taskId, { status: 'running', progress: { lastSeq: -1, excerpt: 'x' } }); // lastSeq 非法
    report(target.ws, taskId, { status: 'running' }, false); // ok=false
    await settle();

    expect(ledgerRow(taskId)).toMatchObject({ status: 'pending', session_id: null });
    expect(leader.inbox.filter((e) => e.kind === 'evt'
      && [EVT.taskProgress, EVT.taskCompleted, EVT.taskFailed, EVT.taskInterrupted].includes(e.method))).toHaveLength(0);

    // 连接不炸：合法上报照常受理
    report(target.ws, taskId, { status: 'running', sessionId: 'sess-ok', progress: { lastSeq: 2, excerpt: 'ok' } });
    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    expect(prog.payload).toEqual({ taskId, sessionId: 'sess-ok', lastSeq: 2, excerpt: 'ok' });
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('RM2-7：excerpt="" 视同缺省受理——进度拍照转且 lastSeq 不丢，转发用占位（leader 恒见非空摘要）', async () => {
    const { leader, target, groupId } = await setupGroup('rmtree');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));

    report(target.ws, taskId, { status: 'running', sessionId: 'sess-x', progress: { lastSeq: 7, excerpt: '' } });
    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    expect(prog.payload).toEqual({ taskId, sessionId: 'sess-x', lastSeq: 7, excerpt: '（无文本增量）' });
    expect(ledgerRow(taskId)).toMatchObject({ status: 'running', session_id: 'sess-x' });

    // progress 整段缺省同款占位（T10 目标侧恒非空双保险；hub 侧兜底一致）
    report(target.ws, taskId, { status: 'running', progress: { lastSeq: 9, excerpt: '' } });
    const beat = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { lastSeq: number }).lastSeq === 9);
    expect((beat.payload as { excerpt: string }).excerpt).toBe('（无文本增量）');
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// kind 透传（M2.1 T11b——目标侧 waiting-input 等待标注随进度拍直达 leader 推送面）
// ────────────────────────────────────────────────────────────────────────────

describe('task.report kind 透传（T11b）', () => {
  it('kind 合法值（waiting-input）透传——迁移拍与进度拍的 task.progress 载荷均含 kind', async () => {
    const { leader, target, groupId } = await setupGroup('delkind');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    // 迁移拍（pending→running）：kind 随转发
    report(target.ws, taskId, {
      status: 'running', sessionId: 'sess-k',
      progress: { lastSeq: 1, excerpt: '开始执行', kind: 'waiting-input' },
    });
    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
    expect(prog.payload).toEqual({
      taskId, sessionId: 'sess-k', lastSeq: 1, excerpt: '开始执行', kind: 'waiting-input',
    });
    // 进度拍（running→running 同态重复上报）：kind 同样透传（leader 看板等待态依据）
    report(target.ws, taskId, {
      status: 'running',
      progress: { lastSeq: 3, excerpt: '等待用户确认写入', kind: 'waiting-input' },
    });
    const beat = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { lastSeq: number }).lastSeq === 3);
    expect(beat.payload).toEqual({
      taskId, sessionId: 'sess-k', lastSeq: 3, excerpt: '等待用户确认写入', kind: 'waiting-input',
    });
    leader.ws.close(); target.ws.close();
  });

  it('kind 非法值丢弃不抛（先例 permissionTier）：task.progress 载荷不含 kind 键，其余字段完好', async () => {
    const { leader, target, groupId } = await setupGroup('delkbad');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
    // 非法全集：未知串 / 非串值 / null——丢弃 ≠ 拒绝：进度拍照常受理转发
    const bads: unknown[] = ['running', 42, null];
    for (let i = 0; i < bads.length; i++) {
      const lastSeq = i + 2;
      report(target.ws, taskId, {
        status: 'running', sessionId: 'sess-b',
        progress: { lastSeq, excerpt: '进度正常', kind: bads[i] },
      });
      const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
        && (e.payload as { lastSeq: number }).lastSeq === lastSeq);
      expect(prog.payload, `非法值 ${JSON.stringify(bads[i])} 应丢弃而非拒绝`).toEqual({
        taskId, sessionId: 'sess-b', lastSeq, excerpt: '进度正常',
      });
      expect(prog.payload).not.toHaveProperty('kind');
    }
    // 账本照常回填（丢弃不伤受理语义）
    expect(ledgerRow(taskId)).toMatchObject({ status: 'running', session_id: 'sess-b' });
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 审计日志（§6 审计链——logger 形态）
// ────────────────────────────────────────────────────────────────────────────

describe('审计日志', () => {
  it('delegate 受理 / 状态迁移 / 取消 / 离线拒派 / 非法迁移拒绝 均落审计', async () => {
    const infoSpy = vi.spyOn(logger, 'info');
    const warnSpy = vi.spyOn(logger, 'warn');
    try {
      const { leader, target, groupId } = await setupGroup('audit');
      const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId));
      expect(infoSpy.mock.calls.some(([msg, meta]) =>
        msg === '委派已受理' && (meta as { taskId: string }).taskId === taskId)).toBe(true);

      report(target.ws, taskId, { status: 'running', sessionId: 's' });
      await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress);
      expect(infoSpy.mock.calls.some(([msg, meta]) =>
        msg === '委派状态迁移' && (meta as { taskId: string; from: string; to: string }).taskId === taskId
        && (meta as { from: string; to: string }).from === 'pending' && (meta as { from: string; to: string }).to === 'running')).toBe(true);

      report(target.ws, taskId, { status: 'running', progress: { lastSeq: 5, excerpt: 'b' } }); // 进度拍：无迁移日志
      await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
        && (e.payload as { lastSeq: number }).lastSeq === 5);
      // 按 taskId 收窄：上一用例收尾断连触发的迁移日志可能迟到落进本 spy 窗口
      const migrations = infoSpy.mock.calls.filter(([msg, meta]) =>
        msg === '委派状态迁移' && (meta as { taskId: string }).taskId === taskId);
      expect(migrations).toHaveLength(1);

      const cancelled = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-a2' }));
      await callRpc(leader, RPC.task.cancel, { taskId: cancelled });
      expect(infoSpy.mock.calls.some(([msg, meta]) =>
        msg === '委派已取消' && (meta as { taskId: string }).taskId === cancelled)).toBe(true);

      report(target.ws, taskId, { status: 'done' }); // 合法
      await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted);
      report(target.ws, taskId, { status: 'running' }); // 终态非法迁移
      await settle();
      expect(warnSpy.mock.calls.some(([msg, meta]) =>
        msg === '委派上报被拒：非法状态迁移' && (meta as { taskId: string }).taskId === taskId)).toBe(true);

      // 离线拒派审计：断开目标 → 委派 → node_offline + 审计（裁定①）
      target.ws.close();
      await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
        && (e.payload as { nodeId: string }).nodeId === target.nodeId);
      const off = await callRpc(leader, RPC.task.delegate,
        delegateReq(groupId, target.nodeId, { workspaceName: 'ws-a3' }));
      expect(errOf(off).code).toBe('node_offline');
      expect(infoSpy.mock.calls.some(([msg, meta]) =>
        msg === '委派被拒：目标离线' && (meta as { groupId: string }).groupId === groupId)).toBe(true);
      leader.ws.close();
    } finally {
      infoSpy.mockRestore();
      warnSpy.mockRestore();
    }
  });
});
