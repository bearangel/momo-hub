// hub 远程查看转发层契约锁（M2e spec 2026-10-24 §3.1「远程查看」/§3.2 view.appended/
// §5.5 双层同意/§8 RF#1 + T13）——真 socket 三客户端（leader + 目标节点 + 组外者）
// 全真 wire 语义：六方法转发矩阵（透传响应/错误码保序/id 关联不串答）/ consent
// 生命周期（默认 true / false forbidden / 重报覆盖 / 断线清除回默认——RM2-9）/
// view.tail → view.appended 定向（多 leader 扇出 / 会话隔离 / 无订阅丢弃+日志）/
// 订阅生命周期（subscribe 幂等 / unsubscribe 注销 / leader 与目标断开双侧自动
// 注销——RF#1 断开后零泄漏）/ 守卫矩阵（非 leader / 非同组 / consent false /
// 离线 / 在途断开结算 / 伪造应答）/ view.consent 上行处理（形状校验 / 未知节点拒）。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import { NodeRegistry } from '../src/registry';
import { RemoteViewRouter } from '../src/remote-view';
import { logger } from '../src/logger';
import {
  EVT, PROTOCOL_VERSION, RPC, VIEW_WIRE, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope, type RequestEnvelope, type ViewMessage,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-view-'));
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
  await server.close();
  db.close();
});

// ── 工具（delegations.test.ts 同款客户端骨架 + 重连支持）──────────────────────

interface NodeCred { nodeId: string; deviceToken: string }

function connectCred(cred: NodeCred) {
  return new Promise<{ ws: WebSocket; nodeId: string; cred: NodeCred; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
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
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION }))));
    ws.on('error', reject);
    const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
      new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
    void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
      if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
      else resolve({ ws, nodeId: cred.nodeId, cred, inbox, next });
    });
  });
}

async function connect(name: string) {
  const cred = enrollNode(db, identity.enrollmentToken, name);
  if ('error' in cred) throw new Error('enroll 失败');
  return connectCred(cred);
}

type Client = Awaited<ReturnType<typeof connect>>;

/** 发 RPC 并保留请求信封（转发矩阵需比对 leader 原始 id 与转发新 id）。 */
function callRpcEnv(client: Client, method: string, payload: unknown): { env: RequestEnvelope; res: Promise<Envelope> } {
  const env = makeRequest(method, payload);
  client.ws.send(encodeEnvelope(env));
  return { env, res: client.next((e) => e.kind === 'res' && e.id === env.id) };
}

async function callRpc(client: Client, method: string, payload: unknown): Promise<Envelope> {
  return (await callRpcEnv(client, method, payload)).res;
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

/** 目标节点等待 hub 转发的 req 信封（view.* 六方法原 method + 新 id）。
 *  excludeIds：同方法多次转发时排除已消费的信封（inbox 只增不减，须显式去重）。 */
function awaitForwarded(target: Client, method: string, excludeIds: string[] = []): Promise<RequestEnvelope> {
  return target.next((e) => e.kind === 'req' && e.method === method && !excludeIds.includes(e.id)) as Promise<RequestEnvelope>;
}

/** 目标节点按转发 req 的 id 回 res（wire 约定：id 关联）。 */
function replyTarget(target: Client, req: Envelope, ok: boolean, payload: unknown): void {
  if (req.kind !== 'req') throw new Error('非 req 信封');
  target.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: req.id, method: req.method, ok, payload }));
}

async function reportConsent(client: Client, allowed: boolean): Promise<Envelope> {
  return callRpc(client, VIEW_WIRE.consent, { allowed });
}

async function sendTail(client: Client, sessionId: string, events: ViewMessage[]): Promise<Envelope> {
  return callRpc(client, VIEW_WIRE.tail, { sessionId, events });
}

const msg = (seq: number, content: string): ViewMessage => ({ seq, at: 1_700_000_000_000 + seq, role: 'assistant', content });

const settle = (ms = 200): Promise<void> => new Promise((r) => setTimeout(r, ms));

function viewReq(method: string, targetNodeId: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { targetNodeId, ...extra };
}

// 六方法样本载荷（hub 只校验 targetNodeId，其余字段逐字节透传——目标端权威校验）
const SAMPLES: Array<{ method: string; req: (t: string) => Record<string, unknown>; reply: Record<string, unknown> }> = [
  { method: RPC.view.workspace, req: (t) => viewReq(RPC.view.workspace, t, { workspaceName: 'ws-a' }), reply: { overview: { name: 'ws-a', description: '', sessionCount: 2 } } },
  { method: RPC.view.sessions, req: (t) => viewReq(RPC.view.sessions, t, { workspaceName: 'ws-a', beforeSeq: 10, limit: 20 }), reply: { sessions: [{ sessionId: 's1', title: 't', lastSeq: 5, updatedAt: 1 }] } },
  { method: RPC.view.messages, req: (t) => viewReq(RPC.view.messages, t, { sessionId: 's1', fromSeq: 0, limit: 50 }), reply: { messages: [msg(1, 'hi')], hasMore: false } },
  { method: RPC.view.tasks, req: (t) => viewReq(RPC.view.tasks, t, { workspaceName: 'ws-a' }), reply: { tasks: [{ taskId: 't1', title: 'x', status: 'done', updatedAt: 1 }] } },
  { method: RPC.view.subscribe, req: (t) => viewReq(RPC.view.subscribe, t, { sessionId: 's1' }), reply: { ok: true } },
  { method: RPC.view.unsubscribe, req: (t) => viewReq(RPC.view.unsubscribe, t, { sessionId: 's1' }), reply: { ok: true } },
];

// ────────────────────────────────────────────────────────────────────────────
// 六方法转发矩阵
// ────────────────────────────────────────────────────────────────────────────

describe('六方法转发矩阵', () => {
  it('六方法全转发：req 信封原 method + 新 id + 载荷逐字节透传；目标 res 透传 leader（ok 载荷等值）', async () => {
    const { leader, target, outsider } = await setupGroup('fwd');
    for (const s of SAMPLES) {
      const { env, res } = callRpcEnv(leader, s.method, s.req(target.nodeId));
      const fwd = await awaitForwarded(target, s.method);
      expect(fwd.method, s.method).toBe(s.method);
      expect(fwd.id, `${s.method} 转发 id 必须新造`).not.toBe(env.id);
      expect(fwd.payload).toEqual(s.req(target.nodeId));
      replyTarget(target, fwd, true, s.reply);
      const r = await res;
      expect(r.kind === 'res' && r.ok, s.method).toBe(true);
      expect(resPayload(r)).toEqual(s.reply);
    }
    // 转发定向：组外者与发起 leader 以外连接不收转发 req
    await settle();
    expect(outsider.inbox.filter((e) => e.kind === 'req' && e.method.startsWith('view.'))).toHaveLength(0);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('错误码保序透传：目标 not_found/bad_request 原码原文到 leader；未知码回 internal 兜底', async () => {
    const { leader, target } = await setupGroup('fwderr');
    const seenIds: string[] = [];
    const cases: Array<{ reply: Record<string, unknown>; expectCode: string; expectMessage?: string }> = [
      { reply: { code: 'not_found', message: '会话不存在' }, expectCode: 'not_found', expectMessage: '会话不存在' },
      { reply: { code: 'bad_request', message: 'limit 非法' }, expectCode: 'bad_request', expectMessage: 'limit 非法' },
      { reply: { code: 'weird_code', message: 'x' }, expectCode: 'internal' },
      { reply: null, expectCode: 'internal' },
    ];
    for (const c of cases) {
      const { res } = callRpcEnv(leader, RPC.view.messages, viewReq(RPC.view.messages, target.nodeId, { sessionId: 's', fromSeq: 0, limit: 10 }));
      const fwd = await awaitForwarded(target, RPC.view.messages, seenIds);
      seenIds.push(fwd.id);
      replyTarget(target, fwd, false, c.reply);
      const r = await res;
      const e = errOf(r);
      expect(e.code).toBe(c.expectCode);
      if (c.expectMessage !== undefined) expect(e.message).toBe(c.expectMessage);
    }
    leader.ws.close(); target.ws.close();
  });

  it('并发保序：同方法两连发 + 跨方法一发，乱序应答各 id 各答不串线', async () => {
    const { leader, target } = await setupGroup('fwdpar');
    const a = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId, { workspaceName: 'w-a' }));
    const b = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId, { workspaceName: 'w-b' }));
    const c = callRpcEnv(leader, RPC.view.tasks, viewReq(RPC.view.tasks, target.nodeId, { workspaceName: 'w-c' }));
    const fa = await awaitForwarded(target, RPC.view.workspace);
    const fb = await target.next((e) => e.kind === 'req' && e.method === RPC.view.workspace && e.id !== fa.id) as Promise<RequestEnvelope>;
    const fc = await awaitForwarded(target, RPC.view.tasks);
    expect(new Set([fa.id, fb.id, fc.id]).size).toBe(3);
    // 乱序应答：c → b → a
    replyTarget(target, fc, true, { mark: 'C' });
    replyTarget(target, fb, true, { mark: 'B' });
    replyTarget(target, fa, true, { mark: 'A' });
    expect(resPayload(await a.res)).toEqual({ mark: 'A' });
    expect(resPayload(await b.res)).toEqual({ mark: 'B' });
    expect(resPayload(await c.res)).toEqual({ mark: 'C' });
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// consent 生命周期（§5.5 + RM2-9）
// ────────────────────────────────────────────────────────────────────────────

describe('consent 生命周期', () => {
  it('默认 true：未上报即可查看；上报 false → 五个数据/订阅方法 forbidden 且零转发；unsubscribe 不受阻断（清理路径豁免）；重报 true 恢复', async () => {
    const { leader, target } = await setupGroup('cons');
    // 默认 true：一次成功转发为证
    const ok1 = callRpcEnv(leader, RPC.view.tasks, viewReq(RPC.view.tasks, target.nodeId));
    const fwd1 = await awaitForwarded(target, RPC.view.tasks);
    replyTarget(target, fwd1, true, { tasks: [] });
    expect((await ok1.res).kind === 'res' && (await ok1.res).ok).toBe(true);

    const rep = await reportConsent(target, false);
    expect(rep.kind === 'res' && rep.ok).toBe(true);

    const blocked = [RPC.view.workspace, RPC.view.sessions, RPC.view.messages, RPC.view.tasks, RPC.view.subscribe];
    for (const m of blocked) {
      const r = await callRpc(leader, m, viewReq(m, target.nodeId, m === RPC.view.messages || m === RPC.view.subscribe ? { sessionId: 's' } : {}));
      expect(errOf(r).code, m).toBe('forbidden');
    }
    await settle();
    // 拒绝发生在转发之前——目标不收这批 req（首个 view.tasks 转发除外）
    expect(target.inbox.filter((e) => e.kind === 'req' && e.method === RPC.view.workspace)).toHaveLength(0);
    expect(target.inbox.filter((e) => e.kind === 'req' && e.method === RPC.view.subscribe)).toHaveLength(0);

    // unsubscribe 豁免 consent（清理路径不得被目标阻断——文件头约定）：照常转发
    const unsub = callRpcEnv(leader, RPC.view.unsubscribe, viewReq(RPC.view.unsubscribe, target.nodeId, { sessionId: 's' }));
    const fwdU = await awaitForwarded(target, RPC.view.unsubscribe);
    replyTarget(target, fwdU, true, { ok: true });
    expect((await unsub.res).kind === 'res' && (await unsub.res).ok).toBe(true);

    // 重报 true（开关变更即时上报）：立即恢复
    await reportConsent(target, true);
    const ok2 = callRpcEnv(leader, RPC.view.tasks, viewReq(RPC.view.tasks, target.nodeId));
    const fwd2 = await awaitForwarded(target, RPC.view.tasks, [fwd1.id]);
    replyTarget(target, fwd2, true, { tasks: [] });
    expect((await ok2.res).kind === 'res' && (await ok2.res).ok).toBe(true);
    leader.ws.close(); target.ws.close();
  });

  it('断线清除回默认 true（RM2-9）：false → 断开 → 同凭据重连未重报即可查看，无陈旧 false 残留', async () => {
    const { leader, target } = await setupGroup('consrm');
    await reportConsent(target, false);
    const denied = await callRpc(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    expect(errOf(denied).code).toBe('forbidden');

    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === target.nodeId);
    const back = await connectCred(target.cred);

    // 重连后节点尚未重报 consent——hub 已回默认 true
    const probe = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId, { workspaceName: 'w' }));
    const fwd = await awaitForwarded(back, RPC.view.workspace);
    replyTarget(back, fwd, true, { overview: { name: 'w', description: '', sessionCount: 0 } });
    expect((await probe.res).kind === 'res' && (await probe.res).ok).toBe(true);
    leader.ws.close(); back.ws.close();
  });

  it('consent 状态 per-node 隔离：T1 关闭不影响 leader 查看 T2', async () => {
    const leader = await connect('iso-leader');
    const t1 = await connect('iso-t1');
    const t2 = await connect('iso-t2');
    const created = await callRpc(leader, RPC.group.create, { groupName: 'Team-iso' });
    const { groupId } = resPayload(created) as { groupId: string };
    const invited = await callRpc(leader, RPC.group.invite, { groupId });
    const { inviteCode } = resPayload(invited) as { inviteCode: string };
    await callRpc(t1, RPC.group.join, { inviteCode });
    await callRpc(t2, RPC.group.join, { inviteCode });
    await reportConsent(t1, false);

    const probe = callRpcEnv(leader, RPC.view.tasks, viewReq(RPC.view.tasks, t2.nodeId));
    const fwd = await awaitForwarded(t2, RPC.view.tasks);
    replyTarget(t2, fwd, true, { tasks: [] });
    expect((await probe.res).kind === 'res' && (await probe.res).ok).toBe(true);
    leader.ws.close(); t1.ws.close(); t2.ws.close();
  });

  it('consent 上报落审计日志', async () => {
    const spy = vi.spyOn(logger, 'info');
    try {
      const { target } = await setupGroup('conslog');
      await reportConsent(target, false);
      expect(spy.mock.calls.some(([m, meta]) =>
        m === '查看开关上报' && (meta as { nodeId: string; allowed: boolean }).allowed === false)).toBe(true);
      // teardown 断开在 spy 内触发其它日志无妨——只断言存在性
    } finally {
      spy.mockRestore();
    }
  });
});

// ────────────────────────────────────────────────────────────────────────────
// view.tail → view.appended 定向（MC3）
// ────────────────────────────────────────────────────────────────────────────

describe('view.tail → view.appended 定向', () => {
  it('订阅后 tail → 订阅端收 view.appended（载荷精确）；未订阅 leader 与组外者不收', async () => {
    const { leader, target, outsider } = await setupGroup('tail');
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    expect(fwdSub.payload).toEqual(viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    replyTarget(target, fwdSub, true, { ok: true });
    expect(resPayload(await sub.res)).toEqual({ ok: true });

    const events = [msg(3, '第三条'), msg(4, '第四条')];
    const tailRes = await sendTail(target, 'sess-1', events);
    expect(tailRes.kind === 'res' && tailRes.ok).toBe(true);
    const appended = await leader.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended);
    expect(appended.payload).toEqual({ sessionId: 'sess-1', events });

    await settle();
    expect(outsider.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(0);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('扇出：两 leader（各自组）订阅同会话各收一份；会话隔离——订阅 sess-2 者不收 sess-1 的 tail', async () => {
    const target = await connect('fan-target');
    const leaderA = await connect('fan-leader-a');
    const leaderB = await connect('fan-leader-b');
    // 两 leader 各建组，target 均入组（MC6 多组）
    const groups: string[] = [];
    for (const l of [leaderA, leaderB]) {
      const created = await callRpc(l, RPC.group.create, { groupName: `Team-fan-${l.nodeId.slice(0, 4)}` });
      const { groupId } = resPayload(created) as { groupId: string };
      const invited = await callRpc(l, RPC.group.invite, { groupId });
      await callRpc(target, RPC.group.join, { inviteCode: (resPayload(invited) as { inviteCode: string }).inviteCode });
      groups.push(groupId);
    }
    // A 订 sess-1；B 订 sess-1 与 sess-2
    const seenIds: string[] = [];
    for (const [l, sess] of [[leaderA, 'sess-1'], [leaderB, 'sess-1'], [leaderB, 'sess-2']] as const) {
      const sub = callRpcEnv(l, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: sess }));
      const fwd = await awaitForwarded(target, RPC.view.subscribe, seenIds);
      seenIds.push(fwd.id);
      replyTarget(target, fwd, true, { ok: true });
      expect(resPayload(await sub.res)).toEqual({ ok: true });
    }

    await sendTail(target, 'sess-1', [msg(1, 'a')]);
    const a1 = await leaderA.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended);
    expect(a1.payload).toEqual({ sessionId: 'sess-1', events: [msg(1, 'a')] });
    const b1 = await leaderB.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended
      && (e.payload as { sessionId: string }).sessionId === 'sess-1');
    expect(b1.payload).toEqual({ sessionId: 'sess-1', events: [msg(1, 'a')] });
    await settle();
    expect(leaderB.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(1); // 只 sess-1 一份

    await sendTail(target, 'sess-2', [msg(9, 'z')]);
    const b2 = await leaderB.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended
      && (e.payload as { sessionId: string }).sessionId === 'sess-2');
    expect(b2.payload).toEqual({ sessionId: 'sess-2', events: [msg(9, 'z')] });
    await settle();
    expect(leaderA.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(1); // A 恒一份
    leaderA.ws.close(); leaderB.ws.close(); target.ws.close();
  });

  it('无订阅丢弃 + 日志：unsubscribe 注销后 tail 无 view.appended；hub 对目标仍回 ok（不炸上行）', async () => {
    const { leader, target } = await setupGroup('taildrop');
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwdSub, true, { ok: true });
    expect(resPayload(await sub.res)).toEqual({ ok: true });

    const unsub = callRpcEnv(leader, RPC.view.unsubscribe, viewReq(RPC.view.unsubscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdUnsub = await awaitForwarded(target, RPC.view.unsubscribe);
    replyTarget(target, fwdUnsub, true, { ok: true });
    expect(resPayload(await unsub.res)).toEqual({ ok: true });

    const spy = vi.spyOn(logger, 'info');
    try {
      const tailRes = await sendTail(target, 'sess-1', [msg(5, '无人订阅')]);
      expect(tailRes.kind === 'res' && tailRes.ok).toBe(true);
      await settle();
      expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(0);
      expect(spy.mock.calls.some(([m, meta]) =>
        m === '尾部事件无订阅，丢弃' && (meta as { sessionId: string }).sessionId === 'sess-1')).toBe(true);
    } finally {
      spy.mockRestore();
    }
    leader.ws.close(); target.ws.close();
  });

  it('tail 形状校验：events 非数组/超 200 条/条目缺字段/seq 负数 → bad_request；合法空批无订阅 → ok', async () => {
    const { leader, target } = await setupGroup('tailbad');
    // 先建一个订阅使校验路径带真实订阅态（空批在无订阅时同样 ok）
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwdSub, true, { ok: true });
    await sub.res;

    const bads: Array<{ name: string; payload: Record<string, unknown> }> = [
      { name: 'events 非数组', payload: { sessionId: 'sess-1', events: 'x' } },
      { name: 'events 超 200 条', payload: { sessionId: 'sess-1', events: Array.from({ length: 201 }, (_, i) => msg(i, 'x')) } },
      { name: '条目缺 content', payload: { sessionId: 'sess-1', events: [{ seq: 1, at: 1, role: 'user' }] } },
      { name: 'seq 负数', payload: { sessionId: 'sess-1', events: [{ seq: -1, at: 1, role: 'user', content: 'x' }] } },
      { name: 'sessionId 缺失', payload: { events: [] } },
    ];
    for (const c of bads) {
      const r = await callRpc(target, VIEW_WIRE.tail, c.payload);
      expect(errOf(r).code, `用例：${c.name}`).toBe('bad_request');
    }
    const empty = await callRpc(target, VIEW_WIRE.tail, { sessionId: 'sess-1', events: [] });
    expect(empty.kind === 'res' && empty.ok).toBe(true);
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 订阅生命周期（RF#1 断开后零泄漏）
// ────────────────────────────────────────────────────────────────────────────

describe('订阅生命周期', () => {
  it('重复 subscribe 幂等：同 leader 同会话两次订阅，一次 tail 只收一份 view.appended', async () => {
    const { leader, target } = await setupGroup('idem');
    const seenIds: string[] = [];
    for (let i = 0; i < 2; i++) {
      const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
      const fwd = await awaitForwarded(target, RPC.view.subscribe, seenIds);
      seenIds.push(fwd.id);
      replyTarget(target, fwd, true, { ok: true });
      expect(resPayload(await sub.res)).toEqual({ ok: true });
    }
    await sendTail(target, 'sess-1', [msg(7, '只此一份')]);
    const appended = await leader.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended);
    expect(appended.payload).toEqual({ sessionId: 'sess-1', events: [msg(7, '只此一份')] });
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(1);
    leader.ws.close(); target.ws.close();
  });

  it('RF#1 leader 断开自动注销：订阅后 leader 断开 → tail 零推送；leader 同凭据重连后仍零推送（登记不复活）', async () => {
    const { leader, target } = await setupGroup('leakL');
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwdSub, true, { ok: true });
    await sub.res;

    leader.ws.close();
    await target.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === leader.nodeId);

    const t1 = await sendTail(target, 'sess-1', [msg(1, 'x')]);
    expect(t1.kind === 'res' && t1.ok).toBe(true);
    const back = await connectCred(leader.cred);
    const t2 = await sendTail(target, 'sess-1', [msg(2, 'y')]);
    expect(t2.kind === 'res' && t2.ok).toBe(true);
    await settle();
    expect(back.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(0);
    back.ws.close(); target.ws.close();
  });

  it('RF#1 目标断开自动注销：订阅后 target 断开 → target 同凭据重连后的 tail 不再推送（订阅随断开注销）', async () => {
    const { leader, target } = await setupGroup('leakT');
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwdSub, true, { ok: true });
    await sub.res;

    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === target.nodeId);
    const back = await connectCred(target.cred);

    const t = await sendTail(back, 'sess-1', [msg(1, '订阅已注销')]);
    expect(t.kind === 'res' && t.ok).toBe(true);
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(0);
    leader.ws.close(); back.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 守卫矩阵（§6 leader-only + §5.5 consent + 离线）
// ────────────────────────────────────────────────────────────────────────────

describe('守卫矩阵', () => {
  it('非 leader 成员 / 组外 leader / 非同组目标 / ghost 目标 → forbidden；载荷坏 → bad_request', async () => {
    const { leader, target, outsider } = await setupGroup('guard');
    // 非 leader（普通成员 target 调查看，含自看）
    const byMember = await callRpc(target, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    expect(errOf(byMember).code).toBe('forbidden');
    // 组外 leader 查看他人组目标
    await callRpc(outsider, RPC.group.create, { groupName: 'Team-guard-o' });
    const byOutsider = await callRpc(outsider, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    expect(errOf(byOutsider).code).toBe('forbidden');
    // leader 目标为非本组成员
    const toOutsider = await callRpc(leader, RPC.view.workspace, viewReq(RPC.view.workspace, outsider.nodeId));
    expect(errOf(toOutsider).code).toBe('forbidden');
    // ghost 目标节点
    const ghost = await callRpc(leader, RPC.view.workspace, viewReq(RPC.view.workspace, 'node-ghost'));
    expect(errOf(ghost).code).toBe('forbidden');
    // 载荷坏
    const noTarget = await callRpc(leader, RPC.view.workspace, {});
    expect(errOf(noTarget).code).toBe('bad_request');
    await settle();
    expect(target.inbox.filter((e) => e.kind === 'req' && e.method.startsWith('view.'))).toHaveLength(0);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('离线守卫：五方法 → node_offline；unsubscribe 离线仍 ok（本地注销已完成，豁免转发）', async () => {
    const { leader, target } = await setupGroup('goff');
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === target.nodeId);

    const five = [RPC.view.workspace, RPC.view.sessions, RPC.view.messages, RPC.view.tasks, RPC.view.subscribe];
    for (const m of five) {
      const r = await callRpc(leader, m, viewReq(m, target.nodeId, m === RPC.view.messages || m === RPC.view.subscribe ? { sessionId: 's' } : {}));
      expect(errOf(r).code, m).toBe('node_offline');
    }
    const unsub = await callRpc(leader, RPC.view.unsubscribe, viewReq(RPC.view.unsubscribe, target.nodeId, { sessionId: 's' }));
    expect(unsub.kind === 'res' && unsub.ok).toBe(true);
    leader.ws.close();
  });

  it('在途断开结算：目标收到转发后断开 → leader 即收 node_offline（不悬到超时）', async () => {
    const { leader, target } = await setupGroup('gcut');
    const { res } = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    await awaitForwarded(target, RPC.view.workspace); // 转发已达目标——故意不答
    target.ws.close();
    const r = await res;
    expect(errOf(r).code).toBe('node_offline');
    leader.ws.close();
  });

  it('伪造应答忽略：非目标节点持转发 id 回 res 无效；目标迟到的重复应答静默忽略', async () => {
    const { leader, target, outsider } = await setupGroup('gspoof');
    const { res } = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    const fwd = await awaitForwarded(target, RPC.view.workspace);
    // outsider 伪造同 id 应答——不得被结算
    outsider.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: fwd.id, method: RPC.view.workspace, ok: true, payload: { hax: true } }));
    await settle();
    replyTarget(target, fwd, true, { real: true });
    expect(resPayload(await res)).toEqual({ real: true });
    // 目标重复应答同 id——静默忽略，leader 恒一份
    replyTarget(target, fwd, true, { dup: true });
    await settle();
    const answers = leader.inbox.filter((e) => e.kind === 'res' && e.id !== undefined
      && e.method === RPC.view.workspace && e.kind === 'res');
    expect(answers).toHaveLength(1);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });

  it('leader 在途断开不炸 hub：迟到应答静默吞掉；hub 后续链路照常', async () => {
    const { leader, target } = await setupGroup('glive');
    const { res: deadRes } = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
    const fwd = await awaitForwarded(target, RPC.view.workspace);
    leader.ws.close();
    await target.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === leader.nodeId);
    replyTarget(target, fwd, true, { to: 'nobody' });
    await settle(); // hub 不得因向死 socket 回 res 而炸
    // hub 仍活：consent 上报与新的合法查看照常
    const rep = await reportConsent(target, true);
    expect(rep.kind === 'res' && rep.ok).toBe(true);
    void deadRes; // 该 promise 由断开结算为错误——不等待（避免挂起）
    target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// view.consent 上行处理
// ────────────────────────────────────────────────────────────────────────────

describe('view.consent 上行处理', () => {
  it('形状校验：allowed 非布尔/缺失/载荷非对象 → bad_request；合法上报 res ok', async () => {
    const { target } = await setupGroup('cshape');
    const bads: unknown[] = [{ allowed: 'yes' }, {}, 'x', null, { allowed: 1 }];
    for (const p of bads) {
      const r = await callRpc(target, VIEW_WIRE.consent, p);
      expect(errOf(r).code).toBe('bad_request');
    }
    const ok = await callRpc(target, VIEW_WIRE.consent, { allowed: true });
    expect(ok.kind === 'res' && ok.ok).toBe(true);
    target.ws.close();
  });

  it('未知节点拒：非在线注册节点的上报 → forbidden（单元——真实链路 post-hello 恒在线，防未来接线漏洞）', async () => {
    const registry = new NodeRegistry();
    const router = new RemoteViewRouter(db, registry);
    const ghost = await router.handleConsent({ db, registry, nodeId: 'node-not-registered' }, { allowed: false });
    expect(ghost.ok).toBe(false);
    if (!ghost.ok) expect(ghost.error.code).toBe('forbidden');
  });
});

// ────────────────────────────────────────────────────────────────────────────
// T13 评审收口（Important①/② + Minor③）
// ────────────────────────────────────────────────────────────────────────────

describe('T13 评审收口', () => {
  it('Important① 订阅存续期复检同组：target 退组后 tail 不扇出（同组时正常扇出为对照）', async () => {
    const { leader, target, groupId } = await setupGroup('recheck');
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'sess-1' }));
    const fwdSub = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwdSub, true, { ok: true });
    expect(resPayload(await sub.res)).toEqual({ ok: true });

    // 对照：退组前 tail 正常扇出
    await sendTail(target, 'sess-1', [msg(1, '同组时可见')]);
    const a1 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.viewAppended);
    expect((a1.payload as { events: ViewMessage[] }).events).toEqual([msg(1, '同组时可见')]);

    // target 退组 → 订阅登记仍在（hub 不主动杀），但复检同组失败 → 不扇出
    await callRpc(target, RPC.group.leave, { groupId });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string; kind: string }).nodeId === target.nodeId
      && (e.payload as { kind: string }).kind === 'left');
    const tailRes = await sendTail(target, 'sess-1', [msg(2, '退组后不可见')]);
    expect(tailRes.kind === 'res' && tailRes.ok).toBe(true);
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.viewAppended)).toHaveLength(1); // 仍只有退组前那份
    leader.ws.close(); target.ws.close();
  });

  it('Important② unsubscribe 转发超时回 ok（幂等清理——本地注销先行，目标不应答不卡死退订）；其他方法超时仍 internal', async () => {
    const server2 = createHubServer({ db, identity, dataDir, viewForwardTimeoutMs: 150 });
    const base2 = await server2.listen(0, '127.0.0.1');
    const wsUrl2 = base2.replace(/^http/, 'ws') + '/ws';
    try {
      const connect2 = async (name: string): Promise<Client> => {
        const cred = enrollNode(db, identity.enrollmentToken, name);
        if ('error' in cred) throw new Error('enroll 失败');
        return new Promise((resolve, reject) => {
          const ws = new WebSocket(wsUrl2, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
          const inbox: Envelope[] = [];
          const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
          ws.on('message', (data) => {
            const e = parseEnvelope(String(data));
            if (!e) return;
            inbox.push(e);
            const i = waiters.findIndex((w) => w.match(e));
            if (i >= 0) waiters.splice(i, 1)[0]!.resolve(e);
          });
          ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION }))));
          ws.on('error', reject);
          const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
            new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
          void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
            if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
            else resolve({ ws, nodeId: cred.nodeId, cred, inbox, next });
          });
        });
      };
      const leader = await connect2('to-leader');
      const target = await connect2('to-target');
      const created = await callRpc(leader, RPC.group.create, { groupName: 'Team-timeout' });
      const { groupId } = resPayload(created) as { groupId: string };
      const invited = await callRpc(leader, RPC.group.invite, { groupId });
      await callRpc(target, RPC.group.join, { inviteCode: (resPayload(invited) as { inviteCode: string }).inviteCode });

      // unsubscribe：转发到达目标但目标静默 → 超时后 leader 收 ok（幂等清理）
      const unsub = callRpcEnv(leader, RPC.view.unsubscribe, viewReq(RPC.view.unsubscribe, target.nodeId, { sessionId: 's' }));
      await awaitForwarded(target, RPC.view.unsubscribe); // 故意不答
      const r1 = await unsub.res;
      expect(r1.kind === 'res' && r1.ok).toBe(true);

      // 对照：view.workspace 同款静默 → 超时 internal
      const probe = callRpcEnv(leader, RPC.view.workspace, viewReq(RPC.view.workspace, target.nodeId));
      await awaitForwarded(target, RPC.view.workspace); // 故意不答
      const r2 = await probe.res;
      expect(errOf(r2).code).toBe('internal');
      leader.ws.close(); target.ws.close();
    } finally {
      await server2.close();
    }
  });

  it('Minor③ 登记侧 sessionId 界长：超过 SESSION_ID_MAX(128) 的 subscribe/unsubscribe → bad_request', async () => {
    const { leader, target } = await setupGroup('sbound');
    const long = 'x'.repeat(129);
    const s1 = await callRpc(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: long }));
    expect(errOf(s1).code).toBe('bad_request');
    const s2 = await callRpc(leader, RPC.view.unsubscribe, viewReq(RPC.view.unsubscribe, target.nodeId, { sessionId: long }));
    expect(errOf(s2).code).toBe('bad_request');
    // 128 位内仍正常走转发路径（目标应收到转发 req）
    const sub = callRpcEnv(leader, RPC.view.subscribe, viewReq(RPC.view.subscribe, target.nodeId, { sessionId: 'y'.repeat(128) }));
    const fwd = await awaitForwarded(target, RPC.view.subscribe);
    replyTarget(target, fwd, true, { ok: true });
    expect(resPayload(await sub.res)).toEqual({ ok: true });
    leader.ws.close(); target.ws.close();
  });
});
