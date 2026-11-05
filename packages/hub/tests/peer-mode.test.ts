// 平行模式（peer）hub 侧行为锁（spec 2026-11-05 §3/§7，协议 1.10.0）——
// 全真 socket wire 语义：建组绑定 / 绑定三 RPC 投影 / peer 委派守卫（任意成员 +
// draft 载荷白名单逐键拒 + 目标须绑定）/ 生命周期拍 pending / 发起者取消 /
// 镜像 acceptsMirror 闸 + applyTo:binding / 查看全拒 / 入组版本门 / lf 组零变化回归。
import WebSocket from 'ws';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type Database from 'better-sqlite3';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, type HubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  EVT, PROTOCOL_VERSION, RPC, TASK_WIRE, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope,
} from 'momo-hub-protocol';

const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-peer-')));
const identity = ensureHubIdentity(db);
let server: HubServer;
let wsUrl: string;

function enroll(name: string): { nodeId: string; deviceToken: string } {
  const r = enrollNode(db, identity.enrollmentToken, name);
  if ('nodeId' in r) return r;
  throw new Error('enroll 失败');
}

function connect(name: string, protoVer: string = PROTOCOL_VERSION) {
  return new Promise<{ ws: WebSocket; nodeId: string; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enroll(name);
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
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer, nodeName: name }))));
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

function errCode(e: Envelope): string {
  if (e.kind !== 'res' || e.ok) throw new Error('预期错误响应');
  return (e.payload as { code: string }).code;
}

function errMsg(e: Envelope): string {
  if (e.kind !== 'res' || e.ok) throw new Error('预期错误响应');
  return (e.payload as { message: string }).message;
}

/** 目标节点发 task.report（res 信封 id=taskId——wire 约定）。 */
function report(client: Client, taskId: string, payload: Record<string, unknown>): void {
  client.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: taskId, method: TASK_WIRE.report, ok: true, payload }));
}

async function setupPeerGroup(): Promise<{ a: Client; b: Client; groupId: string }> {
  const a = await connect('peer-leader');
  const b = await connect('peer-member');
  const created = await callRpc(a, RPC.group.create, { groupName: '需求研发测试', mode: 'peer', bindWorkspaceName: '需求空间' });
  const groupId = (created.payload as { groupId: string }).groupId;
  const invite = await callRpc(a, RPC.group.invite, { groupId });
  await callRpc(b, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
  await callRpc(b, RPC.group.bind, { groupId, workspaceName: '研发空间' });
  return { a, b, groupId };
}

beforeAll(async () => {
  server = createHubServer({ db, identity });
  const url = await server.listen(0, '127.0.0.1');
  wsUrl = url.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => { await server.close(); db.close(); });

describe('peer 建组与绑定', () => {
  it('peer 建组成功：mode 投影 + 组长绑定；缺绑定 bad_request', async () => {
    const a = await connect('bind-leader');
    const bad = await callRpc(a, RPC.group.create, { groupName: '无绑定', mode: 'peer' });
    expect(errCode(bad)).toBe('bad_request');

    const okRes = await callRpc(a, RPC.group.create, { groupName: '绑定组', mode: 'peer', bindWorkspaceName: '需求空间' });
    expect(okRes.kind === 'res' && okRes.ok).toBe(true);
    const groupId = (okRes.payload as { groupId: string }).groupId;
    const list = await callRpc(a, RPC.group.list, {});
    const group = ((list.payload as { groups: Array<Record<string, unknown>> }).groups)
      .find((g) => g.groupId === groupId)!;
    expect(group.mode).toBe('peer');
    const leader = (group.members as Array<Record<string, unknown>>).find((m) => m.nodeId === a.nodeId)!;
    expect(leader.bound).toBe(true);
    expect(leader.boundWorkspaceName).toBe('需求空间');
    expect(leader.acceptsMirror).toBe(false);
    a.ws.close();
  });

  it('未知 mode 值按 leader-follower 兜底（旧客户端零变化）；lf 组无绑定投影键', async () => {
    const a = await connect('lf-leader');
    const okRes = await callRpc(a, RPC.group.create, { groupName: 'lf 组', mode: 'weird-mode' });
    const groupId = (okRes.payload as { groupId: string }).groupId;
    const list = await callRpc(a, RPC.group.list, {});
    const group = ((list.payload as { groups: Array<Record<string, unknown>> }).groups)
      .find((g) => g.groupId === groupId)!;
    expect(group.mode).toBe('leader-follower');
    expect((group.members as Array<Record<string, unknown>>)[0]!.bound).toBeUndefined();
    a.ws.close();
  });

  it('绑定三 RPC：bind 换绑保开关 / setMirrorAccept 须先绑定 / unbind 幂等 / 非成员 forbidden / lf 组 bad_request', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    // setMirrorAccept 前置（B 未开镜像）：先开
    const on = await callRpc(b, RPC.group.setMirrorAccept, { groupId, accepts: true });
    expect(on.kind === 'res' && on.ok).toBe(true);
    // 换绑保留开关
    await callRpc(b, RPC.group.bind, { groupId, workspaceName: '研发空间2' });
    let list = await callRpc(a, RPC.group.list, {});
    let group = ((list.payload as { groups: Array<Record<string, unknown>> }).groups).find((g) => g.groupId === groupId)!;
    let memberB = (group.members as Array<Record<string, unknown>>).find((m) => m.nodeId === b.nodeId)!;
    expect(memberB.boundWorkspaceName).toBe('研发空间2');
    expect(memberB.acceptsMirror).toBe(true);
    // unbind → bound=false；幂等二次 unbind ok
    await callRpc(b, RPC.group.unbind, { groupId });
    await callRpc(b, RPC.group.unbind, { groupId });
    list = await callRpc(a, RPC.group.list, {});
    group = ((list.payload as { groups: Array<Record<string, unknown>> }).groups).find((g) => g.groupId === groupId)!;
    memberB = (group.members as Array<Record<string, unknown>>).find((m) => m.nodeId === b.nodeId)!;
    expect(memberB.bound).toBe(false);
    expect(memberB.boundWorkspaceName).toBeUndefined();
    // 解绑后 setMirrorAccept → bad_request
    const afterUnbind = await callRpc(b, RPC.group.setMirrorAccept, { groupId, accepts: true });
    expect(errCode(afterUnbind)).toBe('bad_request');
    // 非成员 bind → forbidden
    const stranger = await connect('peer-stranger');
    const strangerBind = await callRpc(stranger, RPC.group.bind, { groupId, workspaceName: '陌生空间' });
    expect(errCode(strangerBind)).toBe('forbidden');
    // lf 组 bind → bad_request
    const lfCreate = await callRpc(stranger, RPC.group.create, { groupName: 'lf-bind' });
    const lfGroupId = (lfCreate.payload as { groupId: string }).groupId;
    const lfBind = await callRpc(stranger, RPC.group.bind, { groupId: lfGroupId, workspaceName: '空间' });
    expect(errCode(lfBind)).toBe('bad_request');
    stranger.ws.close(); a.ws.close(); b.ws.close();
  });
});

describe('peer 委派守卫（载荷白名单逐键拒）', () => {
  it('非组长成员可 draft 委派给已绑定节点；task.assigned 带 draft+priority；账本 pending', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const res = await callRpc(b, RPC.task.delegate, {
      groupId, targetNodeId: a.nodeId, workspaceName: '研发空间',
      task: { title: '实现登录接口', prompt: '实现登录接口并自测', draft: true, priority: 8 },
    });
    expect(res.kind === 'res' && res.ok).toBe(true);
    const taskId = (res.payload as { taskId: string }).taskId;
    const assigned = await a.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned);
    const payload = assigned.payload as { taskId: string; task: { draft?: boolean; priority?: number } };
    expect(payload.taskId).toBe(taskId);
    expect(payload.task.draft).toBe(true);
    expect(payload.task.priority).toBe(8);
    const listRes = await callRpc(b, RPC.task.list, { groupId });
    const row = ((listRes.payload as { tasks: Array<Record<string, unknown>> }).tasks).find((t) => t.taskId === taskId)!;
    expect(row.status).toBe('pending');
    a.ws.close(); b.ws.close();
  });

  it('peer 组非 draft 任务书 → bad_request（模式纯净）', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const res = await callRpc(b, RPC.task.delegate, {
      groupId, targetNodeId: a.nodeId, workspaceName: 'x',
      task: { title: 't', prompt: 'p' },
    });
    expect(errCode(res)).toBe('bad_request');
    a.ws.close(); b.ws.close();
  });

  it('draft 与编排四键逐一互斥：agentSlug / team / dependsOn / permissionTier 各整包拒 bad_request', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const base = { groupId, targetNodeId: a.nodeId, workspaceName: 'x' };
    const cases: Array<Record<string, unknown>> = [
      { title: 't', prompt: 'p', draft: true, agentSlug: 'coder' },
      { title: 't', prompt: 'p', draft: true, team: { members: [{ name: 'a', isLeader: true }, { name: 'b', isLeader: false }] } },
      { title: 't', prompt: 'p', draft: true, dependsOn: ['T-1'] },
      { title: 't', prompt: 'p', draft: true, permissionTier: 'readonly' },
      { title: 't', prompt: 'p', draft: 'yes' },
      { title: 't', prompt: 'p', priority: 5 },
      { title: 't', prompt: 'p', draft: true, priority: 11 },
    ];
    for (const task of cases) {
      const res = await callRpc(b, RPC.task.delegate, { ...base, task });
      expect(errCode(res), JSON.stringify(task)).toBe('bad_request');
    }
    a.ws.close(); b.ws.close();
  });

  it('目标未绑定 → forbidden；发送方自身无需绑定；非成员发送方 → forbidden', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const c = await connect('peer-unbound');
    const invite = await callRpc(a, RPC.group.invite, { groupId });
    await callRpc(c, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
    // C 未绑定，但 C 作为发送方委派给已绑定的 A → ok（只有目标需要绑定）
    const fromUnbound = await callRpc(c, RPC.task.delegate, {
      groupId, targetNodeId: a.nodeId, workspaceName: 'x',
      task: { title: 't', prompt: 'p', draft: true },
    });
    expect(fromUnbound.kind === 'res' && fromUnbound.ok).toBe(true);
    // 目标 C 未绑定 → forbidden
    const toUnbound = await callRpc(b, RPC.task.delegate, {
      groupId, targetNodeId: c.nodeId, workspaceName: 'x',
      task: { title: 't', prompt: 'p', draft: true },
    });
    expect(errCode(toUnbound)).toBe('forbidden');
    // 非成员发送方 → forbidden
    const stranger = await connect('peer-outsider');
    const outsiderCall = await callRpc(stranger, RPC.task.delegate, {
      groupId, targetNodeId: a.nodeId, workspaceName: 'x',
      task: { title: 't', prompt: 'p', draft: true },
    });
    expect(errCode(outsiderCall)).toBe('forbidden');
    stranger.ws.close(); c.ws.close(); a.ws.close(); b.ws.close();
  });

  it('lf 组回归：成员委派仍 forbidden（leader-only）；leader 带 draft → bad_request', async () => {
    const leader = await connect('lf2-leader');
    const member = await connect('lf2-member');
    const created = await callRpc(leader, RPC.group.create, { groupName: 'lf 回归组' });
    const groupId = (created.payload as { groupId: string }).groupId;
    const invite = await callRpc(leader, RPC.group.invite, { groupId });
    await callRpc(member, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
    const memberCall = await callRpc(member, RPC.task.delegate, {
      groupId, targetNodeId: leader.nodeId, workspaceName: 'x', task: { title: 't', prompt: 'p' },
    });
    expect(errCode(memberCall)).toBe('forbidden');
    expect(errMsg(memberCall)).toContain('仅组长');
    const draftCall = await callRpc(leader, RPC.task.delegate, {
      groupId, targetNodeId: member.nodeId, workspaceName: 'x',
      task: { title: 't', prompt: 'p', draft: true },
    });
    expect(errCode(draftCall)).toBe('bad_request');
    leader.ws.close(); member.ws.close();
  });
});

describe('peer 生命周期拍与终态回传', () => {
  it('pending 拍（received/arranged）不改账本、直转发起方；running/failed 链路照常；终态后 pending 拍静默忽略', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    // b（成员）委派给 a（组长，已绑定）——发送方 b，接收方 a
    const res = await callRpc(b, RPC.task.delegate, {
      groupId, targetNodeId: a.nodeId, workspaceName: '研发空间',
      task: { title: '联调任务', prompt: '完成联调', draft: true },
    });
    const taskId = (res.payload as { taskId: string }).taskId;
    await a.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned);

    report(a, taskId, { status: 'pending', progress: { lastSeq: 0, excerpt: '草稿已落板', kind: 'received' } });
    const beat1 = await b.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress && (e.payload as { taskId: string }).taskId === taskId);
    expect((beat1.payload as { kind?: string }).kind).toBe('received');
    report(a, taskId, { status: 'pending', progress: { lastSeq: 1, excerpt: '已指派 coder', kind: 'arranged' } });
    const beat2 = await b.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress && (e.payload as { taskId: string; kind?: string }).taskId === taskId && (e.payload as { kind?: string }).kind === 'arranged');
    expect(beat2).toBeDefined();

    // 账本仍 pending（生命周期拍不迁移状态）
    let listRes = await callRpc(b, RPC.task.list, { groupId });
    let row = ((listRes.payload as { tasks: Array<Record<string, unknown>> }).tasks).find((t) => t.taskId === taskId)!;
    expect(row.status).toBe('pending');

    report(a, taskId, { status: 'running', sessionId: 'sess-1', progress: { lastSeq: 2, excerpt: '执行中' } });
    const beat3 = await b.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress && (e.payload as { taskId: string; lastSeq: number }).taskId === taskId && (e.payload as { lastSeq: number }).lastSeq === 2);
    expect(beat3).toBeDefined();
    listRes = await callRpc(b, RPC.task.list, { groupId });
    row = ((listRes.payload as { tasks: Array<Record<string, unknown>> }).tasks).find((t) => t.taskId === taskId)!;
    expect(row.status).toBe('running');

    report(a, taskId, { status: 'failed', reason: 'declined:需求变更不做' });
    const fail = await b.next((e) => e.kind === 'evt' && e.method === EVT.taskFailed && (e.payload as { taskId: string }).taskId === taskId);
    expect((fail.payload as { reason?: string }).reason).toBe('declined:需求变更不做');

    // 终态后 pending 拍忽略（无事件产出——用 inbox 时序验证：发拍后短歇无新 progress）
    const before = b.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskProgress).length;
    report(a, taskId, { status: 'pending', progress: { lastSeq: 3, excerpt: '迟到拍', kind: 'received' } });
    await new Promise((r) => setTimeout(r, 150));
    const after = b.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskProgress).length;
    expect(after).toBe(before);
    a.ws.close(); b.ws.close();
  });

  it('peer 取消：非发起者 forbidden；发起者 ok + task.cancelled 定向目标', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const c = await connect('peer-cancel-3rd');
    const invite = await callRpc(a, RPC.group.invite, { groupId });
    await callRpc(c, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
    await callRpc(c, RPC.group.bind, { groupId, workspaceName: '测试空间' });

    const res = await callRpc(b, RPC.task.delegate, {
      groupId, targetNodeId: c.nodeId, workspaceName: '研发空间',
      task: { title: 't', prompt: 'p', draft: true },
    });
    const taskId = (res.payload as { taskId: string }).taskId;
    await c.next((e) => e.kind === 'evt' && e.method === EVT.taskAssigned);

    const thirdParty = await callRpc(a, RPC.task.cancel, { taskId });
    expect(errCode(thirdParty)).toBe('forbidden');

    const byInitiator = await callRpc(b, RPC.task.cancel, { taskId });
    expect(byInitiator.kind === 'res' && byInitiator.ok).toBe(true);
    const cancelled = await c.next((e) => e.kind === 'evt' && e.method === EVT.taskCancelled && (e.payload as { taskId: string }).taskId === taskId);
    expect(cancelled).toBeDefined();
    a.ws.close(); b.ws.close(); c.ws.close();
  });
});

describe('peer 镜像（acceptsMirror 闸 + applyTo:binding）', () => {
  it('未接受 → forbidden；接受后两段式 apply 带 applyTo=binding；回执结算与组内广播', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    db.prepare(`INSERT INTO workspace_snapshots_meta (snapshot_id, workspace_name, version, by_node, checksum, size_bytes, created_at)
      VALUES ('snap-peer-1', '需求空间', 'v1', ?, 'csum', 10, ?)`).run(a.nodeId, Date.now());

    const mirrorPayload = { groupId, targetNodeId: b.nodeId, workspaceName: '需求空间', snapshotId: 'snap-peer-1', strategy: 'replace' as const };
    const denied = await callRpc(a, RPC.workspace.mirror, mirrorPayload);
    expect(errCode(denied)).toBe('forbidden');
    expect(errMsg(denied)).toContain('未接受本组镜像');

    await callRpc(b, RPC.group.setMirrorAccept, { groupId, accepts: true });
    const accepted = await callRpc(a, RPC.workspace.mirror, mirrorPayload);
    expect(accepted.kind === 'res' && accepted.ok).toBe(true);
    const mirrorId = (accepted.payload as { mirrorId: string }).mirrorId;

    const apply = await b.next((e) => e.kind === 'req' && e.method === 'mirror.apply' && e.id === mirrorId);
    expect((apply.payload as { applyTo?: string }).applyTo).toBe('binding');
    b.ws.send(encodeEnvelope({
      v: 1, kind: 'res', id: mirrorId, method: 'mirror.receipt', ok: true,
      payload: { mirrorId, appliedResources: [] },
    }));
    const result = await a.next((e) => e.kind === 'res' && e.method === 'mirror.result' && e.id === mirrorId);
    expect(result.kind === 'res' && result.ok).toBe(true);
    a.ws.close(); b.ws.close();
  });

  it('lf 组镜像回归：apply 载荷不带 applyTo 键（旧 wire 逐字节不变）', async () => {
    const leader = await connect('lf-mirror-leader');
    const member = await connect('lf-mirror-member');
    const created = await callRpc(leader, RPC.group.create, { groupName: 'lf 镜像回归' });
    const groupId = (created.payload as { groupId: string }).groupId;
    const invite = await callRpc(leader, RPC.group.invite, { groupId });
    await callRpc(member, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
    db.prepare(`INSERT INTO workspace_snapshots_meta (snapshot_id, workspace_name, version, by_node, checksum, size_bytes, created_at)
      VALUES ('snap-lf-1', '主空间', 'v1', ?, 'csum', 10, ?)`).run(leader.nodeId, Date.now());
    const accepted = await callRpc(leader, RPC.workspace.mirror, {
      groupId, targetNodeId: member.nodeId, workspaceName: '主空间', snapshotId: 'snap-lf-1', strategy: 'replace',
    });
    expect(accepted.kind === 'res' && accepted.ok).toBe(true);
    const mirrorId = (accepted.payload as { mirrorId: string }).mirrorId;
    const apply = await member.next((e) => e.kind === 'req' && e.method === 'mirror.apply' && e.id === mirrorId);
    expect((apply.payload as Record<string, unknown>).applyTo).toBeUndefined();
    member.ws.send(encodeEnvelope({
      v: 1, kind: 'res', id: mirrorId, method: 'mirror.receipt', ok: true,
      payload: { mirrorId, appliedResources: [] },
    }));
    await leader.next((e) => e.kind === 'res' && e.method === 'mirror.result' && e.id === mirrorId);
    leader.ws.close(); member.ws.close();
  });
});

describe('peer 查看全拒 + 入组版本门', () => {
  it('组长调 view.* → forbidden（平行组不提供节点查看）', async () => {
    const { a, b, groupId } = await setupPeerGroup();
    const view = await callRpc(a, RPC.view.workspace, { groupId, targetNodeId: b.nodeId });
    expect(errCode(view)).toBe('forbidden');
    expect(errMsg(view)).toContain('平行组不提供节点查看');
    a.ws.close(); b.ws.close();
  });

  it('1.9.0 节点 join peer 组 → node_unsupported；调绑定三 RPC → unsupported_method', async () => {
    const { a, groupId } = await setupPeerGroup();
    const invite = await callRpc(a, RPC.group.invite, { groupId });
    const oldClient = await connect('old-node-1.9', '1.9.0');
    const joinRes = await callRpc(oldClient, RPC.group.join, { inviteCode: (invite.payload as { inviteCode: string }).inviteCode });
    expect(errCode(joinRes)).toBe('node_unsupported');
    const bindRes = await callRpc(oldClient, RPC.group.bind, { groupId, workspaceName: 'x' });
    expect(errCode(bindRes)).toBe('unsupported_method');
    a.ws.close(); oldClient.ws.close();
  });
});
