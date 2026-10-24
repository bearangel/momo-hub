// 工作组内核契约锁（M2b spec §3.1/§3.2/§4/§6）——真实 socket 全真 wire 语义：
// 迁移 v3 三表 / 组生命周期 / 邀请码撤销 not_found / 角色矩阵（leader-only
// forbidden / 非成员 forbidden / 未注册方法照旧 unknown_method）/ group.changed
// 组内广播（离线成员跳过）/ NodeRegistry.sendTo 定向投递（在线 sent / 离线 offline）。
import WebSocket from 'ws';
import { EventEmitter } from 'node:events';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type Database from 'better-sqlite3';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, type HubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import { NodeRegistry } from '../src/registry';
import {
  EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope, type EventEnvelope,
} from 'momo-hub-protocol';

const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-grp-')));
const identity = ensureHubIdentity(db);
let server: HubServer;
let wsUrl: string;

function enroll(name: string): { nodeId: string; deviceToken: string } {
  const r = enrollNode(db, identity.enrollmentToken, name);
  if ('nodeId' in r) return r;
  throw new Error('enroll 失败');
}

/** 便捷客户端：自动过 hello，收集全部信封，支持条件等待（M0 server.integration 同款）。 */
function connect(name: string) {
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

/** 组 RPC 请求便捷面：发 req 信封并按【请求 id】等对应 res（同方法多次调用不得串答）。 */
function callRpc(client: Awaited<ReturnType<typeof connect>>, method: string, payload: unknown): Promise<Envelope> {
  const env = makeRequest(method, payload);
  client.ws.send(encodeEnvelope(env));
  return client.next((e) => e.kind === 'res' && e.id === env.id);
}

function resPayload(e: Envelope): Record<string, unknown> {
  if (e.kind !== 'res') throw new Error('非 res 信封');
  return (e.payload ?? {}) as Record<string, unknown>;
}

function errCode(e: Envelope): string {
  if (e.kind !== 'res' || e.ok) throw new Error('预期错误响应');
  return (e.payload as { code: string }).code;
}

beforeAll(async () => {
  server = createHubServer({ db, identity });
  const url = await server.listen(0, '127.0.0.1');
  wsUrl = url.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => { await server.close(); db.close(); });

// ────────────────────────────────────────────────────────────────────────────
// 迁移 v3（spec §4 三表 SQL 逐字）
// ────────────────────────────────────────────────────────────────────────────

describe('迁移 v3 组三表', () => {
  it('schema_version=5（v3 单条多语句追加）；三表存在；重开幂等', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-mig-'));
    const d1: Database.Database = openHubDatabase(dir);
    const version = d1.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string };
    expect(version.value).toBe('5');
    const tables = (d1.prepare(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('groups', 'group_members', 'group_invites')`,
    ).all() as Array<{ name: string }>).map((r) => r.name).sort();
    expect(tables).toEqual(['group_invites', 'group_members', 'groups']);
    d1.close();
    // 重开幂等：不重复应用、不炸
    const d2 = openHubDatabase(dir);
    expect((d2.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string }).value).toBe('5');
    d2.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// NodeRegistry.sendTo（定向投递原语——RPC 层 node_offline 语义的源头）
// ────────────────────────────────────────────────────────────────────────────

class FakeWs extends EventEmitter {
  sent: string[] = [];
  failSend = false;
  send(data: string): void {
    if (this.failSend) throw new Error('send failed (dead socket)');
    this.sent.push(data);
  }
}

describe('NodeRegistry.sendTo', () => {
  it('在线 → sent 且信封精确投递到目标连接', () => {
    const registry = new NodeRegistry();
    const ws = new FakeWs();
    registry.add({ nodeId: 'n1', nodeName: 'a', ws, alive: true });
    const evt: EventEnvelope = { v: 1, kind: 'evt', method: EVT.groupChanged, payload: { groupId: 'g', kind: 'joined', nodeId: 'n1' } };
    expect(registry.sendTo('n1', evt)).toBe('sent');
    expect(ws.sent).toHaveLength(1);
    expect(parseEnvelope(ws.sent[0]!)).toEqual(evt);
  });

  it('未注册节点 / 发送抛错 → offline（RPC 层据此映射 node_offline，不留静默积压）', () => {
    const registry = new NodeRegistry();
    const evt: EventEnvelope = { v: 1, kind: 'evt', method: EVT.groupChanged, payload: {} };
    expect(registry.sendTo('ghost', evt)).toBe('offline');
    const ws = new FakeWs();
    ws.failSend = true;
    registry.add({ nodeId: 'n2', nodeName: 'b', ws, alive: true });
    expect(registry.sendTo('n2', evt)).toBe('offline');
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 组生命周期与 group.changed 组内广播（真实 socket）
// ────────────────────────────────────────────────────────────────────────────

describe('组管理 RPC', () => {
  it('生命周期：create（创建者即 leader）→ invite → join → list（成员+在线态）', async () => {
    const a = await connect('grp-a');
    const b = await connect('grp-b');
    const created = await callRpc(a, RPC.group.create, { groupName: 'TeamA' });
    expect(created.kind === 'res' && created.ok).toBe(true);
    const { groupId, nodeId } = resPayload(created) as { groupId: string; nodeId: string };
    expect(nodeId).toBe(a.nodeId);

    const invited = await callRpc(a, RPC.group.invite, { groupId });
    expect(invited.kind === 'res' && invited.ok).toBe(true);
    const { inviteCode } = resPayload(invited) as { inviteCode: string };
    expect(typeof inviteCode).toBe('string');
    expect(inviteCode.length).toBeGreaterThan(10);

    const joined = await callRpc(b, RPC.group.join, { inviteCode });
    expect(joined.kind === 'res' && joined.ok).toBe(true);
    expect(resPayload(joined)).toEqual({ groupId });

    // 组内广播：A 与 B 都收到 joined（B 自己的入组事件）
    const evtA = await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    expect(evtA.payload).toEqual({ groupId, kind: 'joined', nodeId: b.nodeId });
    await b.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === b.nodeId);

    const listed = await callRpc(a, RPC.group.list, {});
    expect(listed.kind === 'res' && listed.ok).toBe(true);
    const groups = (resPayload(listed) as { groups: Array<{ groupId: string; groupName: string; leaderNodeId: string; members: Array<{ nodeId: string; online: boolean }> }> }).groups;
    const mine = groups.find((g) => g.groupId === groupId);
    expect(mine?.groupName).toBe('TeamA');
    expect(mine?.leaderNodeId).toBe(a.nodeId);
    expect(mine?.members.map((m) => m.nodeId).sort()).toEqual([a.nodeId, b.nodeId].sort());
    // 双方均在线（真实 socket presence 融合）
    expect(mine?.members.every((m) => m.online)).toBe(true);

    a.ws.close();
    b.ws.close();
  });

  it('group.changed 只投组内：非成员在线节点收不到', async () => {
    const a = await connect('cast-a');
    const c = await connect('cast-c');
    const created = await callRpc(a, RPC.group.create, { groupName: 'TeamCast' });
    const { groupId } = resPayload(created) as { groupId: string };
    const invited = await callRpc(a, RPC.group.invite, { groupId });
    const { inviteCode } = resPayload(invited) as { inviteCode: string };
    const b = await connect('cast-b');
    await callRpc(b, RPC.group.join, { inviteCode });
    await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    // 等一拍：C 不在本组，不该有任何 group.changed 进它的收件箱
    await new Promise((res) => setTimeout(res, 300));
    const leaked = c.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.groupChanged);
    expect(leaked).toHaveLength(0);
    a.ws.close();
    b.ws.close();
    c.ws.close();
  });

  it('组内广播跳过离线成员不炸：B 入组后断开，后续事件仍达 A', async () => {
    const a = await connect('off-a');
    const created = await callRpc(a, RPC.group.create, { groupName: 'TeamOff' });
    const { groupId } = resPayload(created) as { groupId: string };
    const invited = await callRpc(a, RPC.group.invite, { groupId });
    const { inviteCode } = resPayload(invited) as { inviteCode: string };
    const b = await connect('off-b');
    await callRpc(b, RPC.group.join, { inviteCode });
    await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    b.ws.close();
    await new Promise((res) => setTimeout(res, 300)); // 等 close 摘除注册表

    const third = await connect('off-c');
    await callRpc(third, RPC.group.join, { inviteCode }); // 幂等入组新节点 → 组内广播
    const evt = await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === third.nodeId);
    expect((evt.payload as { kind: string }).kind).toBe('joined');
    a.ws.close();
    third.ws.close();
  });

  it('list 只见自己所在的组（组间可见性不做，§9）', async () => {
    const a = await connect('scope-a');
    const outsider = await connect('scope-x');
    await callRpc(a, RPC.group.create, { groupName: 'TeamScope' });
    const listed = await callRpc(outsider, RPC.group.list, {});
    expect(resPayload(listed)).toEqual({ groups: [] });
    a.ws.close();
    outsider.ws.close();
  });

  it('join 幂等：重复凭码入组返回同 groupId，成员不重复', async () => {
    const a = await connect('idem-a');
    const b = await connect('idem-b');
    const { groupId } = resPayload(await callRpc(a, RPC.group.create, { groupName: 'TeamIdem' })) as { groupId: string };
    const { inviteCode } = resPayload(await callRpc(a, RPC.group.invite, { groupId })) as { inviteCode: string };
    const first = await callRpc(b, RPC.group.join, { inviteCode });
    const second = await callRpc(b, RPC.group.join, { inviteCode });
    expect(resPayload(first)).toEqual({ groupId });
    expect(resPayload(second)).toEqual({ groupId });
    const groups = (resPayload(await callRpc(a, RPC.group.list, {})) as { groups: Array<{ groupId: string; members: unknown[] }> }).groups;
    const mine = groups.find((g) => g.groupId === groupId);
    expect(mine?.members).toHaveLength(2);
    a.ws.close();
    b.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 邀请码撤销（再生成即撤销旧码——not_found）
// ────────────────────────────────────────────────────────────────────────────

describe('邀请码撤销', () => {
  it('再生成撤销旧码：旧码 join → not_found；新码 join → ok；乱码 → not_found', async () => {
    const a = await connect('inv-a');
    const b = await connect('inv-b');
    const { groupId } = resPayload(await callRpc(a, RPC.group.create, { groupName: 'TeamInv' })) as { groupId: string };
    const first = resPayload(await callRpc(a, RPC.group.invite, { groupId })) as { inviteCode: string };
    const second = resPayload(await callRpc(a, RPC.group.invite, { groupId })) as { inviteCode: string };
    expect(first.inviteCode).not.toBe(second.inviteCode);

    const stale = await callRpc(b, RPC.group.join, { inviteCode: first.inviteCode });
    expect(errCode(stale)).toBe('not_found');
    const garbage = await callRpc(b, RPC.group.join, { inviteCode: 'no-such-code' });
    expect(errCode(garbage)).toBe('not_found');
    const fresh = await callRpc(b, RPC.group.join, { inviteCode: second.inviteCode });
    expect(resPayload(fresh)).toEqual({ groupId });
    a.ws.close();
    b.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 角色矩阵（leader-only 校验层）
// ────────────────────────────────────────────────────────────────────────────

describe('角色矩阵', () => {
type Connected = Awaited<ReturnType<typeof connect>>;
  async function setup(pref: string): Promise<{ a: Connected; b: Connected; c: Connected; groupId: string; inviteCode: string }> {
    const a = await connect(`${pref}-l`);
    const b = await connect(`${pref}-m`);
    const c = await connect(`${pref}-o`);
    const { groupId } = resPayload(await callRpc(a, RPC.group.create, { groupName: `Team-${pref}` })) as { groupId: string };
    const { inviteCode } = resPayload(await callRpc(a, RPC.group.invite, { groupId })) as { inviteCode: string };
    await callRpc(b, RPC.group.join, { inviteCode });
    return { a, b, c, groupId, inviteCode };
  }

  it('非 leader 成员 / 非成员调 leader-only transfer → forbidden', async () => {
    const { a, b, c, groupId } = await setup('tx');
    const byMember = await callRpc(b, RPC.group.transfer, { groupId, toNodeId: b.nodeId });
    expect(errCode(byMember)).toBe('forbidden');
    const byOutsider = await callRpc(c, RPC.group.transfer, { groupId, toNodeId: b.nodeId });
    expect(errCode(byOutsider)).toBe('forbidden');
    a.ws.close(); b.ws.close(); c.ws.close();
  });

  it('leader 转移：成功 + leaderChanged 广播；旧 leader 再转移 → forbidden；新 leader 可转回', async () => {
    const { a, b, groupId } = await setup('tf');
    const ok = await callRpc(a, RPC.group.transfer, { groupId, toNodeId: b.nodeId });
    expect(ok.kind === 'res' && ok.ok).toBe(true);
    const evt = await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { kind: string }).kind === 'leaderChanged');
    expect(evt.payload).toEqual({ groupId, kind: 'leaderChanged', nodeId: b.nodeId });

    const stale = await callRpc(a, RPC.group.transfer, { groupId, toNodeId: a.nodeId });
    expect(errCode(stale)).toBe('forbidden');
    const back = await callRpc(b, RPC.group.transfer, { groupId, toNodeId: a.nodeId });
    expect(back.kind === 'res' && back.ok).toBe(true);
    a.ws.close(); b.ws.close();
  });

  it('transfer 到非成员 → forbidden；不存在组 → not_found；缺参 → bad_request；自转移幂等 ok', async () => {
    const { a, b, c, groupId } = await setup('tv');
    const toOutsider = await callRpc(a, RPC.group.transfer, { groupId, toNodeId: c.nodeId });
    expect(errCode(toOutsider)).toBe('forbidden');
    const ghost = await callRpc(a, RPC.group.transfer, { groupId: 'no-such-group', toNodeId: b.nodeId });
    expect(errCode(ghost)).toBe('not_found');
    const missing = await callRpc(a, RPC.group.transfer, { groupId });
    expect(errCode(missing)).toBe('bad_request');
    const self = await callRpc(a, RPC.group.transfer, { groupId, toNodeId: a.nodeId });
    expect(self.kind === 'res' && self.ok).toBe(true);
    a.ws.close(); b.ws.close(); c.ws.close();
  });

  it('invite 非成员 → forbidden；leave 非成员 → forbidden；leader 退组 → forbidden；成员退组 ok + left 广播', async () => {
    const { a, b, c, groupId } = await setup('lv');
    const inviteOutsider = await callRpc(c, RPC.group.invite, { groupId });
    expect(errCode(inviteOutsider)).toBe('forbidden');
    const leaveOutsider = await callRpc(c, RPC.group.leave, { groupId });
    expect(errCode(leaveOutsider)).toBe('forbidden');
    const leaveLeader = await callRpc(a, RPC.group.leave, { groupId });
    expect(errCode(leaveLeader)).toBe('forbidden');

    const leaveMember = await callRpc(b, RPC.group.leave, { groupId });
    expect(leaveMember.kind === 'res' && leaveMember.ok).toBe(true);
    const evt = await a.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { kind: string }).kind === 'left');
    expect(evt.payload).toEqual({ groupId, kind: 'left', nodeId: b.nodeId });
    // 退组后 b 不再是成员：后续 invite → forbidden
    const after = await callRpc(b, RPC.group.invite, { groupId });
    expect(errCode(after)).toBe('forbidden');
    a.ws.close(); b.ws.close(); c.ws.close();
  });

  it('create 组名非法 → bad_request（空/超长/非字符串）', async () => {
    const a = await connect('nm-a');
    for (const bad of ['', '   ', 'x'.repeat(65), 123]) {
      const res = await callRpc(a, RPC.group.create, { groupName: bad });
      expect(errCode(res)).toBe('bad_request');
    }
    // 边界内合法：64 字符
    const okRes = await callRpc(a, RPC.group.create, { groupName: 'y'.repeat(64) });
    expect(okRes.kind === 'res' && okRes.ok).toBe(true);
    a.ws.close();
  });

  it('未注册的 leader-only 方法（mirror/task.*/view.*）照旧 unknown_method', async () => {
    const a = await connect('un-a');
    for (const method of [RPC.workspace.mirror, RPC.task.delegate, RPC.task.cancel, RPC.view.workspace, RPC.view.subscribe]) {
      const res = await callRpc(a, method, {});
      expect(errCode(res)).toBe('unknown_method');
    }
    a.ws.close();
  });

  it('join/invite/leave 缺参 → bad_request；组不存在 → not_found', async () => {
    const a = await connect('bp-a');
    const joinNoCode = await callRpc(a, RPC.group.join, {});
    expect(errCode(joinNoCode)).toBe('bad_request');
    const inviteNoGroup = await callRpc(a, RPC.group.invite, {});
    expect(errCode(inviteNoGroup)).toBe('bad_request');
    const leaveGhost = await callRpc(a, RPC.group.leave, { groupId: 'ghost' });
    expect(errCode(leaveGhost)).toBe('not_found');
    const inviteGhost = await callRpc(a, RPC.group.invite, { groupId: 'ghost' });
    expect(errCode(inviteGhost)).toBe('not_found');
    a.ws.close();
  });
});
