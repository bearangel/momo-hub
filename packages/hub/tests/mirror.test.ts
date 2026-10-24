// hub workspace.mirror 两段式路由契约锁（M2c spec §3.1/§3.2/§5.2/§6 + T6）——
// 真 socket 双客户端（leader + 目标节点）全真 wire 语义：
// 转达/回执全链（mirror.apply 定向 → 先返 accepted → mirror.receipt 回执 →
// workspace.mirrored 组内广播 + mirror.result 定向回 leader）/ conflict 两段透传 /
// 并发对账（同工作空间+同目标在途 → mirror_in_progress）/ 离线（受理前 node_offline、
// 受理后断连清理）/ leader+同组校验矩阵 / 回执健壮性（未知 id、伪造来源、重复回执）。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  EVT, MIRROR_WIRE, PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope, type RequestEnvelope,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-mir-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let wsUrl: string;
let baseUrl: string;

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  wsUrl = baseUrl.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => { await server.close(); db.close(); });

// ── 工具 ────────────────────────────────────────────────────────────────────

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

/** HTTP 快照上传（T5 端点）——镜像的 snapshotId 来源。 */
async function uploadSnapshot(workspaceName: string, version: string): Promise<string> {
  const cred = enrollNode(db, identity.enrollmentToken, 'mir-archiver');
  if ('error' in cred) throw new Error('enroll 失败');
  const fd = new FormData();
  fd.append('manifest', new Blob([JSON.stringify({
    workspaceName, version,
    resources: [{ type: 'agent', slug: 'coder-pro' }],
    config: { agentMembers: [{ name: 'coder', slug: 'coder-pro' }], teams: [], mcpEnabled: [], skills: [], pluginsEnabled: [], providerRefs: [], prefs: {} },
  })], { type: 'application/json' }), 'manifest.json');
  fd.append('payload', new Blob([new Uint8Array(Buffer.from(`tar.gz of ${workspaceName} ${version}`))], { type: 'application/gzip' }), 'payload.tar.gz');
  const res = await fetch(`${baseUrl}/workspaces/snapshots`, {
    method: 'POST', headers: { authorization: `Bearer ${cred.deviceToken}` }, body: fd,
  });
  if (res.status !== 201) throw new Error(`快照上传失败：${res.status}`);
  const body = await res.json() as { snapshotId: string };
  return body.snapshotId;
}

/** 建组：leader（创建者）+ target（凭码入组）。 */
async function setupGroup(pref: string): Promise<{ leader: Client; target: Client; outsider: Client; groupId: string }> {
  const leader = await connect(`${pref}-leader`);
  const target = await connect(`${pref}-target`);
  const outsider = await connect(`${pref}-outsider`);
  const created = await callRpc(leader, RPC.group.create, { groupName: `Team-${pref}` });
  const { groupId } = resPayload(created) as { groupId: string };
  const invited = await callRpc(leader, RPC.group.invite, { groupId });
  const { inviteCode } = resPayload(invited) as { inviteCode: string };
  await callRpc(target, RPC.group.join, { inviteCode });
  // 等 leader 收到 target 的 joined 广播（保证组员落库已生效再继续）
  await leader.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
    && (e.payload as { nodeId: string; kind: string }).nodeId === target.nodeId);
  return { leader, target, outsider, groupId };
}

function mirrorReq(groupId: string, targetNodeId: string, workspaceName: string, snapshotId: string, confirm?: boolean): Record<string, unknown> {
  const req: Record<string, unknown> = { groupId, targetNodeId, workspaceName, snapshotId, strategy: 'replace' };
  if (confirm !== undefined) req.confirm = confirm;
  return req;
}

/** 目标节点等 mirror.apply 并回执。可按 mirrorId 精确匹配（多段镜像不串答）；
 *  confirm 断言可选（undefined=不得携带该键）。 */
async function targetApplyAndReceipt(
  target: Client,
  receipt: { ok: true; appliedResources: string[] } | { ok: false; code: string; message: string; diffSummary?: { added: number; removed: number; changed: number } },
  opts: { mirrorId?: string; expectConfirm?: boolean } = {},
): Promise<{ applyEnv: RequestEnvelope; mirrorId: string }> {
  const applyEnv = await target.next((e) => e.kind === 'req' && e.method === MIRROR_WIRE.apply
    && (opts.mirrorId === undefined || e.id === opts.mirrorId));
  if (applyEnv.kind !== 'req') throw new Error('非 req 信封');
  const payload = applyEnv.payload as { mirrorId: string; confirm?: boolean };
  if (opts.expectConfirm !== undefined) expect(payload.confirm).toBe(opts.expectConfirm);
  const resBody: Record<string, unknown> = receipt.ok
    ? { appliedResources: receipt.appliedResources }
    : { code: receipt.code, message: receipt.message, ...(receipt.diffSummary ? { diffSummary: receipt.diffSummary } : {}) };
  target.ws.send(encodeEnvelope({
    v: 1, kind: 'res', id: applyEnv.id, method: MIRROR_WIRE.receipt, ok: receipt.ok, payload: resBody,
  }));
  return { applyEnv, mirrorId: payload.mirrorId };
}

// ── 全链：转达 → accepted → 回执 → 广播 + 定向结果 ─────────────────────────────

describe('workspace.mirror 全链', () => {
  it('转达/回执全链：apply 定向（载荷精确）→ 先返 accepted → 回执 → workspace.mirrored 组内广播 + mirror.result 定向 leader', async () => {
    const snapshotId = await uploadSnapshot('ws-full', '1');
    const { leader, target, outsider, groupId } = await setupGroup('full');

    const mirror = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-full', snapshotId));
    expect(mirror.kind === 'res' && mirror.ok).toBe(true);
    const accepted = resPayload(mirror) as { accepted: boolean; mirrorId: string };
    expect(accepted.accepted).toBe(true);
    const mirrorId = accepted.mirrorId;

    const { applyEnv } = await targetApplyAndReceipt(target, { ok: true, appliedResources: ['coder-pro', 'mcp-fetch'] });
    // 转达形态：req 信封 id=mirrorId（对账键），载荷四字段精确、无 confirm 键
    expect(applyEnv.id).toBe(mirrorId);
    expect(applyEnv.payload).toEqual({ mirrorId, snapshotId, workspaceName: 'ws-full', strategy: 'replace' });

    // 定向结果（两段式第二段——res 信封）
    const result = await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId);
    expect(result.kind === 'res' && result.ok).toBe(true);
    expect(result.payload).toEqual({ mirrorId, appliedResources: ['coder-pro', 'mcp-fetch'] });

    // 组内广播：leader 与 target 都收到；outsider 收不到
    const expectedEvt = { groupId, targetNodeId: target.nodeId, workspaceName: 'ws-full', snapshotId, appliedResources: ['coder-pro', 'mcp-fetch'] };
    const evtL = await leader.next((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored);
    expect(evtL.payload).toEqual(expectedEvt);
    const evtT = await target.next((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored);
    expect(evtT.payload).toEqual(expectedEvt);
    await new Promise((r) => setTimeout(r, 250));
    expect(outsider.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored)).toHaveLength(0);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });
});

// ── conflict 两段式 ─────────────────────────────────────────────────────────

describe('conflict 两段式', () => {
  it('目标返 conflict+diffSummary → hub 透传 leader（无组内广播）；confirm=true 重发直落', async () => {
    const snapshotId = await uploadSnapshot('ws-conf', '1');
    const { leader, target, groupId } = await setupGroup('conf');

    // 第一段：conflict 回执
    const first = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-conf', snapshotId));
    const mirrorId1 = (resPayload(first) as { mirrorId: string }).mirrorId;
    await targetApplyAndReceipt(target, {
      ok: false, code: 'conflict', message: '目标工作空间已存在',
      diffSummary: { added: 1, removed: 2, changed: 3 },
    });
    const result1 = await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId1);
    expect(result1.kind === 'res' && result1.ok).toBe(false);
    expect(result1.payload).toEqual({
      mirrorId: mirrorId1, code: 'conflict', message: '目标工作空间已存在',
      diffSummary: { added: 1, removed: 2, changed: 3 },
    });
    await new Promise((r) => setTimeout(r, 250));
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored)).toHaveLength(0);

    // 第二段：confirm=true 重发 → 直落 → 广播 + 成功结果
    const second = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-conf', snapshotId, true));
    expect(second.kind === 'res' && second.ok).toBe(true);
    const mirrorId2 = (resPayload(second) as { mirrorId: string }).mirrorId;
    expect(mirrorId2).not.toBe(mirrorId1);
    await targetApplyAndReceipt(target, { ok: true, appliedResources: ['coder-pro'] }, { mirrorId: mirrorId2, expectConfirm: true });
    const result2 = await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId2);
    expect(result2.kind === 'res' && result2.ok).toBe(true);
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored);
    leader.ws.close(); target.ws.close();
  });
});

// ── 并发对账（Review Focus #2）──────────────────────────────────────────────

describe('并发对账', () => {
  it('同 workspaceName+target 在途 → 新请求 conflict mirror_in_progress；不同工作空间/不同目标放行；回执后登记清空', async () => {
    const snapshotId = await uploadSnapshot('ws-race', '1');
    const snapshotId2 = await uploadSnapshot('ws-race2', '1');
    const { leader, target, groupId } = await setupGroup('race');
    const third = await connect('race-third');
    const invited = await callRpc(leader, RPC.group.invite, { groupId });
    await callRpc(third, RPC.group.join, { inviteCode: (resPayload(invited) as { inviteCode: string }).inviteCode });

    // 第一发：受理（target 静默——制造在途）
    const first = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-race', snapshotId));
    const mirrorId1 = (resPayload(first) as { mirrorId: string }).mirrorId;
    const apply1 = await target.next((e) => e.kind === 'req' && e.method === MIRROR_WIRE.apply);

    // 同工作空间+同目标：拒（后到拒）
    const dup = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-race', snapshotId));
    const dupErr = errOf(dup);
    expect(dupErr.code).toBe('conflict');
    expect(dupErr.message).toContain('mirror_in_progress');

    // 不同工作空间+同目标：放行
    const otherWs = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-race2', snapshotId2));
    expect(otherWs.kind === 'res' && otherWs.ok).toBe(true);
    const mirrorId2 = (resPayload(otherWs) as { mirrorId: string }).mirrorId;
    await target.next((e) => e.kind === 'req' && e.method === MIRROR_WIRE.apply && e.id === mirrorId2);

    // 同工作空间+不同目标：放行
    const otherTarget = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, third.nodeId, 'ws-race', snapshotId));
    expect(otherTarget.kind === 'res' && otherTarget.ok).toBe(true);

    // 第一发回执 → 登记清空 → 同工作空间+同目标可再发
    target.ws.send(encodeEnvelope({
      v: 1, kind: 'res', id: (apply1 as RequestEnvelope).id, method: MIRROR_WIRE.receipt, ok: true, payload: { appliedResources: [] },
    }));
    await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId1);
    const again = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-race', snapshotId));
    expect(again.kind === 'res' && again.ok).toBe(true);

    leader.ws.close(); target.ws.close(); third.ws.close();
  });
});

// ── 离线语义 ────────────────────────────────────────────────────────────────

describe('离线语义', () => {
  it('受理前目标离线 → node_offline（不落 accepted，登记回滚——重发仍 node_offline 而非 mirror_in_progress）', async () => {
    const snapshotId = await uploadSnapshot('ws-off', '1');
    const { leader, target, groupId } = await setupGroup('off');
    target.ws.close();
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.nodeOffline && (e.payload as { nodeId: string }).nodeId === target.nodeId);

    const res = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-off', snapshotId));
    expect(errOf(res).code).toBe('node_offline');
    // 登记已回滚：再次请求仍是 node_offline（而不是被残留的 mirror_in_progress 卡死）
    const again = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-off', snapshotId));
    expect(errOf(again).code).toBe('node_offline');

    // 重连（同 nodeId——enroll 轮换 token）后可正常受理
    const back = await connect('off-target');
    const ok = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-off', snapshotId));
    expect(ok.kind === 'res' && ok.ok).toBe(true);
    leader.ws.close(); back.ws.close();
  });

  it('受理后目标断连 → 登记清理 + leader 收 mirror.result node_offline；无 workspace.mirrored 广播；重连后可再发', async () => {
    const snapshotId = await uploadSnapshot('ws-disc', '1');
    const { leader, target, groupId } = await setupGroup('disc');

    const accepted = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-disc', snapshotId));
    const mirrorId = (resPayload(accepted) as { mirrorId: string }).mirrorId;
    await target.next((e) => e.kind === 'req' && e.method === MIRROR_WIRE.apply); // 在途成立

    target.ws.close();
    const result = await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId);
    expect(result.kind === 'res' && result.ok).toBe(false);
    expect(result.payload).toEqual({ mirrorId, code: 'node_offline', message: expect.any(String) });
    await new Promise((r) => setTimeout(r, 250));
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.workspaceMirrored)).toHaveLength(0);

    // 登记已清：同工作空间+同目标重连后可再发
    const back = await connect('disc-target');
    const again = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-disc', snapshotId));
    expect(again.kind === 'res' && again.ok).toBe(true);
    leader.ws.close(); back.ws.close();
  });
});

// ── leader + 同组校验矩阵 ───────────────────────────────────────────────────

describe('leader + 同组校验矩阵', () => {
  it('非 leader 成员 / 非成员 → forbidden；ghost 组 → not_found；缺参/坏 strategy/坏 confirm/超长 workspaceName → bad_request', async () => {
    const snapshotId = await uploadSnapshot('ws-guard', '1');
    const { leader, target, outsider, groupId } = await setupGroup('guard');

    const byTarget = await callRpc(target, RPC.workspace.mirror, mirrorReq(groupId, leader.nodeId, 'ws-guard', snapshotId));
    expect(errOf(byTarget).code).toBe('forbidden');
    const byOutsider = await callRpc(outsider, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-guard', snapshotId));
    expect(errOf(byOutsider).code).toBe('forbidden');
    const ghost = await callRpc(leader, RPC.workspace.mirror, mirrorReq('no-such-group', target.nodeId, 'ws-guard', snapshotId));
    expect(errOf(ghost).code).toBe('not_found');

    const bads: Array<{ name: string; payload: Record<string, unknown> }> = [
      { name: '缺 groupId', payload: mirrorReq('', target.nodeId, 'ws-guard', snapshotId) },
      { name: '缺 targetNodeId', payload: mirrorReq(groupId, '', 'ws-guard', snapshotId) },
      { name: '缺 workspaceName', payload: mirrorReq(groupId, target.nodeId, '', snapshotId) },
      { name: '缺 snapshotId', payload: mirrorReq(groupId, target.nodeId, 'ws-guard', '') },
      { name: '坏 strategy', payload: { ...mirrorReq(groupId, target.nodeId, 'ws-guard', snapshotId), strategy: 'merge' } },
      { name: '坏 confirm（非布尔）', payload: { ...mirrorReq(groupId, target.nodeId, 'ws-guard', snapshotId), confirm: 'yes' } },
      { name: '超长 workspaceName（>128）', payload: mirrorReq(groupId, target.nodeId, 'w'.repeat(129), snapshotId) },
    ];
    for (const c of bads) {
      const res = await callRpc(leader, RPC.workspace.mirror, c.payload);
      expect(errOf(res).code, `用例：${c.name}`).toBe('bad_request');
    }

    // 目标不是本组成员 → forbidden
    const toOutsider = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, outsider.nodeId, 'ws-guard', snapshotId));
    expect(errOf(toOutsider).code).toBe('forbidden');
    // 快照不存在 → not_found
    const ghostSnap = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-guard', 'no-such-snapshot'));
    expect(errOf(ghostSnap).code).toBe('not_found');
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });
});

// ── 回执健壮性 ──────────────────────────────────────────────────────────────

describe('回执健壮性', () => {
  it('未知 id 的 res 静默忽略（不炸 hub，不影响后续镜像）', async () => {
    const { leader, target, groupId } = await setupGroup('rob');
    const snapshotId = await uploadSnapshot('ws-rob', '1');
    target.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: 'bogus-id', method: MIRROR_WIRE.receipt, ok: true, payload: { appliedResources: [] } }));
    await new Promise((r) => setTimeout(r, 150));

    const res = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-rob', snapshotId));
    expect(res.kind === 'res' && res.ok).toBe(true);
    const mirrorId = (resPayload(res) as { mirrorId: string }).mirrorId;
    await targetApplyAndReceipt(target, { ok: true, appliedResources: [] });
    await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId);
    leader.ws.close(); target.ws.close();
  });

  it('非目标节点伪造回执 → 忽略（真实回执仍可达）；重复回执 → 只结算一次', async () => {
    const { leader, target, outsider, groupId } = await setupGroup('forg');
    const snapshotId = await uploadSnapshot('ws-forg', '1');
    const res = await callRpc(leader, RPC.workspace.mirror, mirrorReq(groupId, target.nodeId, 'ws-forg', snapshotId));
    const mirrorId = (resPayload(res) as { mirrorId: string }).mirrorId;

    // outsider 伪造回执：无效（不是 pending 的目标节点）
    outsider.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: mirrorId, method: MIRROR_WIRE.receipt, ok: true, payload: { appliedResources: ['fake'] } }));
    await new Promise((r) => setTimeout(r, 200));
    expect(leader.inbox.filter((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId)).toHaveLength(0);

    // 真实回执 ×2（重复）：第一份结算，第二份忽略——leader 只收到一份 result
    target.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: mirrorId, method: MIRROR_WIRE.receipt, ok: true, payload: { appliedResources: ['coder-pro'] } }));
    await leader.next((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId);
    target.ws.send(encodeEnvelope({ v: 1, kind: 'res', id: mirrorId, method: MIRROR_WIRE.receipt, ok: true, payload: { appliedResources: ['dup'] } }));
    await new Promise((r) => setTimeout(r, 200));
    const results = leader.inbox.filter((e) => e.kind === 'res' && e.method === MIRROR_WIRE.result && e.id === mirrorId);
    expect(results).toHaveLength(1);
    leader.ws.close(); target.ws.close(); outsider.ws.close();
  });
});
