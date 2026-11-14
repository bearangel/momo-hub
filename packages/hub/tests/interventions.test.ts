// task.intervene hub 侧行为锁（1.11.0 任务生命周期 v2，spec 2026-11-13 §8.2/§8.5/
// §8.6）——真 socket 双客户端（leader/发起方 + worker/执行节点）全真 wire 语义：
// lf 全链（task.intervention 定向 + 账本 waiting + delegation_interventions 行
// pending + payload_json 内容序列化）/ peer 中性进度（内容零泄漏——无事件、无
// title/prompt/interventionId 字样）/ 来源校验 forbidden / 终态 conflict /
// 幂等重发（裁定⑤：事件重发 + 行仍单条）/ intervention_stale / 发起方低版本
// 降级中性进度（防半开）/ 载荷校验矩阵 bad_request。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  EVT, PROTOCOL_VERSION, RPC, TASK_WIRE, encodeEnvelope, makeRequest, parseEnvelope,
  type DelegationStatus, type Envelope,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-iv-'));
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

// ── 工具（delegations.test.ts 同款客户端骨架）────────────────────────────────

function connect(name: string, protoVer: string = PROTOCOL_VERSION) {
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
    // protoVer 可注入（发起方低版本用例：1.10 老发起方过 hello）
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

function resPayload(e: Envelope): Record<string, unknown> {
  if (e.kind !== 'res') throw new Error('非 res 信封');
  return (e.payload ?? {}) as Record<string, unknown>;
}

function errOf(e: Envelope): { code: string; message: string } {
  if (e.kind !== 'res' || e.ok) throw new Error('预期错误响应');
  return e.payload as { code: string; message: string };
}

/** 建主从组：leader（创建者）+ target（凭码入组）。 */
async function setupGroup(pref: string): Promise<{ leader: Client; target: Client; groupId: string }> {
  const leader = await connect(`${pref}-leader`);
  const target = await connect(`${pref}-target`);
  const created = await callRpc(leader, RPC.group.create, { groupName: `Team-${pref}` });
  const { groupId } = resPayload(created) as { groupId: string };
  const invited = await callRpc(leader, RPC.group.invite, { groupId });
  const { inviteCode } = resPayload(invited) as { inviteCode: string };
  await callRpc(target, RPC.group.join, { inviteCode });
  await leader.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
    && (e.payload as { nodeId: string; kind: string }).nodeId === target.nodeId);
  return { leader, target, groupId };
}

/** 建平行组：a（组长+发起方，绑定空间）+ b（成员，绑定承接空间）——peer 草稿链路前提。 */
async function setupPeerGroup(pref: string): Promise<{ a: Client; b: Client; groupId: string }> {
  const a = await connect(`${pref}-peer-a`);
  const b = await connect(`${pref}-peer-b`);
  const created = await callRpc(a, RPC.group.create, {
    groupName: `Team-${pref}-peer`, mode: 'peer', bindWorkspaceName: `ws-${pref}-a`,
  });
  const { groupId } = resPayload(created) as { groupId: string };
  const invited = await callRpc(a, RPC.group.invite, { groupId });
  await callRpc(b, RPC.group.join, { inviteCode: (resPayload(invited) as { inviteCode: string }).inviteCode });
  await callRpc(b, RPC.group.bind, { groupId, workspaceName: `ws-${pref}-b` });
  return { a, b, groupId };
}

interface LedgerRow {
  task_id: string; status: DelegationStatus; session_id: string | null;
  created_at: number; updated_at: number;
}

function ledgerRow(taskId: string): LedgerRow | undefined {
  return db.prepare('SELECT * FROM delegations WHERE task_id = ?').get(taskId) as LedgerRow | undefined;
}

interface InterventionRow {
  id: string; delegation_id: string; run_no: number; kind: string; status: string;
  payload_json: string; answer_json: string | null; answered_by: string | null;
  answered_at: number | null; created_at: number; updated_at: number;
}

function interventionRow(id: string): InterventionRow | undefined {
  return db.prepare('SELECT * FROM delegation_interventions WHERE id = ?').get(id) as InterventionRow | undefined;
}

function delegateReq(groupId: string, targetNodeId: string, opts: { workspaceName?: string; title?: string; prompt?: string; draft?: boolean } = {}): Record<string, unknown> {
  const task: Record<string, unknown> = { title: opts.title ?? '修登录 bug', prompt: opts.prompt ?? '请修复登录失败问题' };
  if (opts.draft !== undefined) task.draft = opts.draft;
  return { groupId, targetNodeId, workspaceName: opts.workspaceName ?? 'ws-iv', task };
}

async function delegateOk(leader: Client, req: Record<string, unknown>): Promise<string> {
  const res = await callRpc(leader, RPC.task.delegate, req);
  if (!(res.kind === 'res' && res.ok)) throw new Error(`delegate 失败：${JSON.stringify(res.payload)}`);
  return (resPayload(res) as { taskId: string }).taskId;
}

/** 目标节点发 task.report（res 信封 id=taskId——wire 约定）。 */
function report(ws: WebSocket, taskId: string, payload: Record<string, unknown>): void {
  ws.send(encodeEnvelope({ v: 1, kind: 'res', id: taskId, method: TASK_WIRE.report, ok: true, payload }));
}

const settle = (ms = 200): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** 合法干预载荷（覆盖全字段），overrides 覆盖指定键（非法矩阵用）。 */
function intervention(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    interventionId: `iv-${randomUUID()}`,
    runNo: 1,
    kind: 'question',
    title: '确认部署窗口',
    prompt: '今晚 22:00 可否执行数据库迁移',
    payload: { risk: 'high', window: '22:00-23:00' },
    options: [
      { id: 'go', label: '按窗口执行', description: '22:00 开始迁移' },
      { id: 'hold', label: '推迟' },
    ],
    deadlineAt: 1893456000000,
    ...overrides,
  };
}

/** 委派并上报 running（等 leader 收到首拍 progress——账本已 running 再 intervene）。 */
async function runningTask(leader: Client, target: Client, groupId: string, pref: string): Promise<string> {
  const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: `ws-${pref}` }));
  report(target.ws, taskId, { status: 'running', sessionId: `sess-${pref}`, progress: { lastSeq: 1, excerpt: '开跑' } });
  await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
    && (e.payload as { taskId: string }).taskId === taskId
    && (e.payload as { lastSeq: number }).lastSeq === 1);
  expect(ledgerRow(taskId)?.status).toBe('running');
  return taskId;
}

// ────────────────────────────────────────────────────────────────────────────
// lf 组全链
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene lf 全链', () => {
  it('running 态上报：res {interventionId} + leader 收 task.intervention（载荷全等于上报）+ 账本 waiting + 干预行 pending（payload_json 内容序列化）', async () => {
    const { leader, target, groupId } = await setupGroup('ivlf');
    const taskId = await runningTask(leader, target, groupId, 'ivlf');
    const iv = intervention({ interventionId: 'iv-lf-1', kind: 'tool_gate' });

    const res = await callRpc(target, RPC.task.intervene, { taskId, intervention: iv });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ interventionId: 'iv-lf-1' });

    const evt = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention);
    expect(evt.payload).toEqual({ taskId, intervention: iv });

    expect(ledgerRow(taskId)?.status).toBe('waiting');
    const row = interventionRow('iv-lf-1');
    expect(row).toMatchObject({
      id: 'iv-lf-1', delegation_id: taskId, run_no: 1, kind: 'tool_gate',
      status: 'pending', answer_json: null, answered_by: null, answered_at: null,
    });
    // payload_json = 内容字段（title/prompt/payload/options/deadline——id/run_no/kind 有专属列）
    expect(JSON.parse(row!.payload_json)).toEqual({
      title: '确认部署窗口',
      prompt: '今晚 22:00 可否执行数据库迁移',
      payload: { risk: 'high', window: '22:00-23:00' },
      options: [
        { id: 'go', label: '按窗口执行', description: '22:00 开始迁移' },
        { id: 'hold', label: '推迟' },
      ],
      deadlineAt: 1893456000000,
    });
    // 定向：执行节点自身不收 task.intervention
    await settle();
    expect(target.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// peer 组中性进度（spec §8.6——平行组不转发请求内容）
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene peer 中性进度', () => {
  it('peer 草稿任务干预：发起方只收中性 task.progress（waiting-input），无 task.intervention 事件、无 title/prompt/interventionId 字样；账本 pending 守卫不动、行照落', async () => {
    const { a, b, groupId } = await setupPeerGroup('ivpeer');
    const taskId = await delegateOk(a, delegateReq(groupId, b.nodeId, {
      workspaceName: 'ws-ivpeer', title: '评估迁移方案', prompt: '请评估并给出结论', draft: true,
    }));
    expect(ledgerRow(taskId)?.status).toBe('pending');

    const iv = intervention({
      interventionId: 'iv-peer-1', kind: 'question',
      title: '机密标题不得外泄', prompt: '机密正文不得外泄',
    });
    const res = await callRpc(b, RPC.task.intervene, { taskId, intervention: iv });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ interventionId: 'iv-peer-1' });

    const prog = await a.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === taskId
      && (e.payload as { excerpt: string }).excerpt === '执行方等待人工处理');
    expect(prog.payload).toMatchObject({
      taskId, sessionId: '', excerpt: '执行方等待人工处理', kind: 'waiting-input',
    });
    // controller 定约：中性进度 lastSeq 恒 0（毫秒时间戳会污染消费侧单调去重）
    expect((prog.payload as { lastSeq: number }).lastSeq).toBe(0);

    await settle();
    expect(a.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(0);
    const leaked = JSON.stringify(a.inbox);
    expect(leaked).not.toContain('机密标题不得外泄');
    expect(leaked).not.toContain('机密正文不得外泄');
    expect(leaked).not.toContain('iv-peer-1');

    // pending→waiting 非法边（spec §8.1 迁移表）：账本守卫不动；干预行照落 pending
    expect(ledgerRow(taskId)?.status).toBe('pending');
    expect(interventionRow('iv-peer-1')).toMatchObject({ delegation_id: taskId, status: 'pending', kind: 'question' });
    a.ws.close(); b.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 来源校验 / 终态冲突
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene 来源与状态守卫', () => {
  it('非执行节点（leader）调 intervene → forbidden；不落行', async () => {
    const { leader, target, groupId } = await setupGroup('ivsrc');
    const taskId = await runningTask(leader, target, groupId, 'ivsrc');
    const res = await callRpc(leader, RPC.task.intervene, { taskId, intervention: intervention() });
    expect(errOf(res).code).toBe('forbidden');
    await settle();
    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE delegation_id = ?')
      .get(taskId) as { n: number }).n).toBe(0);
    leader.ws.close(); target.ws.close();
  });

  it('终态（done）后 intervene → conflict；不落行、无事件', async () => {
    const { leader, target, groupId } = await setupGroup('ivterm');
    const taskId = await runningTask(leader, target, groupId, 'ivterm');
    report(target.ws, taskId, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(ledgerRow(taskId)?.status).toBe('done');

    const res = await callRpc(target, RPC.task.intervene, { taskId, intervention: intervention() });
    expect(errOf(res).code).toBe('conflict');
    await settle();
    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE delegation_id = ?')
      .get(taskId) as { n: number }).n).toBe(0);
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 幂等（裁定⑤：重发也重发事件）与 stale
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene 幂等与 stale', () => {
  it('同 interventionId 二次上报：res 同 id + 事件重发（裁定⑤）+ 行仍单条 + 账本仍 waiting', async () => {
    const { leader, target, groupId } = await setupGroup('ividem');
    const taskId = await runningTask(leader, target, groupId, 'ividem');
    const iv = intervention({ interventionId: 'iv-idem-1' });

    const res1 = await callRpc(target, RPC.task.intervene, { taskId, intervention: iv });
    expect(res1.kind === 'res' && res1.ok).toBe(true);
    const e1 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention);
    expect(ledgerRow(taskId)?.status).toBe('waiting');

    const res2 = await callRpc(target, RPC.task.intervene, { taskId, intervention: iv });
    expect(res2.kind === 'res' && res2.ok).toBe(true);
    expect(resPayload(res2)).toEqual({ interventionId: 'iv-idem-1' });
    const e2 = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention && e !== e1);
    expect((e2.payload as { intervention: { interventionId: string } }).intervention.interventionId).toBe('iv-idem-1');

    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE id = ?')
      .get('iv-idem-1') as { n: number }).n).toBe(1);
    expect(interventionRow('iv-idem-1')?.status).toBe('pending');
    expect(ledgerRow(taskId)?.status).toBe('waiting');
    leader.ws.close(); target.ws.close();
  });

  it('行已 answered（直查 SQL 预置）→ intervention_stale；不重发事件', async () => {
    const { leader, target, groupId } = await setupGroup('ivstale');
    const taskId = await runningTask(leader, target, groupId, 'ivstale');
    const now = Date.now();
    db.prepare(`INSERT INTO delegation_interventions
      (id, delegation_id, run_no, kind, status, payload_json, answer_json, answered_by, answered_at, created_at, updated_at)
      VALUES ('iv-stale-1', ?, 1, 'question', 'answered', ?, ?, ?, ?, ?, ?)`)
      .run(taskId, JSON.stringify({ title: '已答', prompt: '已答' }), JSON.stringify({ text: 'ok' }),
        leader.nodeId, now, now, now);

    const res = await callRpc(target, RPC.task.intervene, {
      taskId, intervention: intervention({ interventionId: 'iv-stale-1' }),
    });
    expect(errOf(res).code).toBe('intervention_stale');
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(0);
    // 行保持预置 answered 态（不被重写）；账本停在 running（不被推 waiting）
    expect(interventionRow('iv-stale-1')?.status).toBe('answered');
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); target.ws.close();
  });

  it('跨任务复用同 interventionId（A 落行后 B 用同 id）→ conflict + B 账本不动 + B 发起方零事件（归属守卫）', async () => {
    const { leader, target, groupId } = await setupGroup('ivxid');
    const taskA = await runningTask(leader, target, groupId, 'ivxid-a');
    const taskB = await runningTask(leader, target, groupId, 'ivxid-b');
    // A 任务先落行 iv-x-1（leader 收到 A 的 intervention 事件）
    const resA = await callRpc(target, RPC.task.intervene, {
      taskId: taskA, intervention: intervention({ interventionId: 'iv-x-1' }),
    });
    expect(resA.kind === 'res' && resA.ok).toBe(true);
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention
      && (e.payload as { taskId: string }).taskId === taskA);
    const evtCountA = leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention).length;

    // B 任务撞同 id：拒绝 + 账本/事件/行三不动
    const resB = await callRpc(target, RPC.task.intervene, {
      taskId: taskB, intervention: intervention({ interventionId: 'iv-x-1' }),
    });
    expect(errOf(resB).code).toBe('conflict');
    await settle();
    expect(ledgerRow(taskB)?.status).toBe('running');
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(evtCountA);
    expect(interventionRow('iv-x-1')?.delegation_id).toBe(taskA);
    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE delegation_id = ?')
      .get(taskB) as { n: number }).n).toBe(0);
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 发起方低版本降级（spec §8.5 防半开——老发起方读不懂新事件）
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene 发起方版本门', () => {
  it('lf 组发起方 protoVer 1.10.0：intervene 受理（行 pending + 账本 waiting）但只发中性进度，无 task.intervention', async () => {
    const leader = await connect('ivold-leader', '1.10.0');
    const target = await connect('ivold-target');
    const created = await callRpc(leader, RPC.group.create, { groupName: 'Team-ivold' });
    const { groupId } = resPayload(created) as { groupId: string };
    const invited = await callRpc(leader, RPC.group.invite, { groupId });
    await callRpc(target, RPC.group.join, { inviteCode: (resPayload(invited) as { inviteCode: string }).inviteCode });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.groupChanged
      && (e.payload as { nodeId: string }).nodeId === target.nodeId);

    const taskId = await runningTask(leader, target, groupId, 'ivold');
    const iv = intervention({ interventionId: 'iv-old-1', title: '低版本发起方不该看到', prompt: '低版本正文不该看到' });
    const res = await callRpc(target, RPC.task.intervene, { taskId, intervention: iv });
    expect(res.kind === 'res' && res.ok).toBe(true);

    const prog = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === taskId
      && (e.payload as { excerpt: string }).excerpt === '执行方等待人工处理');
    expect(prog.payload).toMatchObject({ taskId, excerpt: '执行方等待人工处理', kind: 'waiting-input' });
    expect((prog.payload as { lastSeq: number }).lastSeq).toBe(0);
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskIntervention)).toHaveLength(0);
    expect(JSON.stringify(leader.inbox)).not.toContain('低版本发起方不该看到');

    expect(ledgerRow(taskId)?.status).toBe('waiting');
    expect(interventionRow('iv-old-1')?.status).toBe('pending');
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 载荷校验矩阵
// ────────────────────────────────────────────────────────────────────────────

describe('task.intervene 载荷校验矩阵', () => {
  it('interventionId 空 / runNo 0 / 非法 kind / title 空 / options 项缺 label → bad_request；缺 taskId → bad_request；ghost taskId → not_found；全部不落行', async () => {
    const { leader, target, groupId } = await setupGroup('ivbad');
    const taskId = await runningTask(leader, target, groupId, 'ivbad');
    const bads: Array<{ name: string; iv: Record<string, unknown> }> = [
      { name: 'interventionId 空串', iv: intervention({ interventionId: '' }) },
      { name: 'runNo 0', iv: intervention({ runNo: 0 }) },
      { name: 'runNo 非整数', iv: intervention({ runNo: 1.5 }) },
      { name: '非法 kind', iv: intervention({ kind: 'escalate' }) },
      { name: 'title 空串', iv: intervention({ title: '' }) },
      { name: 'options 非数组', iv: intervention({ options: 'a,b' }) },
      { name: 'options 项缺 label', iv: intervention({ options: [{ id: 'x' }] }) },
      { name: 'deadlineAt 非正整数', iv: intervention({ deadlineAt: 0 }) },
    ];
    for (const c of bads) {
      const res = await callRpc(target, RPC.task.intervene, { taskId, intervention: c.iv });
      expect(errOf(res).code, `用例：${c.name}`).toBe('bad_request');
    }
    expect(errOf(await callRpc(target, RPC.task.intervene, { intervention: intervention() })).code).toBe('bad_request');
    expect(errOf(await callRpc(target, RPC.task.intervene, { taskId: 'no-such-task', intervention: intervention() })).code).toBe('not_found');
    await settle();
    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE delegation_id = ?')
      .get(taskId) as { n: number }).n).toBe(0);
    leader.ws.close(); target.ws.close();
  });
});
