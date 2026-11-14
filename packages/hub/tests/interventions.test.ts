// task.intervene hub 侧行为锁（1.11.0 任务生命周期 v2，spec 2026-11-13 §8.2/§8.5/
// §8.6）——真 socket 双客户端（leader/发起方 + worker/执行节点）全真 wire 语义：
// lf 全链（task.intervention 定向 + 账本 waiting + delegation_interventions 行
// pending + payload_json 内容序列化）/ peer 中性进度（内容零泄漏——无事件、无
// title/prompt/interventionId 字样）/ 来源校验 forbidden / 终态 conflict /
// 幂等重发（裁定⑤：事件重发 + 行仍单条）/ intervention_stale / 发起方低版本
// 降级中性进度（防半开）/ 载荷校验矩阵 bad_request。
// task.answer 结构化答复分支（H5，spec §8.2/§8.6）：发起方代答（approve + 结构化）/
// worker 本地答回传（顶层 decision 兼容映射）/ first_wins 双答竞态 / peer 越权
// forbidden_responder + target 放行 / 四错误态 / 账本非 waiting 不动。
// task.accept / task.reject（H6，spec §8.1/§8.2）：发起方验收闭环——submitted→done
// （accepted_at）+ task.accepted 定向 worker / submitted→running（run_no+1 打回新
// run，submitted_at 保留历史）+ task.rejected（reason）；错误态（not_submitted 幂等
// 拒 / reason 空白 bad_request / 非 by_node_id forbidden / ghost not_found）；peer
// 与 lf 两模式发起方权威（peer 发起方=by_node_id 非组长）；离线容忍（事件 tolerate）。
// H7（report submitted 接入 + task.interventions 对账）：submitted 上报全链
// （账本迁移 + submitted_at/run_no 落列 + task.submitted 定向发起方，peer 照发）/
// deliverable 通道矩阵（report.reason JSON.parse 成对象才透传）/ waiting→submitted
// 合法 / 错误态（pending·终态·坏 runNo 全拒）/ interventions 权限矩阵（两端可查、
// 第三方 forbidden、缺省本节点相关全部）/ 投影完整性（payload_json+专属列合并还原
// InterventionRecord 全字段）/ 断线重连对账（origin 离线期间 worker 干预+自答+提交
// → 重连拉取收敛，无重复行）。
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

function openSocket(cred: { nodeId: string; deviceToken: string }, name: string, protoVer: string = PROTOCOL_VERSION) {
  return new Promise<{ ws: WebSocket; nodeId: string; deviceToken: string; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
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
      else resolve({ ws, nodeId: cred.nodeId, deviceToken: cred.deviceToken, inbox, next });
    });
  });
}

function connect(name: string, protoVer: string = PROTOCOL_VERSION) {
  const cred = enrollNode(db, identity.enrollmentToken, name);
  if ('error' in cred) throw new Error('enroll 失败');
  return openSocket(cred, name, protoVer);
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
  run_no: number | null; submitted_at: number | null; accepted_at: number | null;
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

/** lf 组走完「委派 → running → intervene（账本 waiting + 行 pending + 发起方已收
 *  task.intervention）」链，返回 taskId——task.answer 干预分支用例的公共前置。 */
async function waitingIntervention(leader: Client, target: Client, groupId: string, pref: string, ivId: string): Promise<string> {
  const taskId = await runningTask(leader, target, groupId, pref);
  const res = await callRpc(target, RPC.task.intervene, {
    taskId, intervention: intervention({ interventionId: ivId }),
  });
  if (!(res.kind === 'res' && res.ok)) throw new Error('intervene 失败');
  await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention
    && (e.payload as { taskId: string }).taskId === taskId);
  expect(ledgerRow(taskId)?.status).toBe('waiting');
  expect(interventionRow(ivId)?.status).toBe('pending');
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

// ────────────────────────────────────────────────────────────────────────────
// task.answer 结构化答复（1.11.0 H5——interventionId 在场走干预分支，spec §8.2/§8.6）
// ────────────────────────────────────────────────────────────────────────────

describe('task.answer 发起方代答（lf 组）', () => {
  it('tool_gate 语义 approve：res answered + 行 answered（answer_json 直查）+ worker 收 task.interventionAnswered（answer/answeredBy=leader）+ 账本 waiting→running；答方自身不收对端事件', async () => {
    const { leader, target, groupId } = await setupGroup('anslf');
    const taskId = await waitingIntervention(leader, target, groupId, 'anslf', 'iv-ans-1');

    const res = await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-1', answer: { decision: 'approve' },
    });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ disposition: 'answered' });

    const evt = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered);
    expect(evt.payload).toEqual({
      taskId, interventionId: 'iv-ans-1',
      answer: { decision: 'approve' }, answeredBy: leader.nodeId,
    });

    const row = interventionRow('iv-ans-1');
    expect(row).toMatchObject({ status: 'answered', answered_by: leader.nodeId });
    expect(row!.answered_at).not.toBeNull();
    expect(JSON.parse(row!.answer_json!)).toEqual({ decision: 'approve' });
    expect(ledgerRow(taskId)?.status).toBe('running');

    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('question 结构化（selected+text）：同上形状，answer_json 存结构化答复原样', async () => {
    const { leader, target, groupId } = await setupGroup('ansq');
    const taskId = await waitingIntervention(leader, target, groupId, 'ansq', 'iv-ans-2');
    const answer = { selected: ['go'], text: '按窗口执行' };

    const res = await callRpc(leader, RPC.task.answer, { taskId, interventionId: 'iv-ans-2', answer });
    expect(resPayload(res)).toEqual({ disposition: 'answered' });

    const evt = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered);
    expect(evt.payload).toEqual({
      taskId, interventionId: 'iv-ans-2', answer, answeredBy: leader.nodeId,
    });
    expect(JSON.parse(interventionRow('iv-ans-2')!.answer_json!)).toEqual(answer);
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); target.ws.close();
  });
});

describe('task.answer worker 本地答回传', () => {
  it('target 调 answer（顶层 decision 兼容映射，无 answer 段）：res answered + leader 侧收事件（answeredBy=target）+ answer_json 落映射后 decision + 账本 waiting→running', async () => {
    const { leader, target, groupId } = await setupGroup('answ');
    const taskId = await waitingIntervention(leader, target, groupId, 'answ', 'iv-ans-w1');

    const res = await callRpc(target, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-w1', decision: 'deny',
    });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ disposition: 'answered' });

    const evt = await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered);
    expect(evt.payload).toEqual({
      taskId, interventionId: 'iv-ans-w1',
      answer: { decision: 'deny' }, answeredBy: target.nodeId,
    });
    expect(JSON.parse(interventionRow('iv-ans-w1')!.answer_json!)).toEqual({ decision: 'deny' });
    expect(interventionRow('iv-ans-w1')?.answered_by).toBe(target.nodeId);
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); target.ws.close();
  });
});

describe('task.answer first_wins 双答竞态', () => {
  it('leader 与 target 顺序双答：第一 answered、第二 intervention_stale；行 answer_json/answered_by 保持第一份', async () => {
    const { leader, target, groupId } = await setupGroup('ansfx');
    const taskId = await waitingIntervention(leader, target, groupId, 'ansfx', 'iv-ans-fx1');

    const res1 = await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-fx1', answer: { decision: 'approve' },
    });
    expect(res1.kind === 'res' && res1.ok).toBe(true);
    expect(resPayload(res1)).toEqual({ disposition: 'answered' });

    const res2 = await callRpc(target, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-fx1', answer: { text: '本地也已答复' },
    });
    expect(errOf(res2).code).toBe('intervention_stale');

    const row = interventionRow('iv-ans-fx1');
    expect(row?.answered_by).toBe(leader.nodeId);
    expect(JSON.parse(row!.answer_json!)).toEqual({ decision: 'approve' });
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); target.ws.close();
  });
});

describe('task.answer peer 组越权与放行', () => {
  it('发起方代答 → forbidden_responder（行不动）；target 本地答 → 正常（行 answered + 账本 pending 不动 + 发起方零干预事件零泄漏）', async () => {
    const { a, b, groupId } = await setupPeerGroup('anspeer');
    const taskId = await delegateOk(a, delegateReq(groupId, b.nodeId, {
      workspaceName: 'ws-anspeer', title: '评估迁移方案', prompt: '请评估并给出结论', draft: true,
    }));
    const res = await callRpc(b, RPC.task.intervene, {
      taskId,
      intervention: intervention({ interventionId: 'iv-ans-p1', title: '机密问题不外泄', prompt: '机密正文不外泄' }),
    });
    expect(res.kind === 'res' && res.ok).toBe(true);
    await a.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === taskId
      && (e.payload as { excerpt: string }).excerpt === '执行方等待人工处理');
    expect(ledgerRow(taskId)?.status).toBe('pending');

    const denied = await callRpc(a, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-p1', answer: { decision: 'approve' },
    });
    expect(errOf(denied).code).toBe('forbidden_responder');
    expect(interventionRow('iv-ans-p1')?.status).toBe('pending');

    const ok = await callRpc(b, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-p1', answer: { selected: ['go'] },
    });
    expect(resPayload(ok)).toEqual({ disposition: 'answered' });
    expect(interventionRow('iv-ans-p1')).toMatchObject({ status: 'answered', answered_by: b.nodeId });
    expect(JSON.parse(interventionRow('iv-ans-p1')!.answer_json!)).toEqual({ selected: ['go'] });
    expect(ledgerRow(taskId)?.status).toBe('pending');

    // §8.6 平行组两干预事件均不转发：发起方无 interventionAnswered，interventionId 不泄漏
    await settle();
    expect(a.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered)).toHaveLength(0);
    expect(JSON.stringify(a.inbox)).not.toContain('iv-ans-p1');
    a.ws.close(); b.ws.close();
  });
});

describe('task.answer 干预分支错误态', () => {
  it('intervention_not_found / 全空 answer bad_request / 非法载荷 bad_request / 跨任务 conflict / 终态 conflict——全部不落账不改行', async () => {
    const { leader, target, groupId } = await setupGroup('anserr');
    const taskId = await waitingIntervention(leader, target, groupId, 'anserr', 'iv-ans-e1');

    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'no-such-iv', answer: { decision: 'approve' },
    })).code).toBe('intervention_not_found');
    // 全空 answer：无 answer 段且无顶层 decision / answer 空对象 / decision 非法枚举
    expect(errOf(await callRpc(leader, RPC.task.answer, { taskId, interventionId: 'iv-ans-e1' })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-e1', answer: {},
    })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-e1', decision: 'accept',
    })).code).toBe('bad_request');
    // interventionId / answer 字段形态非法
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: '', answer: { decision: 'approve' },
    })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-e1', answer: { selected: [123] },
    })).code).toBe('bad_request');
    expect(interventionRow('iv-ans-e1')?.status).toBe('pending');

    // 跨任务归属：task2 用 task1 的干预 id → conflict
    const task2 = await runningTask(leader, target, groupId, 'anserr2');
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId: task2, interventionId: 'iv-ans-e1', answer: { decision: 'approve' },
    })).code).toBe('conflict');

    // 终态：task 走 done 后答复 → conflict；行保持 pending
    report(target.ws, taskId, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(ledgerRow(taskId)?.status).toBe('done');
    expect(errOf(await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-e1', answer: { decision: 'approve' },
    })).code).toBe('conflict');
    expect(interventionRow('iv-ans-e1')?.status).toBe('pending');
    leader.ws.close(); target.ws.close();
  });

  it('非发起方非执行节点的第三节点答复 → forbidden', async () => {
    const { leader, target, groupId } = await setupGroup('ans3rd');
    const taskId = await waitingIntervention(leader, target, groupId, 'ans3rd', 'iv-ans-t1');
    const third = await connect('ans3rd-bystander');
    await callRpc(third, RPC.group.join, { inviteCode: (resPayload(await callRpc(leader, RPC.group.invite, { groupId })) as { inviteCode: string }).inviteCode });
    const res = await callRpc(third, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-t1', answer: { decision: 'approve' },
    });
    expect(errOf(res).code).toBe('forbidden');
    expect(interventionRow('iv-ans-t1')?.status).toBe('pending');
    leader.ws.close(); target.ws.close(); third.ws.close();
  });

  it('账本非 waiting（running，等待已解除）：行照答、账本不动（保持 running）', async () => {
    const { leader, target, groupId } = await setupGroup('ansrun');
    const taskId = await waitingIntervention(leader, target, groupId, 'ansrun', 'iv-ans-r1');
    // worker 无 kind 上报解除等待 → 账本回 running（干预行仍 pending）
    report(target.ws, taskId, { status: 'running', sessionId: 'sess-ansrun' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === taskId
      && (e.payload as { lastSeq: number }).lastSeq === 0);
    expect(ledgerRow(taskId)?.status).toBe('running');

    const res = await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-ans-r1', answer: { decision: 'approve' },
    });
    expect(resPayload(res)).toEqual({ disposition: 'answered' });
    expect(interventionRow('iv-ans-r1')?.status).toBe('answered');
    expect(ledgerRow(taskId)?.status).toBe('running');
    leader.ws.close(); target.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// task.accept / task.reject（H6，spec 2026-11-13 §8.1/§8.2——发起方验收闭环）
// ────────────────────────────────────────────────────────────────────────────

/** 真实路径驱动 submitted 态（H7 起 report submitted 已接通，原 SQL 预置退役）：
 *  worker 上报 status='submitted'（runNo 指定值/缺省 1），轮询账本收敛到 submitted
 *  再返回——跨 socket 顺序不保证，以账本为准（不用固定 sleep）。 */
async function driveToSubmitted(target: Client, taskId: string, runNo = 1): Promise<void> {
  report(target.ws, taskId, { status: 'submitted', runNo });
  for (let i = 0; i < 100 && ledgerRow(taskId)?.status !== 'submitted'; i++) await settle(20);
  expect(ledgerRow(taskId)?.status).toBe('submitted');
}

describe('task.accept 全链', () => {
  it('submitted 态 accept：res {runNo} + 账本 done + accepted_at 落列 + worker 收 task.accepted {taskId, runNo}；发起方自身不收事件', async () => {
    const { leader, target, groupId } = await setupGroup('acclf');
    const taskId = await runningTask(leader, target, groupId, 'acclf');
    await driveToSubmitted(target, taskId);

    const res = await callRpc(leader, RPC.task.accept, { taskId });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ runNo: 1 });

    const evt = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskAccepted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });

    const row = ledgerRow(taskId);
    expect(row?.status).toBe('done');
    expect(row?.accepted_at).not.toBeNull();
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskAccepted)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

describe('task.reject 全链', () => {
  it('submitted(run_no=1) reject：res {runNo:2} + 账本 running + run_no=2 + submitted_at 保留 + accepted_at 仍空 + worker 收 task.rejected {taskId, runNo:2, reason}', async () => {
    const { leader, target, groupId } = await setupGroup('rjlf');
    const taskId = await runningTask(leader, target, groupId, 'rjlf');
    await driveToSubmitted(target, taskId, 1);

    const res = await callRpc(leader, RPC.task.reject, { taskId, reason: '边界条件未覆盖，请补充空输入用例' });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ runNo: 2 });

    const evt = await target.next((e) => e.kind === 'evt' && e.method === EVT.taskRejected
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(evt.payload).toEqual({ taskId, runNo: 2, reason: '边界条件未覆盖，请补充空输入用例' });

    const row = ledgerRow(taskId);
    expect(row?.status).toBe('running');
    expect(row?.run_no).toBe(2);
    expect(row?.submitted_at).not.toBeNull();
    expect(row?.accepted_at).toBeNull();
    await settle();
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskRejected)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

describe('task.accept / task.reject 错误态', () => {
  it('非 submitted（running）accept / reject → not_submitted，账本不动', async () => {
    const { leader, target, groupId } = await setupGroup('acrun');
    const taskId = await runningTask(leader, target, groupId, 'acrun');
    expect(ledgerRow(taskId)?.status).toBe('running');

    expect(errOf(await callRpc(leader, RPC.task.accept, { taskId })).code).toBe('not_submitted');
    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId, reason: '打回' })).code).toBe('not_submitted');
    await settle();
    expect(ledgerRow(taskId)).toMatchObject({ status: 'running', run_no: null, accepted_at: null });
    expect(target.inbox.filter((e) => e.kind === 'evt'
      && (e.method === EVT.taskAccepted || e.method === EVT.taskRejected))).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('done 后再 accept → not_submitted（幂等拒绝）；打回后的 running 再 reject → not_submitted', async () => {
    const { leader, target, groupId } = await setupGroup('acidem');
    const taskId = await runningTask(leader, target, groupId, 'acidem');
    await driveToSubmitted(target, taskId);
    await callRpc(leader, RPC.task.accept, { taskId });
    expect(ledgerRow(taskId)?.status).toBe('done');
    expect(errOf(await callRpc(leader, RPC.task.accept, { taskId })).code).toBe('not_submitted');

    const task2 = await runningTask(leader, target, groupId, 'acidem2');
    await driveToSubmitted(target, task2);
    await callRpc(leader, RPC.task.reject, { taskId: task2, reason: '返工' });
    expect(ledgerRow(task2)).toMatchObject({ status: 'running', run_no: 2 });
    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId: task2, reason: '再打回' })).code).toBe('not_submitted');
    expect(errOf(await callRpc(leader, RPC.task.accept, { taskId: task2 })).code).toBe('not_submitted');
    leader.ws.close(); target.ws.close();
  });

  it('reason 缺失 / 空串 / 纯空白 → bad_request；缺 taskId → bad_request；ghost taskId → not_found', async () => {
    const { leader, target, groupId } = await setupGroup('acbad');
    const taskId = await runningTask(leader, target, groupId, 'acbad');
    await driveToSubmitted(target, taskId);

    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId, reason: '' })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId, reason: '   ' })).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.accept, {})).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.reject, {})).code).toBe('bad_request');
    expect(errOf(await callRpc(leader, RPC.task.accept, { taskId: 'no-such-task' })).code).toBe('not_found');
    expect(errOf(await callRpc(leader, RPC.task.reject, { taskId: 'no-such-task', reason: 'r' })).code).toBe('not_found');
    await settle();
    expect(ledgerRow(taskId)).toMatchObject({ status: 'submitted', accepted_at: null });
    expect(target.inbox.filter((e) => e.kind === 'evt'
      && (e.method === EVT.taskAccepted || e.method === EVT.taskRejected))).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('非发起方（第三方 join 组成员）accept / reject → forbidden，账本不动', async () => {
    const { leader, target, groupId } = await setupGroup('ac3rd');
    const taskId = await runningTask(leader, target, groupId, 'ac3rd');
    await driveToSubmitted(target, taskId);
    const third = await connect('ac3rd-bystander');
    await callRpc(third, RPC.group.join, { inviteCode: (resPayload(await callRpc(leader, RPC.group.invite, { groupId })) as { inviteCode: string }).inviteCode });

    expect(errOf(await callRpc(third, RPC.task.accept, { taskId })).code).toBe('forbidden');
    expect(errOf(await callRpc(third, RPC.task.reject, { taskId, reason: '越权打回' })).code).toBe('forbidden');
    await settle();
    expect(ledgerRow(taskId)).toMatchObject({ status: 'submitted', accepted_at: null });
    expect(target.inbox.filter((e) => e.kind === 'evt'
      && (e.method === EVT.taskAccepted || e.method === EVT.taskRejected))).toHaveLength(0);
    leader.ws.close(); target.ws.close(); third.ws.close();
  });

  it('执行节点（target）非发起方 → forbidden', async () => {
    const { leader, target, groupId } = await setupGroup('acsrc');
    const taskId = await runningTask(leader, target, groupId, 'acsrc');
    await driveToSubmitted(target, taskId);
    expect(errOf(await callRpc(target, RPC.task.accept, { taskId })).code).toBe('forbidden');
    expect(ledgerRow(taskId)?.status).toBe('submitted');
    leader.ws.close(); target.ws.close();
  });
});

describe('task.accept / task.reject 两模式发起方权威', () => {
  it('peer 组非组长成员（by_node_id）委派草稿并 accept：正常受理（锁死不入 LEADER_ONLY_METHODS 的裁定）+ worker 收 task.accepted', async () => {
    const { a, b, groupId } = await setupPeerGroup('acpeer');
    // b（普通成员，非组长 leader_node_id=a）作为发起方委派草稿给 a（建组已绑定承接空间）
    const taskId = await delegateOk(b, delegateReq(groupId, a.nodeId, {
      workspaceName: 'ws-acpeer', title: '评估迁移方案', prompt: '请评估并给出结论', draft: true,
    }));
    // 真实路径纪律（H7）：peer 草稿 pending→submitted 非法边（spec §8.1），先报
    // running 进合法源态再提交——worker studio 同序
    report(a.ws, taskId, { status: 'running', sessionId: 'sess-acpeer' });
    for (let i = 0; i < 100 && ledgerRow(taskId)?.status !== 'running'; i++) await settle(20);
    await driveToSubmitted(a, taskId);

    const res = await callRpc(b, RPC.task.accept, { taskId });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(resPayload(res)).toEqual({ runNo: 1 });

    const evt = await a.next((e) => e.kind === 'evt' && e.method === EVT.taskAccepted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    expect(ledgerRow(taskId)).toMatchObject({ status: 'done', run_no: 1 });
    expect(ledgerRow(taskId)?.accepted_at).not.toBeNull();

    // 同链打回方向：b 对新 submitted 任务 reject 正常（peer 提交/验收三事件照发，§8.6 仅干预两事件不发）
    const task2 = await delegateOk(b, delegateReq(groupId, a.nodeId, {
      workspaceName: 'ws-acpeer', title: '复核结论', prompt: '请复核', draft: true,
    }));
    report(a.ws, task2, { status: 'running', sessionId: 'sess-acpeer2' });
    for (let i = 0; i < 100 && ledgerRow(task2)?.status !== 'running'; i++) await settle(20);
    await driveToSubmitted(a, task2, 1);
    const res2 = await callRpc(b, RPC.task.reject, { taskId: task2, reason: '结论缺数据支撑' });
    expect(resPayload(res2)).toEqual({ runNo: 2 });
    const evt2 = await a.next((e) => e.kind === 'evt' && e.method === EVT.taskRejected
      && (e.payload as { taskId: string }).taskId === task2);
    expect(evt2.payload).toEqual({ taskId: task2, runNo: 2, reason: '结论缺数据支撑' });
    expect(ledgerRow(task2)).toMatchObject({ status: 'running', run_no: 2 });
    a.ws.close(); b.ws.close();
  });
});

describe('task.accept 离线容忍', () => {
  it('worker（target）断线后 accept 仍成功：submitted 不被离线迁移收割 + 事件 tolerate + 账本 done', async () => {
    const { leader, target, groupId } = await setupGroup('acoff');
    const taskId = await runningTask(leader, target, groupId, 'acoff');
    await driveToSubmitted(target, taskId);
    target.ws.close();
    await settle();
    // submitted 不在 onNodeOffline 收割清单（pending/running/waiting）——待验收态跨断线存活
    expect(ledgerRow(taskId)?.status).toBe('submitted');

    const res = await callRpc(leader, RPC.task.accept, { taskId });
    expect(res.kind === 'res' && res.ok).toBe(true);
    expect(ledgerRow(taskId)).toMatchObject({ status: 'done' });
    expect(ledgerRow(taskId)?.accepted_at).not.toBeNull();
    leader.ws.close();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// H7：task.report submitted 接入 + task.interventions 对账
// ────────────────────────────────────────────────────────────────────────────

function awaitSubmitted(by: Client, taskId: string, runNo: number): Promise<Envelope> {
  return by.next((e) => e.kind === 'evt' && e.method === EVT.taskSubmitted
    && (e.payload as { taskId: string }).taskId === taskId
    && (e.payload as { runNo: number }).runNo === runNo);
}

describe('task.report submitted 全链（H7）', () => {
  it('running 报 submitted：账本 submitted + submitted_at/run_no 落列 + origin 收 task.submitted {taskId, runNo, deliverable}；target 不收', async () => {
    const { leader, target, groupId } = await setupGroup('sub1');
    const taskId = await runningTask(leader, target, groupId, 'sub1');
    const before = Date.now();
    report(target.ws, taskId, { status: 'submitted', runNo: 1, reason: '{"summary":"修复完成，回归全绿"}' });

    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1, deliverable: { summary: '修复完成，回归全绿' } });

    const row = ledgerRow(taskId);
    expect(row?.status).toBe('submitted');
    expect(row?.run_no).toBe(1);
    expect(row?.submitted_at).not.toBeNull();
    expect(row!.submitted_at!).toBeGreaterThanOrEqual(before);
    await settle();
    expect(target.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskSubmitted)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('打回后新 run 提交不带 runNo：载荷缺省沿用账本行值（run_no=2），事件 runNo=2', async () => {
    const { leader, target, groupId } = await setupGroup('sub1b');
    const taskId = await runningTask(leader, target, groupId, 'sub1b');
    await driveToSubmitted(target, taskId, 1);
    const rj = await callRpc(leader, RPC.task.reject, { taskId, reason: '补充空输入用例' });
    expect(resPayload(rj)).toEqual({ runNo: 2 });
    expect(ledgerRow(taskId)?.status).toBe('running');

    report(target.ws, taskId, { status: 'submitted' });
    const evt = await awaitSubmitted(leader, taskId, 2);
    expect(evt.payload).toEqual({ taskId, runNo: 2 });
    expect(ledgerRow(taskId)).toMatchObject({ status: 'submitted', run_no: 2 });
    leader.ws.close(); target.ws.close();
  });

  it('peer 组照发（spec 裁定 #9 提交/验收跨节点）：peer 草稿 submitted 上报 → 发起方收 task.submitted', async () => {
    const { a, b, groupId } = await setupPeerGroup('subpeer');
    const taskId = await delegateOk(b, delegateReq(groupId, a.nodeId, {
      workspaceName: 'ws-subpeer', title: '评估迁移方案', prompt: '请评估并给出结论', draft: true,
    }));
    expect(ledgerRow(taskId)?.status).toBe('pending');
    // peer 草稿不经 running 拍直接 submitted：pending→submitted 非法——先经 received 拍
    // 仍不改账本（pending 生命周期拍），故本用例走账本合法路径：a 先报 running
    report(a.ws, taskId, { status: 'running', sessionId: 'sess-subpeer' });
    await b.next((e) => e.kind === 'evt' && e.method === EVT.taskProgress
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(ledgerRow(taskId)?.status).toBe('running');

    report(a.ws, taskId, { status: 'submitted', runNo: 1, reason: '{"summary":"peer 提交照发"}' });
    const evt = await awaitSubmitted(b, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1, deliverable: { summary: 'peer 提交照发' } });
    expect(ledgerRow(taskId)).toMatchObject({ status: 'submitted', run_no: 1 });
    a.ws.close(); b.ws.close();
  });
});

describe('task.submitted deliverable 通道矩阵（report.reason → deliverable 约定）', () => {
  it('reason 非 JSON 串：事件不带 deliverable 键', async () => {
    const { leader, target, groupId } = await setupGroup('dlv1');
    const taskId = await runningTask(leader, target, groupId, 'dlv1');
    report(target.ws, taskId, { status: 'submitted', runNo: 1, reason: '完成，产物在 workspace 输出目录' });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    leader.ws.close(); target.ws.close();
  });

  it('reason 缺席：事件不带 deliverable 键', async () => {
    const { leader, target, groupId } = await setupGroup('dlv2');
    const taskId = await runningTask(leader, target, groupId, 'dlv2');
    report(target.ws, taskId, { status: 'submitted', runNo: 1 });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    leader.ws.close(); target.ws.close();
  });

  it('reason 合法 JSON 对象：透传 deliverable（嵌套结构保真）', async () => {
    const { leader, target, groupId } = await setupGroup('dlv3');
    const taskId = await runningTask(leader, target, groupId, 'dlv3');
    report(target.ws, taskId, {
      status: 'submitted', runNo: 1,
      reason: '{"summary":"重构完成","files":["a.ts","b.ts"],"tests":{"passed":12,"failed":0}}',
    });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({
      taskId, runNo: 1,
      deliverable: { summary: '重构完成', files: ['a.ts', 'b.ts'], tests: { passed: 12, failed: 0 } },
    });
    leader.ws.close(); target.ws.close();
  });

  it('reason 为 JSON 但非对象（数字）：不带 deliverable 键', async () => {
    const { leader, target, groupId } = await setupGroup('dlv4');
    const taskId = await runningTask(leader, target, groupId, 'dlv4');
    report(target.ws, taskId, { status: 'submitted', runNo: 1, reason: '42' });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    leader.ws.close(); target.ws.close();
  });
});

describe('waiting→submitted 合法迁移', () => {
  it('等人工中被发起方答复恢复 running 后提交：waiting→running→submitted 全链', async () => {
    const { leader, target, groupId } = await setupGroup('wsub1');
    const taskId = await waitingIntervention(leader, target, groupId, 'wsub1', 'iv-wsub-1');
    const ans = await callRpc(leader, RPC.task.answer, {
      taskId, interventionId: 'iv-wsub-1', answer: { decision: 'approve' },
    });
    expect(resPayload(ans)).toEqual({ disposition: 'answered' });
    expect(ledgerRow(taskId)?.status).toBe('running');

    report(target.ws, taskId, { status: 'submitted', runNo: 1 });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    expect(ledgerRow(taskId)?.status).toBe('submitted');
    leader.ws.close(); target.ws.close();
  });

  it('waiting 直接报 submitted（无解除拍）：同样合法迁移', async () => {
    const { leader, target, groupId } = await setupGroup('wsub2');
    const taskId = await waitingIntervention(leader, target, groupId, 'wsub2', 'iv-wsub-2');
    expect(ledgerRow(taskId)?.status).toBe('waiting');

    report(target.ws, taskId, { status: 'submitted', runNo: 1 });
    const evt = await awaitSubmitted(leader, taskId, 1);
    expect(evt.payload).toEqual({ taskId, runNo: 1 });
    expect(ledgerRow(taskId)?.status).toBe('submitted');
    leader.ws.close(); target.ws.close();
  });
});

describe('task.report submitted 错误态', () => {
  it('pending 报 submitted：canTransition 拒——账本不动、无事件', async () => {
    const { leader, target, groupId } = await setupGroup('sube1');
    const taskId = await delegateOk(leader, delegateReq(groupId, target.nodeId, { workspaceName: 'ws-sube1' }));
    expect(ledgerRow(taskId)?.status).toBe('pending');

    report(target.ws, taskId, { status: 'submitted', runNo: 1 });
    await settle();
    expect(ledgerRow(taskId)).toMatchObject({ status: 'pending', run_no: null, submitted_at: null });
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskSubmitted)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('终态（done）报 submitted：既有终态语义——账本不动、无事件', async () => {
    const { leader, target, groupId } = await setupGroup('sube2');
    const taskId = await runningTask(leader, target, groupId, 'sube2');
    report(target.ws, taskId, { status: 'done' });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskCompleted
      && (e.payload as { taskId: string }).taskId === taskId);
    expect(ledgerRow(taskId)?.status).toBe('done');

    report(target.ws, taskId, { status: 'submitted', runNo: 1 });
    await settle();
    expect(ledgerRow(taskId)?.status).toBe('done');
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskSubmitted)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });

  it('runNo 非正整数（0/2.5/字符串）：载荷整包拒——账本不动、无事件', async () => {
    const { leader, target, groupId } = await setupGroup('sube3');
    const taskId = await runningTask(leader, target, groupId, 'sube3');
    for (const bad of [0, 2.5, '1']) {
      report(target.ws, taskId, { status: 'submitted', runNo: bad });
    }
    await settle();
    expect(ledgerRow(taskId)?.status).toBe('running');
    expect(leader.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.taskSubmitted)).toHaveLength(0);
    leader.ws.close(); target.ws.close();
  });
});

describe('task.interventions 权限矩阵', () => {
  it('两端可查（by/target 视图同含 pending 行）；第三方 forbidden；缺省=本节点相关全部（by 查含多任务行；无关节点空清单；ghost not_found）', async () => {
    const { leader, target, groupId } = await setupGroup('ivq1');
    const third = await connect('ivq1-bystander');
    await callRpc(third, RPC.group.join, { inviteCode: (resPayload(await callRpc(leader, RPC.group.invite, { groupId })) as { inviteCode: string }).inviteCode });

    const taskA = await runningTask(leader, target, groupId, 'ivq1-a');
    const taskB = await runningTask(leader, target, groupId, 'ivq1-b');
    await callRpc(target, RPC.task.intervene, { taskId: taskA, intervention: intervention({ interventionId: 'iv-q-a1' }) });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention
      && (e.payload as { taskId: string }).taskId === taskA);
    await callRpc(target, RPC.task.intervene, { taskId: taskB, intervention: intervention({ interventionId: 'iv-q-b1' }) });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention
      && (e.payload as { taskId: string }).taskId === taskB);

    // taskId 在场：发起方可查（视图含 pending 行）
    const byView = await callRpc(leader, RPC.task.interventions, { taskId: taskA });
    expect(byView.kind === 'res' && byView.ok).toBe(true);
    const byRows = (resPayload(byView) as { interventions: Array<{ interventionId: string; status: string }> }).interventions;
    expect(byRows).toHaveLength(1);
    expect(byRows[0]).toMatchObject({ interventionId: 'iv-q-a1', status: 'pending' });

    // 执行节点可查（同视图）
    const tView = await callRpc(target, RPC.task.interventions, { taskId: taskA });
    expect(resPayload(tView)).toEqual(resPayload(byView));

    // 第三方（组内非两端节点）→ forbidden
    expect(errOf(await callRpc(third, RPC.task.interventions, { taskId: taskA })).code).toBe('forbidden');

    // 缺省：by 查全部相关（含 taskA+taskB 多任务行）
    const byAll = await callRpc(leader, RPC.task.interventions, {});
    const byAllIds = (resPayload(byAll) as { interventions: Array<{ interventionId: string }> }).interventions
      .map((r) => r.interventionId).sort();
    expect(byAllIds).toEqual(['iv-q-a1', 'iv-q-b1']);

    // 缺省：target 查全部相关（两任务的执行节点）
    const tAll = await callRpc(target, RPC.task.interventions, {});
    expect((resPayload(tAll) as { interventions: Array<{ interventionId: string }> }).interventions
      .map((r) => r.interventionId).sort()).toEqual(['iv-q-a1', 'iv-q-b1']);

    // 缺省：无关节点空清单（非 forbidden——无存在性泄漏面）
    const thirdAll = await callRpc(third, RPC.task.interventions, {});
    expect(resPayload(thirdAll)).toEqual({ interventions: [] });

    // ghost taskId → not_found
    expect(errOf(await callRpc(leader, RPC.task.interventions, { taskId: 'no-such-task' })).code).toBe('not_found');
    leader.ws.close(); target.ws.close(); third.ws.close();
  });
});

describe('task.interventions 投影完整性', () => {
  it('全字段行经 res 还原与 InterventionRecord 逐字段相等：pending 行不带 answer 三键；answered 行含 answer/answeredBy/answeredAt', async () => {
    const { leader, target, groupId } = await setupGroup('ivp1');
    const taskId = await runningTask(leader, target, groupId, 'ivp1');
    await callRpc(target, RPC.task.intervene, {
      taskId, intervention: intervention({ interventionId: 'iv-proj-1' }),
    });
    await leader.next((e) => e.kind === 'evt' && e.method === EVT.taskIntervention
      && (e.payload as { taskId: string }).taskId === taskId);

    const pending = await callRpc(leader, RPC.task.interventions, { taskId });
    const pendingRows = (resPayload(pending) as { interventions: unknown[] }).interventions;
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toEqual({
      interventionId: 'iv-proj-1',
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
      status: 'pending',
      createdAt: expect.any(Number),
    });

    const answer = { selected: ['go'], text: '按窗口执行' };
    await callRpc(leader, RPC.task.answer, { taskId, interventionId: 'iv-proj-1', answer });
    await target.next((e) => e.kind === 'evt' && e.method === EVT.taskInterventionAnswered
      && (e.payload as { taskId: string }).taskId === taskId);

    const answered = await callRpc(leader, RPC.task.interventions, { taskId });
    const answeredRows = (resPayload(answered) as { interventions: unknown[] }).interventions;
    expect(answeredRows).toHaveLength(1);
    expect(answeredRows[0]).toEqual({
      interventionId: 'iv-proj-1',
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
      status: 'answered',
      answer,
      answeredBy: leader.nodeId,
      answeredAt: expect.any(Number),
      createdAt: expect.any(Number),
    });
    leader.ws.close(); target.ws.close();
  });
});

describe('断线重连对账（origin 离线期间 worker 干预+自答+提交）', () => {
  it('origin 断线：worker intervene（事件 tolerate）→ 自答 → report submitted → origin 重连拉 task.interventions 见 answered 行 + task.list 见 submitted；无重复行、状态收敛', async () => {
    const { leader, target, groupId } = await setupGroup('ivre');
    const taskId = await runningTask(leader, target, groupId, 'ivre');
    const ivId = 'iv-recon-1';

    // origin 断线（by_node_id 离线不触发收割——只有 target 离线才 interrupted）
    leader.ws.close();
    await settle();

    // worker 干预：发起方离线（originConn 查无 → 低版本分支中性进度，离线 tolerate）
    // ——账本照落 waiting + 行 pending，事件面零阻塞
    const iv = await callRpc(target, RPC.task.intervene, { taskId, intervention: intervention({ interventionId: ivId }) });
    expect(iv.kind === 'res' && iv.ok).toBe(true);
    expect(ledgerRow(taskId)?.status).toBe('waiting');
    expect(interventionRow(ivId)?.status).toBe('pending');

    // worker 自答（本地答回传通道——对端事件 tolerate）
    const ans = await callRpc(target, RPC.task.answer, { taskId, interventionId: ivId, answer: { decision: 'approve' } });
    expect(resPayload(ans)).toEqual({ disposition: 'answered' });
    expect(interventionRow(ivId)).toMatchObject({ status: 'answered', answered_by: target.nodeId });

    // worker 提交（task.submitted 事件 tolerate）
    report(target.ws, taskId, { status: 'submitted', runNo: 1, reason: '{"summary":"离线期间已交付"}' });
    for (let i = 0; i < 100 && ledgerRow(taskId)?.status !== 'submitted'; i++) await settle(20);
    expect(ledgerRow(taskId)).toMatchObject({ status: 'submitted', run_no: 1 });

    // origin 重连（同身份复用 deviceToken——nodeId 不变保两端权限）
    const origin = await openSocket({ nodeId: leader.nodeId, deviceToken: leader.deviceToken }, 'ivre-leader');

    // 拉取对账：看到 answered 行（含答复与答方）
    const pull1 = await callRpc(origin, RPC.task.interventions, { taskId });
    const rows1 = (resPayload(pull1) as { interventions: unknown[] }).interventions;
    expect(rows1).toHaveLength(1);
    expect(rows1[0]).toMatchObject({
      interventionId: ivId, status: 'answered', answeredBy: target.nodeId,
      answer: { decision: 'approve' },
    });

    // 幂等：重复拉取无重复行、内容稳定
    const pull2 = await callRpc(origin, RPC.task.interventions, { taskId });
    expect((resPayload(pull2) as { interventions: unknown[] }).interventions).toHaveLength(1);
    expect((db.prepare('SELECT COUNT(*) AS n FROM delegation_interventions WHERE delegation_id = ?')
      .get(taskId) as { n: number }).n).toBe(1);

    // task.list 收敛：origin 看到任务 submitted（含 runNo/submittedAt 投影）
    const list = await callRpc(origin, RPC.task.list, { groupId });
    const rec = (resPayload(list) as { tasks: Array<{ taskId: string; status: string; runNo?: number; submittedAt?: number }> })
      .tasks.find((t) => t.taskId === taskId);
    expect(rec).toMatchObject({ status: 'submitted', runNo: 1 });
    expect(rec?.submittedAt).not.toBeUndefined();
    origin.ws.close(); target.ws.close();
  });
});
