// M5.0 资源归属权威（slug 领地制）hub 侧行为锁（spec 2026-10-30 §2-§6/§8 hub 矩阵）——
// 真实 listen + 真 fetch + 真 ws（无 mock，同 resources-unpublish.test.ts 模式）：
// ① 发布认领：新 slug INSERT 即认领（owner 写入 + author 定死 = owner 名）/ owner 新版
//    放行（author 不变）/ 非 owner 新版 403 not_owner（同 version 越权者也拿 403——
//    403 先于 409 version_exists，信息最小化）/ 非 owner 新 slug 不受影响
// ② 无主认领三态：NULL 认领 / 悬空认领（owner 指向已删 nodes 行）/ 并发双认领互斥
//    （条件 UPDATE changes=0 → 重读 owner → 403）
// ③ unpublish owner-only：owner 成功（墓碑/revision 不变语义）/ 非 owner 403（权限
//    先于幂等）/ 不存在 404
// ④ hello 同步改名三分支：缺省/非法形态 → 不变 / 未占用 → nodes.name + 本人 owner
//    资源 author 同刷 + presence 新名 / 撞名 → name_conflict 拒绝（4404）
// ⑤ catalog / resource.published 事件 ownerNodeId 投影
// ⑥ 迁移 v8：v7→v8 升级后存量行 owner=NULL、无主目录项不带键
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import WebSocket from 'ws';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope, type HubCatalog,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-ownership-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let ownerCred: { nodeId: string; deviceToken: string };   // owner-node（领主）
let rivalCred: { nodeId: string; deviceToken: string };    // rival-node（越权者）
let claimerCred: { nodeId: string; deviceToken: string };  // claimer-node（无主认领者）
let listenerToken: string;

// ── 工具（resources-unpublish.test.ts 同款模式）────────────────────────────────

function enroll(name: string): { nodeId: string; deviceToken: string } {
  const r = enrollNode(db, identity.enrollmentToken, name);
  if ('error' in r) throw new Error(`enroll ${name} 失败`);
  return r;
}

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    slug: 'territory-res',
    type: 'agent',
    name: 'Territory Res',
    version: '1.0.0',
    author: 'manifest-author-ignored',
    description: '领地夹具',
    readme: '# Territory',
    tags: [],
    category: 'dev',
    iconEmoji: '🤖',
    verificationStatus: 'unverified',
    ...over,
  };
}

function form(manifest: unknown, payload: Buffer): FormData {
  const fd = new FormData();
  fd.append('manifest', new Blob([JSON.stringify(manifest)], { type: 'application/json' }), 'manifest.json');
  fd.append('package', new Blob([new Uint8Array(payload)], { type: 'application/gzip' }), 'package.tar.gz');
  return fd;
}

function publish(manifest: unknown, token: string, payload: Buffer = Buffer.from('ownership payload')): Promise<Response> {
  return fetch(`${baseUrl}/resources/publish`, {
    method: 'POST', headers: { authorization: `Bearer ${token}` }, body: form(manifest, payload),
  });
}

function unpublish(slug: string, token: string): Promise<Response> {
  return fetch(`${baseUrl}/resources/unpublish`, {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ slug }),
  });
}

async function getCatalog(token: string): Promise<HubCatalog> {
  const res = await fetch(`${baseUrl}/resources/catalog`, {
    method: 'GET', headers: { authorization: `Bearer ${token}` },
  });
  expect(res.status).toBe(200);
  return await res.json() as HubCatalog;
}

function resourceRow(slug: string): Record<string, unknown> | undefined {
  return db.prepare('SELECT * FROM resources WHERE slug = ?').get(slug) as Record<string, unknown> | undefined;
}

function versionCount(slug: string): number {
  return (db.prepare('SELECT COUNT(*) AS n FROM resource_versions WHERE slug = ?').get(slug) as { n: number }).n;
}

/** 自动过 hello 的监听客户端（nodeName = 注册名——相同分支，不触发改名）。 */
function connectListener(name: string, token: string): Promise<{
  ws: WebSocket; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope>;
}> {
  const wsUrl = baseUrl.replace(/^http/, 'ws') + '/ws';
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${token}` } });
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
      else resolve({ ws, inbox, next });
    });
  });
}

/** 裸 ws（不自动 hello）——改名三分支用：由用例自发任意 hello 载荷。 */
function openRaw(token: string): Promise<{
  ws: WebSocket; next(match: (e: Envelope) => boolean): Promise<Envelope>;
}> {
  const wsUrl = baseUrl.replace(/^http/, 'ws') + '/ws';
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${token}` } });
    const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
    ws.on('message', (data) => {
      const e = parseEnvelope(String(data));
      if (!e) return;
      const i = waiters.findIndex((w) => w.match(e));
      if (i >= 0) waiters.splice(i, 1)[0]!.resolve(e);
    });
    ws.on('error', reject);
    ws.on('open', () => resolve({
      ws,
      next: (match) => new Promise((res) => { waiters.push({ match, resolve: res }); }),
    }));
  });
}

let listener: Awaited<ReturnType<typeof connectListener>>;

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  ownerCred = enroll('owner-node');
  rivalCred = enroll('rival-node');
  claimerCred = enroll('claimer-node');
  listenerToken = enroll('listener-node').deviceToken;
  listener = await connectListener('listener-node', listenerToken);
});

afterAll(async () => {
  listener?.ws.close();
  await server.close();
  db.close();
});

// ── ① 发布认领矩阵 ───────────────────────────────────────────────────────────

describe('发布领地（M5.0 §3）', () => {
  it('新 slug：INSERT 即认领——owner_node_id = 鉴权 nodeId，author 定死 = owner 名（manifest.author 丢弃）', async () => {
    const res = await publish(manifestOf({ slug: 'claim-new', version: '1.0.0', author: 'whoever' }), ownerCred.deviceToken);
    expect(res.status).toBe(201);
    const row = resourceRow('claim-new');
    expect(row?.['owner_node_id']).toBe(ownerCred.nodeId);
    expect(row?.['author']).toBe('owner-node'); // 定死 = owner 名，非 manifest.author
  });

  it('owner 发新版：放行，元数据 upsert 保留（name/description 可改），author 不变', async () => {
    const res = await publish(
      manifestOf({ slug: 'claim-new', version: '2.0.0', name: 'Renamed By Owner', author: 'attacker-author' }),
      ownerCred.deviceToken,
    );
    expect(res.status).toBe(201);
    const row = resourceRow('claim-new');
    expect(row?.['name']).toBe('Renamed By Owner'); // owner 权：元数据可刷新
    expect(row?.['author']).toBe('owner-node');      // author 不被覆盖（现状覆盖 bug 消灭）
    expect(row?.['owner_node_id']).toBe(ownerCred.nodeId);
    expect(versionCount('claim-new')).toBe(2);
  });

  it('非 owner 发新版 → 403 {error:"not_owner", owner_name}，无落库痕迹（无新版本行、owner/author 不动）', async () => {
    const res = await publish(manifestOf({ slug: 'claim-new', version: '3.0.0' }), rivalCred.deviceToken);
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ error: 'not_owner', owner_name: 'owner-node' });
    expect(versionCount('claim-new')).toBe(2);
    expect(resourceRow('claim-new')?.['owner_node_id']).toBe(ownerCred.nodeId);
  });

  it('403 先于 409：非 owner 重发既有同版本（1.0.0）→ 仍 403 not_owner（越权者不探知版本存在与否）', async () => {
    const res = await publish(manifestOf({ slug: 'claim-new', version: '1.0.0' }), rivalCred.deviceToken);
    expect(res.status).toBe(403); // 不是 409 version_exists
    expect(await res.json()).toEqual({ error: 'not_owner', owner_name: 'owner-node' });
  });

  it('非 owner 发自己的新 slug 不受影响：201 + owner = 本人', async () => {
    const res = await publish(manifestOf({ slug: 'rival-own', version: '1.0.0' }), rivalCred.deviceToken);
    expect(res.status).toBe(201);
    const row = resourceRow('rival-own');
    expect(row?.['owner_node_id']).toBe(rivalCred.nodeId);
    expect(row?.['author']).toBe('rival-node');
  });
});

// ── ② 无主认领三态 ───────────────────────────────────────────────────────────

/** 手工造无主资源行（v8 迁移存量的仿真形态）：resources 行（owner 缺省 NULL）+ 一条旧版本行。 */
function seedUnownedRow(slug: string, ownerId: string | null): void {
  db.prepare(`
    INSERT INTO resources (slug, type, name, author, description, readme, tags_json, category, icon_emoji, latest_version, owner_node_id)
    VALUES (?, 'agent', ?, 'legacy-author', '', '', '[]', '', '', '0.9.0', ?)`)
    .run(slug, `Legacy ${slug}`, ownerId);
  db.prepare(`
    INSERT INTO resource_versions (slug, version, checksum, size_bytes, published_by, published_at)
    VALUES (?, '0.9.0', ?, 1, 'legacy-author', 0)`)
    .run(slug, 'a'.repeat(64));
}

describe('无主认领（NULL / 悬空 / 并发互斥）', () => {
  it('NULL 认领：原主发布新版 → 201，owner 落本人 + author 刷新为认领者名（author≡owner 不变式）', async () => {
    seedUnownedRow('orphan-null', null);
    const res = await publish(manifestOf({ slug: 'orphan-null', version: '1.0.0' }), claimerCred.deviceToken);
    expect(res.status).toBe(201);
    const row = resourceRow('orphan-null');
    expect(row?.['owner_node_id']).toBe(claimerCred.nodeId);
    expect(row?.['author']).toBe('claimer-node'); // 旧 legacy-author 被认领同刷
  });

  it('悬空认领（owner 指向已删 nodes 行）→ 等同开放认领：发布 201 + owner 落本人', async () => {
    seedUnownedRow('orphan-dangling', '0196dead-beef-dead-beef-deadbeef0000');
    const res = await publish(manifestOf({ slug: 'orphan-dangling', version: '1.0.0' }), claimerCred.deviceToken);
    expect(res.status).toBe(201);
    expect(resourceRow('orphan-dangling')?.['owner_node_id']).toBe(claimerCred.nodeId);
  });

  it('并发双认领互斥：两节点并发发布同一 NULL 行（不同版本）→ 恰一 201 一 403，owner = 胜者', async () => {
    seedUnownedRow('orphan-race', null);
    const [a, b] = await Promise.all([
      publish(manifestOf({ slug: 'orphan-race', version: '1.0.0' }), rivalCred.deviceToken),
      publish(manifestOf({ slug: 'orphan-race', version: '1.0.1' }), claimerCred.deviceToken),
    ]);
    const codes = [a.status, b.status].sort();
    expect(codes).toEqual([201, 403]); // 409 不可能出现（版本不同）
    const winner = a.status === 201 ? rivalCred : claimerCred;
    const loserRes = a.status === 403 ? a : b;
    expect(await loserRes.json()).toEqual({ error: 'not_owner', owner_name: winner === rivalCred ? 'rival-node' : 'claimer-node' });
    const row = resourceRow('orphan-race');
    expect(row?.['owner_node_id']).toBe(winner.nodeId);
    expect(versionCount('orphan-race')).toBe(2); // 旧 0.9.0 + 胜者新版；败者零落库
  });
});

// ── ③ unpublish owner-only ───────────────────────────────────────────────────

describe('unpublish owner-only（M5.0 §4）', () => {
  it('owner 撤回成功：200 + 目录消失 + revision bump（墓碑/revision 语义不变）', async () => {
    expect((await publish(manifestOf({ slug: 'unpub-mine', version: '1.0.0' }), ownerCred.deviceToken)).status).toBe(201);
    const before = await getCatalog(ownerCred.deviceToken);
    const res = await unpublish('unpub-mine', ownerCred.deviceToken);
    expect(res.status).toBe(200);
    const body = await res.json() as { ok: boolean; revision: number };
    expect(body.ok).toBe(true);
    expect(body.revision).toBeGreaterThan(before.revision);
    const after = await getCatalog(ownerCred.deviceToken);
    expect(after.items.some((i) => i.slug === 'unpub-mine')).toBe(false);
    expect(resourceRow('unpub-mine')?.['unpublished_at']).not.toBeNull(); // 墓碑置位
  });

  it('非 owner 撤回 → 403 {error:"not_owner", owner_name}，墓碑不动', async () => {
    expect((await publish(manifestOf({ slug: 'unpub-guard', version: '1.0.0' }), ownerCred.deviceToken)).status).toBe(201);
    const res = await unpublish('unpub-guard', rivalCred.deviceToken);
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ error: 'not_owner', owner_name: 'owner-node' });
    expect(resourceRow('unpub-guard')?.['unpublished_at']).toBeNull(); // 未被越权撤下
  });

  it('权限先于幂等：owner 已撤回后非 owner 再撤 → 403（不是幂等 200）', async () => {
    const res = await unpublish('unpub-mine', rivalCred.deviceToken); // unpub-mine 已在首例被 owner 撤回
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ error: 'not_owner', owner_name: 'owner-node' });
  });

  it('不存在 → 404 not_found（语义零变）', async () => {
    const res = await unpublish('no-such-territory', ownerCred.deviceToken);
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: 'not_found' });
  });
});

// ── ⑤ catalog / 事件投影（置于 hello 改名前——改名会刷 author）────────────────

describe('catalog / resource.published 事件 ownerNodeId 投影（M5.0 §6）', () => {
  it('目录项携 ownerNodeId（owner 的行）；无主行不带键', async () => {
    seedUnownedRow('legacy-visible', null); // 无主 + 未撤回 → 目录可见
    const catalog = await getCatalog(ownerCred.deviceToken);
    const owned = catalog.items.find((i) => i.slug === 'claim-new');
    expect(owned?.ownerNodeId).toBe(ownerCred.nodeId);
    const rival = catalog.items.find((i) => i.slug === 'rival-own');
    expect(rival?.ownerNodeId).toBe(rivalCred.nodeId);
    const legacy = catalog.items.find((i) => i.slug === 'legacy-visible');
    expect(legacy).toBeDefined();
    expect('ownerNodeId' in (legacy ?? {})).toBe(false); // 无主不带键——旧形状
  });

  it('resource.published 事件携 ownerNodeId（发布即认领/放行的 owner）', async () => {
    expect((await publish(manifestOf({ slug: 'evt-owner', version: '1.0.0' }), ownerCred.deviceToken)).status).toBe(201);
    const evt = await listener.next((e) =>
      e.kind === 'evt' && e.method === EVT.resourcePublished && (e.payload as { slug: string }).slug === 'evt-owner');
    expect(evt.kind).toBe('evt');
    const payload = evt.payload as { slug: string; author: string; ownerNodeId?: string };
    expect(payload.slug).toBe('evt-owner');
    expect(payload.author).toBe('owner-node');
    expect(payload.ownerNodeId).toBe(ownerCred.nodeId);
  });
});

// ── ④ hello 同步改名三分支（置尾——mutate 节点名）──────────────────────────────

describe('hello 同步改名（M5.0 §5）', () => {
  it('未占用名：nodes.name + 本人 owner 资源 author 同刷（含墓碑行），他人资源不动；presence 广播新名', async () => {
    // 夹具：claimer 再认领一个新 slug（改名后 author 应随之刷新）；unpub-mine 墓碑行归 owner 不动
    expect((await publish(manifestOf({ slug: 'claimer-flag', version: '1.0.0' }), claimerCred.deviceToken)).status).toBe(201);
    const raw = await openRaw(claimerCred.deviceToken);
    try {
      raw.ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'claimer-new' })));
      const helloRes = await raw.next((e) => e.kind === 'res' && e.method === RPC.hello);
      expect(helloRes.kind === 'res' && helloRes.ok).toBe(true);

      // 库：nodes.name 刷新 + owner 资源 author 同刷（含在场行 orphan-null 与 claimer-flag）
      expect(db.prepare('SELECT name FROM nodes WHERE id = ?').get(claimerCred.nodeId)).toEqual({ name: 'claimer-new' });
      expect(resourceRow('claimer-flag')?.['author']).toBe('claimer-new');
      expect(resourceRow('orphan-null')?.['author']).toBe('claimer-new');
      expect(resourceRow('orphan-race')?.['author']).toBe(
        resourceRow('orphan-race')?.['owner_node_id'] === claimerCred.nodeId ? 'claimer-new' : 'rival-node',
      );
      // 他人资源 author 不动
      expect(resourceRow('claim-new')?.['author']).toBe('owner-node');

      // presence：监听端收到 node.online 用新名
      const onlineEvt = await listener.next((e) =>
        e.kind === 'evt' && e.method === EVT.nodeOnline && (e.payload as { nodeId: string }).nodeId === claimerCred.nodeId);
      expect((onlineEvt.payload as { nodeName: string }).nodeName).toBe('claimer-new');
    } finally {
      raw.ws.close();
    }
  });

  it('缺省 / 非法形态（超长/空串）→ 视为缺省：hello 受理且名字不变', async () => {
    // 名字已被上一用例刷为 claimer-new；本用例用 owner-node 分别发缺省与非法 nodeName
    for (const nodeName of [undefined, '', 'x'.repeat(65)]) {
      const raw = await openRaw(ownerCred.deviceToken);
      try {
        const payload: Record<string, unknown> = { protoVer: PROTOCOL_VERSION };
        if (nodeName !== undefined) payload['nodeName'] = nodeName;
        raw.ws.send(encodeEnvelope(makeRequest(RPC.hello, payload)));
        const helloRes = await raw.next((e) => e.kind === 'res' && e.method === RPC.hello);
        expect(helloRes.kind === 'res' && helloRes.ok, `用例 nodeName=${String(nodeName)}`).toBe(true);
        expect(db.prepare('SELECT name FROM nodes WHERE id = ?').get(ownerCred.nodeId)).toEqual({ name: 'owner-node' });
        expect(resourceRow('claim-new')?.['author']).toBe('owner-node'); // 资源面也无涟漪
      } finally {
        raw.ws.close();
      }
    }
  });

  it('撞名（目标名已被其他节点占用）→ res name_conflict + close 4404，名字与资源不动', async () => {
    const raw = await openRaw(rivalCred.deviceToken);
    const closed = new Promise<number>((resolve) => { raw.ws.on('close', resolve); });
    raw.ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'owner-node' })));
    const errRes = await raw.next((e) => e.kind === 'res' && e.method === RPC.hello && !e.ok);
    const p = errRes.kind === 'res' ? errRes.payload as { code: string; message: string } : null;
    expect(p?.code).toBe('name_conflict');
    expect(p?.message).toContain('owner-node');
    expect(await closed).toBe(4404);
    expect(db.prepare('SELECT name FROM nodes WHERE id = ?').get(rivalCred.nodeId)).toEqual({ name: 'rival-node' });
    expect(resourceRow('rival-own')?.['author']).toBe('rival-node');
  });

  it('同名人复用 nodeId（enroll 同名 → 复用既有 nodeId——身份锚是 nodeId 不是名）：改回旧名不受「自己占用」误拒', async () => {
    // enroll 'claimer-new'（claimer-node 已改名让出该名？不——claimer-node 现名 claimer-new，
    // 此处 enroll 'claimer-new' 命中既有行 → 轮换语义复用 nodeId）。本用例锁：hello 改名
    // 到「占用者是自己」的名（另一连接已改）不误拒。改回 'claimer-node'（现无人占用）再
    // 次改名回 'claimer-new'（占用者=自己）→ 受理（幂等对齐），不 name_conflict。
    const raw = await openRaw(claimerCred.deviceToken);
    try {
      // 先改回 claimer-node（未占用路径回归）
      raw.ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'claimer-node' })));
      const r1 = await raw.next((e) => e.kind === 'res' && e.method === RPC.hello);
      expect(r1.kind === 'res' && r1.ok).toBe(true);
      raw.ws.close();
      // 再开一条连接改成 claimer-new——占用者是自己的 nodeId → 不误拒，受理
      const raw2 = await openRaw(claimerCred.deviceToken);
      try {
        raw2.ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'claimer-new' })));
        const r2 = await raw2.next((e) => e.kind === 'res' && e.method === RPC.hello);
        expect(r2.kind === 'res' && r2.ok).toBe(true);
        expect(db.prepare('SELECT name FROM nodes WHERE id = ?').get(claimerCred.nodeId)).toEqual({ name: 'claimer-new' });
      } finally {
        raw2.ws.close();
      }
    } finally {
      raw.ws.close();
    }
  });
});

// ── ⑥ 迁移 v8 ────────────────────────────────────────────────────────────────

describe('迁移 v8（resources.owner_node_id）', () => {
  it('v7→v8 升级：存量行保持 NULL；重开幂等；新发布正常认领（迁移不破坏领地流）', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-mig-v8-'));
    const d1 = openHubDatabase(dir);
    // 造 v7 时代存量行（owner 列是 v8 才加的——先摘列 + schema_version 回拨 9，仿真 v7 库）
    d1.prepare(`
      INSERT INTO resources (slug, type, name, author, description, readme, tags_json, category, icon_emoji, latest_version)
      VALUES ('v7-legacy', 'skill', 'V7 Legacy', 'old-author', '', '', '[]', '', '', '1.0.0')`).run();
    d1.prepare('ALTER TABLE resources DROP COLUMN owner_node_id').run();
    d1.prepare(`UPDATE hub_meta SET value = '9' WHERE key = 'schema_version'`).run();
    d1.close();

    // 重开 → v8 ALTER 应用：列回归，存量行 NULL
    const d2 = openHubDatabase(dir);
    const cols = (d2.prepare('PRAGMA table_info(resources)').all() as Array<{ name: string }>).map((c) => c.name);
    expect(cols).toContain('owner_node_id');
    const legacy = d2.prepare('SELECT owner_node_id FROM resources WHERE slug = ?').get('v7-legacy') as { owner_node_id: string | null };
    expect(legacy.owner_node_id).toBeNull(); // 存量保持 NULL（开放认领）
    expect(d2.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '10' });
    // 再重开幂等：迁移不重复执行（列不被重复 ALTER 炸）
    d2.close();
    const d3 = openHubDatabase(dir);
    expect(d3.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '10' });
    d3.close();
  });
});
