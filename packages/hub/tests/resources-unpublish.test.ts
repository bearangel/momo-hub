// hub 资源撤回与目录修订号契约锁（spec 2026-10-29 M3.4 §2.2，协议 1.5.0）——
// 真实 listen + 真 fetch + 真 ws（无 mock，同 resources-catalog.test.ts 模式）：
// ① 撤回全链：撤回 → 目录消失 → 下载仍在（历史版本保留）→ 版本行/blob 不删
// ② 幂等：已撤回再撤 = 200 且无状态变化（revision 不 bump、不二次广播）
// ③ republish 恢复：同 slug 再发布 → 清墓碑回目录 + revision 单调
// ④ revision：v7 迁移初值 + publish/unpublish 单调递增 + catalog 响应携带
// ⑤ resource.unpublished 事件广播 {slug, by, at} 三字段逐项
// ⑥ 错误矩阵：401 未鉴权 / 404 无此资源 / 400 坏-缺-非串 slug
// ⑦ 迁移 v7：unpublished_at 墓碑列 + hub_meta.catalog_revision='1' + 重开幂等
import { mkdtempSync, readdirSync } from 'node:fs';
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

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-unpublish-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let deviceToken: string;
let listener: { ws: WebSocket; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> };

// ── 工具（resources-catalog.test.ts 同款模式）────────────────────────────────

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    slug: 'lifecycle-res',
    type: 'agent',
    name: 'Lifecycle Res',
    version: '1.0.0',
    author: 'ignored',
    description: '生命周期夹具',
    readme: '# Lifecycle',
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

/** 发布并读完响应体（连接回可复用态）；返回 status + 已解析 body。 */
async function publish(manifest: unknown, payload: Buffer): Promise<{ status: number; body: unknown }> {
  const res = await fetch(`${baseUrl}/resources/publish`, {
    method: 'POST', headers: { authorization: `Bearer ${deviceToken}` }, body: form(manifest, payload),
  });
  const body = await res.json().catch(() => null);
  return { status: res.status, body };
}

function unpublish(slug: string, token: string | null = deviceToken): Promise<Response> {
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (token !== null) headers.authorization = `Bearer ${token}`;
  return fetch(`${baseUrl}/resources/unpublish`, {
    method: 'POST', headers, body: JSON.stringify({ slug }),
  });
}

async function getCatalog(): Promise<HubCatalog> {
  const res = await fetch(`${baseUrl}/resources/catalog`, {
    method: 'GET', headers: { authorization: `Bearer ${deviceToken}` },
  });
  expect(res.status).toBe(200);
  return await res.json() as HubCatalog;
}

function getDownload(slug: string, version: string): Promise<Response> {
  return fetch(`${baseUrl}/resources/${slug}/${version}/download`, {
    method: 'GET', headers: { authorization: `Bearer ${deviceToken}` },
  });
}

async function connectListener(name: string, token: string): Promise<typeof listener> {
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

function resourceRow(slug: string): Record<string, unknown> | undefined {
  return db.prepare('SELECT * FROM resources WHERE slug = ?').get(slug) as Record<string, unknown> | undefined;
}

function versionRowCount(slug: string): number {
  return (db.prepare('SELECT COUNT(*) AS n FROM resource_versions WHERE slug = ?').get(slug) as { n: number }).n;
}

// ── 夹具 ────────────────────────────────────────────────────────────────────

const PAYLOAD_V1 = Buffer.from('lifecycle-res v1 payload');
const PAYLOAD_V2 = Buffer.from('lifecycle-res v2 payload');

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  const publisher = enrollNode(db, identity.enrollmentToken, 'publisher-node');
  if ('error' in publisher) throw new Error('enroll 失败');
  deviceToken = publisher.deviceToken;
  const listenerCred = enrollNode(db, identity.enrollmentToken, 'listener-node');
  if ('error' in listenerCred) throw new Error('enroll 失败');
  listener = await connectListener('listener-node', listenerCred.deviceToken);
});

afterAll(async () => {
  listener?.ws.close();
  await server.close();
  db.close();
});

// ── 用例 ────────────────────────────────────────────────────────────────────

describe('撤回全链（目录消失 / 下载仍在 / 版本行与 blob 不删）', () => {
  it('发布两版 → 撤回 → 目录无此项，两版下载均 200，版本行与 blob 完整', async () => {
    expect((await publish(manifestOf({ version: '1.0.0' }), PAYLOAD_V1)).status).toBe(201);
    expect((await publish(manifestOf({ version: '2.0.0' }), PAYLOAD_V2)).status).toBe(201);
    const before = await getCatalog();
    expect(before.items.some((i) => i.slug === 'lifecycle-res')).toBe(true);

    const res = await unpublish('lifecycle-res');
    expect(res.status).toBe(200);
    const body = await res.json() as { ok: boolean; slug: string; revision: number };
    expect(body.ok).toBe(true);
    expect(body.slug).toBe('lifecycle-res');
    expect(body.revision).toBeGreaterThan(before.revision); // 撤回 bump revision

    // 目录消失（墓碑过滤）
    const after = await getCatalog();
    expect(after.items.some((i) => i.slug === 'lifecycle-res')).toBe(false);
    expect(after.revision).toBe(body.revision);

    // 历史版本下载保留（roadmap 裁定：下载端点不感知撤回）
    for (const [version, payload] of [['1.0.0', PAYLOAD_V1], ['2.0.0', PAYLOAD_V2]] as const) {
      const dl = await getDownload('lifecycle-res', version);
      expect(dl.status, `下载 ${version}`).toBe(200);
      const bytes = Buffer.from(await dl.arrayBuffer());
      expect(bytes.equals(payload)).toBe(true);
    }

    // 不删版本行（两版全在）与 blob（内容寻址文件在盘）
    expect(versionRowCount('lifecycle-res')).toBe(2);
    const blobDir = join(dataDir, 'blobs');
    expect(readdirSync(blobDir).filter((f) => f.endsWith('.tar.gz')).length).toBeGreaterThanOrEqual(2);
  });
});

describe('幂等：已撤回再撤 = 200 且无状态变化', () => {
  it('第二次撤回 200、revision 不 bump、无二次广播、版本行仍全量', async () => {
    expect((await publish(manifestOf({ slug: 'idem-res', version: '1.0.0' }), Buffer.from('idem payload'))).status).toBe(201);
    const first = await unpublish('idem-res');
    expect(first.status).toBe(200);
    const rev1 = (await first.json() as { revision: number }).revision;

    const second = await unpublish('idem-res');
    expect(second.status).toBe(200); // 幂等：不 404、不 409
    const body2 = await second.json() as { ok: boolean; revision: number };
    expect(body2.ok).toBe(true);
    expect(body2.revision).toBe(rev1); // 无状态变化 → revision 不动

    const catalog = await getCatalog();
    expect(catalog.items.some((i) => i.slug === 'idem-res')).toBe(false);
    expect(catalog.revision).toBe(rev1);
    expect(versionRowCount('idem-res')).toBe(1);

    // 无二次广播：等待一个广播窗口后 inbox 中该 slug 的 unpublished 事件恰一条
    await new Promise((r) => setTimeout(r, 150));
    const events = listener.inbox.filter((e) =>
      e.kind === 'evt' && e.method === EVT.resourceUnpublished
      && (e.payload as { slug: string }).slug === 'idem-res');
    expect(events).toHaveLength(1);
  });
});

describe('republish 恢复（publish 清墓碑）+ revision 单调', () => {
  it('撤回后同 slug 再发布新版本 → 目录恢复、墓碑清空、revision 逐步单调', async () => {
    expect((await publish(manifestOf({ slug: 'revive-res', version: '1.0.0' }), Buffer.from('revive 1'))).status).toBe(201);
    const afterPublish1 = (await getCatalog()).revision;

    const un = await unpublish('revive-res');
    expect(un.status).toBe(200);
    const afterUnpublish = (await un.json() as { revision: number }).revision;
    expect(afterUnpublish).toBeGreaterThan(afterPublish1);
    expect((await getCatalog()).items.some((i) => i.slug === 'revive-res')).toBe(false);

    // 版本不可变：同 slug+version 重发布仍 409（清墓碑不豁免三锁）
    const dup = await publish(manifestOf({ slug: 'revive-res', version: '1.0.0' }), Buffer.from('dup bytes'));
    expect(dup.status).toBe(409);
    expect(dup.body).toEqual({ error: 'version_exists' });

    expect((await publish(manifestOf({ slug: 'revive-res', version: '2.0.0' }), Buffer.from('revive 2'))).status).toBe(201);
    const afterRepublish = (await getCatalog()).revision;
    expect(afterRepublish).toBeGreaterThan(afterUnpublish); // publish 同事务 bump

    // 目录恢复可见：两版全量、latest=2.0.0（semver max）
    const catalog = await getCatalog();
    const item = catalog.items.find((i) => i.slug === 'revive-res');
    expect(item).toBeDefined();
    expect(item?.version).toBe('2.0.0');
    expect(item?.versions).toEqual(['2.0.0', '1.0.0']);

    // 墓碑清空（DB 层断言）
    expect(resourceRow('revive-res')?.['unpublished_at']).toBeNull();
    expect(versionRowCount('revive-res')).toBe(2);
  });
});

describe('catalog 响应带 revision（B10）', () => {
  it('revision 为正整数且 v7 迁移初值 1 起单调（多次发布只增不减）', async () => {
    const first = await getCatalog();
    expect(Number.isInteger(first.revision)).toBe(true);
    expect(first.revision).toBeGreaterThanOrEqual(1);
    expect((await publish(manifestOf({ slug: 'revision-res', version: '1.0.0' }), Buffer.from('revision payload'))).status).toBe(201);
    const second = await getCatalog();
    expect(second.revision).toBeGreaterThan(first.revision);
  });
});

describe('resource.unpublished 事件广播', () => {
  it('撤回成功后在线 ws 客户端收到 resource.unpublished——{slug, by, at} 三字段逐项', async () => {
    expect((await publish(manifestOf({ slug: 'evt-unpub-res', version: '1.0.0' }), Buffer.from('evt unpub payload'))).status).toBe(201);
    const res = await unpublish('evt-unpub-res');
    expect(res.status).toBe(200);

    const evt = await listener.next((e) =>
      e.kind === 'evt' && e.method === EVT.resourceUnpublished
      && (e.payload as { slug: string }).slug === 'evt-unpub-res');
    expect(evt.kind).toBe('evt');
    expect(evt.method).toBe(EVT.resourceUnpublished);
    const payload = evt.payload as { slug: string; by: string; at: number };
    expect(payload.slug).toBe('evt-unpub-res');
    expect(payload.by).toBe('publisher-node'); // 执行撤回的鉴权节点名（信任根在 hub）
    expect(typeof payload.at).toBe('number');
    expect(payload.at).toBeGreaterThan(0);
  });
});

describe('错误矩阵', () => {
  it('401：无 Authorization / 错 token → unauthorized', async () => {
    const noAuth = await unpublish('lifecycle-res', null);
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await unpublish('lifecycle-res', 'wrong-token');
    expect(badAuth.status).toBe(401);
    expect(await badAuth.json()).toEqual({ error: 'unauthorized' });
  });

  it('404：无此资源（合法 slug 但 resources 表无行）', async () => {
    const res = await unpublish('no-such-res');
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: 'not_found' });
  });

  it('400：坏 slug（大写+空格 / shell 元字符）/ 缺 slug / 非字符串 slug → invalid_slug', async () => {
    for (const body of [{ slug: 'Bad Slug' }, { slug: 'a;rm -rf' }, {}, { slug: 123 }]) {
      const res = await fetch(`${baseUrl}/resources/unpublish`, {
        method: 'POST',
        headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      expect(res.status, `用例：${JSON.stringify(body)}`).toBe(400);
      expect(await res.json(), `用例：${JSON.stringify(body)}`).toEqual({ error: 'invalid_slug' });
    }
  });
});

// ── 迁移 v7（resources.unpublished_at 墓碑列 + catalog_revision 初值）────────

describe('迁移 v7', () => {
  it('新库：unpublished_at 列存在且默认 NULL；catalog_revision=1；重开幂等', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-mig-v7-'));
    const d1 = openHubDatabase(dir);
    // 列存在（ALTER 已应用）且缺省 NULL
    const cols = (d1.prepare('PRAGMA table_info(resources)').all() as Array<{ name: string }>).map((c) => c.name);
    expect(cols).toContain('unpublished_at');
    expect((d1.prepare('SELECT COUNT(*) AS n FROM resources WHERE unpublished_at IS NOT NULL').get() as { n: number }).n).toBe(0);
    // hub_meta 不重建（v1 既有键仍在）+ 新键初值
    expect(d1.prepare("SELECT value FROM hub_meta WHERE key = 'catalog_revision'").get()).toEqual({ value: '1' });
    // M5.0 v8 归属列追加后 schema_version=10（v7 时为 9）
    expect(d1.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '10' });
    d1.close();

    // 重开幂等：迁移不重复执行、初值不被覆盖
    const d2 = openHubDatabase(dir);
    expect(d2.prepare("SELECT value FROM hub_meta WHERE key = 'catalog_revision'").get()).toEqual({ value: '1' });
    expect(d2.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '10' });
    d2.close();
  });
});
