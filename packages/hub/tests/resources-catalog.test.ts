// hub 资源目录与下载端点契约锁（spec 2026-10-24 M1 §3.1/§3.2/§3.3/§5 + T6 对齐口径）——
// 真实 listen + 真 fetch + 真 ws（无 mock）：
// ① publish 两个版本 → catalog 字段逐项断言（latest_version / versions 新→旧 /
//    downloadUrl 按请求 origin 组装 / checksum / sizeBytes / publishedAt / author /
//    verificationStatus）+ catalog 整体过 validateHubCatalog（协议单源校验）
// ② download 流 sha256 复算 == X-Checksum-Sha256 头 == DB 值，X-Published-By == published_by
// ③ 404：未知 slug / 未知 version / 坏 slug / 坏 version（早拒——错误体是端点自造的
//    {error:'not_found'}，与路由级 404（Fastify 默认体）可区分）
// ④ 401：catalog / download 未鉴权与错 token
// ⑤ 事件链：发布成功 → 在线 ws 客户端收 resource.published（六字段逐项）
import { createHash } from 'node:crypto';
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
  EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope, validateHubCatalog,
  type Envelope, type HubCatalog, type HubResourceMeta,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-catalog-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let deviceToken: string;
let listener: { ws: WebSocket; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> };

// ── 工具 ────────────────────────────────────────────────────────────────────

function sha256(data: Buffer | string): string {
  return createHash('sha256').update(data).digest('hex');
}

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    slug: 'catalog-res',
    type: 'agent',
    name: 'Catalog Res',
    version: '1.0.0',
    author: 'manifest-author', // 丢弃：落库 author 取鉴权节点名（R-M1-8）
    description: '目录项描述',
    readme: '# Catalog Res',
    tags: ['alpha', 'beta'],
    category: 'dev',
    iconEmoji: '🤖',
    verificationStatus: 'unverified',
    ...over,
  };
}

/** Blob 构造：Buffer 走 Uint8Array 拷贝（Buffer 的 ArrayBufferLike 与 BlobPart 类型不兼容）。 */
function asBlob(data: Buffer | string, type: string): Blob {
  return new Blob([typeof data === 'string' ? data : new Uint8Array(data)], { type });
}

function form(manifest: unknown, payload: Buffer): FormData {
  const fd = new FormData();
  fd.append('manifest', asBlob(JSON.stringify(manifest), 'application/json'), 'manifest.json');
  fd.append('package', asBlob(payload, 'application/gzip'), 'package.tar.gz');
  return fd;
}

function publish(manifest: unknown, payload: Buffer, token: string | null = deviceToken): Promise<Response> {
  const headers: Record<string, string> = {};
  if (token !== null) headers.authorization = `Bearer ${token}`;
  return fetch(`${baseUrl}/resources/publish`, { method: 'POST', headers, body: form(manifest, payload) });
}

/** catalog 拉取（默认带鉴权头）。 */
function getCatalog(headers: Record<string, string> = {}): Promise<Response> {
  return fetch(`${baseUrl}/resources/catalog`, {
    method: 'GET',
    headers: { authorization: `Bearer ${deviceToken}`, ...headers },
  });
}

function getDownload(slug: string, version: string, token: string | null = deviceToken): Promise<Response> {
  const headers: Record<string, string> = {};
  if (token !== null) headers.authorization = `Bearer ${token}`;
  return fetch(`${baseUrl}/resources/${encodeURIComponent(slug)}/${encodeURIComponent(version)}/download`, {
    method: 'GET', headers,
  });
}

function versionRow(slug: string, version: string): Record<string, unknown> | undefined {
  return db.prepare('SELECT * FROM resource_versions WHERE slug = ? AND version = ?')
    .get(slug, version) as Record<string, unknown> | undefined;
}

// ── ws 监听端（M0 server.integration.test.ts 同款模式：真 socket + 真 hello）──

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

// ── 夹具 ────────────────────────────────────────────────────────────────────

const PAYLOAD_V1 = Buffer.from('catalog-res v1 payload');
const PAYLOAD_V2 = Buffer.from('catalog-res v2 payload');

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  const publisher = enrollNode(db, identity.enrollmentToken, 'publisher-node');
  if ('error' in publisher) throw new Error('enroll 失败');
  deviceToken = publisher.deviceToken;
  const listenerCred = enrollNode(db, identity.enrollmentToken, 'listener-node');
  if ('error' in listenerCred) throw new Error('enroll 失败');
  // 监听端必须先在线（发布事件只发给在线节点）
  listener = await connectListener('listener-node', listenerCred.deviceToken);

  // 两个版本：v1.0.0 → v2.0.0（catalog 的 versions 应为新→旧）
  const first = await publish(manifestOf({ version: '1.0.0' }), PAYLOAD_V1);
  if (first.status !== 201) throw new Error(`首次发布失败：${first.status}`);
  const second = await publish(manifestOf({ version: '2.0.0' }), PAYLOAD_V2);
  if (second.status !== 201) throw new Error(`二次发布失败：${second.status}`);
});

afterAll(async () => {
  listener?.ws.close();
  await server.close();
  db.close();
});

// ── 用例 ────────────────────────────────────────────────────────────────────

describe('GET /resources/catalog', () => {
  it('目录项逐字段组装：latest_version / versions 新→旧 / downloadUrl 按请求 origin / checksum / sizeBytes / publishedAt / author / verificationStatus', async () => {
    const res = await getCatalog();
    expect(res.status).toBe(200);
    const body = await res.json() as HubCatalog;
    // 顶层：catalog wire 版本 = 协议版本；updatedAt 为 ISO 串
    expect(body.version).toBe(PROTOCOL_VERSION);
    expect(typeof body.updatedAt).toBe('string');
    expect(Number.isNaN(Date.parse(body.updatedAt))).toBe(false);

    const item = body.items.find((i) => i.slug === 'catalog-res');
    expect(item).toBeDefined();
    const meta = item as HubResourceMeta;
    expect(meta.type).toBe('agent');
    expect(meta.name).toBe('Catalog Res');
    expect(meta.description).toBe('目录项描述');
    expect(meta.readme).toBe('# Catalog Res');
    expect(meta.tags).toEqual(['alpha', 'beta']);
    expect(meta.category).toBe('dev');
    expect(meta.iconEmoji).toBe('🤖');
    // hub 恒为 unverified（表无该列，恒定值）
    expect(meta.verificationStatus).toBe('unverified');
    // author 直映射 resources.author（= 鉴权节点名，R-M1-8）
    expect(meta.author).toBe('publisher-node');
    // latest = 最近发布（时序）
    expect(meta.version).toBe('2.0.0');
    // versions 新→旧
    expect(meta.versions).toEqual(['2.0.0', '1.0.0']);
    // checksum / sizeBytes 取最新版本行（服务端自算值）
    const v2 = versionRow('catalog-res', '2.0.0');
    expect(meta.checksum).toBe(sha256(PAYLOAD_V2));
    expect(meta.checksum).toBe(v2?.['checksum']);
    expect(meta.sizeBytes).toBe(PAYLOAD_V2.length);
    expect(meta.sizeBytes).toBe(v2?.['size_bytes']);
    expect(meta.publishedAt).toBe(v2?.['published_at']);
    expect(typeof meta.publishedAt).toBe('number');
    // downloadUrl 按请求 origin 组装（反代场景天然正确）
    expect(meta.downloadUrl).toBe(`${baseUrl}/resources/catalog-res/2.0.0/download`);

    // 整份 catalog 过协议单源校验（protocol 包 validateHubCatalog）
    expect(validateHubCatalog(body)).not.toBeNull();
  });

  it('items 按发布时间倒序（publishedAt 降序），多条目各自独立组装', async () => {
    const res = await publish(manifestOf({ slug: 'later-res', name: 'Later Res', version: '1.0.0' }), Buffer.from('later payload'));
    expect(res.status).toBe(201);

    const body = await (await getCatalog()).json() as HubCatalog;
    const publishedAts = body.items.map((i) => i.publishedAt);
    expect(publishedAts).toEqual([...publishedAts].sort((a, b) => b - a));
    const later = body.items.find((i) => i.slug === 'later-res');
    expect(later?.name).toBe('Later Res');
    expect(later?.versions).toEqual(['1.0.0']);
    expect(later?.downloadUrl).toBe(`${baseUrl}/resources/later-res/1.0.0/download`);
  });

  it('downloadUrl 用反代头（X-Forwarded-Proto / X-Forwarded-Host）——公网 origin 形态正确', async () => {
    const res = await getCatalog({ 'x-forwarded-proto': 'https', 'x-forwarded-host': 'hub.example.com' });
    expect(res.status).toBe(200);
    const body = await res.json() as HubCatalog;
    const item = body.items.find((i) => i.slug === 'catalog-res');
    expect(item?.downloadUrl).toBe('https://hub.example.com/resources/catalog-res/2.0.0/download');
  });

  it('未鉴权 401：catalog 无 Authorization / 错 token → unauthorized', async () => {
    const noAuth = await fetch(`${baseUrl}/resources/catalog`, { method: 'GET' });
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await fetch(`${baseUrl}/resources/catalog`, {
      method: 'GET', headers: { authorization: 'Bearer wrong-token' },
    });
    expect(badAuth.status).toBe(401);
    expect(await badAuth.json()).toEqual({ error: 'unauthorized' });
  });
});

describe('GET /resources/{slug}/{version}/download', () => {
  it('下载流 sha256 复算 == X-Checksum-Sha256 头 == DB 值；X-Published-By 取 published_by', async () => {
    const res = await getDownload('catalog-res', '2.0.0');
    expect(res.status).toBe(200);
    const header = res.headers.get('x-checksum-sha256');
    expect(header).toBe(sha256(PAYLOAD_V2));
    expect(header).toBe(versionRow('catalog-res', '2.0.0')?.['checksum']);
    expect(res.headers.get('x-published-by')).toBe('publisher-node');
    const bytes = Buffer.from(await res.arrayBuffer());
    expect(bytes.equals(PAYLOAD_V2)).toBe(true);
    // 客户端独立复算（纵深校验：hub 头 + 客户端复算同锚）
    expect(sha256(bytes)).toBe(header);
  });

  it('旧版本同样可下载（版本不可变，历史版本可复现）', async () => {
    const res = await getDownload('catalog-res', '1.0.0');
    expect(res.status).toBe(200);
    expect(res.headers.get('x-checksum-sha256')).toBe(sha256(PAYLOAD_V1));
    const bytes = Buffer.from(await res.arrayBuffer());
    expect(bytes.equals(PAYLOAD_V1)).toBe(true);
  });

  it('404：未知 slug / 未知 version / 坏 slug / 坏 version（早拒，错误体是端点自造）', async () => {
    const cases: Array<{ name: string; slug: string; version: string }> = [
      { name: '未知 slug', slug: 'no-such-res', version: '1.0.0' },
      { name: '未知 version', slug: 'catalog-res', version: '9.9.9' },
      { name: '坏 slug（空格）', slug: 'Bad Slug', version: '1.0.0' },
      { name: '坏 slug（shell 元字符）', slug: 'a;rm -rf', version: '1.0.0' },
      { name: '坏 version（空格）', slug: 'catalog-res', version: '1.0.0 beta' },
      { name: '坏 version（斜杠）', slug: 'catalog-res', version: '1.0.0/../x' },
    ];
    for (const c of cases) {
      const res = await getDownload(c.slug, c.version);
      expect(res.status, `用例：${c.name}`).toBe(404);
      // 端点自造的 404（路由级 404 是 Fastify 默认体）——证明走了端点早拒分支
      expect(await res.json(), `用例：${c.name}`).toEqual({ error: 'not_found' });
    }
  });

  it('未鉴权 401：download 无 Authorization / 错 token → unauthorized', async () => {
    const noAuth = await getDownload('catalog-res', '2.0.0', null);
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await getDownload('catalog-res', '2.0.0', 'wrong-token');
    expect(badAuth.status).toBe(401);
    expect(await badAuth.json()).toEqual({ error: 'unauthorized' });
  });
});

describe('发布事件广播（resource.published）', () => {
  it('发布成功后在线 ws 客户端收到 resource.published——六字段逐项断言', async () => {
    const res = await publish(
      manifestOf({ slug: 'evt-res', type: 'skill', name: 'Evt Res', version: '1.0.0' }),
      Buffer.from('evt payload'),
    );
    expect(res.status).toBe(201);

    const evt = await listener.next((e) =>
      e.kind === 'evt' && e.method === EVT.resourcePublished
      && (e.payload as { slug: string }).slug === 'evt-res');
    expect(evt.kind).toBe('evt');
    expect(evt.method).toBe(EVT.resourcePublished);
    const payload = evt.payload as {
      slug: string; type: string; name: string; version: string; author: string; at: number;
    };
    expect(payload.slug).toBe('evt-res');
    expect(payload.type).toBe('skill');
    expect(payload.name).toBe('Evt Res');
    expect(payload.version).toBe('1.0.0');
    expect(payload.author).toBe('publisher-node');
    expect(typeof payload.at).toBe('number');
    expect(payload.at).toBeGreaterThan(0);
  });
});
