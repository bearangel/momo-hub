// hub 资源发布端点契约锁（spec 2026-10-24 M1 §3.1/§3.4/§4 + 裁定 R-M1-4/R-M1-5）——
// 真实 listen + fetch FormData 上传（wire 语义全真，非 mock）：
// 201 落库落盘 / 校验拒绝矩阵 / 版本不可变 409（含并发双层）/ 超限 413 临时件零残留 /
// 未鉴权 401 / 坏 multipart 400 / verificationStatus 归一化落库。
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import { PUBLISH_MAX_BYTES, normalizePublishManifest } from '../src/resources';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-publish-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let deviceToken: string;

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  const cred = enrollNode(db, identity.enrollmentToken, 'publisher-node');
  if ('error' in cred) throw new Error('enroll 失败');
  deviceToken = cred.deviceToken;
});
afterAll(async () => { await server.close(); db.close(); });

// ── 工具 ────────────────────────────────────────────────────────────────────

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    slug: 'coder-pro',
    type: 'agent',
    name: 'Coder Pro',
    version: '1.0.0',
    author: 'publisher-node',
    description: '一个 coder agent',
    readme: '# Coder Pro',
    tags: ['code', 'agent'],
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

/** 组 multipart 请求体：manifest（JSON part）+ package（binary part）。 */
function form(manifest: unknown, payload: Buffer | null = Buffer.from('fake tar.gz bytes')): FormData {
  const fd = new FormData();
  const text = typeof manifest === 'string' ? manifest : JSON.stringify(manifest);
  fd.append('manifest', asBlob(text, 'application/json'), 'manifest.json');
  if (payload !== null) fd.append('package', asBlob(payload, 'application/gzip'), 'package.tar.gz');
  return fd;
}

function publish(body: FormData, token: string | null = deviceToken): Promise<Response> {
  const headers: Record<string, string> = {};
  if (token !== null) headers.authorization = `Bearer ${token}`;
  return fetch(`${baseUrl}/resources/publish`, { method: 'POST', headers, body });
}

function blobFiles(): string[] {
  const dir = join(dataDir, 'blobs');
  if (!existsSync(dir)) return [];
  return readdirSync(dir);
}

function resourceRow(slug: string): Record<string, unknown> | undefined {
  return db.prepare('SELECT * FROM resources WHERE slug = ?').get(slug) as Record<string, unknown> | undefined;
}

// ── 用例 ────────────────────────────────────────────────────────────────────

describe('POST /resources/publish', () => {
  it('正常发布 201：响应含 slug/version/checksum/sizeBytes，DB 两表落行，blob 按内容寻址落盘', async () => {
    const payload = Buffer.from('tar.gz payload for coder-pro v1');
    const res = await publish(form(manifestOf(), payload));
    expect(res.status).toBe(201);
    const body = await res.json() as { slug: string; version: string; checksum: string; sizeBytes: number };
    expect(body.slug).toBe('coder-pro');
    expect(body.version).toBe('1.0.0');
    expect(body.sizeBytes).toBe(payload.length);
    // C6：校验和由服务端自算（不信客户端自报）
    expect(body.checksum).toBe(createHash('sha256').update(payload).digest('hex'));

    const resRow = resourceRow('coder-pro');
    expect(resRow).toBeDefined();
    expect(resRow?.['type']).toBe('agent');
    expect(resRow?.['name']).toBe('Coder Pro');
    expect(resRow?.['author']).toBe('publisher-node');
    expect(resRow?.['latest_version']).toBe('1.0.0');
    expect(resRow?.['tags_json']).toBe('["code","agent"]');

    const verRow = db.prepare('SELECT * FROM resource_versions WHERE slug = ? AND version = ?')
      .get('coder-pro', '1.0.0') as Record<string, unknown> | undefined;
    expect(verRow).toBeDefined();
    expect(verRow?.['checksum']).toBe(body.checksum);
    expect(verRow?.['size_bytes']).toBe(payload.length);
    expect(verRow?.['published_by']).toBe('publisher-node');
    expect(typeof verRow?.['published_at']).toBe('number');

    const blobPath = join(dataDir, 'blobs', `${body.checksum}.tar.gz`);
    expect(existsSync(blobPath)).toBe(true);
    expect(readFileSync(blobPath).equals(payload)).toBe(true);
    expect(blobFiles().filter((f) => f.startsWith('.tmp-'))).toHaveLength(0);
  });

  it('校验拒绝矩阵：坏 slug → invalid_slug / 坏 version → invalid_version / 坏 type 与缺字段 → invalid_manifest / 非 JSON → invalid_manifest', async () => {
    const cases: Array<{ name: string; slug: string; manifest: unknown; expect: string }> = [
      { name: '坏 slug（大写+空格）', slug: 'bad-slug-1', manifest: manifestOf({ slug: 'Coder Pro' }), expect: 'invalid_slug' },
      { name: '坏 slug（shell 元字符）', slug: 'bad-slug-2', manifest: manifestOf({ slug: 'a;rm -rf' }), expect: 'invalid_slug' },
      { name: '坏 version（空格）', slug: 'bad-ver-1', manifest: manifestOf({ slug: 'bad-ver-1', version: '1.0.0 beta' }), expect: 'invalid_version' },
      { name: '坏 version（斜杠）', slug: 'bad-ver-2', manifest: manifestOf({ slug: 'bad-ver-2', version: '1.0.0/../x' }), expect: 'invalid_version' },
      { name: '坏 type', slug: 'bad-type-1', manifest: manifestOf({ slug: 'bad-type-1', type: 'widget' }), expect: 'invalid_manifest' },
      { name: '缺 name', slug: 'bad-field-1', manifest: (() => { const m = manifestOf({ slug: 'bad-field-1' }); delete m['name']; return m; })(), expect: 'invalid_manifest' },
      { name: 'tags 非数组', slug: 'bad-field-2', manifest: manifestOf({ slug: 'bad-field-2', tags: 'code' }), expect: 'invalid_manifest' },
      { name: 'manifest 非 JSON', slug: 'bad-json-1', manifest: 'not-json-at-all', expect: 'invalid_manifest' },
    ];
    for (const c of cases) {
      const res = await publish(form(c.manifest));
      expect(res.status, `用例：${c.name}`).toBe(400);
      expect(await res.json(), `用例：${c.name}`).toEqual({ error: c.expect });
      // 被拒的发布不得留下任何落库痕迹
      expect(resourceRow(c.slug), `用例：${c.name} 落库痕迹`).toBeUndefined();
    }
    expect(db.prepare('SELECT COUNT(*) AS n FROM resource_versions').get()).toEqual({ n: 1 }); // 仅首个正常发布
  });

  it('版本不可变：同 slug+version 重复发布 → 409 version_exists，DB 行数不变', async () => {
    const m = manifestOf({ slug: 'dup-res', version: '2.0.0' });
    const first = await publish(form(m));
    expect(first.status).toBe(201);
    const second = await publish(form(m, Buffer.from('different bytes')));
    expect(second.status).toBe(409);
    expect(await second.json()).toEqual({ error: 'version_exists' });
    expect(db.prepare('SELECT COUNT(*) AS n FROM resource_versions WHERE slug = ?').get('dup-res')).toEqual({ n: 1 });
  });

  it('并发同版本发布：恰一 201 一 409（预查 + 唯一键兜底双层）', async () => {
    const m = manifestOf({ slug: 'race-res', version: '3.0.0' });
    const [a, b] = await Promise.all([
      publish(form(m, Buffer.from('race payload'))),
      publish(form(m, Buffer.from('race payload'))),
    ]);
    const codes = [a.status, b.status].sort();
    expect(codes).toEqual([201, 409]);
    const conflict = a.status === 409 ? a : b;
    expect(await conflict.json()).toEqual({ error: 'version_exists' });
    expect(db.prepare('SELECT COUNT(*) AS n FROM resource_versions WHERE slug = ?').get('race-res')).toEqual({ n: 1 });
    expect(db.prepare('SELECT COUNT(*) AS n FROM resources WHERE slug = ?').get('race-res')).toEqual({ n: 1 });
  });

  it('早答后连接仍可用：坏 manifest + 大 body → 400，随后同端正常发布 201（半截请求不悬挂 socket）', async () => {
    const fd = new FormData();
    fd.append('manifest', asBlob(JSON.stringify(manifestOf({ slug: 'Bad Slug' })), 'application/json'), 'manifest.json');
    fd.append('package', asBlob(Buffer.alloc(4 * 1024 * 1024, 0x42), 'application/gzip'), 'package.tar.gz');
    const bad = await publish(fd);
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: 'invalid_slug' });

    const ok = await publish(form(manifestOf({ slug: 'after-bad', version: '1.0.0' }), Buffer.from('after bad')));
    expect(ok.status).toBe(201);
    expect(resourceRow('after-bad')?.['latest_version']).toBe('1.0.0');
  });

  it('manifest 以 application/json 字段 part 上传（插件预解析成对象）同样受理', async () => {
    const boundary = '----momohubtest';
    const manifestText = JSON.stringify(manifestOf({ slug: 'json-field-res', version: '1.0.0' }));
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="manifest"',
      'Content-Type: application/json',
      '',
      manifestText,
      `--${boundary}`,
      'Content-Disposition: form-data; name="package"; filename="package.tar.gz"',
      'Content-Type: application/gzip',
      '',
      'raw multipart payload',
      `--${boundary}--`,
      '',
    ].join('\r\n');
    const res = await fetch(`${baseUrl}/resources/publish`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${deviceToken}`,
        'content-type': `multipart/form-data; boundary=${boundary}`,
      },
      body,
    });
    expect(res.status).toBe(201);
    expect(resourceRow('json-field-res')?.['latest_version']).toBe('1.0.0');
  });

  it('唯一键兜底机制：resource_versions 的 (slug, version) 主键在库层强制唯一（并发窗口 409 的兜底）', () => {
    let code: string | undefined;
    try {
      db.prepare(`INSERT INTO resource_versions (slug, version, checksum, size_bytes, published_by, published_at)
                  VALUES (?, ?, ?, ?, ?, ?)`)
        .run('race-res', '3.0.0', 'a'.repeat(64), 1, 'publisher-node', Date.now());
    } catch (err) {
      code = (err as { code?: string }).code;
    }
    expect(code).toBe('SQLITE_CONSTRAINT_PRIMARYKEY');
  });

  it('未鉴权 401：无 Authorization / 错 token → unauthorized，且不落库', async () => {
    const noAuth = await publish(form(manifestOf({ slug: 'no-auth' })), null);
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await publish(form(manifestOf({ slug: 'no-auth' })), 'wrong-token');
    expect(badAuth.status).toBe(401);
    expect(await badAuth.json()).toEqual({ error: 'unauthorized' });
    expect(resourceRow('no-auth')).toBeUndefined();
  });

  it('坏 multipart 400 invalid_manifest：非 multipart content-type / multipart 形态损坏 / 缺 manifest part / 缺 package part', async () => {
    const notMultipart = await fetch(`${baseUrl}/resources/publish`, {
      method: 'POST',
      headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'application/json' },
      body: JSON.stringify(manifestOf({ slug: 'bad-mp' })),
    });
    expect(notMultipart.status).toBe(400);
    expect(await notMultipart.json()).toEqual({ error: 'invalid_manifest' });

    const broken = await fetch(`${baseUrl}/resources/publish`, {
      method: 'POST',
      headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'multipart/form-data; boundary=xyz' },
      body: 'this is not a multipart body at all',
    });
    expect(broken.status).toBe(400);
    expect(await broken.json()).toEqual({ error: 'invalid_manifest' });

    // 缺 package part
    const noPackage = await publish(form(manifestOf({ slug: 'no-pkg' }), null));
    expect(noPackage.status).toBe(400);
    expect(await noPackage.json()).toEqual({ error: 'invalid_manifest' });

    // 缺 manifest part（只有 package）
    const fd = new FormData();
    fd.append('package', asBlob(Buffer.from('bytes'), 'application/gzip'), 'package.tar.gz');
    const noManifest = await publish(fd);
    expect(noManifest.status).toBe(400);
    expect(await noManifest.json()).toEqual({ error: 'invalid_manifest' });

    expect(resourceRow('bad-mp')).toBeUndefined();
    expect(resourceRow('no-pkg')).toBeUndefined();
  });

  it('manifest 以普通字段 part（无 filename）上传同样受理——发布端不强制 part 形态', async () => {
    const fd = new FormData();
    fd.append('manifest', JSON.stringify(manifestOf({ slug: 'field-res', version: '1.0.0' })));
    fd.append('package', asBlob(Buffer.from('field payload'), 'application/gzip'), 'package.tar.gz');
    const res = await publish(fd);
    expect(res.status).toBe(201);
    expect(resourceRow('field-res')?.['latest_version']).toBe('1.0.0');
  });

  it('R-M1-5：verificationStatus 缺省 / 非 unverified → 归一为 unverified（不 400），落库值恒 unverified', async () => {
    // 归一化契约（服务端信任根：客户端不必对齐该字段）
    const missing = manifestOf({ slug: 'norm-res' });
    delete missing['verificationStatus'];
    const norm1 = normalizePublishManifest(missing);
    expect(norm1.ok).toBe(true);
    expect(norm1.ok && norm1.manifest.verificationStatus).toBe('unverified');

    const wrong = manifestOf({ slug: 'norm-res-2', verificationStatus: 'verified' });
    const norm2 = normalizePublishManifest(wrong);
    expect(norm2.ok).toBe(true);
    expect(norm2.ok && norm2.manifest.verificationStatus).toBe('unverified');

    // 端到端：带非 unverified 的 manifest 仍发布成功（不 400）
    const res1 = await publish(form(missing));
    expect(res1.status).toBe(201);
    const res2 = await publish(form(wrong));
    expect(res2.status).toBe(201);
    expect(resourceRow('norm-res')?.['author']).toBe('publisher-node');
    expect(resourceRow('norm-res-2')?.['author']).toBe('publisher-node');
  });
});

describe('publish 超限（413）', () => {
  it('注入小上限后超限 → 413 too_large，临时件零残留，DB 无落行（同一代码路径，默认 200MB）', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-publish-limit-'));
    const limitDb = openHubDatabase(dir);
    const limitIdentity = ensureHubIdentity(limitDb);
    const limitServer = createHubServer({ db: limitDb, identity: limitIdentity, dataDir: dir, resourceMaxBytes: 64 * 1024 });
    const url = await limitServer.listen(0, '127.0.0.1');
    const cred = enrollNode(limitDb, limitIdentity.enrollmentToken, 'limit-node');
    if ('error' in cred) throw new Error('enroll 失败');
    try {
      const fd = new FormData();
      fd.append('manifest', asBlob(JSON.stringify(manifestOf({ slug: 'big-res' })), 'application/json'), 'manifest.json');
      fd.append('package', asBlob(Buffer.alloc(256 * 1024, 0x41), 'application/gzip'), 'package.tar.gz');
      const res = await fetch(`${url}/resources/publish`, {
        method: 'POST', headers: { authorization: `Bearer ${cred.deviceToken}` }, body: fd,
      });
      expect(res.status).toBe(413);
      expect(await res.json()).toEqual({ error: 'too_large' });
      const files = existsSync(join(dir, 'blobs')) ? readdirSync(join(dir, 'blobs')) : [];
      expect(files.filter((f) => f.startsWith('.tmp-'))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith('.tar.gz'))).toHaveLength(0);
      expect(limitDb.prepare('SELECT COUNT(*) AS n FROM resources').get()).toEqual({ n: 0 });
      expect(limitDb.prepare('SELECT COUNT(*) AS n FROM resource_versions').get()).toEqual({ n: 0 });
    } finally {
      // 关闭必须能收口：半截请求会让 socket 悬挂（连接不可复用 → close 一直等）
      await expect(Promise.race([
        limitServer.close(),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error('server.close 超时：socket 悬挂')), 5000)),
      ])).resolves.toBeUndefined();
      limitDb.close();
    }
  });

  it('默认上限为 200MB（spec C5）', () => {
    expect(PUBLISH_MAX_BYTES).toBe(200 * 1024 * 1024);
  });
});
