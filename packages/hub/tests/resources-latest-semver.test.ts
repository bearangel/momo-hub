// hub latest semver 语义契约锁（M2.3 T2b / 裁定书 p1-0 §4.2 H1/H2，v0.1.5）——
// 真 hub 实例 + 真 HTTP 发布（无 mock，同 resources-catalog.test.ts 模式）：
// ① 乱序发布：2.0.0 后发低版本 1.5.0 → catalog latest 仍 2.0.0（时序回退修复，H1）
//    且 checksum/sizeBytes/publishedAt 取 semver max 版本行（不是最近发布行）
// ② 字典序陷阱：10.0.0 vs 2.0.0 / 1.10.0 vs 1.9.0 → 数值段比较（'10'>'2'、'10'>'9'）
// ③ 单版本目录不炸（versions 单元素、validateHubCatalog 通过）
// ④ versions 全集 semver 降序（H2——保持 versions[0]==version 不变式）
// ⑤ 旧数据容忍：DB latest_version 列持有低值（旧 hub 乱序发布遗留）→ 目录输出仍按
//    versions 全集 semver max 计算（目录不再以该列为 latest 源；发布 upsert 不动该列）
// ⑥ compareVersions 单元语义（hub 私有实现；studio 侧 T2 另有单源，双仓语义一致由
//    studio 契约测试锁）：缺段容忍 / 预发布低于正式版 / 预发布标识符比较 / 非法段按 0 /
//    build metadata 忽略 / 数值段非字典序
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import { compareVersions } from '../src/resources';
import { validateHubCatalog, type HubCatalog, type HubResourceMeta } from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-latest-semver-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let deviceToken: string;

function sha256(data: Buffer | string): string {
  return createHash('sha256').update(data).digest('hex');
}

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    slug: 'semver-res',
    type: 'skill',
    name: 'Semver Res',
    version: '1.0.0',
    author: 'ignored',
    description: 'latest semver 语义夹具',
    readme: '# Semver',
    tags: [],
    category: 'dev',
    iconEmoji: '🧩',
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

async function publish(manifest: unknown, payload: Buffer): Promise<Response> {
  const res = await fetch(`${baseUrl}/resources/publish`, {
    method: 'POST',
    headers: { authorization: `Bearer ${deviceToken}` },
    body: form(manifest, payload),
  });
  await res.arrayBuffer(); // 读完响应体，连接回可复用态
  return res;
}

async function getCatalog(): Promise<HubCatalog> {
  const res = await fetch(`${baseUrl}/resources/catalog`, {
    method: 'GET',
    headers: { authorization: `Bearer ${deviceToken}` },
  });
  expect(res.status).toBe(200);
  return await res.json() as HubCatalog;
}

async function catalogItem(slug: string): Promise<HubResourceMeta> {
  const body = await getCatalog();
  const item = body.items.find((i) => i.slug === slug);
  if (item === undefined) throw new Error(`目录缺 slug=${slug}`);
  return item;
}

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  const cred = enrollNode(db, identity.enrollmentToken, 'semver-node');
  if ('error' in cred) throw new Error('enroll 失败');
  deviceToken = cred.deviceToken;
});

afterAll(async () => {
  await server.close();
  db.close();
});

// ── ① 乱序发布：latest 不随低版本回退（H1 主修点）────────────────────────────

describe('乱序发布：latest = versions 全集 semver max（H1）', () => {
  it('2.0.0 → 1.5.0（低版本后发）：latest 仍 2.0.0，checksum/sizeBytes/publishedAt 取 2.0.0 版本行', async () => {
    // 发布序：1.0.0 → 2.0.0 → 1.5.0（乱序：最近发布是低版本）
    const seq: Array<{ version: string; payload: Buffer }> = [
      { version: '1.0.0', payload: Buffer.from('oo payload 1.0.0') },
      { version: '2.0.0', payload: Buffer.from('oo payload 2.0.0') },
      { version: '1.5.0', payload: Buffer.from('oo payload 1.5.0') },
    ];
    for (const s of seq) {
      const res = await publish(manifestOf({ slug: 'oo-res', version: s.version }), s.payload);
      expect(res.status, `发布 ${s.version}`).toBe(201);
    }

    const item = await catalogItem('oo-res');
    // H1 主断言：latest 不回退（旧实现 = 最近发布 1.5.0）
    expect(item.version).toBe('2.0.0');
    expect(item.downloadUrl).toBe(`${baseUrl}/resources/oo-res/2.0.0/download`);
    // meta 字段取 semver max 版本行（2.0.0 的服务端自算值），不是最近发布行（1.5.0）
    expect(item.checksum).toBe(sha256(seq[1]!.payload));
    expect(item.sizeBytes).toBe(seq[1]!.payload.length);
    const v2Row = db.prepare('SELECT published_at FROM resource_versions WHERE slug = ? AND version = ?')
      .get('oo-res', '2.0.0') as { published_at: number };
    expect(item.publishedAt).toBe(v2Row.published_at);
    // versions 全集 semver 降序（H2）：versions[0] == version 不变式
    expect(item.versions).toEqual(['2.0.0', '1.5.0', '1.0.0']);
    expect(item.versions[0]).toBe(item.version);
  });

  it('乱序目录仍过协议单源校验（validateHubCatalog）', async () => {
    const body = await getCatalog();
    expect(validateHubCatalog(body)).not.toBeNull();
  });
});

// ── ② 字典序陷阱：数值段比较（不是字符串序）──────────────────────────────────

describe('字典序陷阱：10.0.0 vs 2.0.0 / 1.10.0 vs 1.9.0（数值段比较）', () => {
  it('2.0.0 后发 10.0.0：latest 是 10.0.0（"10" > "2" 数值比较，非字典序）', async () => {
    expect((await publish(manifestOf({ slug: 'trap-res', version: '2.0.0' }), Buffer.from('trap 2'))).status).toBe(201);
    expect((await publish(manifestOf({ slug: 'trap-res', version: '10.0.0' }), Buffer.from('trap 10'))).status).toBe(201);
    const item = await catalogItem('trap-res');
    expect(item.version).toBe('10.0.0');
    expect(item.versions).toEqual(['10.0.0', '2.0.0']);
    expect(item.downloadUrl).toBe(`${baseUrl}/resources/trap-res/10.0.0/download`);
  });

  it('1.9.0 后发 1.10.0：latest 是 1.10.0（次段 10 > 9）', async () => {
    expect((await publish(manifestOf({ slug: 'minor-res', version: '1.9.0' }), Buffer.from('minor 9'))).status).toBe(201);
    expect((await publish(manifestOf({ slug: 'minor-res', version: '1.10.0' }), Buffer.from('minor 10'))).status).toBe(201);
    const item = await catalogItem('minor-res');
    expect(item.version).toBe('1.10.0');
    expect(item.versions).toEqual(['1.10.0', '1.9.0']);
  });
});

// ── ③ 单版本目录不炸 ─────────────────────────────────────────────────────────

describe('单版本目录', () => {
  it('仅一个版本的 slug：正常成项，versions 单元素，过协议校验', async () => {
    expect((await publish(manifestOf({ slug: 'solo-res', version: '0.3.7' }), Buffer.from('solo payload'))).status).toBe(201);
    const item = await catalogItem('solo-res');
    expect(item.version).toBe('0.3.7');
    expect(item.versions).toEqual(['0.3.7']);
    expect(item.downloadUrl).toBe(`${baseUrl}/resources/solo-res/0.3.7/download`);
    const body = await getCatalog();
    expect(validateHubCatalog(body)).not.toBeNull();
  });
});

// ── ⑤ 旧数据容忍：latest_version 列持有旧值不作目录 latest 源 ────────────────

describe('旧数据：DB latest_version 列低值，目录仍按 semver max 计算', () => {
  it('手工把 oo-res 的 latest_version 改成 1.5.0（模拟旧 hub 乱序发布遗留）→ 目录 latest 仍 2.0.0', async () => {
    db.prepare("UPDATE resources SET latest_version = '1.5.0' WHERE slug = 'oo-res'").run();
    const item = await catalogItem('oo-res');
    // 目录输出与 latest_version 列脱钩（该列降级为内部「最近发布」记录）
    expect(item.version).toBe('2.0.0');
    expect(item.checksum).toBe(sha256('oo payload 2.0.0'));
    expect(item.versions).toEqual(['2.0.0', '1.5.0', '1.0.0']);
  });

  it('再发布 3.0.0 后目录 latest 前进到 3.0.0（正常发布链不受列旧值影响）', async () => {
    expect((await publish(manifestOf({ slug: 'oo-res', version: '3.0.0' }), Buffer.from('oo payload 3.0.0'))).status).toBe(201);
    const item = await catalogItem('oo-res');
    expect(item.version).toBe('3.0.0');
    expect(item.versions).toEqual(['3.0.0', '2.0.0', '1.5.0', '1.0.0']);
  });
});

// ── ⑥ compareVersions 单元语义（hub 私有实现）────────────────────────────────

describe('compareVersions（hub 私有 semver 比较）', () => {
  it('数值段比较（非字典序）', () => {
    expect(compareVersions('10.0.0', '2.0.0')).toBeGreaterThan(0);
    expect(compareVersions('1.10.0', '1.9.0')).toBeGreaterThan(0);
    expect(compareVersions('2.0.0', '10.0.0')).toBeLessThan(0);
    expect(compareVersions('1.0.0', '1.0.0')).toBe(0);
  });

  it('缺段容忍：缺段按 0（1.0 == 1.0.0 == 1）', () => {
    expect(compareVersions('1.0', '1.0.0')).toBe(0);
    expect(compareVersions('1', '1.0.0')).toBe(0);
    expect(compareVersions('1.0.1', '1.0')).toBeGreaterThan(0);
  });

  it('预发布低于正式版：1.0.0-alpha < 1.0.0', () => {
    expect(compareVersions('1.0.0-alpha', '1.0.0')).toBeLessThan(0);
    expect(compareVersions('1.0.0', '1.0.0-alpha')).toBeGreaterThan(0);
  });

  it('预发布标识符比较：数值比较 / 数值 < 字母 / 前缀短的低', () => {
    expect(compareVersions('1.0.0-alpha.1', '1.0.0-alpha.2')).toBeLessThan(0);
    expect(compareVersions('1.0.0-alpha', '1.0.0-alpha.1')).toBeLessThan(0);
    expect(compareVersions('1.0.0-1', '1.0.0-alpha')).toBeLessThan(0); // 数值标识符 < 字母标识符
    expect(compareVersions('1.0.0-alpha', '1.0.0-beta')).toBeLessThan(0);
  });

  it('非法段按 0：1.x.0 == 1.0.0；空段 == 0；v 前缀段按 0', () => {
    expect(compareVersions('1.x.0', '1.0.0')).toBe(0);
    expect(compareVersions('1..0', '1.0.0')).toBe(0);
    expect(compareVersions('v2.0.0', '2.0.0')).toBeLessThan(0); // 非法首段 'v2' 按 0 → (0,0,0) < (2,0,0)
    expect(compareVersions('v2.0.0', 'x.0.0')).toBe(0); // 双侧非法段同按 0 → 相等
  });

  it('build metadata 忽略：1.0.0+build.1 == 1.0.0', () => {
    expect(compareVersions('1.0.0+build.1', '1.0.0')).toBe(0);
  });
});
