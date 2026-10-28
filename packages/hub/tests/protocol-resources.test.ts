// protocol 1.1.0 资源契约测试（spec 2026-10-24 M1 §3.2/§3.3/§3.4 + 裁定 RM1-1）：
// 类型 round-trip / S1 常量正反例与拷贝锁 / validateHubManifest 好-坏矩阵 /
// validateHubCatalog 正反例 / EVT 目录扩展。
import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  EVT,
  SHA256_HEX_PATTERN,
  SLUG_PATTERN,
  VERSION_PATTERN,
  isValidHubChecksum,
  isValidHubSlug,
  isValidHubVersion,
  validateHubCatalog,
  validateHubManifest,
  type HubCatalog,
  type HubPublishManifest,
  type HubResourceMeta,
} from 'momo-hub-protocol';

const sha256hex = (s: string): string => createHash('sha256').update(s).digest('hex');

function validMeta(): HubResourceMeta {
  return {
    slug: 'coder-pro',
    type: 'agent',
    name: 'Coder Pro',
    version: '1.2.3+build',
    author: 'workstation-1',
    description: '一台代码机',
    readme: '# readme',
    tags: ['code', 'cli'],
    category: 'dev',
    iconEmoji: '🤖',
    verificationStatus: 'unverified',
    downloadUrl: 'http://hub:7950/resources/coder-pro/1.2.3+build/download',
    checksum: sha256hex('pkg-bytes'),
    sizeBytes: 1024,
    publishedAt: 1_700_000_000_000,
    versions: ['1.2.3+build', '1.2.2'],
  };
}

function validManifest(): HubPublishManifest {
  return {
    slug: 'coder-pro',
    type: 'agent',
    name: 'Coder Pro',
    version: '1.2.3+build',
    author: 'workstation-1',
    description: '一台代码机',
    readme: '# readme',
    tags: ['code', 'cli'],
    category: 'dev',
    iconEmoji: '🤖',
    verificationStatus: 'unverified',
  };
}

describe('S1 常量（拷贝自 momo-studio marketplace/types.ts:60-66）', () => {
  it('与源仓逐字一致（跨仓拷贝一致性锁）', () => {
    expect(SLUG_PATTERN.source).toBe('^[a-z0-9][a-z0-9-._]*$');
    expect(SLUG_PATTERN.flags).toBe('');
    expect(VERSION_PATTERN.source).toBe('^[0-9A-Za-z.+-]+$');
    expect(VERSION_PATTERN.flags).toBe('');
    expect(SHA256_HEX_PATTERN.source).toBe('^[a-f0-9]{64}$');
    expect(SHA256_HEX_PATTERN.flags).toBe('i');
  });

  it('slug 正反例：好 a-b.c_d；坏 A/b! 等', () => {
    expect(isValidHubSlug('a-b.c_d')).toBe(true);
    expect(isValidHubSlug('a1')).toBe(true);
    expect(isValidHubSlug('A/b!')).toBe(false); // 大写开头 + shell 元字符
    expect(isValidHubSlug('-lead')).toBe(false); // 必须字母数字开头
    expect(isValidHubSlug('a b')).toBe(false);
    expect(isValidHubSlug('')).toBe(false);
  });

  it('version 正反例：好 1.2.3+build；坏 1.2.3 x 等', () => {
    expect(isValidHubVersion('1.2.3+build')).toBe(true);
    expect(isValidHubVersion('1.0.0-rc.1')).toBe(true);
    expect(isValidHubVersion('1.2.3 x')).toBe(false); // 空格不在白名单
    expect(isValidHubVersion('1.2.3?')).toBe(false);
    expect(isValidHubVersion('')).toBe(false);
  });

  it('checksum 正反例：64 位 hex；长度不足/非 hex 拒绝', () => {
    expect(isValidHubChecksum(sha256hex('x'))).toBe(true);
    expect(isValidHubChecksum('a'.repeat(63))).toBe(false);
    expect(isValidHubChecksum('g'.repeat(64))).toBe(false);
    expect(isValidHubChecksum('')).toBe(false);
  });
});

describe('validateHubManifest', () => {
  it('合法 manifest：归一化返回，未知字段容忍且被剥除', () => {
    const out = validateHubManifest({ ...validManifest(), extraField: 1 });
    expect(out).toEqual(validManifest());
    expect(out).not.toHaveProperty('extraField');
  });

  it('坏矩阵：缺 type / type 越枚举 / 非对象输入', () => {
    const m = validManifest();
    const { type: _omitType, ...noType } = m;
    expect(validateHubManifest(noType)).toBeNull();
    expect(validateHubManifest({ ...m, type: 'app' })).toBeNull();
    expect(validateHubManifest(null)).toBeNull();
    expect(validateHubManifest('str')).toBeNull();
    expect(validateHubManifest([m])).toBeNull();
  });

  it('坏矩阵：S1 字段不合法（slug 越字符集/超长、version 越字符集）', () => {
    const m = validManifest();
    expect(validateHubManifest({ ...m, slug: 'Bad/Slug!' })).toBeNull();
    expect(validateHubManifest({ ...m, slug: 'a'.repeat(65) })).toBeNull(); // >64
    expect(validateHubManifest({ ...m, version: '1.2.3 x' })).toBeNull();
  });

  it('坏矩阵：readme 超 64KB、name 超长、name 缺失', () => {
    const m = validManifest();
    expect(validateHubManifest({ ...m, readme: 'a'.repeat(64 * 1024 + 1) })).toBeNull();
    expect(validateHubManifest({ ...m, readme: 'a'.repeat(64 * 1024) })).not.toBeNull(); // 恰好 64KB 合法
    expect(validateHubManifest({ ...m, name: 'n'.repeat(129) })).toBeNull();
    const { name: _omitName, ...noName } = m;
    expect(validateHubManifest(noName)).toBeNull();
  });

  it('坏矩阵：tags 非数组 / 含非字符串；verificationStatus 非 unverified', () => {
    const m = validManifest();
    expect(validateHubManifest({ ...m, tags: 'code' })).toBeNull();
    expect(validateHubManifest({ ...m, tags: ['ok', 1] })).toBeNull();
    expect(validateHubManifest({ ...m, verificationStatus: 'verified' })).toBeNull();
  });
});

describe('类型与 HubCatalog round-trip（RM1-1）', () => {
  it('HubResourceMeta/HubCatalog JSON round-trip 结构不变', () => {
    const catalog: HubCatalog = {
      version: '1',
      revision: 7,
      updatedAt: '2026-10-24T00:00:00.000Z',
      items: [validMeta()],
    };
    const back = JSON.parse(JSON.stringify(catalog)) as HubCatalog;
    expect(back).toEqual(catalog);
    expect(validateHubCatalog(back)).toEqual(catalog);
  });

  it('validateHubCatalog：合法目录归一化通过；items 空数组合法', () => {
    const catalog: HubCatalog = { version: '1', revision: 3, updatedAt: '2026-10-24T00:00:00.000Z', items: [] };
    expect(validateHubCatalog(catalog)).toEqual(catalog);
    const withItem: HubCatalog = { ...catalog, items: [validMeta()] };
    expect(validateHubCatalog(withItem)).toEqual(withItem);
  });

  it('validateHubCatalog：顶层结构缺失 / items 非数组 / 非对象 → null', () => {
    expect(validateHubCatalog(null)).toBeNull();
    expect(validateHubCatalog('catalog')).toBeNull();
    const { version: _omitVersion, ...noVersion } = {
      version: '1',
      updatedAt: 't',
      items: [],
    };
    expect(validateHubCatalog(noVersion)).toBeNull();
    expect(validateHubCatalog({ version: '1', items: [] })).toBeNull(); // 缺 updatedAt
    expect(validateHubCatalog({ version: '1', updatedAt: 't', items: 'x' })).toBeNull();
  });

  it('validateHubCatalog：items 逐项校验——item 过 manifest 同款字段校验', () => {
    const bad = validMeta();
    bad.slug = 'Bad/Slug!';
    expect(validateHubCatalog({ version: '1', updatedAt: 't', items: [bad] })).toBeNull();
  });

  it('validateHubCatalog：checksum 必须合法 sha256', () => {
    const meta = validMeta();
    meta.checksum = 'not-a-hash';
    expect(validateHubCatalog({ version: '1', updatedAt: 't', items: [meta] })).toBeNull();
  });

  it('validateHubCatalog：versions 必须是字符串数组；downloadUrl/sizeBytes/publishedAt 类型校验', () => {
    const base = { version: '1', updatedAt: 't' } as const;
    const v1 = validMeta();
    (v1.versions as unknown[]) = [1, 2];
    expect(validateHubCatalog({ ...base, items: [v1] })).toBeNull();
    const v2 = validMeta();
    delete (v2 as Partial<HubResourceMeta>).versions;
    expect(validateHubCatalog({ ...base, items: [v2] })).toBeNull();
    const v3 = validMeta();
    v3.downloadUrl = 123 as unknown as string;
    expect(validateHubCatalog({ ...base, items: [v3] })).toBeNull();
    const v4 = validMeta();
    v4.sizeBytes = '1024' as unknown as number;
    expect(validateHubCatalog({ ...base, items: [v4] })).toBeNull();
    const v5 = validMeta();
    v5.publishedAt = 'now' as unknown as number;
    expect(validateHubCatalog({ ...base, items: [v5] })).toBeNull();
  });

  it('validateHubCatalog：未知顶层字段容忍', () => {
    const catalog = { ...{ version: '1', updatedAt: 't', items: [] }, nextField: true };
    expect(validateHubCatalog(catalog)).toEqual({ version: '1', revision: 0, updatedAt: 't', items: [] });
  });
});

describe('HubCatalog.revision（1.5.0 M3.4 B10）', () => {
  const base = { version: '1', updatedAt: 't', items: [] } as const;

  it('在场：非负整数 → round-trip 保留', () => {
    expect(validateHubCatalog({ ...base, revision: 42 })).toEqual({ ...base, revision: 42 });
    expect(validateHubCatalog({ ...base, revision: 0 })).toEqual({ ...base, revision: 0 });
  });

  it('缺省：旧 hub（1.4）不发送 → 容错归一 0（字段新增非破坏，studio 消费侧防御）', () => {
    expect(validateHubCatalog({ ...base })).toEqual({ ...base, revision: 0 });
  });

  it('非法：字符串 / 负数 / 非整数 / NaN → null', () => {
    expect(validateHubCatalog({ ...base, revision: '7' })).toBeNull();
    expect(validateHubCatalog({ ...base, revision: -1 })).toBeNull();
    expect(validateHubCatalog({ ...base, revision: 1.5 })).toBeNull();
    expect(validateHubCatalog({ ...base, revision: Number.NaN })).toBeNull();
  });
});

describe('EVT 目录扩展（spec M1 §3.3）', () => {
  it('resourcePublished 常量稳定；既有常量不回退', () => {
    expect(EVT.resourcePublished).toBe('resource.published');
    expect(EVT.nodeOnline).toBe('node.online');
    expect(EVT.nodeOffline).toBe('node.offline');
  });

  it('ResourcePublishedEvent 形状', () => {
    const evt = {
      slug: 'coder-pro',
      type: 'agent' as const,
      name: 'Coder Pro',
      version: '1.2.3',
      author: 'workstation-1',
      at: 1_700_000_000_000,
    };
    expect(Object.keys(evt).sort()).toEqual(['at', 'author', 'name', 'slug', 'type', 'version']);
  });

  it('resourceUnpublished 常量稳定（1.5.0 M3.4 B12 新增）', () => {
    expect(EVT.resourceUnpublished).toBe('resource.unpublished');
  });

  it('ResourceUnpublishedEvent 形状 {slug, by, at}', () => {
    const evt = { slug: 'coder-pro', by: 'workstation-1', at: 1_700_000_000_000 };
    expect(Object.keys(evt).sort()).toEqual(['at', 'by', 'slug']);
  });
});
