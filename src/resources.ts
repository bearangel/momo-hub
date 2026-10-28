// 资源分发契约（spec 2026-10-24 M1 §3.2/§3.4 + 裁定 RM1-1）——
// 四类型 / HubCatalog / HubResourceMeta / 发布 manifest 与服务端校验。
// hub 与 studio 双端消费；字段对齐 momo-studio MarketplaceItem（无 variants/installCount，
// 新增 publishedBy→author / publishedAt / downloadUrl / checksum / sizeBytes / versions）。

/** hub 资源类型：marketplace 三类 + plugin（整体设计钦定四类）。 */
export type HubResourceType = 'agent' | 'mcp' | 'skill' | 'plugin';

/** 单条 hub 资源元数据（spec M1 §3.2 全字段）。 */
export interface HubResourceMeta {
  slug: string;
  type: HubResourceType;
  name: string;
  version: string; // 最新版本（v0.1.5 起 = versions 全集 semver 最大，非「最近发布」）
  author: string; // 发布节点名
  description: string;
  readme: string;
  tags: string[];
  category: string;
  iconEmoji: string;
  verificationStatus: 'unverified'; // hub 恒为 unverified（私有仓无分级）
  downloadUrl: string; // `${hubBase}/resources/{slug}/{version}/download`
  checksum: string; // sha256 hex，强制非空（C2）
  sizeBytes: number;
  publishedAt: number;
  versions: string[]; // 全部版本号，semver 降序（最大在前；v0.1.5 前为发布时序新→旧）
}

export interface HubCatalog {
  version: string;
  updatedAt: string;
  items: HubResourceMeta[];
}

/** 发布 manifest：meta 字段减去 downloadUrl/checksum/sizeBytes/publishedAt/versions
 *  （发布时客户端不报这些——C6，服务端自算）。 */
export interface HubPublishManifest {
  slug: string;
  type: HubResourceType;
  name: string;
  version: string;
  author: string;
  description: string;
  readme: string;
  tags: string[];
  category: string;
  iconEmoji: string;
  verificationStatus: 'unverified';
}

// ────────────────────────────────────────────────────────────────────────────
// S1 常量：拷贝自 momo-studio electron/src/main/marketplace/types.ts:60-66
// （跨仓不引依赖；两仓一致性由 packages/hub/tests/protocol-resources.test.ts 锁）。
// 白名单字符集是安装链的注入防线——slug/version 会被拼进文件系统路径与
// 子进程参数，任何 shell 元字符都必须在这里被拒绝。
// ────────────────────────────────────────────────────────────────────────────

/** slug 白名单：小写字母/数字开头，后续允许小写字母/数字/连字符/点/下划线 */
export const SLUG_PATTERN = /^[a-z0-9][a-z0-9-._]*$/;

/** version 白名单：数字/字母/点/加号/减号（semver 宽松超集） */
export const VERSION_PATTERN = /^[0-9A-Za-z.+-]+$/;

/** sha256 hex 校验和（64 位十六进制） */
export const SHA256_HEX_PATTERN = /^[a-f0-9]{64}$/i;

export function isValidHubSlug(slug: string): boolean {
  return SLUG_PATTERN.test(slug);
}

export function isValidHubVersion(version: string): boolean {
  return VERSION_PATTERN.test(version);
}

export function isValidHubChecksum(checksum: string): boolean {
  return SHA256_HEX_PATTERN.test(checksum);
}

// DoS 下限（spec M1 §3.4）
const SLUG_MAX = 64;
const NAME_MAX = 128;
const README_MAX_BYTES = 64 * 1024;

const isStr = (x: unknown): x is string => typeof x === 'string';

function isStrArray(x: unknown): x is string[] {
  return Array.isArray(x) && x.every((s) => isStr(s));
}

const textEncoder = new TextEncoder();

function manifestFrom(raw: Record<string, unknown>): HubPublishManifest | null {
  const type = raw.type;
  if (type !== 'agent' && type !== 'mcp' && type !== 'skill' && type !== 'plugin') return null;
  if (!isStr(raw.slug) || !isValidHubSlug(raw.slug) || raw.slug.length > SLUG_MAX) return null;
  if (!isStr(raw.version) || !isValidHubVersion(raw.version)) return null;
  if (!isStr(raw.name) || raw.name.length === 0 || raw.name.length > NAME_MAX) return null;
  if (!isStr(raw.author) || raw.author.length === 0) return null;
  if (!isStr(raw.description)) return null;
  if (!isStr(raw.readme) || textEncoder.encode(raw.readme).length > README_MAX_BYTES) return null;
  if (!isStrArray(raw.tags)) return null;
  if (!isStr(raw.category)) return null;
  if (!isStr(raw.iconEmoji)) return null;
  if (raw.verificationStatus !== 'unverified') return null;
  return {
    slug: raw.slug,
    type,
    name: raw.name,
    version: raw.version,
    author: raw.author,
    description: raw.description,
    readme: raw.readme,
    tags: [...raw.tags],
    category: raw.category,
    iconEmoji: raw.iconEmoji,
    verificationStatus: 'unverified',
  };
}

/** 发布 manifest part 结构校验（spec M1 §3.4，对齐 validateCatalog 强度）：
 *  type 枚举四类、slug/version 过 S1、字符串字段、tags 数组、长度下限；
 *  未知字段容忍（剥除）；任何已知字段缺失/类型不符返回 null。 */
export function validateHubManifest(raw: unknown): HubPublishManifest | null {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return null;
  return manifestFrom(raw as Record<string, unknown>);
}

/** 完整 HubResourceMeta 校验（裁定 RM1-1）：manifest 同款字段校验 +
 *  downloadUrl 字符串、checksum 必须合法 sha256、sizeBytes/publishedAt 数字、
 *  versions 字符串数组。非法返回 null。 */
function validateHubMeta(raw: unknown): HubResourceMeta | null {
  const base = validateHubManifest(raw);
  if (base === null || typeof raw !== 'object' || raw === null) return null;
  const m = raw as Record<string, unknown>;
  if (!isStr(m.downloadUrl) || m.downloadUrl.length === 0) return null;
  if (!isStr(m.checksum) || !isValidHubChecksum(m.checksum)) return null;
  if (typeof m.sizeBytes !== 'number' || !Number.isFinite(m.sizeBytes) || m.sizeBytes < 0) return null;
  if (typeof m.publishedAt !== 'number' || !Number.isFinite(m.publishedAt) || !Number.isInteger(m.publishedAt)) return null;
  if (!isStrArray(m.versions)) return null;
  return {
    ...base,
    downloadUrl: m.downloadUrl,
    checksum: m.checksum,
    sizeBytes: m.sizeBytes,
    publishedAt: m.publishedAt,
    versions: [...m.versions],
  };
}

/** HubCatalog 结构校验（裁定 RM1-1）：顶层 version/updatedAt/items +
 *  items 逐项全字段校验。非法返回 null（T9 catalog 消费侧防御）。 */
export function validateHubCatalog(raw: unknown): HubCatalog | null {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return null;
  const c = raw as Record<string, unknown>;
  if (!isStr(c.version) || !isStr(c.updatedAt) || !Array.isArray(c.items)) return null;
  const items: HubResourceMeta[] = [];
  for (const item of c.items) {
    const meta = validateHubMeta(item);
    if (meta === null) return null;
    items.push(meta);
  }
  return { version: c.version, updatedAt: c.updatedAt, items };
}
