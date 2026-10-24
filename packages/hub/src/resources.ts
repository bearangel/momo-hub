// hub 资源发布端点（spec 2026-10-24 M1 §3.1/§3.4/§4 + 裁定 C4/C5/C6/R-M1-4/R-M1-5）——
// multipart 两 part：`manifest`（JSON）+ `package`（binary 流）。
// 信任根在 hub：校验和服务端流式自算（客户端只传内容，C6）；版本不可变：同 slug+version → 409（C4）。
// 409 双层（R-M1-4）：预查给明确错误体（快路径），并发窗口撞唯一键由事务回滚兜底——错误体同款。
import multipart, { type Multipart } from '@fastify/multipart';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type Database from 'better-sqlite3';
import { createReadStream, statSync } from 'node:fs';
import { PassThrough, Readable } from 'node:stream';
import {
  EVT, PROTOCOL_VERSION, isValidHubSlug, isValidHubVersion, validateHubManifest,
  type HubCatalog, type HubPublishManifest, type HubResourceMeta, type HubResourceType,
  type ResourcePublishedEvent,
} from 'momo-hub-protocol';
import { BlobTooLargeError, openBlob, saveBlobStream } from './blobs';
import { verifyDeviceToken } from './auth';
import type { NodeRegistry } from './registry';
import { logger } from './logger';

/** 发布体上限（spec C5：200MB/包，与 marketplace MAX_DOWNLOAD_BYTES 一致）。 */
export const PUBLISH_MAX_BYTES = 200 * 1024 * 1024;

/** manifest part 内存上限（防 DoS；超限按坏形态 400 invalid_manifest）。 */
const MANIFEST_MAX_BYTES = 256 * 1024;

/** slug 长度上限（spec §3.4 DoS 下限）。 */
const SLUG_MAX = 64;

/** 发布端点错误码（wire 契约）。 */
export type PublishErrorCode =
  | 'unauthorized'        // 401
  | 'invalid_manifest'    // 400：形态坏 / manifest 字段非法 / 缺 part
  | 'invalid_slug'        // 400：slug 非法
  | 'invalid_version'     // 400：version 非法
  | 'version_exists'      // 409：同 slug+version 已发布（版本不可变）
  | 'too_large';          // 413：超过大小上限

export type PublishManifestResult =
  | { ok: true; manifest: HubPublishManifest }
  | { ok: false; error: 'invalid_manifest' | 'invalid_slug' | 'invalid_version' };

export interface ResourceRouteOpts {
  db: Database.Database;
  dataDir: string;
  /** 在线节点注册表——发布事件广播用。 */
  registry: NodeRegistry;
  /** 公网 origin（RM1-11）：设置后 catalog 的 downloadUrl 独占使用该 origin；
   *  未设置则用请求自身的协议 + Host 头。 */
  publicOrigin?: string;
  /** 发布体上限（默认 PUBLISH_MAX_BYTES；测试注入小值走同一代码路径）。 */
  maxBytes?: number;
}

/**
 * 发布 manifest 归一化 + 校验。
 * R-M1-5：verificationStatus 缺省 / 非 'unverified' 一律归一为 'unverified'（信任根在 hub，
 * 发布端不必对齐此字段）——不是 400。落库与响应里的验证态恒为 'unverified'。
 * 错误码分流：slug / version 是字段级可判定错误，单独成码；其余（type 枚举、字段类型、
 * 长度下限）归 invalid_manifest。
 */
export function normalizePublishManifest(raw: unknown): PublishManifestResult {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return { ok: false, error: 'invalid_manifest' };
  }
  const rec = raw as Record<string, unknown>;
  const slug = rec['slug'];
  if (typeof slug !== 'string' || slug.length > SLUG_MAX || !isValidHubSlug(slug)) {
    return { ok: false, error: 'invalid_slug' };
  }
  const version = rec['version'];
  if (typeof version !== 'string' || !isValidHubVersion(version)) {
    return { ok: false, error: 'invalid_version' };
  }
  // 归一化后再走 S1 校验（validateHubManifest 只认 unverified——正是我们要的落库值）。
  const manifest = validateHubManifest({ ...rec, verificationStatus: 'unverified' });
  if (manifest === null) return { ok: false, error: 'invalid_manifest' };
  return { ok: true, manifest };
}

/** 把一个 part 文件流消费干净（内容丢弃，不落盘）。
 *  必须用异步迭代器【读】，不能只挂 'data' 监听或 resume()：
 *  busboy 的文件流靠 push 背压——缓冲区涨到 highWaterMark 会置 `_pause` 并扣住
 *  multipart 的 write 回调；只有真正 read() 才会触发 busboy 的 `_read` → 释放回调 →
 *  底层请求体继续读完。少了这一步：请求体读不完 → socket 悬挂（连接不可复用、
 *  server.close() 挂起）。 */
async function discardPartFile(file: Readable): Promise<void> {
  try {
    const it = file[Symbol.asyncIterator]();
    for (;;) {
      const next = await it.next();
      if (next.done) break; // 内容丢弃：只读到流尾
    }
  } catch {
    // 流已销毁/出错：字节无处可去，忽略（响应已发出）
  }
}

/** 读 manifest part（有界内存）：字段 part 取值，文件 part 逐块读，超限返回 null。
 *  超限后仍把流尾消费干净——中途弃流会销毁 part 流 → busboy 停读（背压）→ 请求体收不完
 *  → 连接悬挂（连接不可复用、服务关闭挂起）。 */
async function readManifestPart(part: Multipart): Promise<string | null> {
  if (part.type === 'field') {
    if (part.valueTruncated) return null; // 字段被 multipart limits 截断 → 不可信
    // 字段 part 若声明 Content-Type: application/json，插件已把它解析成对象——回写成 JSON 文本再走统一解析。
    const raw: unknown = part.value;
    const value = typeof raw === 'string'
      ? raw
      : (raw === null || raw === undefined ? '' : JSON.stringify(raw));
    return Buffer.byteLength(value, 'utf8') > MANIFEST_MAX_BYTES ? null : value;
  }
  const chunks: Buffer[] = [];
  let size = 0;
  let overLimit = false;
  for await (const chunk of part.file) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    size += buf.length;
    if (overLimit) continue; // 已超限：继续消费流尾（弃流会悬挂连接），内容丢弃
    if (size > MANIFEST_MAX_BYTES) { overLimit = true; chunks.length = 0; continue; }
    chunks.push(buf);
  }
  return overLimit ? null : Buffer.concat(chunks).toString('utf8');
}

/** package part 的字节流；字段 part 形态非法（非字符串/被截断）时返回 null → 400 invalid_manifest。
 *  文件 part 走一层 PassThrough 包装：saveBlobStream 超限时管道会销毁源流——
 *  直接把 part.file 交出去，它会被销毁 → busboy 停读（背压）→ 请求体收不完 →
 *  socket 悬挂（连接不可复用、服务关闭挂起）。包装层在管道关闭时把底层 part 流继续
 *  drain 掉，保证请求能收尾、超限内容不落盘。 */
function packageStream(part: Multipart): NodeJS.ReadableStream | null {
  if (part.type !== 'file') {
    if (part.valueTruncated || typeof part.value !== 'string') return null;
    return Readable.from([Buffer.from(part.value, 'utf8')]);
  }
  const file = part.file;
  const wrapper = new PassThrough();
  const drain = (): void => { void discardPartFile(file); };
  wrapper.on('error', drain);
  wrapper.on('close', drain);
  file.on('error', () => { wrapper.destroy(); });
  file.pipe(wrapper);
  return wrapper;
}

/** SQLite 唯一键冲突（resource_versions 的 (slug, version) 主键）——并发窗口兜底 409。 */
function isUniqueViolation(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const code = (err as { code?: unknown }).code;
  return code === 'SQLITE_CONSTRAINT_PRIMARYKEY' || code === 'SQLITE_CONSTRAINT_UNIQUE';
}

// ────────────────────────────────────────────────────────────────────────────
// 资源目录与下载（spec 2026-10-24 M1 §3.1/§3.2 + T6 对齐口径）
// ────────────────────────────────────────────────────────────────────────────

/** 目录项来源行：resources JOIN「最新发布」版本行（LEFT JOIN——缺版本行则该项不成项）。 */
interface CatalogItemRow {
  slug: string;
  type: HubResourceType;
  name: string;
  author: string;
  description: string;
  readme: string;
  tags_json: string;
  category: string;
  icon_emoji: string;
  latest_version: string;
  checksum: string | null;
  size_bytes: number | null;
  published_by: string | null;
  published_at: number | null;
}

/** 请求 origin（downloadUrl 组装用，RM1-11 信任收敛）：
 *  - 配置了 publicOrigin（运维显式声明的公网 origin）→ 独占使用，不看任何请求头
 *  - 未配置 → 用请求自身的协议 + Host 头（直暴形态）
 *  刻意【不】采信 X-Forwarded-Proto / X-Forwarded-Host：那两个头由客户端自由填写，
 *  首个 token 最易伪造——采信它等于让任意节点把其他节点的下载地址指向别处。
 *  反代场景的正解是配 MOMO_HUB_PUBLIC_ORIGIN（见 README Caddy 段）。
 *  已知边界：Host 头缺失时回落 'localhost'（HTTP/2 无 Host 头；M1 不涉及 HTTP/2）。 */
function requestOrigin(req: FastifyRequest, publicOrigin: string | undefined): string {
  if (publicOrigin !== undefined) return publicOrigin;
  const host = req.headers.host ?? 'localhost';
  return `${req.protocol}://${host}`;
}

/** HTTP 头值清洗（RM1-10）：Node 的 setHeader 只接受 Latin-1 且禁 CR/LF——
 *  节点名可含非 ASCII（如中文）与控制字符（enrollment 只做长度/trim 校验），
 *  直接回填头会抛 ERR_INVALID_CHAR → 下载恒 500。
 *  规则：剥控制字符（含 CR/LF）；剩余字符 >0xFF 的按 UTF-8 逐字节百分号编码
 *  （与 URL query 编码同款，客户端可 decodeURIComponent 回原名）；
 *  清洗后为空 → 头不可用 → 省略该头（不 500）。
 *  边界：0x80–0xFF（Latin-1）原样保留（Node 允许原样进头）——这类字符不走
 *  百分号编码，客户端按 latin1 解码即可还原。 */
export function sanitizeHeaderValue(raw: string): string | null {
  let out = '';
  for (const ch of raw) {
    const code = ch.codePointAt(0) ?? 0;
    if (code <= 0x1f || code === 0x7f) continue; // 控制字符（含 CR/LF）剥除
    if (code <= 0xff) {
      out += ch; // Latin-1：Node 允许原样进头
      continue;
    }
    for (const byte of Buffer.from(ch, 'utf8')) {
      out += `%${byte.toString(16).toUpperCase().padStart(2, '0')}`;
    }
  }
  return out.length > 0 ? out : null;
}

/** 解析 tags_json（落库是 JSON 文本；解析失败按空数组兜底，不让一条脏数据毁掉整份目录）。 */
function parseTags(tagsJson: string): string[] {
  try {
    const parsed: unknown = JSON.parse(tagsJson);
    if (Array.isArray(parsed) && parsed.every((t) => typeof t === 'string')) return parsed as string[];
    return [];
  } catch {
    return [];
  }
}

/** 组装 hub 目录（spec §3.2 HubCatalog）。
 *  - items：resources JOIN「最近发布」的版本行（latest_version 存的就是最近一次发布的版本）
 *  - versions：该 slug 的全部版本号，新→旧（published_at 倒序，同毫秒以 rowid 兜底）
 *  - downloadUrl：按请求 origin 拼绝对地址（反代场景天然正确）
 *  - author 直映射 resources.author（= 鉴权节点名，R-M1-8）；verificationStatus 恒 'unverified'（表无该列） */
export function buildHubCatalog(db: Database.Database, origin: string): HubCatalog {
  const itemRows = db.prepare(`
    SELECT r.slug, r.type, r.name, r.author, r.description, r.readme, r.tags_json, r.category,
           r.icon_emoji, r.latest_version,
           v.checksum, v.size_bytes, v.published_by, v.published_at
    FROM resources r
    LEFT JOIN resource_versions v ON v.slug = r.slug AND v.version = r.latest_version
  `).all() as CatalogItemRow[];

  // versions：全量版本号新→旧（一次查询全取，按 slug 分组——避免逐项 N+1）
  const versionRows = db.prepare(
    'SELECT slug, version FROM resource_versions ORDER BY published_at DESC, rowid DESC',
  ).all() as Array<{ slug: string; version: string }>;
  const versionsBySlug = new Map<string, string[]>();
  for (const row of versionRows) {
    const list = versionsBySlug.get(row.slug);
    if (list) list.push(row.version);
    else versionsBySlug.set(row.slug, [row.version]);
  }

  const items: HubResourceMeta[] = [];
  for (const row of itemRows) {
    // 最新版本行缺失（数据不一致）→ 该项不成项（无 checksum/sizeBytes 不可用）
    if (row.checksum === null || row.size_bytes === null || row.published_at === null) continue;
    items.push({
      slug: row.slug,
      type: row.type,
      name: row.name,
      version: row.latest_version,
      author: row.author,
      description: row.description,
      readme: row.readme,
      tags: parseTags(row.tags_json),
      category: row.category,
      iconEmoji: row.icon_emoji,
      verificationStatus: 'unverified',
      downloadUrl: `${origin}/resources/${row.slug}/${row.latest_version}/download`,
      checksum: row.checksum,
      sizeBytes: row.size_bytes,
      publishedAt: row.published_at,
      versions: versionsBySlug.get(row.slug) ?? [row.latest_version],
    });
  }
  // 目录按发布时间倒序（新的在前）——同毫秒以 slug 兜底，保证顺序确定
  items.sort((a, b) => (b.publishedAt - a.publishedAt) || a.slug.localeCompare(b.slug));
  return { version: PROTOCOL_VERSION, updatedAt: new Date().toISOString(), items };
}

export function registerResourceRoutes(app: FastifyInstance, opts: ResourceRouteOpts): void {
  const maxBytes = opts.maxBytes ?? PUBLISH_MAX_BYTES;
  // multipart 插件挂在本作用域（fastify-plugin 不再封装 → 内容类型解析器与 req.parts 装饰对子作用域生效）。
  // fileSize 显式 Infinity：真实上限以实际落盘字节为准（saveBlobStream 手动流控，C5）——
  // 插件默认取 fastify bodyLimit（1MB），会把大包静默截断。
  // fieldSize = manifest 上限：字段形态的 manifest 超限即被截断（valueTruncated）→ 400 invalid_manifest。
  void app.register(multipart, {
    limits: { fileSize: Infinity, fieldSize: MANIFEST_MAX_BYTES, files: 8, fields: 8, parts: 16 },
  });

  // 资源目录（spec §3.1）：Bearer 鉴权 → 目录快照（downloadUrl 按请求 origin 拼装）
  app.get('/resources/catalog', async (req, reply) => {
    const { db } = opts;
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    if (!node) return reply.code(401).send({ error: 'unauthorized' });
    return reply.send(buildHubCatalog(db, requestOrigin(req, opts.publicOrigin)));
  });

  // 资源下载（spec §3.1）：Bearer 鉴权 → 参数早拒 → 版本行 → blob 流 + 校验和/发布者头
  app.get('/resources/:slug/:version/download', async (req, reply) => {
    const { db, dataDir } = opts;
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    if (!node) return reply.code(401).send({ error: 'unauthorized' });

    // 参数早拒：slug/version 必须过 S1 白名单（防路径与日志注入）——不合法与未知同款 404，
    // 不向外区分「参数非法」与「不存在」。
    const params = req.params as { slug?: string; version?: string };
    const slug = typeof params.slug === 'string' ? params.slug : '';
    const version = typeof params.version === 'string' ? params.version : '';
    if (!isValidHubSlug(slug) || !isValidHubVersion(version)) {
      return reply.code(404).send({ error: 'not_found' });
    }

    const row = db.prepare('SELECT checksum, published_by FROM resource_versions WHERE slug = ? AND version = ?')
      .get(slug, version) as { checksum: string; published_by: string } | undefined;
    if (!row) return reply.code(404).send({ error: 'not_found' });

    // blob 文件名即 sha256（内容寻址）：checksum 非法或文件缺失 → 404
    let blobPath: string;
    try {
      blobPath = openBlob(dataDir, row.checksum);
    } catch {
      return reply.code(404).send({ error: 'not_found' });
    }
    let sizeBytes: number;
    try {
      sizeBytes = statSync(blobPath).size;
    } catch {
      return reply.code(404).send({ error: 'not_found' });
    }

    reply.header('X-Checksum-Sha256', row.checksum);
    // 节点名可含非 ASCII/控制字符——头值必须清洗（否则 setHeader 抛错 → 500）
    const publishedBy = sanitizeHeaderValue(row.published_by);
    if (publishedBy !== null) reply.header('X-Published-By', publishedBy);
    reply.header('Content-Type', 'application/gzip');
    reply.header('Content-Length', String(sizeBytes));
    return reply.send(createReadStream(blobPath));
  });

  app.post('/resources/publish', async (req, reply) => {
    const { db, dataDir } = opts;

    // ① 鉴权：device token Bearer（与 /ws 升级同源校验）
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    // 鉴权失败：原始流尚未被 busboy 接管，node 会自动 drain 请求体——直接回错误体即可。
    if (!node) return reply.code(401).send({ error: 'unauthorized' });

    // ② multipart 形态：非 multipart 直接拒（同上，node 自动 drain）
    if (!req.isMultipart()) return reply.code(400).send({ error: 'invalid_manifest' });

    const iterator = req.parts();

    // ③ 收 part：manifest 必须先于 package（契约：manifest 在前）——
    //    manifest 不合法 → 400；package 缺失 → 400；multipart 解析炸 → 400。
    let manifest: HubPublishManifest | null = null;
    let manifestError: PublishErrorCode | null = null;
    // 已从迭代器取出、但可能尚未被消费的 package part（早答路径需要把它排干）。
    let packagePart: Multipart | null = null;
    // package 流已交给 saveBlobStream（包装层负责排干——不能两个消费方同时读一个流）。
    let packageHandedOff = false;

    // 早答的统一出口：回错误体后把剩余 part 消费干净。
    // multipart 解析一开始，原始流就被 busboy 接管（pipe）——node 不再自动 drain；
    // 未被消费的 part 流会让 busboy 背压停读 → 请求体收不完 → socket 悬挂
    // （连接不可复用、服务关闭挂起）。内容早已丢弃，磁盘零残留。
    const drainParts = async (): Promise<void> => {
      try {
        for (;;) {
          const next = await iterator.next();
          if (next.done) break;
          if (next.value.type === 'file') void discardPartFile(next.value.file);
        }
      } catch {
        // 形态损坏：剩余字节无处可去，忽略（响应已发出）
      }
    };
    const fail = (status: number, code: PublishErrorCode): FastifyReply => {
      // 已经取出但未消费的 package part（文件形态）必须在这里排干：
      // busboy 停在当前 part 上 → 请求体读不完 → 连接悬挂（close 挂起、连接不可复用）。
      // 迭代器只能拿到【下一个】part，拿不到手上这个——只能直接把它的流读干净。
      // 已交给 saveBlobStream 的不算（包装层在管道关闭时排干——一个流只能有一个消费方）。
      if (!packageHandedOff && packagePart !== null && packagePart.type === 'file') {
        void discardPartFile(packagePart.file);
      }
      const res = reply.code(status).send({ error: code });
      void drainParts();
      return res;
    };

    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done) break;
        const part = next.value;
        if (part.fieldname === 'manifest' && manifest === null && manifestError === null) {
          const text = await readManifestPart(part);
          if (text === null) { manifestError = 'invalid_manifest'; break; }
          let parsed: unknown;
          try {
            parsed = JSON.parse(text);
          } catch {
            manifestError = 'invalid_manifest';
            break;
          }
          const r = normalizePublishManifest(parsed);
          if (!r.ok) { manifestError = r.error; break; }
          manifest = r.manifest;
          continue;
        }
        if (part.fieldname === 'package' && packagePart === null) {
          if (manifest === null) {
            // package 先于 manifest（或压根没有 manifest）→ 400。
            // 当前 part 已被迭代器交出但未消费——必须在这里排干：否则 busboy 停在当前
            // part → 请求体读不完 → 连接悬挂（与 409 预查早答路径同构）。
            if (part.type === 'file') void discardPartFile(part.file);
            manifestError = 'invalid_manifest';
            break;
          }
          packagePart = part;
          break; // 交给 saveBlobStream 流式收包
        }
        if (part.type === 'file') void discardPartFile(part.file); // 未知 part：丢弃
      }
    } catch {
      return fail(400, 'invalid_manifest'); // multipart 形态损坏
    }

    if (manifestError !== null) return fail(400, manifestError);
    if (manifest === null) return fail(400, 'invalid_manifest');
    if (packagePart === null) return fail(400, 'invalid_manifest');
    // package 是字段 part 且值非字符串（插件预解析成对象）→ 形态非法 → 400
    // （杜绝 "[object Object]" 当 blob 落盘）。
    if (packagePart.type !== 'file' && (packagePart.valueTruncated || typeof packagePart.value !== 'string')) {
      return fail(400, 'invalid_manifest');
    }

    // ④ 版本不可变预查（C4 快路径）：已有同 slug+version → 409
    const dup = db.prepare('SELECT 1 FROM resource_versions WHERE slug = ? AND version = ?')
      .get(manifest.slug, manifest.version);
    if (dup !== undefined) return fail(409, 'version_exists');

    // ⑤ 流式收包：服务端自算 sha256（C6）；超限中断并清临时件（C5）
    //    包装层此刻才建：早答路径（409/400）拿到的是未包装的裸 part 流，由 fail() 排干。
    const packageSource = packageStream(packagePart);
    if (packageSource === null) return fail(400, 'invalid_manifest'); // 防御：形态已在上方校验
    packageHandedOff = true; // 包装层在管道关闭时负责排干底层 part 流
    let checksum: string;
    let sizeBytes: number;
    try {
      const saved = await saveBlobStream(packageSource, maxBytes, dataDir);
      checksum = saved.checksum;
      sizeBytes = saved.sizeBytes;
    } catch (err) {
      if (err instanceof BlobTooLargeError) return fail(413, 'too_large');
      throw err;
    }

    // ⑥ 事务落库：resources upsert latest_version + resource_versions 插入。
    //    唯一键 (slug, version) 冲突 → 事务回滚 → 409（R-M1-4 并发窗口兜底，错误体同款）。
    const publishedAt = Date.now();
    const upsertResource = db.prepare(`
      INSERT INTO resources (slug, type, name, author, description, readme, tags_json, category, icon_emoji, latest_version)
      VALUES (@slug, @type, @name, @author, @description, @readme, @tags_json, @category, @icon_emoji, @latest_version)
      ON CONFLICT(slug) DO UPDATE SET
        type = excluded.type,
        name = excluded.name,
        author = excluded.author,
        description = excluded.description,
        readme = excluded.readme,
        tags_json = excluded.tags_json,
        category = excluded.category,
        icon_emoji = excluded.icon_emoji,
        latest_version = excluded.latest_version`);
    const insertVersion = db.prepare(`
      INSERT INTO resource_versions (slug, version, checksum, size_bytes, published_by, published_at)
      VALUES (?, ?, ?, ?, ?, ?)`);
    const tx = db.transaction(() => {
      upsertResource.run({
        slug: manifest.slug,
        type: manifest.type,
        name: manifest.name,
        author: node.name, // 信任根在 hub（R-M1-8）：author = 鉴权节点名，manifest.author 丢弃
        description: manifest.description,
        readme: manifest.readme,
        tags_json: JSON.stringify(manifest.tags),
        category: manifest.category,
        icon_emoji: manifest.iconEmoji,
        latest_version: manifest.version,
      });
      insertVersion.run(manifest.slug, manifest.version, checksum, sizeBytes, node.name, publishedAt);
    });
    try {
      tx();
    } catch (err) {
      // 唯一键兜底 409：事务已回滚，但尾随 part 仍需排干（否则连接悬挂）
      if (isUniqueViolation(err)) {
        reply.code(409).send({ error: 'version_exists' });
        void drainParts();
        return reply;
      }
      throw err;
    }

    // ⑦ 发布事件广播（spec §3.3/§5）：入库成功后广播全部在线节点 →
    //    studio 侧 HubManager 收到 resource.published → 资源库更新角标。
    //    广播失败不得推翻已提交的发布（DB 已落库）——只记日志。
    try {
      opts.registry.broadcast(EVT.resourcePublished, {
        slug: manifest.slug,
        type: manifest.type,
        name: manifest.name,
        version: manifest.version,
        author: node.name,
        at: publishedAt,
      } satisfies ResourcePublishedEvent);
    } catch (err) {
      logger.warn('发布事件广播失败（发布已入库）', { slug: manifest.slug, err });
    }

    // ⑧ 201：回执含服务端自算的校验和与字节数
    const res = reply.code(201).send({
      slug: manifest.slug,
      version: manifest.version,
      checksum,
      sizeBytes,
    });
    // 尾随 part 排干：package 之后若还有 part，未消费会让 busboy 背压停读 → 连接悬挂
    void drainParts();
    return res;
  });
}
