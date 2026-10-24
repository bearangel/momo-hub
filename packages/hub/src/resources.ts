// hub 资源发布端点（spec 2026-10-24 M1 §3.1/§3.4/§4 + 裁定 C4/C5/C6/R-M1-4/R-M1-5）——
// multipart 两 part：`manifest`（JSON）+ `package`（binary 流）。
// 信任根在 hub：校验和服务端流式自算（客户端只传内容，C6）；版本不可变：同 slug+version → 409（C4）。
// 409 双层（R-M1-4）：预查给明确错误体（快路径），并发窗口撞唯一键由事务回滚兜底——错误体同款。
import multipart, { type Multipart } from '@fastify/multipart';
import type { FastifyInstance, FastifyReply } from 'fastify';
import type Database from 'better-sqlite3';
import { PassThrough, Readable } from 'node:stream';
import {
  isValidHubSlug, isValidHubVersion, validateHubManifest, type HubPublishManifest,
} from 'momo-hub-protocol';
import { BlobTooLargeError, saveBlobStream } from './blobs';
import { verifyDeviceToken } from './auth';
import type { NodeRegistry } from './registry';

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
  /** 在线节点注册表——T7 发布事件广播用（本任务注入不消费）。 */
  registry: NodeRegistry;
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

/** 读 manifest part（有界内存）：字段 part 取值，文件 part 逐块读，超限返回 null。 */
async function readManifestPart(part: Multipart): Promise<string | null> {
  if (part.type === 'field') {
    // 字段 part 若声明 Content-Type: application/json，插件已把它解析成对象——回写成 JSON 文本再走统一解析。
    const raw: unknown = part.value;
    const value = typeof raw === 'string'
      ? raw
      : (raw === null || raw === undefined ? '' : JSON.stringify(raw));
    return Buffer.byteLength(value, 'utf8') > MANIFEST_MAX_BYTES ? null : value;
  }
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of part.file) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string);
    size += buf.length;
    if (size > MANIFEST_MAX_BYTES) return null;
    chunks.push(buf);
  }
  return Buffer.concat(chunks).toString('utf8');
}

/** package part 的字节流。
 *  文件 part 走一层 PassThrough 包装：saveBlobStream 超限时管道会销毁源流——
 *  直接把 part.file 交出去，它会被销毁 → busboy 停读（背压）→ 请求体收不完 →
 *  socket 悬挂（连接不可复用、服务关闭挂起）。包装层在管道关闭时把底层 part 流继续
 *  drain 掉，保证请求能收尾、超限内容不落盘。 */
function packageStream(part: Multipart): NodeJS.ReadableStream {
  if (part.type !== 'file') {
    const value = typeof part.value === 'string' ? part.value : String(part.value ?? '');
    return Readable.from([Buffer.from(value, 'utf8')]);
  }
  const file = part.file;
  const wrapper = new PassThrough();
  const drain = (): void => { file.resume(); };
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

export function registerResourceRoutes(app: FastifyInstance, opts: ResourceRouteOpts): void {
  const maxBytes = opts.maxBytes ?? PUBLISH_MAX_BYTES;
  // multipart 插件挂在本作用域（fastify-plugin 不再封装 → 内容类型解析器与 req.parts 装饰对子作用域生效）。
  // fileSize 显式 Infinity：真实上限以实际落盘字节为准（saveBlobStream 手动流控，C5）——
  // 插件默认取 fastify bodyLimit（1MB），会把大包静默截断。
  void app.register(multipart, {
    limits: { fileSize: Infinity, files: 8, fields: 8, parts: 16 },
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

    // 早答的统一出口：回错误体后把剩余 part 消费干净。
    // multipart 解析一开始，原始流就被 busboy 接管（pipe）——node 不再自动 drain；
    // 未被消费的 part 流会让 busboy 背压停读 → 请求体收不完 → socket 悬挂
    // （连接不可复用、服务关闭挂起）。内容早已丢弃，磁盘零残留。
    const drainParts = async (): Promise<void> => {
      try {
        for (;;) {
          const next = await iterator.next();
          if (next.done) break;
          if (next.value.type === 'file') next.value.file.resume();
        }
      } catch {
        // 形态损坏：剩余字节无处可去，忽略（响应已发出）
      }
    };
    const fail = (status: number, code: PublishErrorCode): FastifyReply => {
      const res = reply.code(status).send({ error: code });
      void drainParts();
      return res;
    };

    // ③ 收 part：manifest 必须先于 package（契约：manifest 在前）——
    //    manifest 不合法 → 400；package 缺失 → 400；multipart 解析炸 → 400。
    let manifest: HubPublishManifest | null = null;
    let manifestError: PublishErrorCode | null = null;
    let packagePart: Multipart | null = null;
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
          if (manifest === null) { manifestError = 'invalid_manifest'; break; }
          packagePart = part;
          break; // 交给 saveBlobStream 流式收包
        }
        if (part.type === 'file') part.file.resume(); // 未知 part：丢弃
      }
    } catch {
      return fail(400, 'invalid_manifest'); // multipart 形态损坏
    }

    if (manifestError !== null) return fail(400, manifestError);
    if (manifest === null) return fail(400, 'invalid_manifest');
    if (packagePart === null) return fail(400, 'invalid_manifest');

    // ④ 版本不可变预查（C4 快路径）：已有同 slug+version → 409
    const dup = db.prepare('SELECT 1 FROM resource_versions WHERE slug = ? AND version = ?')
      .get(manifest.slug, manifest.version);
    if (dup !== undefined) return fail(409, 'version_exists');

    // ⑤ 流式收包：服务端自算 sha256（C6）；超限中断并清临时件（C5）
    let checksum: string;
    let sizeBytes: number;
    try {
      const saved = await saveBlobStream(packageStream(packagePart), maxBytes, dataDir);
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
        author: manifest.author,
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
      if (isUniqueViolation(err)) return reply.code(409).send({ error: 'version_exists' });
      throw err;
    }

    // ⑦ 201：回执含服务端自算的校验和与字节数
    return reply.code(201).send({
      slug: manifest.slug,
      version: manifest.version,
      checksum,
      sizeBytes,
    });
  });
}
