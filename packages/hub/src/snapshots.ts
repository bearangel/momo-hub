// hub 工作空间快照存档（M2c spec 2026-10-24 §3.1/§4 + T5 + RM2-6）——
// POST /workspaces/snapshots（数据面）+ GET /workspaces/snapshots/:snapshotId/download
// （RM2-6，T8 目标节点应用器取包数据面）+ workspace.snapshots RPC（列表，无组校验）。
// 信任根与 M1 发布同源：multipart manifest+payload 两 part、服务端流式自算 checksum（C6）、
// 同 workspaceName+version 不可变 → 409 双层（C4：预查 + 唯一键兜底）、超限 413 零残留。
// 任何已注册节点可存档自己的快照——组校验只约束镜像动作（T6 mirror 才 leader-only）。
import type { FastifyInstance, FastifyReply } from 'fastify';
import type Database from 'better-sqlite3';
import type { Multipart } from '@fastify/multipart';
import { randomUUID } from 'node:crypto';
import { createReadStream, statSync } from 'node:fs';
import { validateSnapshotManifest, type SnapshotManifest } from 'momo-hub-protocol';
import { BlobTooLargeError, openBlob, saveBlobStream } from './blobs';
import { verifyDeviceToken } from './auth';
import type { GroupHandlerContext, RpcOutcome } from './groups';
import {
  PUBLISH_MAX_BYTES, collectManifestAndPayload, discardPartFile, isUniqueViolation, packageStream,
} from './resources';

/** 快照体上限（spec §3.1：tar.gz ≤200MB，与 M1 发布 C5 同值）。 */
export const SNAPSHOT_MAX_BYTES = PUBLISH_MAX_BYTES;

/** 快照 blob 子目录（spec §4：{dataDir}/ws-snapshots/{sha256}.tar.gz——与 M1 blobs 目录隔离）。 */
const SNAPSHOT_SUBDIR = 'ws-snapshots';

/** snapshotId 形态（上传侧 randomUUID 产出的标准 UUID）——下载参数早拒用。 */
const SNAPSHOT_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface SnapshotRouteOpts {
  db: Database.Database;
  dataDir: string;
  /** 快照体上限（默认 SNAPSHOT_MAX_BYTES；测试注入小值走同一代码路径）。 */
  maxBytes?: number;
}

export function registerSnapshotRoutes(app: FastifyInstance, opts: SnapshotRouteOpts): void {
  const maxBytes = opts.maxBytes ?? SNAPSHOT_MAX_BYTES;

  // multipart 插件由 registerResourceRoutes 在同一 instance 上注册（fastify-plugin
  // 无封装边界，装饰与 content-type parser 对同域路由生效）——这里不重复注册
  // （重复注册会因装饰冲突炸启动）。limits 同源：fileSize Infinity（真实上限走
  // saveBlobStream 流控）、fieldSize=manifest 上限。
  app.post('/workspaces/snapshots', async (req, reply) => {
    const { db, dataDir } = opts;

    // ① 鉴权：device token Bearer（与 /ws 升级、资源发布同源校验）
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    if (!node) return reply.code(401).send({ error: 'unauthorized' });

    // ② multipart 形态：非 multipart 直接拒（鉴权失败/形态不对时原始流未被 busboy
    //    接管，node 自动 drain 请求体——直接回错误体即可）
    if (!req.isMultipart()) return reply.code(400).send({ error: 'invalid_manifest' });

    const iterator = req.parts();

    // ③ 收 part（共享收集器——M1 publish 同款 drain 纪律）：manifest 在前、payload 在后
    let payloadPart: Multipart | null = null;
    let payloadHandedOff = false;
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
    const fail = (status: number, code: 'invalid_manifest' | 'version_exists' | 'too_large'): FastifyReply => {
      // 已取出但未交给 saveBlobStream 的 payload part（文件形态）必须排干——
      // 弃流会背压停读 → 请求体收不完 → 连接悬挂（M1 同款教训）。
      if (!payloadHandedOff && payloadPart !== null && payloadPart.type === 'file') {
        void discardPartFile(payloadPart.file);
      }
      const res = reply.code(status).send({ error: code });
      void drainParts();
      return res;
    };

    const collected = await collectManifestAndPayload(iterator, 'manifest', 'payload');
    if (collected === null) return fail(400, 'invalid_manifest'); // multipart 形态损坏
    payloadPart = collected.payloadPart;
    if (collected.payloadBeforeManifest || collected.manifestText === null || payloadPart === null) {
      return fail(400, 'invalid_manifest');
    }
    // payload 是字段 part 且值非字符串（插件预解析成对象）→ 形态非法
    if (payloadPart.type !== 'file' && (payloadPart.valueTruncated || typeof payloadPart.value !== 'string')) {
      return fail(400, 'invalid_manifest');
    }

    // ④ manifest 结构校验（protocol validateSnapshotManifest——S1 同款纪律）
    let manifest: SnapshotManifest;
    try {
      const parsed = validateSnapshotManifest(JSON.parse(collected.manifestText));
      if (parsed === null) return fail(400, 'invalid_manifest');
      manifest = parsed;
    } catch {
      return fail(400, 'invalid_manifest');
    }

    // ⑤ 重复预查（C4 快路径）：同 workspaceName+version 已存档 → 409
    const dup = db.prepare('SELECT 1 FROM workspace_snapshots_meta WHERE workspace_name = ? AND version = ?')
      .get(manifest.workspaceName, manifest.version);
    if (dup !== undefined) return fail(409, 'version_exists');

    // ⑥ 流式收包：服务端自算 sha256（C6）；超限中断并清临时件；落 ws-snapshots/ 子目录
    const payloadSource = packageStream(payloadPart);
    if (payloadSource === null) return fail(400, 'invalid_manifest'); // 防御：形态已在上方校验
    payloadHandedOff = true; // 包装层在管道关闭时负责排干底层 part 流
    let checksum: string;
    let sizeBytes: number;
    try {
      const saved = await saveBlobStream(payloadSource, maxBytes, dataDir, SNAPSHOT_SUBDIR);
      checksum = saved.checksum;
      sizeBytes = saved.sizeBytes;
    } catch (err) {
      if (err instanceof BlobTooLargeError) return fail(413, 'too_large');
      throw err;
    }

    // ⑦ 落库：唯一键 (workspace_name, version) 冲突 → 409（并发窗口兜底，错误体同款）
    const snapshotId = randomUUID();
    try {
      db.prepare(`INSERT INTO workspace_snapshots_meta
                  (snapshot_id, workspace_name, version, by_node, checksum, size_bytes, created_at)
                  VALUES (?, ?, ?, ?, ?, ?, ?)`)
        .run(snapshotId, manifest.workspaceName, manifest.version, node.name, checksum, sizeBytes, Date.now());
    } catch (err) {
      if (isUniqueViolation(err)) {
        reply.code(409).send({ error: 'version_exists' });
        void drainParts();
        return reply;
      }
      throw err;
    }

    // ⑧ 201：回执含 snapshotId 与服务端自算校验和（SnapshotUploadRes）
    const res = reply.code(201).send({ snapshotId, checksum });
    void drainParts(); // 尾随 part 排干（201 后仍有 part 时防连接悬挂）
    return res;
  });

  // 快照下载（RM2-6，spec §3.1）：Bearer 鉴权 → 参数早拒 → meta 行 → blob 流 + 校验和头。
  // 与 M1 资源下载端点同构（/resources/:slug/:version/download）——目标节点应用器
  // （T8 mirror.apply）经此数据面取 tar.gz，checksum 三方对账：响应头 == meta 落库值
  // == 客户端流式复算值。任何已注册节点可下载（与上传/列表同口径，无组校验）。
  app.get('/workspaces/snapshots/:snapshotId/download', async (req, reply) => {
    const { db, dataDir } = opts;

    // ① 鉴权：device token Bearer（与上传、/ws 升级同源校验）
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    if (!node) return reply.code(401).send({ error: 'unauthorized' });

    // ② 参数早拒：snapshotId 必须是 UUID 形态（上传侧全部来自 randomUUID）——
    //    不合法与未知同款 404，不向外区分「参数非法」与「不存在」（M1 同款纪律）。
    const params = req.params as { snapshotId?: string };
    const snapshotId = typeof params.snapshotId === 'string' ? params.snapshotId : '';
    if (!SNAPSHOT_ID_PATTERN.test(snapshotId)) {
      return reply.code(404).send({ error: 'not_found' });
    }

    const row = db.prepare('SELECT checksum, size_bytes FROM workspace_snapshots_meta WHERE snapshot_id = ?')
      .get(snapshotId) as { checksum: string; size_bytes: number } | undefined;
    if (!row) return reply.code(404).send({ error: 'not_found' });

    // ③ blob 文件名即 sha256（内容寻址，ws-snapshots 子目录）：checksum 非法或文件缺失 → 404
    let blobPath: string;
    try {
      blobPath = openBlob(dataDir, row.checksum, SNAPSHOT_SUBDIR);
    } catch {
      return reply.code(404).send({ error: 'not_found' });
    }
    let sizeBytes: number;
    try {
      sizeBytes = statSync(blobPath).size;
    } catch {
      return reply.code(404).send({ error: 'not_found' });
    }

    // ④ 流式回包：X-Checksum-Sha256 = meta 落库值（== 上传时服务端自算值，C6 延续）
    reply.header('X-Checksum-Sha256', row.checksum);
    reply.header('Content-Type', 'application/gzip');
    reply.header('Content-Length', String(sizeBytes));
    return reply.send(createReadStream(blobPath));
  });
}

// ────────────────────────────────────────────────────────────────────────────
// workspace.snapshots RPC（spec §3.1：hub 存档目录列表——无组校验，任何注册节点可调）
// ────────────────────────────────────────────────────────────────────────────

interface SnapshotMetaRow {
  snapshot_id: string;
  workspace_name: string;
  version: string;
  by_node: string;
  created_at: number;
}

/** workspace.snapshots handler：按 workspaceName 可选过滤，新在前（created_at 倒序，
 *  同毫秒 rowid 兜底——与资源目录排序纪律一致）。 */
export function handleWorkspaceSnapshots(_ctx: GroupHandlerContext, payload: unknown): RpcOutcome {
  const raw = typeof payload === 'object' && payload !== null && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)['workspaceName']
    : undefined;
  // 过滤值非字符串 → 忽略过滤（宽松归一，不 400——列表查询无安全面）
  const workspaceName = typeof raw === 'string' && raw.length > 0 ? raw : null;
  const rows = workspaceName !== null
    ? dbRows(_ctx.db, 'WHERE workspace_name = ?', [workspaceName])
    : dbRows(_ctx.db, '', []);
  return {
    ok: true,
    payload: {
      snapshots: rows.map((r) => ({
        snapshotId: r.snapshot_id,
        workspaceName: r.workspace_name,
        version: r.version,
        byNode: r.by_node,
        at: r.created_at,
      })),
    },
  };
}

function dbRows(db: Database.Database, where: string, params: string[]): SnapshotMetaRow[] {
  return db.prepare(
    `SELECT snapshot_id, workspace_name, version, by_node, created_at
     FROM workspace_snapshots_meta ${where}
     ORDER BY created_at DESC, rowid DESC`,
  ).all(...params) as SnapshotMetaRow[];
}
