// 内容寻址 blob 存储（spec 2026-10-24 M1 §4）：{dataDir}/blobs/{sha256}.tar.gz。
// 流式 sha256 + 临时件转正；超限/中断必清临时件，绝不留半个 blob（Review Focus #1）；
// 同内容重传天然幂等去重；孤儿 blob 不回收（M1 无删除，YAGNI）。
import { createHash, randomUUID } from 'node:crypto';
import { createWriteStream, existsSync, mkdirSync, renameSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { SHA256_HEX_PATTERN } from 'momo-hub-protocol';

/** 上传体超过 maxBytes（publish 端点映射 413）。 */
export class BlobTooLargeError extends Error {
  constructor(maxBytes: number) {
    super(`blob exceeds limit: ${maxBytes} bytes`);
    this.name = 'BlobTooLargeError';
  }
}

/** blob 最终路径合成（存在性由调用方查）；checksum 必须过 S1——目录逃逸防线。 */
export function openBlob(dataDir: string, checksum: string): string {
  if (!SHA256_HEX_PATTERN.test(checksum)) throw new Error(`invalid checksum: ${checksum}`);
  return path.join(dataDir, 'blobs', `${checksum}.tar.gz`);
}

/** 流式落盘：边收边算 sha256，全部收完且 ≤maxBytes 才 rename 转正（C6）；
 *  已存在同 checksum blob（内容寻址即同内容）则删临时件幂等返回。 */
export async function saveBlobStream(
  source: NodeJS.ReadableStream,
  maxBytes: number,
  dataDir: string,
): Promise<{ checksum: string; sizeBytes: number }> {
  const blobsDir = path.join(dataDir, 'blobs');
  mkdirSync(blobsDir, { recursive: true });
  const tmpPath = path.join(blobsDir, `.tmp-${randomUUID()}`);
  const hash = createHash('sha256');
  let size = 0;
  const out = createWriteStream(tmpPath);
  // close 监听必须在 pipeline 之前挂上：WriteStream 懒打开——超限/中断可能在
  // open 完成前抛出，须等句柄落地后删临时件，否则 open 回调会把文件再建回来。
  const closed = new Promise<void>((resolve) => out.once('close', () => resolve()));
  try {
    await pipeline(
      source,
      async function* (chunks: AsyncIterable<string | Buffer>): AsyncGenerator<Buffer> {
        for await (const chunk of chunks) {
          const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
          size += buf.length;
          if (size > maxBytes) throw new BlobTooLargeError(maxBytes);
          hash.update(buf);
          yield buf;
        }
      },
      out,
    );
    const checksum = hash.digest('hex');
    const finalPath = openBlob(dataDir, checksum);
    if (existsSync(finalPath)) {
      unlinkSync(tmpPath); // 内容寻址：同名即同内容，幂等去重
    } else {
      renameSync(tmpPath, finalPath);
    }
    return { checksum, sizeBytes: size };
  } catch (err) {
    out.destroy();
    await closed;
    try {
      unlinkSync(tmpPath);
    } catch {
      // 临时件可能从未成功创建，忽略 ENOENT
    }
    throw err;
  }
}
