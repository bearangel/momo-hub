// blobs 内容寻址存储测试（spec 2026-10-24 M1 §4）：
// 流式落盘 + sha256 手算对照 / 幂等去重 / 超限与源中断的临时件清理 /
// openBlob 路径参数防御 / 迁移 v2 两表与幂等回归。
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { BlobTooLargeError, openBlob, saveBlobStream } from '../src/blobs';
import { openHubDatabase } from '../src/db';

function tmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'momo-hub-blobs-'));
}

function tmpFiles(dir: string): string[] {
  return readdirSync(join(dir, 'blobs'));
}

describe('saveBlobStream', () => {
  it('正常流：落盘内容与 checksum 与 node:crypto 手算一致，无残留临时件', async () => {
    const dir = tmpDir();
    const content = Buffer.from('fake tar.gz payload '.repeat(1000), 'utf8');
    const r = await saveBlobStream(Readable.from([content]), 1024 * 1024, dir);
    expect(r.sizeBytes).toBe(content.length);
    expect(r.checksum).toBe(createHash('sha256').update(content).digest('hex'));
    const finalPath = join(dir, 'blobs', `${r.checksum}.tar.gz`);
    expect(existsSync(finalPath)).toBe(true);
    expect(readFileSync(finalPath).equals(content)).toBe(true);
    expect(tmpFiles(dir).every((f) => !f.startsWith('.tmp-'))).toBe(true);
  });

  it('同内容重存：幂等去重，blob 只有一份', async () => {
    const dir = tmpDir();
    const content = Buffer.from('same-bytes', 'utf8');
    const a = await saveBlobStream(Readable.from([content]), 1024, dir);
    const b = await saveBlobStream(Readable.from([content]), 1024, dir);
    expect(b.checksum).toBe(a.checksum);
    expect(b.sizeBytes).toBe(a.sizeBytes);
    expect(tmpFiles(dir)).toEqual([`${a.checksum}.tar.gz`]);
  });

  it('超限：抛 BlobTooLargeError，临时件与半成品目标均被清理', async () => {
    const dir = tmpDir();
    const chunk = Buffer.alloc(100, 0x61);
    await expect(
      saveBlobStream(Readable.from([chunk, chunk, chunk]), 150, dir),
    ).rejects.toBeInstanceOf(BlobTooLargeError);
    expect(tmpFiles(dir).filter((f) => f.startsWith('.tmp-'))).toHaveLength(0);
    expect(tmpFiles(dir).filter((f) => f.endsWith('.tar.gz'))).toHaveLength(0);
  });

  it('源中断（中途 error）：清理临时件并向上抛错', async () => {
    const dir = tmpDir();
    const src = new Readable({
      read() {
        this.push(Buffer.from('half', 'utf8'));
        this.destroy(new Error('stream aborted'));
      },
    });
    await expect(saveBlobStream(src, 1024, dir)).rejects.toThrow('stream aborted');
    expect(tmpFiles(dir).filter((f) => f.startsWith('.tmp-'))).toHaveLength(0);
    expect(tmpFiles(dir).filter((f) => f.endsWith('.tar.gz'))).toHaveLength(0);
  });
});

describe('openBlob', () => {
  it('合法 checksum 合成 blobs/{sha256}.tar.gz 路径（存在性由调用方查）', () => {
    const c = 'a'.repeat(64);
    expect(openBlob('/data', c)).toBe(join('/data', 'blobs', `${c}.tar.gz`));
  });

  it('坏 checksum 路径参数拒绝（目录逃逸 / 非 hex / 长度不足）', () => {
    expect(() => openBlob('/data', '../../etc/passwd')).toThrow();
    expect(() => openBlob('/data', '..')).toThrow();
    expect(() => openBlob('/data', 'zz'.repeat(32))).toThrow(); // 64 位但非 hex
    expect(() => openBlob('/data', 'a'.repeat(63))).toThrow(); // 长度不足
    expect(() => openBlob('/data', '')).toThrow();
  });
});

describe('迁移 v2（spec M1 §4 两表，逐字 SQL）', () => {
  it('resources / resource_versions 两表可用且 schema_version 推进；重开幂等', () => {
    const dir = tmpDir();
    const db = openHubDatabase(dir);
    // v1 即两条语句（hub_meta/nodes），追加 v2 两表后 MIGRATIONS.length = 4
    expect(db.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '4' });
    db.prepare(
      "INSERT INTO resources (slug, type, name, author, latest_version) VALUES ('coder-pro', 'agent', 'Coder Pro', 'ws-1', '1.2.3')",
    ).run();
    db.prepare(
      "INSERT INTO resource_versions (slug, version, checksum, size_bytes, published_by, published_at) VALUES ('coder-pro', '1.2.3', 'c', 1, 'ws-1', 0)",
    ).run();
    db.close();

    const db2 = openHubDatabase(dir); // 迁移幂等：重开不重复执行、不丢数据
    expect(db2.prepare("SELECT value FROM hub_meta WHERE key = 'schema_version'").get()).toEqual({ value: '4' });
    expect(db2.prepare('SELECT COUNT(*) AS n FROM resources').get()).toEqual({ n: 1 });
    expect(db2.prepare('SELECT COUNT(*) AS n FROM resource_versions').get()).toEqual({ n: 1 });
    db2.close();
  });
});
