// hub 身份与迁移：首次生成、重开稳定、可轮换、唯一约束生效。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, rotateEnrollmentToken } from '../src/identity';

describe('hub 身份', () => {
  it('首次初始化生成 hubId 与 enrollment token；重开同目录保持稳定', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    const a = ensureHubIdentity(db);
    expect(a.hubId).toMatch(/^[0-9a-f-]{36}$/);
    expect(a.enrollmentToken.length).toBeGreaterThanOrEqual(32);
    db.close();

    const db2 = openHubDatabase(dir);
    expect(ensureHubIdentity(db2)).toEqual(a);
    db2.close();
  });

  it('轮换 enrollment token 只换 token 不换 hubId', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    const before = ensureHubIdentity(db);
    const next = rotateEnrollmentToken(db);
    expect(next).not.toBe(before.enrollmentToken);
    expect(ensureHubIdentity(db)).toEqual({ hubId: before.hubId, enrollmentToken: next });
    db.close();
  });

  it('nodes 表 token_hash 唯一约束生效', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    db.prepare("INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES ('n1','a','h1',0,0)").run();
    expect(() =>
      db.prepare("INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES ('n2','b','h1',0,0)").run(),
    ).toThrow();
    db.close();
  });
});
