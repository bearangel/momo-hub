// 鉴权语义锁：时序安全比较、轮换语义、哈希落库、名字校验。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode, verifyDeviceToken } from '../src/auth';
import { timingSafeEqualStr } from '../src/timing';

function freshDb() {
  const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-auth-')));
  const identity = ensureHubIdentity(db);
  return { db, identity };
}

describe('timingSafeEqualStr', () => {
  it('等长相等 true；不等长 false；不等内容 false', () => {
    expect(timingSafeEqualStr('aaaa', 'aaaa')).toBe(true);
    expect(timingSafeEqualStr('aaaa', 'aaa')).toBe(false);
    expect(timingSafeEqualStr('aaaa', 'aaab')).toBe(false);
  });
});

describe('enrollNode / verifyDeviceToken', () => {
  it('错 enrollment token 拒绝', () => {
    const { db } = freshDb();
    expect(enrollNode(db, 'wrong-token', 'mac-main')).toEqual({ error: 'invalid_enrollment' });
    db.close();
  });

  it('空名/超长名/纯空白名拒绝', () => {
    const { db, identity } = freshDb();
    expect(enrollNode(db, identity.enrollmentToken, '')).toEqual({ error: 'invalid_name' });
    expect(enrollNode(db, identity.enrollmentToken, '   ')).toEqual({ error: 'invalid_name' });
    expect(enrollNode(db, identity.enrollmentToken, 'x'.repeat(65))).toEqual({ error: 'invalid_name' });
    db.close();
  });

  it('enroll → verify 闭环；DB 只存哈希不存明文', () => {
    const { db, identity } = freshDb();
    const r = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in r)) throw new Error('unreachable');
    expect(verifyDeviceToken(db, r.deviceToken)).toEqual({ nodeId: r.nodeId, name: 'mac-main' });
    const row = db.prepare('SELECT token_hash FROM nodes WHERE id = ?').get(r.nodeId) as { token_hash: string };
    expect(row.token_hash).not.toBe(r.deviceToken);
    db.close();
  });

  it('同名重 enroll = 轮换：nodeId 复用、旧 token 失效、新 token 有效、节点行不膨胀', () => {
    const { db, identity } = freshDb();
    const first = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in first)) throw new Error('unreachable');
    const second = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in second)) throw new Error('unreachable');
    expect(second.nodeId).toBe(first.nodeId);
    expect(second.deviceToken).not.toBe(first.deviceToken);
    expect(verifyDeviceToken(db, first.deviceToken)).toBeNull();
    expect(verifyDeviceToken(db, second.deviceToken)?.nodeId).toBe(first.nodeId);
    const count = db.prepare('SELECT COUNT(*) AS n FROM nodes').get() as { n: number };
    expect(count.n).toBe(1);
    db.close();
  });

  it('未知 token 查询返回 null', () => {
    const { db } = freshDb();
    expect(verifyDeviceToken(db, 'nope')).toBeNull();
    db.close();
  });
});
