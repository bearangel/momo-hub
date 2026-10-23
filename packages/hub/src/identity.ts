// hub 身份：hub_id（稳定标识）+ enrollment_token（信任根，可轮换）。
import { randomBytes, randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';

export interface HubIdentity { hubId: string; enrollmentToken: string }

function readMeta(db: Database.Database, key: string): string | null {
  const row = db.prepare('SELECT value FROM hub_meta WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value ?? null;
}

function writeMeta(db: Database.Database, key: string, value: string): void {
  db.prepare(`INSERT INTO hub_meta (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(key, value);
}

/** 幂等：首次生成持久化，之后原样返回。 */
export function ensureHubIdentity(db: Database.Database): HubIdentity {
  let hubId = readMeta(db, 'hub_id');
  if (!hubId) { hubId = randomUUID(); writeMeta(db, 'hub_id', hubId); }
  let token = readMeta(db, 'enrollment_token');
  if (!token) { token = randomBytes(24).toString('base64url'); writeMeta(db, 'enrollment_token', token); }
  return { hubId, enrollmentToken: token };
}

/** 轮换加入凭证（泄漏处置用）。 */
export function rotateEnrollmentToken(db: Database.Database): string {
  const next = randomBytes(24).toString('base64url');
  writeMeta(db, 'enrollment_token', next);
  return next;
}
