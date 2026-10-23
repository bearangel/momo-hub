// 节点鉴权：enrollment（信任根换 device token）+ device token 校验（sha256 落库）。
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import { timingSafeEqualStr } from './timing';

export type EnrollResult =
  | { nodeId: string; deviceToken: string }
  | { error: 'invalid_enrollment' }
  | { error: 'invalid_name' };

function sha256(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

/** 节点名规范：trim 后 1..64 字符。 */
function validName(name: string): string | null {
  const n = name.trim();
  return n.length >= 1 && n.length <= 64 ? n : null;
}

export function enrollNode(db: Database.Database, enrollmentToken: string, nodeName: string): EnrollResult {
  const name = validName(nodeName);
  if (!name) return { error: 'invalid_name' };
  const row = db.prepare(`SELECT value FROM hub_meta WHERE key = 'enrollment_token'`).get() as { value: string } | undefined;
  if (!row || !timingSafeEqualStr(enrollmentToken, row.value)) {
    return { error: 'invalid_enrollment' };
  }
  const deviceToken = randomBytes(32).toString('base64url');
  const tokenHash = sha256(deviceToken);
  const existing = db.prepare('SELECT id FROM nodes WHERE name = ?').get(name) as { id: string } | undefined;
  const now = Date.now();
  if (existing) {
    // 轮换语义：复用 nodeId，替换 token（旧凭证立即失效）
    db.prepare('UPDATE nodes SET token_hash = ?, last_seen_at = ? WHERE id = ?').run(tokenHash, now, existing.id);
    return { nodeId: existing.id, deviceToken };
  }
  const nodeId = randomUUID();
  db.prepare('INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES (?, ?, ?, ?, ?)')
    .run(nodeId, name, tokenHash, now, now);
  return { nodeId, deviceToken };
}

export function verifyDeviceToken(db: Database.Database, deviceToken: string): { nodeId: string; name: string } | null {
  const row = db.prepare('SELECT id, name FROM nodes WHERE token_hash = ?').get(sha256(deviceToken)) as
    | { id: string; name: string }
    | undefined;
  if (!row) return null;
  db.prepare('UPDATE nodes SET last_seen_at = ? WHERE id = ?').run(Date.now(), row.id);
  return { nodeId: row.id, name: row.name };
}
