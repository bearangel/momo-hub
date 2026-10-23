// SQLite 打开 + 内联 SQL 迁移（momo-studio 纪律：迁移是 TS 常量不是 .sql 文件，
// 保证 tsc 产物自包含）。
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

/** 迁移序列：下标 +1 = schema_version。已应用迁移不可变，只追加。 */
const MIGRATIONS: readonly string[] = [
  // v1
  `CREATE TABLE IF NOT EXISTS hub_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS nodes (
     id TEXT PRIMARY KEY,
     name TEXT NOT NULL UNIQUE,
     token_hash TEXT NOT NULL UNIQUE,
     enrolled_at INTEGER NOT NULL,
     last_seen_at INTEGER NOT NULL
   )`,
];

export function openHubDatabase(dataDir: string): Database.Database {
  mkdirSync(dataDir, { recursive: true });
  const db = new Database(path.join(dataDir, 'hub.db'));
  db.pragma('journal_mode = WAL');
  db.exec('BEGIN');
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS hub_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
    const row = db.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string } | undefined;
    const current = row ? Number(row.value) : 0;
    for (let v = current; v < MIGRATIONS.length; v++) {
      db.exec(MIGRATIONS[v]!);
    }
    if (current < MIGRATIONS.length) {
      db.prepare(`INSERT INTO hub_meta (key, value) VALUES ('schema_version', ?)
                  ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
        .run(String(MIGRATIONS.length));
    }
    db.exec('COMMIT');
    return db;
  } catch (err) {
    db.exec('ROLLBACK');
    db.close();
    throw err;
  }
}
