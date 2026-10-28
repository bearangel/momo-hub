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
  // v2（M1 资源分发，spec 2026-10-24 §4 SQL 逐字）
  `CREATE TABLE IF NOT EXISTS resources (
     slug TEXT PRIMARY KEY,
     type TEXT NOT NULL,            -- 'agent'|'mcp'|'skill'|'plugin'
     name TEXT NOT NULL,
     author TEXT NOT NULL,
     description TEXT NOT NULL DEFAULT '',
     readme TEXT NOT NULL DEFAULT '',
     tags_json TEXT NOT NULL DEFAULT '[]',
     category TEXT NOT NULL DEFAULT '',
     icon_emoji TEXT NOT NULL DEFAULT '',
     latest_version TEXT NOT NULL
   )`,
  `CREATE TABLE IF NOT EXISTS resource_versions (
     slug TEXT NOT NULL,
     version TEXT NOT NULL,
     checksum TEXT NOT NULL,        -- sha256 hex（服务端自算，C6）
     size_bytes INTEGER NOT NULL,
     published_by TEXT NOT NULL,    -- 节点名
     published_at INTEGER NOT NULL,
     PRIMARY KEY (slug, version)
   )`,
  // v3（M2 工作组三表，spec 2026-10-24 §4 SQL 逐字；快照/委派表随 M2c/d 追加）
  `CREATE TABLE IF NOT EXISTS groups (
     id TEXT PRIMARY KEY, name TEXT NOT NULL, leader_node_id TEXT NOT NULL, created_at INTEGER NOT NULL
   );
   CREATE TABLE IF NOT EXISTS group_members (
     group_id TEXT NOT NULL, node_id TEXT NOT NULL, joined_at INTEGER NOT NULL,
     PRIMARY KEY (group_id, node_id)
   );
   CREATE TABLE IF NOT EXISTS group_invites (
     code TEXT PRIMARY KEY, group_id TEXT NOT NULL, created_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0
   )`,
  // v4（M2c 快照表——spec §4 SQL 逐字 + UNIQUE(workspace_name, version)：
  // 同 workspaceName+version 不可变（C4 同款双层 409 的库层兜底）。
  // 迁移不可变纪律：不改写已应用的 v3 串，只追加新条目（delegations 随 M2d 再追加）。
  `CREATE TABLE IF NOT EXISTS workspace_snapshots_meta (
     snapshot_id TEXT PRIMARY KEY, workspace_name TEXT NOT NULL, version TEXT NOT NULL,
     by_node TEXT NOT NULL, checksum TEXT NOT NULL, size_bytes INTEGER NOT NULL, created_at INTEGER NOT NULL,
     UNIQUE (workspace_name, version)
   )`,
  // v5（M2d 委派账本——spec §4 SQL 逐字）。status 七态（M3.1 增 waiting，见 v6）；
  // 迁移合法性由 hub canTransition 单点校验（Review Focus #3），库层只存终值不设约束。
  `CREATE TABLE IF NOT EXISTS delegations (
     task_id TEXT PRIMARY KEY, group_id TEXT NOT NULL, workspace_name TEXT NOT NULL,
     target_node_id TEXT NOT NULL, by_node_id TEXT NOT NULL, title TEXT NOT NULL,
     status TEXT NOT NULL,            -- pending|running|waiting(M3.1)|done|failed|interrupted|cancelled
     session_id TEXT,                 -- 目标节点回填
     created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
   )`,
  // v6（M3.1 HITL L2 远程代答——决策四列，追加纪律）：waiting 态复用既有 status TEXT 列
  // 不加列；决策列全 NULL 缺省（未代答），最新一次代答覆盖（worker 自报 stale 同样记）。
  // 四列各自 ALTER（单条迁移一个 schema_version，v3 多语句同款）。
  `ALTER TABLE delegations ADD COLUMN answered_by TEXT;
   ALTER TABLE delegations ADD COLUMN answer_decision TEXT;
   ALTER TABLE delegations ADD COLUMN answer_message TEXT;
   ALTER TABLE delegations ADD COLUMN answered_at INTEGER`,
  // v7（M3.4 资源生命周期，spec §2.2.1）：resources 墓碑列（撤回 = 目录不可见，
  // 版本行/blob 不删——历史版本下载保留）+ 目录修订号初值（publish/unpublish 同事务
  // 单调 +1）。hub_meta 表 v1 已存在——此处只 INSERT catalog_revision 键，勿重建表。
  `ALTER TABLE resources ADD COLUMN unpublished_at INTEGER;
   INSERT INTO hub_meta (key, value) VALUES ('catalog_revision', '1') ON CONFLICT(key) DO NOTHING`,
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
