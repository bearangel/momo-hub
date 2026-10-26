// hub 工作空间快照存档契约锁（M2c spec §3.1/§4 + T5 + RM2-6）——
// 真实 listen + fetch FormData 上传（wire 语义全真）+ 真 socket ws RPC：
// 201 落库落盘（ws-snapshots/{sha256}.tar.gz，服务端自算 checksum）/ manifest 校验矩阵 /
// 同 workspaceName+version 重复 409（C4 同款双层）/ 超限 413 零残留 / 401 /
// workspace.snapshots 列表（无组校验——任何注册节点可调）/ 下载端点
// （RM2-6：流 checksum 三方对账 == 头 == meta 落库值 / 404 未知与坏 id / 401）/
// 裸 TCP 慢速早答排干纪律（照 M1）。
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type Database from 'better-sqlite3';
import WebSocket from 'ws';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope,
  type Envelope,
} from 'momo-hub-protocol';

const dataDir = mkdtempSync(join(tmpdir(), 'momo-hub-snap-'));
const db = openHubDatabase(dataDir);
const identity = ensureHubIdentity(db);

let server: HubServer;
let baseUrl: string;
let wsUrl: string;
let deviceToken: string;

beforeAll(async () => {
  server = createHubServer({ db, identity, dataDir });
  baseUrl = await server.listen(0, '127.0.0.1');
  wsUrl = baseUrl.replace(/^http/, 'ws') + '/ws';
  const cred = enrollNode(db, identity.enrollmentToken, 'snap-node');
  if ('error' in cred) throw new Error('enroll 失败');
  deviceToken = cred.deviceToken;
});
afterAll(async () => { await server.close(); db.close(); });

// ── 工具 ────────────────────────────────────────────────────────────────────

function manifestOf(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    workspaceName: 'ws-main',
    version: '3',
    resources: [{ type: 'agent', slug: 'coder-pro' }],
    config: {
      agentMembers: [{ name: 'coder', slug: 'coder-pro' }],
      teams: [],
      mcpEnabled: ['fetch'],
      skills: [],
      pluginsEnabled: [],
      providerRefs: [],
      prefs: {},
    },
    ...over,
  };
}

function asBlob(data: Buffer | string, type: string): Blob {
  return new Blob([typeof data === 'string' ? data : new Uint8Array(data)], { type });
}

function form(manifest: unknown, payload: Buffer | null = Buffer.from('fake ws tar.gz bytes')): FormData {
  const fd = new FormData();
  const text = typeof manifest === 'string' ? manifest : JSON.stringify(manifest);
  fd.append('manifest', asBlob(text, 'application/json'), 'manifest.json');
  if (payload !== null) fd.append('payload', asBlob(payload, 'application/gzip'), 'payload.tar.gz');
  return fd;
}

function upload(body: FormData, token: string | null = deviceToken): Promise<Response> {
  const headers: Record<string, string> = {};
  if (token !== null) headers.authorization = `Bearer ${token}`;
  return fetch(`${baseUrl}/workspaces/snapshots`, { method: 'POST', headers, body });
}

function snapshotDirFiles(): string[] {
  const dir = join(dataDir, 'ws-snapshots');
  if (!existsSync(dir)) return [];
  return readdirSync(dir);
}

function metaRows(workspaceName: string): number {
  return (db.prepare('SELECT COUNT(*) AS n FROM workspace_snapshots_meta WHERE workspace_name = ?')
    .get(workspaceName) as { n: number }).n;
}

/** 真 socket ws 客户端（groups.test 同款）：hello → RPC 便捷面。 */
function connectWs(name: string) {
  return new Promise<{ ws: WebSocket; nodeId: string; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enrollNode(db, identity.enrollmentToken, name);
    if ('error' in cred) throw new Error('enroll 失败');
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const inbox: Envelope[] = [];
    const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
    ws.on('message', (data) => {
      const e = parseEnvelope(String(data));
      if (!e) return;
      inbox.push(e);
      const i = waiters.findIndex((w) => w.match(e));
      if (i >= 0) waiters.splice(i, 1)[0]!.resolve(e);
    });
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: name }))));
    ws.on('error', reject);
    const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
      new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
    void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
      if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
      else resolve({ ws, nodeId: cred.nodeId, inbox, next });
    });
  });
}

function callRpc(client: Awaited<ReturnType<typeof connectWs>>, method: string, payload: unknown): Promise<Envelope> {
  const env = makeRequest(method, payload);
  client.ws.send(encodeEnvelope(env));
  return client.next((e) => e.kind === 'res' && e.id === env.id);
}

// ── 裸 TCP 工具（早答排干纪律回归锁——照 M1 resources-publish 形态）──────────────
const RAW_BOUNDARY = '----momohubrawtest';

interface RawResponse { status: number; body: string }

function rawRequestHead(port: number, token: string, bodyLength: number): Buffer {
  return Buffer.from([
    'POST /workspaces/snapshots HTTP/1.1',
    `Host: 127.0.0.1:${port}`,
    `Authorization: Bearer ${token}`,
    `Content-Type: multipart/form-data; boundary=${RAW_BOUNDARY}`,
    `Content-Length: ${bodyLength}`,
    'Connection: keep-alive',
    '', '',
  ].join('\r\n'), 'utf8');
}

function rawMultipartBody(manifestText: string, payload: Buffer, trailing = ''): Buffer {
  const head = `--${RAW_BOUNDARY}\r\nContent-Disposition: form-data; name="manifest"; filename="manifest.json"\r\nContent-Type: application/json\r\n\r\n`;
  const mid = `\r\n--${RAW_BOUNDARY}\r\nContent-Disposition: form-data; name="payload"; filename="payload.tar.gz"\r\nContent-Type: application/gzip\r\n\r\n`;
  const tail = trailing === ''
    ? `\r\n--${RAW_BOUNDARY}--\r\n`
    : `\r\n--${RAW_BOUNDARY}\r\nContent-Disposition: form-data; name="extra"; filename="extra.bin"\r\nContent-Type: application/octet-stream\r\n\r\n${trailing}\r\n--${RAW_BOUNDARY}--\r\n`;
  return Buffer.concat([
    Buffer.from(head, 'utf8'),
    Buffer.from(manifestText, 'utf8'),
    Buffer.from(mid, 'utf8'),
    payload,
    Buffer.from(tail, 'utf8'),
  ]);
}

class RawClient {
  private buf = Buffer.alloc(0);
  private waiters: Array<{ resolve: (r: RawResponse) => void; reject: (e: Error) => void }> = [];

  constructor(private readonly sock: net.Socket) {
    sock.on('data', (d: Buffer) => { this.buf = Buffer.concat([this.buf, d]); this.pump(); });
  }

  private pump(): void {
    while (this.waiters.length > 0) {
      const idx = this.buf.indexOf('\r\n\r\n');
      if (idx < 0) return;
      const head = this.buf.subarray(0, idx).toString('utf8');
      const m = /content-length:\s*(\d+)/i.exec(head);
      const len = m ? Number(m[1]) : 0;
      if (this.buf.length - idx - 4 < len) return;
      const body = this.buf.subarray(idx + 4, idx + 4 + len).toString('utf8');
      this.buf = this.buf.subarray(idx + 4 + len);
      this.waiters.shift()?.resolve({ status: Number(head.split(' ')[1] ?? 0), body });
    }
  }

  async sendRaw(port: number, token: string, body: Buffer, label = '请求'): Promise<RawResponse> {
    this.sock.write(rawRequestHead(port, token, body.length));
    for (let off = 0; off < body.length; off += 8 * 1024) {
      this.sock.write(body.subarray(off, Math.min(off + 8 * 1024, body.length)));
      await new Promise((r) => setTimeout(r, 1));
    }
    return this.next(5000, label);
  }

  async upload(port: number, token: string, manifestText: string, payload: Buffer, trailing = ''): Promise<RawResponse> {
    return this.sendRaw(port, token, rawMultipartBody(manifestText, payload, trailing));
  }

  next(timeoutMs: number, label: string): Promise<RawResponse> {
    return new Promise<RawResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.waiters.findIndex((w) => w.reject === reject);
        if (i >= 0) this.waiters.splice(i, 1);
        reject(new Error(`${label}超时：socket 悬挂`));
      }, timeoutMs);
      this.waiters.push({
        resolve: (r) => { clearTimeout(timer); resolve(r); },
        reject: (e) => { clearTimeout(timer); reject(e); },
      });
      this.pump();
    });
  }
}

/** 专用 hub 实例 + 裸 TCP 连接；close() 含 5s 收口竞速断言。 */
async function rawHub(snapshotMaxBytes?: number): Promise<{
  port: number;
  token: string;
  client: RawClient;
  db: Database.Database;
  close: () => Promise<void>;
}> {
  const dir = mkdtempSync(join(tmpdir(), 'momo-hub-snap-raw-'));
  const rawDb = openHubDatabase(dir);
  const rawIdentity = ensureHubIdentity(rawDb);
  const rawServer = createHubServer({ db: rawDb, identity: rawIdentity, dataDir: dir, snapshotMaxBytes });
  const url = await rawServer.listen(0, '127.0.0.1');
  const cred = enrollNode(rawDb, rawIdentity.enrollmentToken, 'raw-node');
  if ('error' in cred) throw new Error('enroll 失败');
  const port = Number(new URL(url).port);
  const sock = net.connect(port, '127.0.0.1');
  sock.on('error', () => { /* 断言走超时路径 */ });
  await new Promise<void>((resolve) => sock.once('connect', () => resolve()));
  return {
    port,
    token: cred.deviceToken,
    client: new RawClient(sock),
    db: rawDb,
    close: async () => {
      sock.destroy();
      await expect(Promise.race([
        rawServer.close(),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error('server.close 超时：socket 悬挂')), 5000)),
      ])).resolves.toBeUndefined();
      rawDb.close();
    },
  };
}

// ── 迁移（workspace_snapshots_meta——v3 组表同批余下表，spec §4 SQL 逐字）─────────

describe('迁移：workspace_snapshots_meta', () => {
  it('schema_version=7；表存在；重开幂等；(workspace_name, version) 库层唯一（C4 并发兜底）', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-snap-mig-'));
    const d1: Database.Database = openHubDatabase(dir);
    expect((d1.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string }).value).toBe('7');
    expect(d1.prepare(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'workspace_snapshots_meta'`,
    ).get()).toEqual({ name: 'workspace_snapshots_meta' });
    d1.close();
    const d2 = openHubDatabase(dir);
    expect((d2.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string }).value).toBe('7');
    d2.close();

    // 库层唯一键：同 workspace_name+version 二次插入必炸（409 兜底机制的根基）
    let code: string | undefined;
    try {
      db.prepare(`INSERT INTO workspace_snapshots_meta (snapshot_id, workspace_name, version, by_node, checksum, size_bytes, created_at)
                  VALUES ('sid-x', 'uniq-ws', '1', 'n', '${'a'.repeat(64)}', 1, 0)`).run();
      db.prepare(`INSERT INTO workspace_snapshots_meta (snapshot_id, workspace_name, version, by_node, checksum, size_bytes, created_at)
                  VALUES ('sid-y', 'uniq-ws', '1', 'n', '${'b'.repeat(64)}', 1, 0)`).run();
    } catch (err) {
      code = (err as { code?: string }).code;
    }
    expect(code).toBe('SQLITE_CONSTRAINT_UNIQUE');
    db.prepare(`DELETE FROM workspace_snapshots_meta WHERE workspace_name = 'uniq-ws'`).run();
  });
});

// ── POST /workspaces/snapshots ──────────────────────────────────────────────

describe('POST /workspaces/snapshots', () => {
  it('正常上传 201：响应 {snapshotId, checksum}（服务端自算），DB 落行，blob 落 ws-snapshots/，无临时件残留', async () => {
    const payload = Buffer.from('ws snapshot tar.gz payload v3');
    const res = await upload(form(manifestOf(), payload));
    expect(res.status).toBe(201);
    const body = await res.json() as { snapshotId: string; checksum: string };
    expect(typeof body.snapshotId).toBe('string');
    expect(body.snapshotId.length).toBeGreaterThan(10);
    // C6 纪律：校验和由服务端自算（不信客户端自报——客户端根本没报）
    expect(body.checksum).toBe(createHash('sha256').update(payload).digest('hex'));

    const row = db.prepare('SELECT * FROM workspace_snapshots_meta WHERE snapshot_id = ?')
      .get(body.snapshotId) as Record<string, unknown> | undefined;
    expect(row).toBeDefined();
    expect(row?.['workspace_name']).toBe('ws-main');
    expect(row?.['version']).toBe('3');
    expect(row?.['by_node']).toBe('snap-node');
    expect(row?.['checksum']).toBe(body.checksum);
    expect(row?.['size_bytes']).toBe(payload.length);
    expect(typeof row?.['created_at']).toBe('number');

    const blobPath = join(dataDir, 'ws-snapshots', `${body.checksum}.tar.gz`);
    expect(existsSync(blobPath)).toBe(true);
    expect(readFileSync(blobPath).equals(payload)).toBe(true);
    expect(snapshotDirFiles().filter((f) => f.startsWith('.tmp-'))).toHaveLength(0);
  });

  it('manifest 校验矩阵 → 400 invalid_manifest，零落库零落盘', async () => {
    const cfg = manifestOf().config as Record<string, unknown>;
    const itCases: Array<{ name: string; manifest: unknown }> = [
      { name: '空 workspaceName', manifest: manifestOf({ workspaceName: '' }) },
      { name: '超长 workspaceName', manifest: manifestOf({ workspaceName: 'w'.repeat(129) }) },
      { name: '坏 version', manifest: manifestOf({ version: '1.0 x' }) },
      { name: 'resources 非数组', manifest: manifestOf({ resources: 'coder-pro' }) },
      { name: 'resources 坏 type', manifest: manifestOf({ resources: [{ type: 'app', slug: 'a' }] }) },
      { name: 'config 缺失', manifest: manifestOf({ config: null }) },
      { name: 'strList 项超长（≤128 上限）', manifest: manifestOf({ config: { ...cfg, mcpEnabled: ['x'.repeat(129)] } }) },
      { name: 'manifest 非 JSON', manifest: 'not-json' },
    ];
    for (const c of itCases) {
      const res = await upload(form(c.manifest));
      expect(res.status, `用例：${c.name}`).toBe(400);
      expect(await res.json(), `用例：${c.name}`).toEqual({ error: 'invalid_manifest' });
    }
    expect(metaRows('ws-main')).toBe(1); // 仅首个正常上传
  });

  it('缺 part / part 顺序错 / 非 multipart → 400 invalid_manifest', async () => {
    // 缺 payload part
    const noPayload = await upload(form(manifestOf({ workspaceName: 'no-payload' }), null));
    expect(noPayload.status).toBe(400);
    expect(await noPayload.json()).toEqual({ error: 'invalid_manifest' });
    // 缺 manifest part（只有 payload）
    const fd = new FormData();
    fd.append('payload', asBlob(Buffer.from('bytes'), 'application/gzip'), 'payload.tar.gz');
    const noManifest = await upload(fd);
    expect(noManifest.status).toBe(400);
    expect(await noManifest.json()).toEqual({ error: 'invalid_manifest' });
    // 非 multipart content-type
    const notMultipart = await fetch(`${baseUrl}/workspaces/snapshots`, {
      method: 'POST',
      headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'application/json' },
      body: JSON.stringify(manifestOf({ workspaceName: 'bad-ct' })),
    });
    expect(notMultipart.status).toBe(400);
    expect(await notMultipart.json()).toEqual({ error: 'invalid_manifest' });
    expect(metaRows('no-payload')).toBe(0);
    expect(metaRows('bad-ct')).toBe(0);
  });

  it('未鉴权 401：无 Authorization / 错 token → unauthorized，零落库', async () => {
    const noAuth = await upload(form(manifestOf({ workspaceName: 'no-auth' })), null);
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await upload(form(manifestOf({ workspaceName: 'no-auth' })), 'wrong-token');
    expect(badAuth.status).toBe(401);
    expect(metaRows('no-auth')).toBe(0);
  });

  it('同 workspaceName+version 重复 → 409 version_exists（不同内容也拒）；不同 version 同内容 → 201 且 blob 幂等去重', async () => {
    const first = await upload(form(manifestOf({ workspaceName: 'dup-ws', version: '7' }), Buffer.from('dup payload a')));
    expect(first.status).toBe(201);
    const second = await upload(form(manifestOf({ workspaceName: 'dup-ws', version: '7' }), Buffer.from('different bytes')));
    expect(second.status).toBe(409);
    expect(await second.json()).toEqual({ error: 'version_exists' });
    expect(metaRows('dup-ws')).toBe(1);

    // 不同 version + 相同内容：新元数据行，blob 内容寻址去重（单文件）
    const third = await upload(form(manifestOf({ workspaceName: 'dup-ws', version: '8' }), Buffer.from('dup payload a')));
    expect(third.status).toBe(201);
    expect(metaRows('dup-ws')).toBe(2);
    const checksums = (db.prepare('SELECT checksum FROM workspace_snapshots_meta WHERE workspace_name = ?').all('dup-ws') as Array<{ checksum: string }>).map((r) => r.checksum);
    expect(new Set(checksums).size).toBe(1); // 同内容同 checksum
    expect(snapshotDirFiles().filter((f) => f.endsWith('.tar.gz'))).toHaveLength(2); // ws-main + dup-ws 共两个 blob
  });

  it('并发同 version 上传：恰一 201 一 409（预查 + 唯一键兜底双层）', async () => {
    const manifest = manifestOf({ workspaceName: 'race-ws', version: '9' });
    const [a, b] = await Promise.all([
      upload(form(manifest, Buffer.from('race payload'))),
      upload(form(manifest, Buffer.from('race payload'))),
    ]);
    const codes = [a.status, b.status].sort();
    expect(codes).toEqual([201, 409]);
    expect(metaRows('race-ws')).toBe(1);
  });

  it('manifest 以普通字段 part（无 filename）上传同样受理——不强制 part 形态', async () => {
    const fd = new FormData();
    fd.append('manifest', JSON.stringify(manifestOf({ workspaceName: 'field-ws', version: '1' })));
    fd.append('payload', asBlob(Buffer.from('field payload'), 'application/gzip'), 'payload.tar.gz');
    const res = await upload(fd);
    expect(res.status).toBe(201);
    expect(metaRows('field-ws')).toBe(1);
  });
});

describe('快照超限（413）', () => {
  it('注入小上限后超限 → 413 too_large，临时件零残留，DB 零落行，close 5s 收口', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-snap-limit-'));
    const limitDb = openHubDatabase(dir);
    const limitIdentity = ensureHubIdentity(limitDb);
    const limitServer = createHubServer({ db: limitDb, identity: limitIdentity, dataDir: dir, snapshotMaxBytes: 64 * 1024 });
    const url = await limitServer.listen(0, '127.0.0.1');
    const cred = enrollNode(limitDb, limitIdentity.enrollmentToken, 'limit-node');
    if ('error' in cred) throw new Error('enroll 失败');
    try {
      const fd = new FormData();
      fd.append('manifest', asBlob(JSON.stringify(manifestOf({ workspaceName: 'big-ws' })), 'application/json'), 'manifest.json');
      fd.append('payload', asBlob(Buffer.alloc(256 * 1024, 0x41), 'application/gzip'), 'payload.tar.gz');
      const res = await fetch(`${url}/workspaces/snapshots`, {
        method: 'POST', headers: { authorization: `Bearer ${cred.deviceToken}` }, body: fd,
      });
      expect(res.status).toBe(413);
      expect(await res.json()).toEqual({ error: 'too_large' });
      const files = existsSync(join(dir, 'ws-snapshots')) ? readdirSync(join(dir, 'ws-snapshots')) : [];
      expect(files).toHaveLength(0);
      expect((limitDb.prepare('SELECT COUNT(*) AS n FROM workspace_snapshots_meta').get() as { n: number }).n).toBe(0);
    } finally {
      await expect(Promise.race([
        limitServer.close(),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error('server.close 超时：socket 悬挂')), 5000)),
      ])).resolves.toBeUndefined();
      limitDb.close();
    }
  }, 30_000);
});

// ── workspace.snapshots RPC（列表——无组校验）──────────────────────────────────

describe('workspace.snapshots RPC', () => {
  it('列表：字段形状 + byNode=节点名 + 新在前；workspaceName 过滤；无组节点可调', async () => {
    await upload(form(manifestOf({ workspaceName: 'list-ws', version: '1' }), Buffer.from('list-a')));
    await upload(form(manifestOf({ workspaceName: 'list-ws', version: '2' }), Buffer.from('list-b')));
    await upload(form(manifestOf({ workspaceName: 'other-ws', version: '1' }), Buffer.from('list-c')));

    const client = await connectWs('lister-node'); // 未加入任何组——照样可列（无组校验）
    const res = await callRpc(client, RPC.workspace.snapshots, { workspaceName: 'list-ws' });
    expect(res.kind === 'res' && res.ok).toBe(true);
    const { snapshots } = (res.payload ?? {}) as {
      snapshots: Array<{ snapshotId: string; workspaceName: string; version: string; byNode: string; at: number }>;
    };
    expect(snapshots.map((s) => s.version)).toEqual(['2', '1']); // 新在前
    expect(snapshots.every((s) => s.workspaceName === 'list-ws')).toBe(true);
    expect(snapshots.every((s) => s.byNode === 'snap-node')).toBe(true); // 上传者是 snap-node
    expect(snapshots.every((s) => typeof s.at === 'number' && s.at > 0)).toBe(true);
    expect(snapshots.every((s) => typeof s.snapshotId === 'string')).toBe(true);

    const all = await callRpc(client, RPC.workspace.snapshots, {});
    const allList = ((all.payload ?? {}) as { snapshots: Array<{ workspaceName: string }> }).snapshots;
    expect(allList.some((s) => s.workspaceName === 'list-ws')).toBe(true);
    expect(allList.some((s) => s.workspaceName === 'other-ws')).toBe(true);
    client.ws.close();
  });

  it('空目录 → {snapshots: []}；坏过滤值（非字符串）→ 忽略过滤等同全量', async () => {
    const client = await connectWs('empty-lister');
    const ghost = await callRpc(client, RPC.workspace.snapshots, { workspaceName: 'no-such-ws' });
    expect(ghost.kind === 'res' && ghost.ok).toBe(true);
    expect(ghost.payload).toEqual({ snapshots: [] });
    const badFilter = await callRpc(client, RPC.workspace.snapshots, { workspaceName: 123 });
    expect(badFilter.kind === 'res' && badFilter.ok).toBe(true);
    expect(((badFilter.payload ?? {}) as { snapshots: unknown[] }).snapshots.length).toBeGreaterThan(0);
    client.ws.close();
  });
});

// ── GET /workspaces/snapshots/:snapshotId/download（RM2-6）────────────────────

describe('GET /workspaces/snapshots/:snapshotId/download', () => {
  function downloadUrl(snapshotId: string): string {
    return `${baseUrl}/workspaces/snapshots/${encodeURIComponent(snapshotId)}/download`;
  }

  it('正常下载 200：流式 body 复算 checksum == X-Checksum-Sha256 头 == meta 落库值；体逐字节等于上传 payload', async () => {
    const payload = Buffer.from('download me tar.gz payload for rm2-6');
    const up = await upload(form(manifestOf({ workspaceName: 'dl-ws', version: '1' }), payload));
    expect(up.status).toBe(201);
    const { snapshotId } = await up.json() as { snapshotId: string; checksum: string };

    const res = await fetch(downloadUrl(snapshotId), { headers: { authorization: `Bearer ${deviceToken}` } });
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toBe('application/gzip');
    expect(res.headers.get('content-length')).toBe(String(payload.length));

    // 三方对账：流式复算 == 响应头 == meta 落库值（C6 延续——下载方不信任何单端）
    const body = Buffer.from(await res.arrayBuffer());
    const recomputed = createHash('sha256').update(body).digest('hex');
    const headerChecksum = res.headers.get('x-checksum-sha256');
    expect(headerChecksum).toBe(recomputed);
    const metaRow = db.prepare('SELECT checksum FROM workspace_snapshots_meta WHERE snapshot_id = ?')
      .get(snapshotId) as { checksum: string };
    expect(metaRow.checksum).toBe(headerChecksum);

    // 体完整：逐字节等于上传时的 payload（流式回包无截断）
    expect(body.equals(payload)).toBe(true);
  });

  it('未知 snapshotId（合法 UUID 形态但不存在）→ 404 not_found', async () => {
    const res = await fetch(downloadUrl('01234567-89ab-4cde-8f01-234567890abc'), {
      headers: { authorization: `Bearer ${deviceToken}` },
    });
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: 'not_found' });
  });

  it('坏 snapshotId（非 UUID 形态，含路径穿越尝试）→ 404 早拒，与未知同款错误体不区分', async () => {
    const headers = { authorization: `Bearer ${deviceToken}` };
    for (const badId of ['not-a-uuid', 'x', 'snap.tar.gz']) {
      const res = await fetch(downloadUrl(badId), { headers });
      expect(res.status, `坏 id 用例：${badId}`).toBe(404);
      expect(await res.json(), `坏 id 用例：${badId}`).toEqual({ error: 'not_found' });
    }
    // 路径穿越尝试：编码点段不被 URL 规范化，以参数形态抵达路由——形态早拒拦截
    const traversal = await fetch(`${baseUrl}/workspaces/snapshots/%2e%2e%2f%2e%2e%2fblobs%2fx/download`, { headers });
    expect(traversal.status).toBe(404);
    expect(await traversal.json()).toEqual({ error: 'not_found' });
  });

  it('未鉴权 401：无 Authorization / 错 token → unauthorized', async () => {
    const up = await upload(form(manifestOf({ workspaceName: 'dl-auth-ws', version: '1' }), Buffer.from('auth dl')));
    expect(up.status).toBe(201);
    const { snapshotId } = await up.json() as { snapshotId: string };

    const noAuth = await fetch(downloadUrl(snapshotId));
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ error: 'unauthorized' });
    const badAuth = await fetch(downloadUrl(snapshotId), { headers: { authorization: 'Bearer wrong-token' } });
    expect(badAuth.status).toBe(401);
    expect(await badAuth.json()).toEqual({ error: 'unauthorized' });
  });
});

// ── 裸 TCP 早答排干纪律（照 M1 resources-publish 回归锁形态）──────────────────

describe('快照上传早答排干（请求体悬挂）', () => {
  it('409 预查早答：payload part 未被消费也必须排干——同连接后续请求仍 201，close 5s 收口', async () => {
    const hub = await rawHub();
    try {
      const m = JSON.stringify(manifestOf({ workspaceName: 'raw-dup', version: '1' }));
      const first = await hub.client.upload(hub.port, hub.token, m, Buffer.alloc(64 * 1024, 0x41));
      expect(first.status).toBe(201);
      const second = await hub.client.upload(hub.port, hub.token, m, Buffer.alloc(2 * 1024 * 1024, 0x42));
      expect(second.status).toBe(409);
      expect(JSON.parse(second.body)).toEqual({ error: 'version_exists' });
      const third = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-dup', version: '2' })),
        Buffer.from('third payload'),
      );
      expect(third.status).toBe(201);
    } finally {
      await hub.close();
    }
  }, 30_000);

  it('413 超限早答：payload 流必须排干——同连接后续请求仍 201，close 5s 收口', async () => {
    const hub = await rawHub(64 * 1024);
    try {
      const r1 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-big', version: '1' })),
        Buffer.alloc(2 * 1024 * 1024, 0x41),
      );
      expect(r1.status).toBe(413);
      expect(JSON.parse(r1.body)).toEqual({ error: 'too_large' });
      const r2 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-after-big', version: '1' })),
        Buffer.from('after too big payload'),
      );
      expect(r2.status).toBe(201);
    } finally {
      await hub.close();
    }
  }, 30_000);

  it('201 之后有尾随 part：尾随 part 必须排干——同连接后续请求仍 201', async () => {
    const hub = await rawHub();
    try {
      const r1 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-trail', version: '1' })),
        Buffer.from('trailing payload'),
        'x'.repeat(256 * 1024),
      );
      expect(r1.status).toBe(201);
      const r2 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-trail', version: '2' })),
        Buffer.from('after trailing payload'),
      );
      expect(r2.status).toBe(201);
    } finally {
      await hub.close();
    }
  }, 30_000);

  it('坏 manifest（400）后同连接第二个请求仍 201，close 5s 收口', async () => {
    const hub = await rawHub();
    try {
      const r1 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'x'.repeat(129) })),
        Buffer.alloc(2 * 1024 * 1024, 0x43),
      );
      expect(r1.status).toBe(400);
      expect(JSON.parse(r1.body)).toEqual({ error: 'invalid_manifest' });
      const r2 = await hub.client.upload(
        hub.port, hub.token,
        JSON.stringify(manifestOf({ workspaceName: 'raw-after-bad', version: '1' })),
        Buffer.from('after bad payload'),
      );
      expect(r2.status).toBe(201);
    } finally {
      await hub.close();
    }
  }, 30_000);
});
