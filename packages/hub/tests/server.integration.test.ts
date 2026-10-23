// WS 服务集成契约锁：鉴权、hello 协商、双节点互见、心跳判死（真实 socket，wire 语义全真）。
import WebSocket from 'ws';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, type HubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  PROTOCOL_VERSION, RPC, EVT, encodeEnvelope, makeRequest, parseEnvelope, type Envelope,
} from 'momo-hub-protocol';

const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-srv-')));
const identity = ensureHubIdentity(db);
let server: HubServer;
let wsUrl: string;
let baseUrl: string;

function enroll(name: string): { nodeId: string; deviceToken: string } {
  const r = enrollNode(db, identity.enrollmentToken, name);
  if ('nodeId' in r) return r;
  throw new Error('enroll 失败');
}

/** 便捷客户端：自动过 hello，收集全部信封，支持条件等待。 */
function connect(name: string) {
  return new Promise<{ ws: WebSocket; nodeId: string; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enroll(name);
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
      else resolve({ ws, nodeId: cred.nodeId, next });
    });
  });
}

beforeAll(async () => {
  server = createHubServer({ db, identity });
  const url = await server.listen(0, '127.0.0.1');
  baseUrl = url;
  wsUrl = url.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => { await server.close(); db.close(); });

describe('hub ws 服务', () => {
  it('health 端点返回 hubId', async () => {
    const res = await fetch(baseUrl + '/health');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, hubId: identity.hubId });
  });

  it('HTTP enroll：正确凭证成功、错误凭证 400 invalid_enrollment', async () => {
    const ok = await fetch(baseUrl + '/enroll', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enrollmentToken: identity.enrollmentToken, nodeName: 'http-node' }),
    });
    expect(ok.status).toBe(200);
    const body = await ok.json() as { nodeId: string; deviceToken: string };
    expect(typeof body.nodeId).toBe('string');
    const bad = await fetch(baseUrl + '/enroll', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enrollmentToken: 'nope', nodeName: 'x' }),
    });
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: 'invalid_enrollment' });
  });

  it('无 token / 错 token 升级被拒（4401）', async () => {
    for (const headers of [{}, { authorization: 'Bearer nope' }]) {
      const code = await new Promise<number>((resolve) => {
        const ws = new WebSocket(wsUrl, { headers });
        ws.on('close', resolve);
        ws.on('error', () => undefined);
      });
      expect(code).toBe(4401);
    }
  });

  it('双节点互见：B 上线 A 收 node.online；B 断开 A 收 node.offline', async () => {
    const a = await connect('node-a');
    const b = await connect('node-b');
    const onlineEvt = await a.next((e) =>
      e.kind === 'evt' && e.method === EVT.nodeOnline && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    expect((onlineEvt.payload as { nodeName: string }).nodeName).toBe('node-b');
    b.ws.close();
    const offlineEvt = await a.next((e) =>
      e.kind === 'evt' && e.method === EVT.nodeOffline && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    expect(offlineEvt.kind).toBe('evt');
    a.ws.close();
  });

  it('hello 携带不兼容版本 → proto_version_mismatch + 4402 关闭', async () => {
    const cred = enroll('node-c');
    await new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
      ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: '99.0.0' }))));
      ws.on('message', (data) => {
        const e = parseEnvelope(String(data));
        if (e?.kind === 'res' && !e.ok) {
          expect((e.payload as { code: string }).code).toBe('proto_version_mismatch');
        }
      });
      ws.on('close', (code) => { expect(code).toBe(4402); resolve(); });
      ws.on('error', () => undefined);
      setTimeout(() => reject(new Error('超时未关闭')), 5000);
    });
  });

  it('ping → pong；未知方法 → unknown_method；坏信封 → 4400', async () => {
    const a = await connect('node-d');
    a.ws.send(encodeEnvelope(makeRequest(RPC.ping, { ts: 123 })));
    const pong = await a.next((e) => e.kind === 'res' && e.method === RPC.ping);
    expect(pong.kind === 'res' && pong.ok && (pong.payload as { serverTime: number }).serverTime).toBeGreaterThan(0);
    a.ws.send(encodeEnvelope(makeRequest('bogus', {})));
    const err = await a.next((e) => e.kind === 'res' && e.method === 'bogus');
    expect(err.kind === 'res' && (err.payload as { code: string }).code).toBe('unknown_method');
    const closed = new Promise<number>((res) => { a.ws.on('close', res); a.ws.on('error', () => undefined); });
    a.ws.send('not-an-envelope');
    expect(await closed).toBe(4400);
  });

  it('心跳判死：半开连接（不再读 socket）两周期后被踢 → 他人收到 node.offline', async () => {
    // 用注入 keepaliveIntervalMs=100 的独立实例
    const db2 = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-ka-')));
    const id2 = ensureHubIdentity(db2);
    const s2 = createHubServer({ db: db2, identity: id2, keepaliveIntervalMs: 100 });
    const url2 = await s2.listen(0, '127.0.0.1');
    const watcherCred = enrollNode(db2, id2.enrollmentToken, 'watcher');
    const victimCred = enrollNode(db2, id2.enrollmentToken, 'victim');
    if (!('nodeId' in watcherCred) || !('nodeId' in victimCred)) throw new Error('unreachable');

    const watcher = new WebSocket(url2.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${watcherCred.deviceToken}` } });
    const victim = new WebSocket(url2.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${victimCred.deviceToken}` } });
    // victim 过 hello 后 pause socket——不再读 → 不回 pong（半开模拟）
    victim.on('open', () => {
      victim.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'victim' })));
      setTimeout(() => victim.pause(), 50);
    });
    try {
      // 超时必须 reject——兜底 resolve 会让判死失效也照样通过（空洞通过禁令）
      await new Promise<void>((res, reject) => {
        watcher.on('message', (data) => {
          const e = parseEnvelope(String(data));
          if (e?.kind === 'evt' && e.method === EVT.nodeOffline && (e.payload as { nodeId: string }).nodeId === victimCred.nodeId) res();
        });
        watcher.on('open', () => watcher.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION }))));
        setTimeout(() => reject(new Error('心跳判死超时：未收到 victim 的 node.offline')), 5000);
      });
    } finally {
      watcher.close();
      // victim 已 pause：close() 要等对端回握手，在此永挂 → terminate 强断让 s2.close() 收口
      victim.terminate();
      await s2.close();
      db2.close();
    }
  });

  it('hello 超时（注入 200ms）→ 4403', async () => {
    const db3 = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-hello-')));
    const id3 = ensureHubIdentity(db3);
    const s3 = createHubServer({ db: db3, identity: id3, helloTimeoutMs: 200 });
    const url3 = await s3.listen(0, '127.0.0.1');
    const cred = enrollNode(db3, id3.enrollmentToken, 'silent');
    if (!('nodeId' in cred)) throw new Error('unreachable');
    const ws = new WebSocket(url3.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const code = await new Promise<number>((resolve) => { ws.on('close', resolve); ws.on('error', () => undefined); });
    expect(code).toBe(4403);
    await s3.close();
    db3.close();
  });
});
