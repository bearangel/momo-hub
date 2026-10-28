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

/** 便捷客户端：自动过 hello，收集全部信封（deviceToken/inbox 一并暴露给用例），支持条件等待。
 *  protoVer 可注入（M3.0 三态：模拟 1.1.0 低版本 / 1.9.0 高版本节点过 hello）。 */
function connect(name: string, protoVer: string = PROTOCOL_VERSION) {
  return new Promise<{ ws: WebSocket; nodeId: string; deviceToken: string; inbox: Envelope[]; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enroll(name);
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const inbox: Envelope[] = [];
    const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
    ws.on('message', (data) => {
      const e = parseEnvelope(String(data));
      if (!e) return;
      inbox.push(e);
      const i = waiters.findIndex((w) => w.match(e));
      if (i >= 0) waiters.splice(i, 1)[0].resolve(e);
    });
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer, nodeName: name }))));
    ws.on('error', reject);
    const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
      new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
    void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
      if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
      else resolve({ ws, nodeId: cred.nodeId, deviceToken: cred.deviceToken, inbox, next });
    });
  });
}

/** 升级裸连接：刻意不发 hello，open 即 close（升级重试放弃的场景）。 */
function openThenClose(deviceToken: string): Promise<void> {
  return new Promise((res, reject) => {
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${deviceToken}` } });
    ws.on('open', () => ws.close());
    ws.on('close', () => res());
    ws.on('error', (err: Error) => reject(err));
    setTimeout(() => reject(new Error('第二条连接未按时关闭')), 5000);
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

  it('minor 门槛三态：1.1.0 节点调 task.list → unsupported_method 结构化载荷（M3.0 §3.1）', async () => {
    const peer = await connect('node-minor-11', '1.1.0');
    // hello 通过（major 相同），基础方法不受门槛影响
    peer.ws.send(encodeEnvelope(makeRequest(RPC.ping, { ts: 7 })));
    const pong = await peer.next((e) => e.kind === 'res' && e.method === RPC.ping);
    expect(pong.kind === 'res' && pong.ok).toBe(true);
    // 入表方法（requiredMinor=2）低于节点 minor(1) → 可读降级
    peer.ws.send(encodeEnvelope(makeRequest(RPC.task.list, {})));
    const err = await peer.next((e) => e.kind === 'res' && e.method === RPC.task.list);
    expect(err.kind === 'res' && err.ok).toBe(false);
    const payload = (err.kind === 'res' ? err.payload : null) as
      { code: string; message: string; method?: string; requiredMinor?: number } | null;
    expect(payload?.code).toBe('unsupported_method');
    expect(payload?.method).toBe('task.list');
    expect(payload?.requiredMinor).toBe(2);
    expect(payload?.message).toContain('hub=1.3.0');
    expect(payload?.message).toContain('节点=1.1.0');
    expect(payload?.message).toContain('task.list');
    peer.ws.close();
  });

  it('minor 门槛三态：1.2.0 与 1.9.0 节点调 task.list 正常（无 unsupported_method）', async () => {
    for (const ver of ['1.2.0', '1.9.0']) {
      const peer = await connect(`node-minor-${ver.replace(/\./g, '-')}`, ver);
      peer.ws.send(encodeEnvelope(makeRequest(RPC.task.list, {})));
      const res = await peer.next((e) => e.kind === 'res' && e.method === RPC.task.list);
      expect(res.kind === 'res' && res.ok, `节点 ${ver} task.list 应正常受理`).toBe(true);
      const payload = res.kind === 'res' ? res.payload as { code?: string } : null;
      expect(payload?.code, `节点 ${ver} 不应收 unsupported_method`).not.toBe('unsupported_method');
      peer.ws.close();
    }
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

  it('同 nodeId 第二条连接（未 hello）关闭不误摘在线连接，也不误广播 node.offline', async () => {
    const watcher = await connect('dup-watcher');
    const a = await connect('node-dup');
    // 前置：watcher 在线且确实收到了 A 的 node.online（证明它能收到广播流）
    await watcher.next((e) => e.kind === 'evt' && e.method === EVT.nodeOnline
      && (e.payload as { nodeId: string }).nodeId === a.nodeId);

    // 同 token 第二条升级连接：不发 hello 直接关闭（身份不得影响 A 的在线态）
    await openThenClose(a.deviceToken);

    expect(server.onlineNodes().map((n) => n.nodeId)).toContain(a.nodeId);
    // 等一拍让 close 事件的广播（若有）落到 watcher 收件箱
    await new Promise((res) => setTimeout(res, 300));
    const offlineForA = watcher.inbox.filter((e) => e.kind === 'evt' && e.method === EVT.nodeOffline
      && (e.payload as { nodeId: string }).nodeId === a.nodeId);
    expect(offlineForA).toHaveLength(0);

    watcher.ws.close();
    a.ws.close();
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

  it('鉴权失败连接不拖慢 close() 排空（liveSockets 即摘——T9 Minor#2）', async () => {
    const db4 = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-auth-')));
    const id4 = ensureHubIdentity(db4);
    const s4 = createHubServer({ db: db4, identity: id4 });
    const url4 = await s4.listen(0, '127.0.0.1');

    // 坏 token 升级 → 4401；客户端完成 close 握手（连接干净关闭）。但鉴权失败分支
    // 早于 close 处理器注册——修复前该 socket 永驻 liveSockets，close() 排空拖满 500ms
    const ws = new WebSocket(url4.replace(/^http/, 'ws') + '/ws', { headers: { authorization: 'Bearer nope' } });
    const code = await new Promise<number>((resolve) => { ws.on('close', resolve); ws.on('error', () => undefined); });
    expect(code).toBe(4401);

    const t0 = Date.now();
    await s4.close();
    // 修复前：liveSockets 残留死 socket → drain 等满 500ms 上限；修复后即摘即返。
    // 阈值 300ms 居中区分两形态（排空上限 500 固定，判定稳定）
    expect(Date.now() - t0).toBeLessThan(300);
    db4.close();
  });
});
