// hub 服务本体：Fastify(/health /enroll) + WebSocket(/ws)。
// 鉴权链：升级头 Bearer deviceToken → hello（版本协商）→ 注册广播。
// 判死：ws ping 帧 keepalive，两周期无 pong terminate → node.offline。
import fastify from 'fastify';
import websocket from '@fastify/websocket';
import type Database from 'better-sqlite3';
import {
  EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, isCompatible, parseEnvelope,
  type HelloReq, type RpcErrorPayload,
} from 'momo-hub-protocol';
import type { HubIdentity } from './identity';
import { enrollNode, verifyDeviceToken } from './auth';
import { NodeRegistry } from './registry';
import { logger } from './logger';

export interface HubServer {
  listen(port: number, host?: string): Promise<string>;
  close(): Promise<void>;
  onlineNodes(): Array<{ nodeId: string; nodeName: string }>;
}

export interface CreateHubServerOpts {
  db: Database.Database;
  identity: HubIdentity;
  helloTimeoutMs?: number;      // 默认 5000
  keepaliveIntervalMs?: number; // 默认 30000；判死 = 2 × interval
}

interface Session { nodeId: string; nodeName: string; helloDone: boolean }

export function createHubServer(opts: CreateHubServerOpts): HubServer {
  const { db, identity } = opts;
  const helloTimeoutMs = opts.helloTimeoutMs ?? 5000;
  const keepaliveIntervalMs = opts.keepaliveIntervalMs ?? 30_000;
  const registry = new NodeRegistry();
  const app = fastify({ logger: false });
  void app.register(websocket);
  // 路由必须挂在 websocket 插件之后的 register 里：fastify 5.12 的 onRoute 钩子在 .get()
  // 调用时同步跑，而 register() 的插件加载推迟到 ready()——直接紧跟 register(websocket)
  // 写路由会让 /ws 用未改写的 handler（socket 实为 request，表现为升级 500 / close 1006）。
  void app.register(async (instance) => {
    instance.get('/health', async () => ({ ok: true, hubId: identity.hubId }));

    instance.post('/enroll', async (req, reply) => {
      const body = req.body as { enrollmentToken?: unknown; nodeName?: unknown };
      if (typeof body?.enrollmentToken !== 'string' || typeof body?.nodeName !== 'string') {
        return reply.code(400).send({ error: 'invalid_name' });
      }
      const r = enrollNode(db, body.enrollmentToken, body.nodeName);
      if ('error' in r) return reply.code(400).send({ error: r.error });
      logger.info('节点已注册', { nodeId: r.nodeId });
      return reply.send({ nodeId: r.nodeId, deviceToken: r.deviceToken });
    });

    instance.get('/ws', { websocket: true }, (socket, req) => {
      const auth = req.headers.authorization ?? '';
      const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
      const node = token ? verifyDeviceToken(db, token) : null;
      if (!node) { socket.close(4401, 'unauthorized'); return; }

      const session: Session = { nodeId: node.nodeId, nodeName: node.name, helloDone: false };
      const helloTimer = setTimeout(() => socket.close(4403, 'hello_timeout'), helloTimeoutMs);

      const sendRes = (id: string, method: string, ok: boolean, payload: unknown): void => {
        socket.send(encodeEnvelope({ v: 1, kind: 'res', id, method, ok, payload }));
      };
      const errPayload = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

      socket.on('message', (raw: unknown) => {
        const env = parseEnvelope(typeof raw === 'string' ? raw : String(raw));
        if (!env) { socket.close(4400, 'bad_envelope'); return; }
        if (env.kind !== 'req') return; // M0：客户端 res/evt 忽略
        if (!session.helloDone && env.method !== RPC.hello) {
          sendRes(env.id, env.method, false, errPayload('unauthorized', '必须先完成 hello'));
          return;
        }
        switch (env.method) {
          case RPC.hello: {
            const ver = (env.payload as HelloReq | null)?.protoVer ?? '';
            if (!isCompatible(ver, PROTOCOL_VERSION)) {
              sendRes(env.id, RPC.hello, false, errPayload('proto_version_mismatch', `hub=${PROTOCOL_VERSION} node=${ver}`));
              socket.close(4402, 'proto_version_mismatch');
              return;
            }
            clearTimeout(helloTimer);
            session.helloDone = true;
            registry.add({ nodeId: session.nodeId, nodeName: session.nodeName, ws: socket, alive: true });
            sendRes(env.id, RPC.hello, true, {
              hubId: identity.hubId, protoVer: PROTOCOL_VERSION,
              serverTime: Date.now(), onlineNodes: registry.onlineNodes(),
            });
            registry.broadcast(EVT.nodeOnline, { nodeId: session.nodeId, nodeName: session.nodeName, at: Date.now() });
            return;
          }
          case RPC.ping: {
            const ts = (env.payload as { ts: number } | null)?.ts ?? 0;
            sendRes(env.id, RPC.ping, true, { ts, serverTime: Date.now() });
            return;
          }
          default:
            sendRes(env.id, env.method, false, errPayload('unknown_method', env.method));
        }
      });

      // keepalive：服务端 ping，两周期无 pong → terminate（半开判死 → node.offline）
      const ka = setInterval(() => {
        if (!session.helloDone) return;
        const conn = registry.get(session.nodeId);
        if (!conn) return;
        if (!conn.alive) { socket.terminate(); return; }
        conn.alive = false;
        socket.ping();
      }, keepaliveIntervalMs);
      ka.unref?.();
      socket.on('pong', () => { const c = registry.get(session.nodeId); if (c) c.alive = true; });

      socket.on('close', () => {
        clearTimeout(helloTimer);
        clearInterval(ka);
        registry.remove(session.nodeId);
        if (session.helloDone) {
          registry.broadcast(EVT.nodeOffline, { nodeId: session.nodeId, at: Date.now() });
        }
      });
    });
  });

  return {
    async listen(port, host = '0.0.0.0') {
      await app.listen({ port, host });
      const addr = app.server.address();
      const actualPort = typeof addr === 'object' && addr ? addr.port : port;
      return `http://${host === '0.0.0.0' ? '127.0.0.1' : host}:${actualPort}`;
    },
    async close() { await app.close(); },
    onlineNodes: () => registry.onlineNodes(),
  };
}
