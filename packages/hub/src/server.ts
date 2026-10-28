// hub 服务本体：Fastify(/health /enroll) + WebSocket(/ws)。
// 鉴权链：升级头 Bearer deviceToken → hello（版本协商）→ 注册广播。
// 判死：ws ping 帧 keepalive，两周期无 pong terminate → node.offline。
import fastify from 'fastify';
import websocket from '@fastify/websocket';
import type Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { WebSocket } from 'ws';
import {
  EVT, METHOD_REQUIRED_MINOR, PROTOCOL_VERSION, RPC, TASK_WIRE, VIEW_WIRE, encodeEnvelope,
  isCompatible, minorOf, parseEnvelope,
  type HelloReq, type RpcErrorPayload,
} from 'momo-hub-protocol';
import type { HubIdentity } from './identity';
import { enrollNode, verifyDeviceToken } from './auth';
import { normalizePublicOrigin } from './config';
import { NodeRegistry, type NodeConn } from './registry';
import { GROUP_HANDLERS, checkGroupRole, type GroupHandler } from './groups';
import { registerResourceRoutes } from './resources';
import { handleWorkspaceSnapshots, registerSnapshotRoutes } from './snapshots';
import { MirrorRouter } from './mirror';
import { DelegationRouter } from './delegations';
import { RemoteViewRouter, isViewRpcMethod } from './remote-view';
import { logger } from './logger';

export interface HubServer {
  listen(port: number, host?: string): Promise<string>;
  close(): Promise<void>;
  onlineNodes(): Array<{ nodeId: string; nodeName: string }>;
}

export interface CreateHubServerOpts {
  db: Database.Database;
  identity: HubIdentity;
  dataDir?: string;            // blob 存储根目录；缺省从 db 文件路径推导
  resourceMaxBytes?: number;   // publish 上限（默认 200MB；测试注入小值走同一代码路径）
  snapshotMaxBytes?: number;   // 工作空间快照上限（默认 200MB；测试注入小值走同一代码路径）
  helloTimeoutMs?: number;      // 默认 5000
  keepaliveIntervalMs?: number; // 默认 30000；判死 = 2 × interval
  viewForwardTimeoutMs?: number; // 查看转发超时注入（T13 评审 Important② 用例消费；默认 10s）
  answerForwardTimeoutMs?: number; // 代答转发超时注入（M3.1；默认 10s）
  /** 公网 origin（RM1-11）：设置后 catalog 的 downloadUrl 独占使用该 origin；
   *  非法值在此抛错（启动即失败，不静默降级）。 */
  publicOrigin?: string;
}

/** blob 存储根目录推导：优先用 db 所在目录（hub.db 与 blobs 同根），
 *  内存库（无文件路径）退化为独立临时目录——避免多实例互相踩。 */
function deriveDataDir(db: Database.Database): string {
  const name = db.name;
  if (name && name !== ':memory:' && !name.startsWith(':')) {
    return path.dirname(path.resolve(name));
  }
  const dir = path.join(os.tmpdir(), `momo-hub-${randomUUID()}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

// 会话自持有连接：conn 只认本 socket 那一条 NodeConn。
// 绝不回注册表按 nodeId 反查——同 nodeId 并存两条连接时（重连未摘旧的），
// 反查拿到的是「最后一条」连接，旧连接的 pong/心跳标记会串到新连接上造成假杀。
interface Session {
  nodeId: string;
  nodeName: string;
  helloDone: boolean;
  conn: NodeConn | null;
  /** hello 上报的对端协议版本（M3.0 能力协商：分发层 minor 门槛守卫的判定输入；内存态，不落库）。 */
  peerProtoVer: string;
}

export function createHubServer(opts: CreateHubServerOpts): HubServer {
  const { db, identity } = opts;
  const helloTimeoutMs = opts.helloTimeoutMs ?? 5000;
  const keepaliveIntervalMs = opts.keepaliveIntervalMs ?? 30_000;
  const dataDir = opts.dataDir ?? deriveDataDir(db);
  // 公网 origin 归一 + 校验：非法即启动报错（拼错的 origin 会让 downloadUrl 指向错主机）
  const publicOrigin = opts.publicOrigin === undefined ? undefined : normalizePublicOrigin(opts.publicOrigin);
  const registry = new NodeRegistry();
  // 镜像路由器（两段式 pending 登记/回执对账——server 生命周期单例，随连接清理）
  const mirrorRouter = new MirrorRouter(db, registry);
  // 委派路由器（账本状态机 + task.* RPC + 上行 report/answer 对账——同为生命周期单例）
  const delegationRouter = new DelegationRouter(db, registry, {
    ...(opts.answerForwardTimeoutMs !== undefined ? { answerTimeoutMs: opts.answerForwardTimeoutMs } : {}),
  });
  // 远程查看路由器（M2e：六方法定向转发 + consent 内存态 + 订阅登记/断开注销）
  const viewRouter = new RemoteViewRouter(db, registry, {
    ...(opts.viewForwardTimeoutMs !== undefined ? { forwardTimeoutMs: opts.viewForwardTimeoutMs } : {}),
  });
  // 存活 socket 登记：close() 据此排空 close 处理器——M2d 起离线钩子查库
  // （delegations），晚到的 close 事件若越过 close() 落在嵌入方 db.close() 之后，
  // 会以未捕获异常炸进程（index.ts 关停序 server.close()→db.close() 同暴露）。
  const liveSockets = new Set<WebSocket>();
  const app = fastify({ logger: false });
  void app.register(websocket);
  // ws RPC 分发表 = 组管理六方法 + M2c 镜像域两方法 + M2d 委派域四方法（M3.1 增
  // task.answer）+ M2e 查看域十方法（八公开 view.*〔M3.2 增 workspaces/files〕+
  // 私有上行 view.consent/view.tail）。
  // workspace.snapshots/task.list 无组角色限制不进 LEADER_ONLY_METHODS 守卫直通；
  // workspace.mirror/task.delegate/task.cancel/task.answer/view.* 八方法 leader-only
  // 由守卫层校验（task.cancel/task.answer 提取器按 taskId 查 delegations 回填
  // groupId——M2d T9 / M3.1 回填；view.* 提取器载荷 groupId 优先、缺省按
  // targetNodeId 查「leader 与 target 同组」——M2e T13 回填 + M3.2 groupId 路径）；
  // view.consent/view.tail 为节点上行（非 leader-only），不进守卫。
  const viewHandler = (method: string): GroupHandler => (ctx, payload) => viewRouter.handleView(ctx, method, payload);
  const RPC_HANDLERS: Record<string, GroupHandler> = {
    ...GROUP_HANDLERS,
    [RPC.workspace.snapshots]: handleWorkspaceSnapshots,
    [RPC.workspace.mirror]: (ctx, payload) => mirrorRouter.handleMirror(ctx, payload),
    [RPC.task.delegate]: (ctx, payload) => delegationRouter.handleDelegate(ctx, payload),
    [RPC.task.cancel]: (ctx, payload) => delegationRouter.handleCancel(ctx, payload),
    [RPC.task.list]: (ctx, payload) => delegationRouter.handleList(ctx, payload),
    [RPC.task.answer]: (ctx, payload) => delegationRouter.handleAnswer(ctx, payload),
    [RPC.view.workspace]: viewHandler(RPC.view.workspace),
    [RPC.view.sessions]: viewHandler(RPC.view.sessions),
    [RPC.view.messages]: viewHandler(RPC.view.messages),
    [RPC.view.tasks]: viewHandler(RPC.view.tasks),
    [RPC.view.subscribe]: viewHandler(RPC.view.subscribe),
    [RPC.view.unsubscribe]: viewHandler(RPC.view.unsubscribe),
    [RPC.view.workspaces]: viewHandler(RPC.view.workspaces),
    [RPC.view.files]: viewHandler(RPC.view.files),
    [VIEW_WIRE.consent]: (ctx, payload) => viewRouter.handleConsent(ctx, payload),
    [VIEW_WIRE.tail]: (ctx, payload) => viewRouter.handleTail(ctx, payload),
  };
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

    // 资源发布端点（spec M1 §3.1）——multipart 流式收包 + 校验矩阵 + 版本不可变 409。
    // multipart 插件在此注册（fastify-plugin 无封装边界）——后续快照路由共用其装饰与 limits。
    registerResourceRoutes(instance, { db, dataDir, registry, maxBytes: opts.resourceMaxBytes, publicOrigin });

    // 工作空间快照存档端点（M2c spec §3.1）——C6/C4 纪律复用，落 ws-snapshots/。
    registerSnapshotRoutes(instance, { db, dataDir, maxBytes: opts.snapshotMaxBytes });

    instance.get('/ws', { websocket: true }, (socket, req) => {
      liveSockets.add(socket);
      const auth = req.headers.authorization ?? '';
      const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
      const node = token ? verifyDeviceToken(db, token) : null;
      if (!node) {
        // 鉴权失败即摘（T9 Minor#2）：本分支 return 早于 close 处理器注册——
        // 不摘则该 socket 永驻 liveSockets（连接干净关闭也无人 delete），
        // close() 排空恒拖满 500ms 上限
        liveSockets.delete(socket);
        socket.close(4401, 'unauthorized');
        return;
      }

      const session: Session = { nodeId: node.nodeId, nodeName: node.name, helloDone: false, conn: null, peerProtoVer: '' };
      const helloTimer = setTimeout(() => socket.close(4403, 'hello_timeout'), helloTimeoutMs);

      const sendRes = (id: string, method: string, ok: boolean, payload: unknown): void => {
        socket.send(encodeEnvelope({ v: 1, kind: 'res', id, method, ok, payload }));
      };
      const errPayload = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

      socket.on('message', (raw: unknown) => {
        const env = parseEnvelope(typeof raw === 'string' ? raw : String(raw));
        if (!env) { socket.close(4400, 'bad_envelope'); return; }
        if (env.kind === 'evt') return; // 上行 evt 忽略（M0 语义不变）
        if (env.kind === 'res') {
          // 上行 res 按 method 分流：task.report → 委派上报（M2d）；task.answer →
          // 代答转发对账（M3.1，id=转发 id 关联）；view.* 八方法 → 查看转发对账
          // （M2e，id=转发 id 关联）；其余 → 镜像回执（M2c，唯一其余合法项）。
          // 未匹配（迟到/重复/伪造/未知 method）由各路由器静默忽略。
          // hello 前的 res 不可能有对账项，一并忽略。
          if (session.helloDone) {
            if (env.method === TASK_WIRE.report) delegationRouter.acceptReport(session.nodeId, env);
            else if (env.method === RPC.task.answer) delegationRouter.acceptAnswerRes(session.nodeId, env);
            else if (isViewRpcMethod(env.method)) viewRouter.acceptViewRes(session.nodeId, env);
            else mirrorRouter.acceptReceipt(session.nodeId, env);
          }
          return;
        }
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
            session.peerProtoVer = ver;
            // 注册表条目与会话持有的是同一个 NodeConn 引用：
            // ka tick / pong 只经 session.conn 改 alive，不经注册表反查。
            // peerProtoVer 随 conn 入注册表（M3.1：代答转发按目标连接版本复核门槛）。
            const conn: NodeConn = { nodeId: session.nodeId, nodeName: session.nodeName, ws: socket, alive: true, peerProtoVer: ver };
            registry.add(conn);
            session.conn = conn;
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
          default: {
            // RPC 分发（M2b 起多域方法）——leader-only 校验层为 dispatcher 外围守卫：
            // 先查分发表（未注册方法 unknown_method），命中后先过组角色守卫再进 handler。
            // handler 可为异步（mirror 需 await sendTo 的 ws 回调判定）——同 socket 的
            // 响应顺序不保证，res 按 id 关联（wire 契约本就如此）。
            const handler = RPC_HANDLERS[env.method];
            if (!handler) {
              sendRes(env.id, env.method, false, errPayload('unknown_method', env.method));
              return;
            }
            // minor 门槛守卫（M3.0 §3.1 能力协商）：入表方法低于其 requiredMinor 时以
            // unsupported_method 可读降级（载荷带 method/requiredMinor，message 含双方版本）。
            // 未登记者视为基线 2；M3.0 全表=2，1.2 节点（minorOf=2）永不触发——对现状零行为变化。
            const requiredMinor = METHOD_REQUIRED_MINOR[env.method] ?? 2;
            const peerMinor = minorOf(session.peerProtoVer);
            if (peerMinor === null || peerMinor < requiredMinor) {
              sendRes(env.id, env.method, false, {
                code: 'unsupported_method',
                message: `hub=${PROTOCOL_VERSION} 节点=${session.peerProtoVer}：方法 ${env.method} 需要协议 1.${requiredMinor} 及以上，请升级节点`,
                method: env.method,
                requiredMinor,
              });
              return;
            }
            const guard = checkGroupRole(db, session.nodeId, env.method, env.payload);
            if (!guard.ok) {
              sendRes(env.id, env.method, false, guard.error);
              return;
            }
            void (async () => {
              try {
                const out = await handler({ db, registry, nodeId: session.nodeId }, env.payload);
                sendRes(env.id, env.method, out.ok, out.ok ? out.payload : out.error);
              } catch (err) {
                logger.warn('RPC 处理异常', { method: env.method, err });
                sendRes(env.id, env.method, false, errPayload('internal', '内部错误'));
              }
            })();
            return;
          }
        }
      });

      // keepalive：服务端 ping，两周期无 pong → terminate（半开判死 → node.offline）
      const ka = setInterval(() => {
        const conn = session.conn;
        if (!conn) return;
        if (!conn.alive) { socket.terminate(); return; }
        conn.alive = false;
        socket.ping();
      }, keepaliveIntervalMs);
      ka.unref?.();
      // pong 只回写本会话自己的连接：同 nodeId 的另一条连接的 ping/pong 与此无关。
      socket.on('pong', () => { if (session.conn) session.conn.alive = true; });

      socket.on('close', () => {
        liveSockets.delete(socket);
        clearTimeout(helloTimer);
        clearInterval(ka);
        session.conn = null;
        // 仅当摘掉的注册表条目确实是本连接时才广播 node.offline：
        // 同 nodeId 的第二条连接（未 hello 的升级重试、旧 socket 迟到的 close）断开时
        // 在线态未变，注册表与广播流保持一致，不会误报离线。
        if (registry.remove(session.nodeId, socket)) {
          // M2c：受理后断连的目标节点——镜像 pending 逐个结算（登记释放 + 告知 leader）
          mirrorRouter.onNodeOffline(session.nodeId);
          // M2d：该节点非终态委派 pending/running → interrupted + 通知 leader（MC5）
          delegationRouter.onNodeOffline(session.nodeId);
          // M2e：consent 回默认（RM2-9）+ 订阅双侧注销（RF#1）+ 查看在途结算
          viewRouter.onNodeOffline(session.nodeId);
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
    async close() {
      // Node 的 server.close() 只做一次空闲连接清扫：响应刚结束、socket 还没登记进
      // 空闲表的那一瞬会被漏掉——close 回调永远不来（实测偶发复现：hub 停机与测试
      // teardown 都被拖住）。轮询清扫空闲连接：只关空闲的，不打断进行中的请求与 ws 会话。
      const closing = app.close();
      const idleCloser = setInterval(() => { app.server.closeIdleConnections(); }, 50);
      idleCloser.unref();
      try {
        await closing;
      } finally {
        clearInterval(idleCloser);
      }
      // 排空 close 处理器（上限 500ms，防个别僵死连接拖住关停）：liveSockets 清空
      // 即全部 close 处理器同步体已跑完（含查库的离线钩子），嵌入方可安全关 db。
      const drainDeadline = Date.now() + 500;
      while (liveSockets.size > 0 && Date.now() < drainDeadline) {
        await new Promise((r) => setTimeout(r, 20));
      }
    },
    onlineNodes: () => registry.onlineNodes(),
  };
}