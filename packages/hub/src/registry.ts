// 在线节点注册表：nodeId → 连接。广播 evt 信封。
import type { WebSocket } from 'ws';
import { encodeEnvelope, type Envelope, type EventEnvelope } from 'momo-hub-protocol';

/** peerProtoVer：hello 上报的对端协议版本（会话内存态，随连接生灭；M3.1 起
 *  代答转发按目标连接版本复核方法门槛——delegations.handleAnswer）。 */
export interface NodeConn { nodeId: string; nodeName: string; ws: WebSocket; alive: boolean; peerProtoVer: string }

export class NodeRegistry {
  private byId = new Map<string, NodeConn>();

  add(conn: NodeConn): void { this.byId.set(conn.nodeId, conn); }
  /**
   * 身份删除：仅当注册表现存条目就是这条 ws 才摘除并返回。
   * 同 nodeId 的第二条连接（pre-hello 升级重试断开、重连后旧 socket 迟到的 close）
   * 无权摘除存活连接的条目——返回 undefined 供调用方据此判定「本次断开不改变在线态」。
   */
  remove(nodeId: string, ws: WebSocket): NodeConn | undefined {
    const c = this.byId.get(nodeId);
    if (!c || c.ws !== ws) return undefined;
    this.byId.delete(nodeId);
    return c;
  }
  get(nodeId: string): NodeConn | undefined { return this.byId.get(nodeId); }
  onlineNodes(): Array<{ nodeId: string; nodeName: string }> {
    return [...this.byId.values()].map((c) => ({ nodeId: c.nodeId, nodeName: c.nodeName }));
  }
  broadcast(method: string, payload: unknown): void {
    const evt: EventEnvelope = { v: 1, kind: 'evt', method, payload };
    const raw = encodeEnvelope(evt);
    // 单条连接发送失败不得中断其余连接的广播（死 socket 不拖垮全场）
    for (const c of this.byId.values()) {
      try {
        c.ws.send(raw);
      } catch {
        // 连接已断：忽略——它会在 close 时被摘除
      }
    }
  }

  /**
   * 定向投递（M2 spec §3.1）。语义（M2c sendTo 收口裁定 ①，上批评审 binding）：
   * - 注册表查无目标 → 'offline'；
   * - ws.send 走 callback 形态（ws 8：cb(err) 于 nextTick 回报）——CLOSING/CLOSED 等
   *   「同步不抛、静默丢弃」的连接状态由回调 err 判 'offline'，杜绝向僵尸连接报 'sent'
   *   的假阳性（委派链 M2d 同受益）；排队成功 → 'sent'。
   * - 'sent' 仅代表已交付 socket 发送缓冲；对端进程崩溃等更深的丢失由上层协议兜底
   *   （镜像两段式回执超时 / 委派进度对账）。
   * 入参信封不限 evt——镜像链路的 mirror.apply（req）/ mirror.result（res）同走此口。
   */
  sendTo(nodeId: string, env: Envelope): Promise<'sent' | 'offline'> {
    const c = this.byId.get(nodeId);
    if (!c) return Promise.resolve('offline');
    return new Promise((resolve) => {
      try {
        c.ws.send(encodeEnvelope(env), (err?: Error) => { resolve(err ? 'offline' : 'sent'); });
      } catch {
        resolve('offline'); // 同步抛（极端：socket 已销毁）——与回调 err 同判
      }
    });
  }
}