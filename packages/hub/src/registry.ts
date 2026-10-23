// 在线节点注册表：nodeId → 连接。广播 evt 信封。
import type { WebSocket } from 'ws';
import { encodeEnvelope, type EventEnvelope } from 'momo-hub-protocol';

export interface NodeConn { nodeId: string; nodeName: string; ws: WebSocket; alive: boolean }

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
    for (const c of this.byId.values()) c.ws.send(raw);
  }
}