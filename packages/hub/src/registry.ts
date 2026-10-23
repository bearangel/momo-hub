// 在线节点注册表：nodeId → 连接。广播 evt 信封。
import type { WebSocket } from 'ws';
import { encodeEnvelope, type EventEnvelope } from 'momo-hub-protocol';

export interface NodeConn { nodeId: string; nodeName: string; ws: WebSocket; alive: boolean }

export class NodeRegistry {
  private byId = new Map<string, NodeConn>();

  add(conn: NodeConn): void { this.byId.set(conn.nodeId, conn); }
  remove(nodeId: string): NodeConn | undefined { const c = this.byId.get(nodeId); this.byId.delete(nodeId); return c; }
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
