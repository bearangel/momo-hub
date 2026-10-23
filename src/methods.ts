// RPC 方法目录与载荷类型（ws 控制面）。enroll 走 HTTP 数据面（Task 6 的 POST /enroll），
// 其 wire 类型放这里供两端共享。
export const RPC = { hello: 'hello', ping: 'ping' } as const;
export type RpcMethodName = (typeof RPC)[keyof typeof RPC];

export interface HelloReq { protoVer: string; nodeName?: string }
/** onlineNodes：hello 成功时的在线节点快照（后续增量走 node.online/offline 事件） */
export interface HelloRes { hubId: string; protoVer: string; serverTime: number; onlineNodes: Array<{ nodeId: string; nodeName: string }> }

export interface PingReq { ts: number }
export interface PongRes { ts: number; serverTime: number }

export interface EnrollReq { enrollmentToken: string; nodeName: string }
export interface EnrollRes { nodeId: string; deviceToken: string }
