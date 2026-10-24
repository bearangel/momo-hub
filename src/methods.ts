// RPC 方法目录与载荷类型（ws 控制面）。enroll 走 HTTP 数据面（Task 6 的 POST /enroll），
// 其 wire 类型放这里供两端共享。
// 1.2.0（M2）：目录扩为命名空间合并——group/workspace（镜像）/task（委派）/view（远程查看），
// 各域载荷类型在对应域文件；hello/ping 既有扁平项不动（wire 兼容）。
import { GROUP_RPC, WORKSPACE_RPC } from './groups';
import { TASK_RPC } from './delegation';
import { VIEW_RPC } from './remote-view';

export const RPC = {
  hello: 'hello',
  ping: 'ping',
  group: GROUP_RPC,
  workspace: WORKSPACE_RPC,
  task: TASK_RPC,
  view: VIEW_RPC,
} as const;

export type RpcMethodName =
  | 'hello'
  | 'ping'
  | (typeof GROUP_RPC)[keyof typeof GROUP_RPC]
  | (typeof WORKSPACE_RPC)[keyof typeof WORKSPACE_RPC]
  | (typeof TASK_RPC)[keyof typeof TASK_RPC]
  | (typeof VIEW_RPC)[keyof typeof VIEW_RPC];

export interface HelloReq { protoVer: string; nodeName?: string }
/** onlineNodes：hello 成功时的在线节点快照（后续增量走 node.online/offline 事件） */
export interface HelloRes { hubId: string; protoVer: string; serverTime: number; onlineNodes: Array<{ nodeId: string; nodeName: string }> }

export interface PingReq { ts: number }
export interface PongRes { ts: number; serverTime: number }

export interface EnrollReq { enrollmentToken: string; nodeName: string }
export interface EnrollRes { nodeId: string; deviceToken: string }
