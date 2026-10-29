// RPC 方法目录与载荷类型（ws 控制面）。enroll 走 HTTP 数据面（Task 6 的 POST /enroll），
// 其 wire 类型放这里供两端共享。
// 1.2.0（M2）：目录扩为命名空间合并——group/workspace（镜像）/task（委派）/view（远程查看），
// 各域载荷类型在对应域文件；hello/ping 既有扁平项不动（wire 兼容）。
import { GROUP_RPC, WORKSPACE_RPC } from './groups';
import { TASK_RPC } from './delegation';
import { VIEW_RPC, VIEW_WIRE } from './remote-view';

export const RPC = {
  hello: 'hello',
  ping: 'ping',
  group: GROUP_RPC,
  workspace: WORKSPACE_RPC,
  task: TASK_RPC,
  view: VIEW_RPC,
} as const;

/**
 * 方法最低 minor 门槛表（M3.0 §3.1 能力协商）：键为 ws RPC 方法名，值为调用该方法
 * 所需的最低协议 minor。M3.0 将现存全量方法（含 view.consent/view.tail 私有上行）
 * 登记为 2（1.2 基线）；未登记者同样视为基线 2。M3.1+ 新增方法在此登记其所需 minor，
 * hub 分发层据此对低版本节点返回 unsupported_method（可读降级）而非直接执行。
 */
export const METHOD_REQUIRED_MINOR: Record<string, number> = Object.fromEntries(
  [
    RPC.hello,
    RPC.ping,
    ...Object.values(RPC.group),
    ...Object.values(RPC.workspace),
    ...Object.values(RPC.task),
    ...Object.values(RPC.view),
    VIEW_WIRE.consent,
    VIEW_WIRE.tail,
  ].map((method) => [method, 2]),
);
// M3.1（1.3.0）：task.answer 为首个 minor-3 方法——fromEntries 基线 2 之上显式
// 覆盖。1.2 客户端调它由 hub 分发守卫回 unsupported_method（可读降级，载荷带
// {method, requiredMinor:3}）；hub 转发前同样按目标 worker 连接的 peerProtoVer
// 复核门槛（delegations.handleAnswer——低于 3 的 worker 不懂 task.answer 转发）。
METHOD_REQUIRED_MINOR[RPC.task.answer] = 3;
// M3.2（1.4.0）：view.workspaces/view.files 为 minor-4 方法（task.answer 显式
// 覆盖同模式）——1.3 客户端调它由分发守卫回 unsupported_method{method,
// requiredMinor:4} 可读降级；既有 view 六方法仍为基线 2（groupId 可选字段
// 缺省不触发门槛，旧调用零变化）。
// M3.4（1.5.0）：无新 RPC 方法、无门槛登记——本组全部为可选字段/事件
// （SnapshotResourceRef.version? / HubCatalog.revision / resource.unpublished），
// 对旧节点天然宽容（未知事件忽略、未知字段剥除），无需能力协商。
// M4.2（1.6.0）：同上零登记——view 订阅组锚定与 TaskRecord.groupId 全部为
// 可选字段，无新方法；1.5 peer 调任何已登记方法不受影响（全表最大门槛仍为 4）。
// M4.3（1.7.0）：同上零登记——DelegationTask.dependsOn 纯可选字段（委派依赖
// 元数据透传），无新方法；1.6 peer 调任何已登记方法不受影响（全表最大门槛仍为 4）。
METHOD_REQUIRED_MINOR[RPC.view.workspaces] = 4;
METHOD_REQUIRED_MINOR[RPC.view.files] = 4;

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
