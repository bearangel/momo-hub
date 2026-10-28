// res.ok=false 时的 payload 形状（错误码是 wire 契约，只增不改义）。
// M2 新增四码（spec 2026-10-24 M2 §3.3）：forbidden（非 leader/查看被关/非本组成员）、
// node_offline（定向投递目标离线）、not_found（taskId/snapshotId/sessionId 无效）、
// conflict（镜像覆盖两段式确认）。
// M3.1 新增三码（spec 2026-10-28 §3.1）：not_waiting（代答时任务非等待态）、
// node_unsupported（定向目标协议 minor 低于方法门槛，载荷 message 带目标版本）、
// answer_timeout（代答转发超时——账本不动，可重试）。
export type RpcErrorCode =
  | 'unauthorized'
  | 'bad_request'
  | 'proto_version_mismatch'
  | 'unknown_method'
  | 'unsupported_method'
  | 'internal'
  | 'forbidden'
  | 'node_offline'
  | 'not_found'
  | 'conflict'
  | 'not_waiting'
  | 'node_unsupported'
  | 'answer_timeout';

export interface RpcErrorPayload {
  code: RpcErrorCode;
  message: string;
  /** unsupported_method 专用：被拒方法名。可选字段，接收端忽略未知字段（向后兼容）。 */
  method?: string;
  /** unsupported_method 专用：该方法所需最低协议 minor。可选字段，同上向后兼容。 */
  requiredMinor?: number;
}

/** 错误码全集（运行时成员校验用，T13——透传方按表校验未知码；与上方 union 一一对应，只增不改义）。 */
export const RPC_ERROR_CODES: readonly RpcErrorCode[] = [
  'unauthorized',
  'bad_request',
  'proto_version_mismatch',
  'unknown_method',
  'unsupported_method',
  'internal',
  'forbidden',
  'node_offline',
  'not_found',
  'conflict',
  'not_waiting',
  'node_unsupported',
  'answer_timeout',
];
