// res.ok=false 时的 payload 形状（错误码是 wire 契约，只增不改义）。
// M2 新增四码（spec 2026-10-24 M2 §3.3）：forbidden（非 leader/查看被关/非本组成员）、
// node_offline（定向投递目标离线）、not_found（taskId/snapshotId/sessionId 无效）、
// conflict（镜像覆盖两段式确认）。
export type RpcErrorCode =
  | 'unauthorized'
  | 'bad_request'
  | 'proto_version_mismatch'
  | 'unknown_method'
  | 'internal'
  | 'forbidden'
  | 'node_offline'
  | 'not_found'
  | 'conflict';

export interface RpcErrorPayload { code: RpcErrorCode; message: string }
