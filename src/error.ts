// res.ok=false 时的 payload 形状（错误码是 wire 契约，只增不改义）。
export type RpcErrorCode =
  | 'unauthorized'
  | 'bad_request'
  | 'proto_version_mismatch'
  | 'unknown_method'
  | 'internal';

export interface RpcErrorPayload { code: RpcErrorCode; message: string }
