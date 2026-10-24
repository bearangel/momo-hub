// 协议版本纪律：semver；不兼容变更 bump major（重打 tag、studio 跟进 pin）。
// 1.2.0（M2 工作组）：组/镜像/委派/查看 RPC 与事件、四错误码、快照 manifest 校验——
// 全部向后兼容新增（MC7：未知 method/字段容忍已验，major 不动）。
export const PROTOCOL_VERSION = '1.2.0';

export function majorOf(v: string): number | null {
  const m = /^(\d+)\./.exec(v);
  return m ? Number(m[1]) : null;
}

/** major 相等即兼容（minor/patch 向前兼容由「未知字段容忍」保证）。 */
export function isCompatible(client: string, server: string): boolean {
  const a = majorOf(client);
  const b = majorOf(server);
  return a !== null && b !== null && a === b;
}
