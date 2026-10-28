// 协议版本纪律：semver；不兼容变更 bump major（重打 tag、studio 跟进 pin）。
// 1.2.0（M2 工作组）：组/镜像/委派/查看 RPC 与事件、四错误码、快照 manifest 校验——
// 全部向后兼容新增（MC7：未知 method/字段容忍已验，major 不动）。
export const PROTOCOL_VERSION = '1.2.0';

export function majorOf(v: string): number | null {
  const m = /^(\d+)\./.exec(v);
  return m ? Number(m[1]) : null;
}

/** minor 提取：取 `major.minor` 的次段；无该段（如 `'1'`、`'x'`）返回 null。 */
export function minorOf(v: string): number | null {
  const m = /^(\d+)\.(\d+)/.exec(v);
  return m ? Number(m[2]) : null;
}

/** major 相等即兼容（minor/patch 向前兼容由「未知字段容忍」保证）。 */
export function isCompatible(client: string, server: string): boolean {
  const a = majorOf(client);
  const b = majorOf(server);
  return a !== null && b !== null && a === b;
}

/**
 * 能力门槛判定（M3.0 §3.1 语义分层）：minor = 向后兼容的新增（新方法、新可选字段），
 * 同一 major 内 minor 越高能力越多。peer 与本协议包 major 相等且 peer minor ≥
 * requiredMinor 即视为支持该能力；任何非法版本串（major/minor 解析失败）一律 false。
 */
export function protocolSupports(peerVersion: string, requiredMinor: number): boolean {
  const peerMajor = majorOf(peerVersion);
  const peerMinor = minorOf(peerVersion);
  const selfMajor = majorOf(PROTOCOL_VERSION);
  return peerMajor !== null && peerMinor !== null && selfMajor !== null
    && peerMajor === selfMajor && peerMinor >= requiredMinor;
}
