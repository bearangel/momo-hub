// 协议版本纪律：semver；不兼容变更 bump major（重打 tag、studio 跟进 pin）。
// 1.6.0（M4.2 多组终态）：view 订阅组锚定（订阅记录存显式 groupId +
//  view.appended 推送透传 groupId?）+ TaskRecord 增可选 groupId?（delegations
//  .group_id 既有列直接投影，零迁移）——全部为可选字段，无新 RPC 方法、无
//  METHOD_REQUIRED_MINOR 门槛登记（未知字段剥除对旧节点天然宽容，1.5 peer 调
//  任何已登记方法不受影响），major 不动。
// 1.5.0（M3.4 资源生命周期）：快照引用 SnapshotResourceRef.version?（B13 版本钉死，可选
// 透传）+ 目录 HubCatalog.revision（B10 持久化修订号）+ resource.unpublished 撤回事件（B12）
// ——全部为可选字段/事件，无新 RPC 方法、无 METHOD_REQUIRED_MINOR 门槛登记（事件与字段对
// 旧节点天然宽容：未知事件忽略、未知字段剥除），major 不动。
// 1.4.0（M3.2 远端可见性）：view.workspaces（远端空间清单）/ view.files（只读
// 文件树）两 RPC + 全部 view.* 请求可选 groupId（缺省 commonGroupId 旧路径）+
// view.files 限额常量——全部向后兼容新增（MC7：未知 method/字段容忍已验，major 不动）。
// 1.3.0（M3.1 HITL L2 远程代答）：task.answer RPC + waiting 一等态 + 决策列四字段
//  + task.answered 事件 + not_waiting/node_unsupported/answer_timeout 三错误码——
//  全部向后兼容新增（MC7：未知 method/字段容忍已验，major 不动）。
// 1.2.0（M2 工作组）：组/镜像/委派/查看 RPC 与事件、四错误码、快照 manifest 校验——
//  全部向后兼容新增（MC7：未知 method/字段容忍已验，major 不动）。
export const PROTOCOL_VERSION = '1.6.0';

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
