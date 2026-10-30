// 工作组与镜像契约（spec 2026-10-24 M2 §3.1/§3.2 + 裁定 MC2）——
// 组管理六 RPC（node→hub 直答）+ 快照存档/镜像 RPC（leader→hub→目标节点）+
// 快照 manifest 结构校验（S1 同款纪律：字段类型/长度/未知容忍）。
// 快照构建上传（workspace.snapshot.build）走 HTTP 数据面，只定义 wire 类型不入 ws 目录。
import { isValidHubSlug, isValidHubVersion, type HubResourceType } from './resources';

/** 组管理 RPC 目录（wire 契约，只增不改义）。 */
export const GROUP_RPC = {
  create: 'group.create',
  list: 'group.list',
  invite: 'group.invite',
  join: 'group.join',
  leave: 'group.leave',
  transfer: 'group.transfer',
} as const;

/** 镜像 RPC 目录（workspace.snapshot.build 走 HTTP multipart，不在此列）。 */
export const WORKSPACE_RPC = {
  mirror: 'workspace.mirror',
  snapshots: 'workspace.snapshots',
} as const;

// ────────────────────────────────────────────────────────────────────────────
// 组管理载荷与响应（§3.1「工作组管理」表逐行）
// ────────────────────────────────────────────────────────────────────────────

export interface GroupCreateReq { groupName: string }
/** 创建者即 leader；nodeId 回调用者自身。 */
export interface GroupCreateRes { groupId: string; nodeId: string }

/** 空请求载荷（spec §3.1「—」）——调用方发 `{}` 占位。 */
export type GroupListReq = Record<string, never>;

export interface GroupListRes { groups: GroupInfo[] }

export interface GroupInfo {
  groupId: string;
  groupName: string;
  leaderNodeId: string;
  members: GroupMember[];
}

export interface GroupMember {
  nodeId: string;
  name: string;
  online: boolean;
  /** hello 上报的协议版本（M5.3，1.9.0）：在线成员恒带（registry 会话内存态，随
   *  连接生灭）；离线成员不带键（旧形状零变化）——studio 派发闸 / routingPrecheck
   *  nodeCaps（nodeId/name/protoVer）的数据源。 */
  protoVer?: string;
}

export interface GroupInviteReq { groupId: string; enrollmentToken?: string }
/** group.invite 响应：组邀请码——独立于 hub enrollment token（作用域=单组，可撤销）。 */
export interface InviteInfo { inviteCode: string }

export interface GroupJoinReq { inviteCode: string }
export interface GroupJoinRes { groupId: string }

export interface GroupLeaveReq { groupId: string }
export interface GroupLeaveRes { ok: boolean }

/** leader 转移（仅 leader 可调）。 */
export interface GroupTransferReq { groupId: string; toNodeId: string }
export interface GroupTransferRes { ok: boolean }

// ────────────────────────────────────────────────────────────────────────────
// 镜像载荷与响应（§3.1「镜像」表逐行 + §5.2 两段式确认）
// ────────────────────────────────────────────────────────────────────────────

/** POST /workspaces/snapshots 响应（数据面；checksum 服务端自算——C6 纪律）。 */
export interface SnapshotUploadRes { snapshotId: string; checksum: string }

export interface WorkspaceSnapshotsReq { workspaceName?: string }
export interface WorkspaceSnapshotsRes { snapshots: WorkspaceSnapshotMeta[] }

/** 快照元数据（hub 存档目录；可回放旧版 = workspace.mirror 引旧 snapshotId）。 */
export interface WorkspaceSnapshotMeta {
  snapshotId: string;
  workspaceName: string;
  version: string;
  byNode: string;
  at: number;
}

/** 镜像策略：M2 只做整体替换（增量合并属 YAGNI，§9）。 */
export type MirrorStrategy = 'replace';

export interface MirrorRequest {
  groupId: string;
  targetNodeId: string;
  workspaceName: string;
  snapshotId: string;
  strategy: MirrorStrategy;
  /** 两段式确认（§5.2）：目标工作空间已存在且未 confirm → hub 返 conflict + 差异摘要；confirm=true 直落。 */
  confirm?: boolean;
}

/** workspace.mirror 结果（应用在目标节点执行，hub 中转回执汇总）。
 *
 * @deprecated 两段式落地（T6 + spec §3.1 收口）后该同步形态不再产出：第一段响应改用
 *  {@link MirrorAcceptRes}（`{accepted:true, mirrorId}`），终态载荷改用
 *  {@link MirrorResultPayload}（mirror.result res + workspace.mirrored 事件）。
 *  保留仅为协议兼容占位，勿在新代码消费。 */
export interface MirrorResult { ok: boolean; appliedResources: string[] }

/** conflict 附带差异摘要（目标节点实况 vs 快照：新增/移除/变更计数）。 */
export interface MirrorDiffSummary { added: number; removed: number; changed: number }

// ────────────────────────────────────────────────────────────────────────────
// 镜像私有 wire（spec §3.1 两段式裁量 + §5.2/plan T8 目标端语义）——
// 不进 RPC 目录（ws 控制面公开方法），仅供 hub 与 studio 目标端/leader 端消费。
// ────────────────────────────────────────────────────────────────────────────

/** 镜像链路私有方法名（信封 method 字段值）。 */
export const MIRROR_WIRE = {
  /** hub → 目标节点：应用请求（req 信封，id=mirrorId——id 即对账键）。 */
  apply: 'mirror.apply',
  /** 目标节点 → hub：应用回执（res 信封，id=mirrorId；成功载荷 MirrorReceiptOk，
   *  conflict 失败载荷 = RpcErrorPayload + diffSummary 超集）。 */
  receipt: 'mirror.receipt',
  /** hub → leader：两段式第二段结果（res 信封，id=mirrorId，定向投递）。 */
  result: 'mirror.result',
} as const;

/** mirror.apply 请求信封载荷（目标节点据此应用：快照经 HTTP 数据面取，配置整体替换）。 */
export interface MirrorApplyPayload {
  mirrorId: string;
  snapshotId: string;
  workspaceName: string;
  strategy: MirrorStrategy;
  /** 两段式确认（§5.2）：目标工作空间已存在且未 confirm → 回执 conflict + 差异摘要。 */
  confirm?: boolean;
}

/** workspace.mirror 第一段响应（hub 收到即返——异步两段式的受理回执）。 */
export interface MirrorAcceptRes { accepted: boolean; mirrorId: string }

/** mirror.receipt 成功回执载荷（目标节点已应用完毕）。unresolvedRefs = 目标无法
 *  解析的引用（软失败——T8b 应用器语义），hub 原样透传 leader/组内广播。 */
export interface MirrorReceiptOk { appliedResources: string[]; unresolvedRefs?: string[] }

/** mirror.result 第二段载荷：成功 / 失败（conflict 时附差异摘要；code 复用 RpcErrorCode）。 */
export type MirrorResultPayload =
  | { mirrorId: string; appliedResources: string[]; unresolvedRefs?: string[] }
  | { mirrorId: string; code: string; message: string; diffSummary?: MirrorDiffSummary };

// ────────────────────────────────────────────────────────────────────────────
// 快照 manifest（上传的 manifest part；payload part 为 tar.gz ≤200MB）
// ────────────────────────────────────────────────────────────────────────────

/** 快照 manifest：workspaceName/version + 资源引用清单 + 配置段（§5.2 构建清单的结构层）。 */
export interface SnapshotManifest {
  workspaceName: string;
  version: string;
  /** 资源引用清单：目标节点补装依据（缺失 slug → M1 资源仓安装，先装后配）。 */
  resources: SnapshotResourceRef[];
  /** 配置段：红线（无 provider key/无 url 凭证头/无聊天历史）由构建器侧单测锁，hub 只做结构校验。 */
  config: WorkspaceSnapshotConfig;
}

/** 资源引用条目：M1 资源仓 slug（内嵌 custom 定义走 config.agentMembers，不占引用位）。
 *  version?（1.5.0 B13）：版本钉死——在场时目标节点按该版本安装（而非 latest）；
 *  缺省合法（1.4 旧 manifest 零变化，仍走 latest 语义）。 */
export interface SnapshotResourceRef {
  type: HubResourceType;
  slug: string;
  version?: string;
}

/** 配置段：§5.2 全清单（agent 定义/成员/团队/MCP/skill/插件/provider 引用/偏好）。 */
export interface WorkspaceSnapshotConfig {
  agentMembers: SnapshotAgentMember[];
  teams: SnapshotTeam[];
  /** MCP 启用清单——仅 stdio 引用名（红线：不含 url 凭证头）。 */
  mcpEnabled: string[];
  /** skill 清单（slug）。 */
  skills: string[];
  /** 插件启用清单：插件 id + hub slug 引用。 */
  pluginsEnabled: SnapshotPluginRef[];
  /** provider 引用：类型 + 型号偏好（红线：不含 key）。 */
  providerRefs: SnapshotProviderRef[];
  /**
   * 工作空间默认模型绑定（可选段——D4/T16b）：缺省 = 源节点未设默认模型（1.2.0
   * 旧快照无此段照常镜像）；在场时目标节点据此落默认模型偏好（不覆盖本机显式选择）。
   */
  modelBinding?: SnapshotModelBinding;
  /** 工具上限等偏好（标量值）。 */
  prefs: Record<string, string | number | boolean>;
}

/** agent 成员：资源 slug 引用或内嵌 custom 定义（§5.2 二选一；结构层不强制互斥）。 */
export interface SnapshotAgentMember {
  name: string;
  slug?: string;
  customDefinition?: string;
}

export interface SnapshotTeam { name: string; leader: string; members: string[] }

export interface SnapshotPluginRef { pluginId: string; slug: string }

export interface SnapshotProviderRef { provider: string; model?: string }

/**
 * 工作空间默认模型绑定（D4/T16b 可选段，1.2.0 向后兼容）：源节点「设置 → 默认模型」
 * 的全局默认对话模型（defaultChatModel）。provider 为 platform 形态（与 providerRefs
 * 同口径，跨节点可解析）；model 非空（空绑定无意义，构建侧缺省不发段）；
 * 不含 key（红线，key 永在 keychain）。
 */
export interface SnapshotModelBinding { provider: string; model: string }

// ────────────────────────────────────────────────────────────────────────────
// validateSnapshotManifest（S1/C6 同款纪律：字段类型/长度/未知容忍/DoS 下限）
// ────────────────────────────────────────────────────────────────────────────

const WORKSPACE_NAME_MAX = 128; // S1 NAME_MAX 同款
const SLUG_MAX = 64; // S1 SLUG_MAX 同款（resources.ts 未导出，同值拷贝）
const RESOURCE_REFS_MAX = 512;
const AGENT_MEMBERS_MAX = 256;
const TEAMS_MAX = 128;
const NAME_LIST_MAX = 512; // mcpEnabled / skills / team members 等字符串清单
const PLUGINS_MAX = 256;
const PROVIDERS_MAX = 64;
const PREFS_MAX = 64;
const PROVIDER_NAME_MAX = 64;
/** 模型名上限（modelBinding.model——覆盖 glm-5.3 / gpt-4o / 自定义兼容端点名等形态） */
const MODEL_NAME_MAX = 200;
const CUSTOM_DEFINITION_MAX_BYTES = 64 * 1024; // S1 README_MAX_BYTES 同款

const isStr = (x: unknown): x is string => typeof x === 'string';

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function isBoundedStr(x: unknown, max: number): x is string {
  return isStr(x) && x.length > 0 && x.length <= max;
}

const textEncoder = new TextEncoder();

/** strList 逐项长度上限（DoS 下限：防单条超长字符串灌入清单位）。 */
const STR_LIST_ITEM_MAX = 128;

function strListFrom(x: unknown, max: number): string[] | null {
  if (!Array.isArray(x) || x.length > max || !x.every((s) => isStr(s) && s.length <= STR_LIST_ITEM_MAX)) return null;
  return [...x];
}

function isResourceType(x: unknown): x is HubResourceType {
  return x === 'agent' || x === 'mcp' || x === 'skill' || x === 'plugin';
}

/** slug 引用完整校验：S1 白名单 + 长度上限。 */
function isBoundedSlug(x: unknown): x is string {
  return isStr(x) && isValidHubSlug(x) && x.length <= SLUG_MAX;
}

function resourceRefsFrom(raw: unknown): SnapshotResourceRef[] | null {
  if (!Array.isArray(raw) || raw.length > RESOURCE_REFS_MAX) return null;
  const out: SnapshotResourceRef[] = [];
  for (const item of raw) {
    if (!isRecord(item) || !isResourceType(item.type)) return null;
    if (!isBoundedSlug(item.slug)) return null;
    // version?（1.5.0）：缺省合法（1.4 旧 manifest 零变化）；在场必须过 S1 白名单
    // （version 会拼进下载路径，与顶层 version 同强度校验）。
    if (item.version !== undefined && (!isStr(item.version) || !isValidHubVersion(item.version))) return null;
    const ref: SnapshotResourceRef = { type: item.type, slug: item.slug };
    if (item.version !== undefined) ref.version = item.version;
    out.push(ref);
  }
  return out;
}

function agentMembersFrom(raw: unknown): SnapshotAgentMember[] | null {
  if (!Array.isArray(raw) || raw.length > AGENT_MEMBERS_MAX) return null;
  const out: SnapshotAgentMember[] = [];
  for (const item of raw) {
    if (!isRecord(item) || !isBoundedStr(item.name, WORKSPACE_NAME_MAX)) return null;
    if (item.slug !== undefined && !isBoundedSlug(item.slug)) return null;
    if (
      item.customDefinition !== undefined &&
      (!isStr(item.customDefinition) || textEncoder.encode(item.customDefinition).length > CUSTOM_DEFINITION_MAX_BYTES)
    )
      return null;
    const member: SnapshotAgentMember = { name: item.name };
    if (item.slug !== undefined) member.slug = item.slug;
    if (item.customDefinition !== undefined) member.customDefinition = item.customDefinition;
    out.push(member);
  }
  return out;
}

function teamsFrom(raw: unknown): SnapshotTeam[] | null {
  if (!Array.isArray(raw) || raw.length > TEAMS_MAX) return null;
  const out: SnapshotTeam[] = [];
  for (const item of raw) {
    if (!isRecord(item) || !isBoundedStr(item.name, WORKSPACE_NAME_MAX) || !isBoundedStr(item.leader, WORKSPACE_NAME_MAX))
      return null;
    const members = strListFrom(item.members, NAME_LIST_MAX);
    if (members === null) return null;
    out.push({ name: item.name, leader: item.leader, members });
  }
  return out;
}

function pluginsFrom(raw: unknown): SnapshotPluginRef[] | null {
  if (!Array.isArray(raw) || raw.length > PLUGINS_MAX) return null;
  const out: SnapshotPluginRef[] = [];
  for (const item of raw) {
    if (!isRecord(item) || !isBoundedStr(item.pluginId, WORKSPACE_NAME_MAX)) return null;
    if (!isBoundedSlug(item.slug)) return null;
    out.push({ pluginId: item.pluginId, slug: item.slug });
  }
  return out;
}

function providersFrom(raw: unknown): SnapshotProviderRef[] | null {
  if (!Array.isArray(raw) || raw.length > PROVIDERS_MAX) return null;
  const out: SnapshotProviderRef[] = [];
  for (const item of raw) {
    if (!isRecord(item) || !isBoundedStr(item.provider, PROVIDER_NAME_MAX)) return null;
    if (item.model !== undefined && !isStr(item.model)) return null;
    const ref: SnapshotProviderRef = { provider: item.provider };
    if (item.model !== undefined) ref.model = item.model;
    out.push(ref);
  }
  return out;
}

/** modelBinding 可选段：缺省（undefined）合法——1.2.0 旧快照向后兼容；
 *  在场则 provider/model 均非空有界串，非法整包 null（与七子段同强度）。 */
function modelBindingFrom(raw: unknown): SnapshotModelBinding | undefined | null {
  if (raw === undefined) return undefined;
  if (!isRecord(raw) || !isBoundedStr(raw.provider, PROVIDER_NAME_MAX)) return null;
  if (!isBoundedStr(raw.model, MODEL_NAME_MAX)) return null;
  return { provider: raw.provider, model: raw.model };
}

function prefsFrom(raw: unknown): Record<string, string | number | boolean> | null {
  if (!isRecord(raw)) return null;
  const entries = Object.entries(raw);
  if (entries.length > PREFS_MAX) return null;
  const out: Record<string, string | number | boolean> = {};
  for (const [key, value] of entries) {
    if (typeof value === 'number' && !Number.isFinite(value)) return null;
    if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') return null;
    out[key] = value;
  }
  return out;
}

function configFrom(raw: unknown): WorkspaceSnapshotConfig | null {
  if (!isRecord(raw)) return null;
  const agentMembers = agentMembersFrom(raw.agentMembers);
  if (agentMembers === null) return null;
  const teams = teamsFrom(raw.teams);
  if (teams === null) return null;
  const mcpEnabled = strListFrom(raw.mcpEnabled, NAME_LIST_MAX);
  if (mcpEnabled === null) return null;
  const skills = strListFrom(raw.skills, NAME_LIST_MAX);
  if (skills === null) return null;
  const pluginsEnabled = pluginsFrom(raw.pluginsEnabled);
  if (pluginsEnabled === null) return null;
  const providerRefs = providersFrom(raw.providerRefs);
  if (providerRefs === null) return null;
  const prefs = prefsFrom(raw.prefs);
  if (prefs === null) return null;
  const modelBinding = modelBindingFrom(raw.modelBinding);
  if (modelBinding === null) return null;
  const config: WorkspaceSnapshotConfig = { agentMembers, teams, mcpEnabled, skills, pluginsEnabled, providerRefs, prefs };
  if (modelBinding !== undefined) config.modelBinding = modelBinding;
  return config;
}

/** 快照 manifest 结构校验（对齐 validateHubManifest 强度）：顶层 workspaceName/version
 *  过长度与 S1 白名单、resources 引用清单逐项校验、config 配置段七子段 + 可选
 *  modelBinding 段逐项校验；未知字段容忍（剥除）；任何已知字段缺失/类型不符返回 null。 */
export function validateSnapshotManifest(raw: unknown): SnapshotManifest | null {
  if (!isRecord(raw)) return null;
  if (!isBoundedStr(raw.workspaceName, WORKSPACE_NAME_MAX)) return null;
  if (!isStr(raw.version) || !isValidHubVersion(raw.version)) return null;
  const resources = resourceRefsFrom(raw.resources);
  if (resources === null) return null;
  const config = configFrom(raw.config);
  if (config === null) return null;
  return { workspaceName: raw.workspaceName, version: raw.version, resources, config };
}
