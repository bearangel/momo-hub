// 远程查看契约（spec 2026-10-24 M2 §3.1「远程查看」表 + 裁定 MC3）——
// 拉取按 seq 分页 + 尾部订阅（view.appended 定向推）；只读，无任何写方法。
// leader→hub→目标节点定向路由；hub 与 studio 双端消费。
// 1.4.0（M3.2 远端可见性）：增 workspaces（远端空间清单）/ files（只读文件树）
//  两方法；全部 view.* 请求增可选 groupId（消 commonGroupId 多组任意取旧歧义，
//  缺省旧路径向后兼容）；附 view.files 限额常量（双端单源）。
export const VIEW_RPC = {
  workspace: 'view.workspace',
  sessions: 'view.sessions',
  messages: 'view.messages',
  tasks: 'view.tasks',
  subscribe: 'view.subscribe',
  unsubscribe: 'view.unsubscribe',
  workspaces: 'view.workspaces',
  files: 'view.files',
} as const;

/** view.* 共同可选段（M3.2）：groupId 在场时 hub 按该组精确判同组（消
 *  commonGroupId 多组任意取旧歧义）；缺省走旧路径（1.2/1.3 调用者零变化）。 */
export interface ViewGroupIdOpt { groupId?: string }

export interface ViewWorkspaceReq extends ViewGroupIdOpt { targetNodeId: string; workspaceName: string }

/** 目标节点工作空间基本信息（概览段）。 */
export interface ViewWorkspaceOverview {
  name: string;
  description: string;
  sessionCount: number;
}

export interface ViewAgentMember { name: string; agentSlug?: string }
export interface ViewTeam { name: string; leaderName: string; memberNames: string[] }
/** 资源统计：四类计数。 */
export interface ViewResourcesSummary { agent: number; mcp: number; skill: number; plugin: number }

/** view.workspace 响应（§3.1 七字段；目标节点实时组装）。 */
export interface ViewOverview {
  overview: ViewWorkspaceOverview;
  agentMembers: ViewAgentMember[];
  teams: ViewTeam[];
  mcpEnabled: string[];
  skills: string[];
  pluginsEnabled: string[];
  resourcesSummary: ViewResourcesSummary;
}

export interface ViewSessionsReq extends ViewGroupIdOpt { targetNodeId: string; workspaceName: string; beforeSeq?: number; limit: number }
/** delegationId?：执行方 sessions→tasks 联表回带（1.13.0）——发起方定位链精确匹配；非委派会话缺省不带键。 */
export interface ViewSessionSummary { sessionId: string; title: string; lastSeq: number; updatedAt: number; delegationId?: string }
/** 会话列表页（beforeSeq/limit 分页）。 */
export interface ViewSessionPage { sessions: ViewSessionSummary[] }

/**
 * view.messages 请求。wire:2 帧形态下的两种翻页（互斥使用）：
 * - fromSeq>0：取 seq < fromSeq 中最新 limit 帧（加载更早，向后翻）
 * - afterSeq>0：取 seq > afterSeq 升序前 limit 帧（断线补拉，向前追——1.13.1）
 */
export interface ViewMessagesReq extends ViewGroupIdOpt { targetNodeId: string; sessionId: string; fromSeq: number; limit: number; afterSeq?: number }

/** 消息条目：目标节点 message_events 的 wire 投影（seq 天然全序单调；富形态由目标节点序列化进 content）。 */
export interface ViewMessage {
  seq: number;
  at: number;
  role: string;
  content: string;
}

/** 消息回放页（按 seq 分页，hasMore 指示还有更早/更多）。 */
export interface ViewMessagePage { messages: ViewMessage[]; hasMore: boolean }

// ────────────────────────────────────────────────────────────────────────────
// 1.13.0 帧投影（wire v2）：目标节点按消息聚合的帧形态——发起方全宽会话渲染输入。
// 协商：view.subscribe 请求带 wire:2 声明、响应回显 wire:2；未回显 = 旧节点降级旧投影。
// ────────────────────────────────────────────────────────────────────────────

/** 帧投影载荷能力门（不入 METHOD_REQUIRED_MINOR——无新 RPC 方法，沿 TASK_STEER 先例）。 */
export const VIEW_FRAME_REQUIRED_MINOR = 13;

/** 工具帧载荷：一调用一帧（start+result 配对）；result 超长由目标节点截断并置 truncated。 */
export interface ViewToolFrame { name: string; argsExcerpt?: string; result?: string; success?: boolean; truncated?: boolean }

/**
 * 聚合帧（目标端单点聚合，spec 2026-11-17 §4）：
 * - frameId 帧稳定身份（text/user=消息 id；tool=callId；steer=事件 id）——同 id 重发=整帧替换
 * - seq 帧末事件 rowid（全序单调：游标 / 排序 / 去重键）
 * - streaming 仅 text 帧：未终态时每拍重发最新快照，终态后游标才推进过帧末 seq
 */
export interface ViewFrame {
  frameId: string;
  seq: number;
  at: number;
  role: 'user' | 'assistant';
  kind: 'text' | 'tool' | 'user' | 'steer';
  content?: string;
  streaming?: boolean;
  tool?: ViewToolFrame;
}

/** view.messages 的帧页响应（wire:2 协商成立时；与 ViewMessagePage 并存）。 */
export interface ViewFramePage { frames: ViewFrame[]; hasMore: boolean }

/** view.tail 上行的帧形态（与既有 events 形态并存，hub 透传）。 */
export interface ViewTailFramesPayload { sessionId: string; frames: ViewFrame[] }

export interface ViewTasksReq extends ViewGroupIdOpt { targetNodeId: string; workspaceName: string }
/** 目标节点本地任务条目（#T 任务）。 */
export interface ViewLocalTask { taskId: string; title: string; status: string; updatedAt: number }
export interface ViewTasksRes { tasks: ViewLocalTask[] }

/** 尾部订阅：目标节点持续转发该会话新 message_events → hub 定向推 → 订阅端收 view.appended。 */
export interface ViewSubscribeReq extends ViewGroupIdOpt { targetNodeId: string; sessionId: string; wire?: 2 }
/** wire 回显（1.13.0）：请求声明 wire:2 且目标节点支持帧投影时回显；缺省 = 旧投影降级。 */
export interface ViewSubscribeRes { ok: boolean; wire?: 2 }

export interface ViewUnsubscribeReq extends ViewGroupIdOpt { targetNodeId: string; sessionId: string }
export interface ViewUnsubscribeRes { ok: boolean }

// ────────────────────────────────────────────────────────────────────────────
// M3.2 远端可见性（1.4.0）：远端空间清单 + 只读文件树
// ────────────────────────────────────────────────────────────────────────────

export interface ViewWorkspacesReq extends ViewGroupIdOpt { targetNodeId: string }

/**
 * 空间摘要投影：白名单三字段——刻意不含 directoryPath/ownerId 等本机信息
 * （防目标节点本机路径泄漏面，spec §2.3）。
 */
export interface ViewWorkspaceSummary { id: string; name: string; description: string }

export interface ViewWorkspacesRes { workspaces: ViewWorkspaceSummary[] }

/** view.files 请求：op=list（目录列举，path 缺省为工作空间根）/ read（文件读取）。 */
export interface ViewFilesReq extends ViewGroupIdOpt {
  targetNodeId: string;
  workspaceName: string;
  op: 'list' | 'read';
  /** 工作空间内相对路径（越界/符号链接逃逸由目标端 WorkspaceFS.assertInWorkspace 拦截）。 */
  path?: string;
}

/** 目录条目（list 结果项；size 目录恒 0）。 */
export interface ViewFileEntry { name: string; isDirectory: boolean; size: number }

/**
 * view.files 响应两态：list → entries + truncated；read → content（base64）+
 * size + truncated。超限截断以 truncated=true 明示——调用方不得视截断内容为完整数据。
 */
export type ViewFilesRes =
  | { entries: ViewFileEntry[]; truncated: boolean }
  | { content: string; size: number; truncated: boolean };

// view.files 限额（M3.2 §2.1.4，hub 仓单源常量——studio worker pin 后消费同一值）：
// 与既有 view 限额表对齐（read 上限 = TAIL_CONTENT_MAX 同值 256KB）。
/** path 相对路径长度上限（字符）。 */
export const VIEW_FILES_PATH_MAX = 512;
/** 单目录条目上限：超出截断并以 truncated 标记。 */
export const VIEW_FILES_ENTRIES_MAX = 500;
/** 单文件读取字节上限：超出截断并以 truncated 标记；内容 base64 传输。 */
export const VIEW_FILES_READ_MAX = 256 * 1024;

// ────────────────────────────────────────────────────────────────────────────
// 查看私有 wire（T13 约定；hub 侧执行语义见 packages/hub/src/remote-view.ts 文件头）——
// 不进 RPC 目录（ws 控制面公开方法），仅供 hub 与 studio 目标端/leader 端消费。
// ────────────────────────────────────────────────────────────────────────────

/** 查看链路私有方法名（信封 method 字段值）。 */
export const VIEW_WIRE = {
  /**
   * 节点 → hub：查看同意开关上报（req 信封，hub 直接 res 确认 {ok:true}）。
   * 节点 hello 后与开关变更时上行；hub 存内存态 per-nodeId，默认 true（spec §5.5）；
   * 断线清除回默认（RM2-9），重连由节点重报。
   */
  consent: 'view.consent',
  /**
   * 目标节点 → hub：已订阅会话的尾部增量（req 信封，hub 组装 view.appended
   * evt 定向推订阅端 leader，并 res {ok:true} 确认）。
   */
  tail: 'view.tail',
} as const;

/** view.consent 载荷：allowed=false 时 hub 对该节点的 view.* 请求一律 forbidden。 */
export interface ViewConsentPayload { allowed: boolean }

/** view.tail 载荷：该会话新 message_events 批（hub 原样组装进 view.appended）。 */
export interface ViewTailPayload { sessionId: string; events: ViewMessage[] }
