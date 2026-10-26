// 远程查看契约（spec 2026-10-24 M2 §3.1「远程查看」表 + 裁定 MC3）——
// 拉取按 seq 分页 + 尾部订阅（view.appended 定向推）；只读，无任何写方法。
// leader→hub→目标节点定向路由；hub 与 studio 双端消费。
export const VIEW_RPC = {
  workspace: 'view.workspace',
  sessions: 'view.sessions',
  messages: 'view.messages',
  tasks: 'view.tasks',
  subscribe: 'view.subscribe',
  unsubscribe: 'view.unsubscribe',
} as const;

export interface ViewWorkspaceReq { targetNodeId: string; workspaceName: string }

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

export interface ViewSessionsReq { targetNodeId: string; workspaceName: string; beforeSeq?: number; limit: number }
export interface ViewSessionSummary { sessionId: string; title: string; lastSeq: number; updatedAt: number }
/** 会话列表页（beforeSeq/limit 分页）。 */
export interface ViewSessionPage { sessions: ViewSessionSummary[] }

export interface ViewMessagesReq { targetNodeId: string; sessionId: string; fromSeq: number; limit: number }

/** 消息条目：目标节点 message_events 的 wire 投影（seq 天然全序单调；富形态由目标节点序列化进 content）。 */
export interface ViewMessage {
  seq: number;
  at: number;
  role: string;
  content: string;
}

/** 消息回放页（按 seq 分页，hasMore 指示还有更早/更多）。 */
export interface ViewMessagePage { messages: ViewMessage[]; hasMore: boolean }

export interface ViewTasksReq { targetNodeId: string; workspaceName: string }
/** 目标节点本地任务条目（#T 任务）。 */
export interface ViewLocalTask { taskId: string; title: string; status: string; updatedAt: number }
export interface ViewTasksRes { tasks: ViewLocalTask[] }

/** 尾部订阅：目标节点持续转发该会话新 message_events → hub 定向推 → 订阅端收 view.appended。 */
export interface ViewSubscribeReq { targetNodeId: string; sessionId: string }
export interface ViewSubscribeRes { ok: boolean }

export interface ViewUnsubscribeReq { targetNodeId: string; sessionId: string }
export interface ViewUnsubscribeRes { ok: boolean }

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
