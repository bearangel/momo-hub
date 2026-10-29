// protocol 1.2.0/1.3.0/1.4.0/1.5.0/1.6.0 工作组与远程代答/远端可见性/资源生命
// 周期/多组终态契约测试
// （spec 2026-10-24 M2 §3.1/§3.2/§3.3 + 裁定 MC1-MC7 + 2026-10-28 M3.1 §3.1 +
// 2026-10-29 M3.2 §2.1 + 2026-10-29 M3.4 §2.1 + 2026-10-29 M4.2 §2.1/§2.2）：
// RPC 目录常量 wire 稳定锁 / 载荷与响应类型 round-trip / 错误码只增 /
// validateSnapshotManifest 好-坏矩阵（S1 同款纪律）/ 1.1.0 目录零回退兼容。
import { describe, expect, it } from 'vitest';
import {
  EVT,
  METHOD_REQUIRED_MINOR,
  MIRROR_WIRE,
  PROTOCOL_VERSION,
  RPC,
  RPC_ERROR_CODES,
  TASK_WIRE,
  VIEW_FILES_ENTRIES_MAX,
  VIEW_FILES_PATH_MAX,
  VIEW_FILES_READ_MAX,
  isCompatible,
  minorOf,
  protocolSupports,
  validateHubManifest,
  validateSnapshotManifest,
  type DelegationStatus,
  type DelegationTask,
  type GroupChangedEvent,
  type GroupInfo,
  type InviteInfo,
  type MirrorAcceptRes,
  type MirrorApplyPayload,
  type MirrorReceiptOk,
  type MirrorRequest,
  type MirrorResult,
  type MirrorResultPayload,
  type RpcErrorCode,
  type RpcErrorPayload,
  type SnapshotManifest,
  type TaskAnswerReq,
  type TaskAnswerRes,
  type TaskAnsweredEvent,
  type TaskAssignedEvent,
  type TaskProgressEvent,
  type TaskRecord,
  type TaskReportPayload,
  type TaskTerminalEvent,
  type ViewAppendedEvent,
  type ViewFileEntry,
  type ViewFilesReq,
  type ViewFilesRes,
  type ViewMessagePage,
  type ViewOverview,
  type ViewSessionPage,
  type ViewSubscribeReq,
  type ViewSubscribeRes,
  type ViewWorkspaceSummary,
  type ViewWorkspacesReq,
  type ViewWorkspacesRes,
  type WorkspaceMirroredEvent,
  type WorkspaceSnapshotMeta,
} from 'momo-hub-protocol';

// ────────────────────────────────────────────────────────────────────────────
// 版本与兼容（MC7：1.2.0 向后兼容，major 不变）
// ────────────────────────────────────────────────────────────────────────────

describe('版本 1.6.0 与 1.1.0-1.5.0 兼容线', () => {
  it('PROTOCOL_VERSION === 1.6.0', () => {
    expect(PROTOCOL_VERSION).toBe('1.6.0');
  });

  it('1.1.0 ↔ 1.6.0 major 相等即兼容；跨 major 不兼容', () => {
    expect(isCompatible('1.1.0', '1.6.0')).toBe(true);
    expect(isCompatible('1.2.0', '1.6.0')).toBe(true);
    expect(isCompatible('1.3.0', '1.6.0')).toBe(true);
    expect(isCompatible('1.4.0', '1.6.0')).toBe(true);
    expect(isCompatible('1.5.0', '1.6.0')).toBe(true);
    expect(isCompatible('2.0.0', '1.6.0')).toBe(false);
  });

  it('1.1.0 既有目录与校验器零回退（M1 测试零改动全绿的协议侧锚点）', () => {
    expect(RPC.hello).toBe('hello');
    expect(RPC.ping).toBe('ping');
    expect(EVT.nodeOnline).toBe('node.online');
    expect(EVT.nodeOffline).toBe('node.offline');
    expect(EVT.resourcePublished).toBe('resource.published');
    expect(
      validateHubManifest({
        slug: 'coder-pro',
        type: 'agent',
        name: 'Coder Pro',
        version: '1.2.3',
        author: 'workstation-1',
        description: '',
        readme: '',
        tags: [],
        category: 'dev',
        iconEmoji: '🤖',
        verificationStatus: 'unverified',
      }),
    ).not.toBeNull();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// minor 门槛协商（M3.0 §3.1：minor = 向后兼容新增的能力分层）
// ────────────────────────────────────────────────────────────────────────────

describe('minorOf / protocolSupports 矩阵', () => {
  it('minorOf：major.minor 取次段；缺段 / 非法 → null', () => {
    expect(minorOf('1.2.0')).toBe(2);
    expect(minorOf('1.11.3')).toBe(11);
    expect(minorOf('0.9')).toBe(9);
    expect(minorOf('1')).toBeNull();
    expect(minorOf('x.y.z')).toBeNull();
    expect(minorOf('')).toBeNull();
  });

  it('同 major：minor 高于 / 等于门槛 → true；低于门槛 → false', () => {
    expect(protocolSupports('1.3.0', 2)).toBe(true);
    expect(protocolSupports('1.2.0', 2)).toBe(true);
    expect(protocolSupports('1.2.9', 2)).toBe(true);
    expect(protocolSupports('1.1.0', 2)).toBe(false);
    expect(protocolSupports('1.2.0', 3)).toBe(false);
  });

  it('跨 major 一律 false（major 门槛仍由 hello 闸门单独把守）', () => {
    expect(protocolSupports('2.2.0', 2)).toBe(false);
    expect(protocolSupports('0.3.0', 2)).toBe(false);
  });

  it('非法版本串 false（major/minor 任一解析失败的 null 传播）', () => {
    expect(protocolSupports('', 2)).toBe(false);
    expect(protocolSupports('garbage', 2)).toBe(false);
    expect(protocolSupports('1', 1)).toBe(false);
  });

  it('METHOD_REQUIRED_MINOR：M3.0 全表=2 基线，唯 task.answer=3（M3.1）与 view.workspaces/view.files=4（M3.2）显式覆盖', () => {
    const explicit: Record<string, number> = {
      [RPC.task.answer]: 3,
      [RPC.view.workspaces]: 4,
      [RPC.view.files]: 4,
    };
    const entries = Object.entries(METHOD_REQUIRED_MINOR);
    expect(entries.length).toBeGreaterThan(0);
    for (const [method, minor] of entries) {
      expect(minor, `方法 ${method}`).toBe(explicit[method] ?? 2);
    }
    // 现存方法面全量登记（含私有上行 view.consent/view.tail 与 M3.2 两新方法）
    for (const method of [
      RPC.hello, RPC.ping,
      ...Object.values(RPC.group),
      ...Object.values(RPC.workspace),
      ...Object.values(RPC.task),
      ...Object.values(RPC.view),
      'view.consent', 'view.tail',
    ]) {
      expect(METHOD_REQUIRED_MINOR[method], `方法 ${method}`).toBe(explicit[method] ?? 2);
    }
    // 未登记者视为基线 2（约定：消费方 fallback ?? 2）
    expect(METHOD_REQUIRED_MINOR['future.method']).toBeUndefined();
  });

  it('METHOD_REQUIRED_MINOR 无 M4.2 门槛登记：全表无 minor≥5 条目（1.6.0 全部为可选字段——view 订阅组锚定 / view.appended groupId? / TaskRecord.groupId，无新方法；1.5 peer 调任何已登记方法〔含带新可选字段〕不受影响）', () => {
    for (const [method, minor] of Object.entries(METHOD_REQUIRED_MINOR)) {
      expect(minor, `方法 ${method}`).toBeLessThan(5);
    }
    // 全表最大门槛 4 对 1.5.0 peer 恒通过——新能力纯可选字段，无需能力协商
    expect(protocolSupports('1.5.0', 4)).toBe(true);
  });
});

// ────────────────────────────────────────────────────────────────────────────
// RPC 目录常量（wire 契约——字符串一经发布不可变更）
// ────────────────────────────────────────────────────────────────────────────

describe('RPC 目录常量（spec §3.1 表逐项）', () => {
  it('RPC.group 六方法', () => {
    expect(RPC.group).toEqual({
      create: 'group.create',
      list: 'group.list',
      invite: 'group.invite',
      join: 'group.join',
      leave: 'group.leave',
      transfer: 'group.transfer',
    });
  });

  it('RPC.workspace 两方法（build 走 HTTP 数据面，不入 ws 目录）', () => {
    expect(RPC.workspace).toEqual({ mirror: 'workspace.mirror', snapshots: 'workspace.snapshots' });
  });

  it('RPC.task 四方法（M3.1 增 answer）', () => {
    expect(RPC.task).toEqual({
      delegate: 'task.delegate',
      cancel: 'task.cancel',
      list: 'task.list',
      answer: 'task.answer',
    });
  });

  it('RPC.view 八方法（M3.2 增 workspaces/files）', () => {
    expect(RPC.view).toEqual({
      workspace: 'view.workspace',
      sessions: 'view.sessions',
      messages: 'view.messages',
      tasks: 'view.tasks',
      subscribe: 'view.subscribe',
      unsubscribe: 'view.unsubscribe',
      workspaces: 'view.workspaces',
      files: 'view.files',
    });
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 错误码（§3.3 只增不改义）
// ────────────────────────────────────────────────────────────────────────────

describe('RpcErrorCode 扩充', () => {
  it('M2 新增四码入枚举；既有五码不回退', () => {
    const codes: RpcErrorCode[] = [
      'unauthorized',
      'bad_request',
      'proto_version_mismatch',
      'unknown_method',
      'internal',
      'forbidden',
      'node_offline',
      'not_found',
      'conflict',
    ];
    expect(codes).toHaveLength(9);
    const payload: RpcErrorPayload = { code: 'forbidden', message: '非 leader' };
    expect(payload.code).toBe('forbidden');
  });

  it('M3.1 新增三码入枚举（not_waiting / node_unsupported / answer_timeout），既有十码不回退', () => {
    const m31: RpcErrorCode[] = ['not_waiting', 'node_unsupported', 'answer_timeout'];
    // 顺序锁：新三码追加在尾部（只增不改义——既有码相对位置不动）
    expect(RPC_ERROR_CODES.slice(0, -3)).toEqual([
      'unauthorized', 'bad_request', 'proto_version_mismatch', 'unknown_method',
      'unsupported_method', 'internal', 'forbidden', 'node_offline', 'not_found', 'conflict',
    ]);
    expect(RPC_ERROR_CODES.slice(-3)).toEqual(m31);
    expect(RPC_ERROR_CODES).toHaveLength(13);
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 事件目录与载荷（§3.2 逐字段）
// ────────────────────────────────────────────────────────────────────────────

describe('EVT 新增九事件目录', () => {
  it('常量稳定（wire 锁）', () => {
    expect(EVT.groupChanged).toBe('group.changed');
    expect(EVT.taskAssigned).toBe('task.assigned');
    expect(EVT.taskProgress).toBe('task.progress');
    expect(EVT.taskCompleted).toBe('task.completed');
    expect(EVT.taskFailed).toBe('task.failed');
    expect(EVT.taskInterrupted).toBe('task.interrupted');
    expect(EVT.taskCancelled).toBe('task.cancelled');
    expect(EVT.taskAnswered).toBe('task.answered');
    expect(EVT.viewAppended).toBe('view.appended');
    expect(EVT.workspaceMirrored).toBe('workspace.mirrored');
  });
});

describe('事件载荷 round-trip（§3.2 逐字段）', () => {
  it('GroupChangedEvent：kind 三枚举', () => {
    for (const kind of ['joined', 'left', 'leaderChanged'] as const) {
      const evt: GroupChangedEvent = { groupId: 'g1', kind, nodeId: 'n1' };
      expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    }
    expect(Object.keys({ groupId: '', kind: 'joined', nodeId: '' } satisfies GroupChangedEvent).sort()).toEqual([
      'groupId',
      'kind',
      'nodeId',
    ]);
  });

  it('TaskAssignedEvent：嵌套 DelegationTask（title/prompt/agentSlug?）', () => {
    const evt: TaskAssignedEvent = {
      taskId: 't1',
      workspaceName: 'ws',
      task: { title: '修 bug', prompt: '请修复登录失败', agentSlug: 'coder-pro' },
      byNode: 'leader-1',
    };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    const noSlug: DelegationTask = { title: 'T', prompt: 'P' };
    expect(noSlug.agentSlug).toBeUndefined();
  });

  it('TaskProgressEvent：{taskId, sessionId, lastSeq, excerpt, kind?}', () => {
    const evt: TaskProgressEvent = { taskId: 't1', sessionId: 's1', lastSeq: 42, excerpt: '正在编辑…' };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    expect(Object.keys(evt).sort()).toEqual(['excerpt', 'lastSeq', 'sessionId', 'taskId']);
    // kind 可选标注（M2.1 T11b）：缺省合法（0.1.3 载荷兼容）；置位仅 'waiting-input'
    const waiting: TaskProgressEvent = { ...evt, kind: 'waiting-input' };
    expect(Object.keys(waiting).sort()).toEqual(['excerpt', 'kind', 'lastSeq', 'sessionId', 'taskId']);
  });

  it('终态四事件共用形状：{taskId, reason?}', () => {
    const minimal: TaskTerminalEvent = { taskId: 't1' };
    const withReason: TaskTerminalEvent = { taskId: 't1', reason: 'declined' };
    expect(JSON.parse(JSON.stringify(minimal))).toEqual({ taskId: 't1' });
    expect(withReason.reason).toBe('declined');
  });

  it('ViewAppendedEvent：{sessionId, events[]}；M4.2 可选 groupId（缺省不带键——旧订阅形状载荷零变化）', () => {
    const evt: ViewAppendedEvent = {
      sessionId: 's1',
      events: [{ seq: 1, at: 1_700_000_000_000, role: 'assistant', content: 'hi' }],
    };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    expect(evt).not.toHaveProperty('groupId');
    // 在场：订阅时透传记录的组锚定值（显式 groupId 订阅专属）
    const anchored: ViewAppendedEvent = { ...evt, groupId: 'g-42' };
    expect(JSON.parse(JSON.stringify(anchored))).toEqual(anchored);
    expect(Object.keys(anchored).sort()).toEqual(['events', 'groupId', 'sessionId']);
  });

  it('WorkspaceMirroredEvent：五字段全量', () => {
    const evt: WorkspaceMirroredEvent = {
      groupId: 'g1',
      targetNodeId: 'n2',
      workspaceName: 'ws',
      snapshotId: 'snap-1',
      appliedResources: ['coder-pro', 'mcp-fetch'],
    };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    expect(Object.keys(evt).sort()).toEqual([
      'appliedResources',
      'groupId',
      'snapshotId',
      'targetNodeId',
      'workspaceName',
    ]);
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 组 / 镜像 / 委派 / 查看载荷与响应类型 round-trip（§3.1 表逐行）
// ────────────────────────────────────────────────────────────────────────────

describe('组管理类型 round-trip', () => {
  it('GroupInfo/GroupMember 形状（group.list 行）', () => {
    const group: GroupInfo = {
      groupId: 'g1',
      groupName: '主力组',
      leaderNodeId: 'node-a',
      members: [
        { nodeId: 'node-a', name: '工作台 1', online: true },
        { nodeId: 'node-b', name: '工作台 2', online: false },
      ],
    };
    expect(JSON.parse(JSON.stringify(group))).toEqual(group);
  });

  it('InviteInfo：{inviteCode}（独立于 hub enrollment token）', () => {
    const invite: InviteInfo = { inviteCode: 'grp-abc123' };
    expect(JSON.parse(JSON.stringify(invite))).toEqual(invite);
    expect(Object.keys(invite)).toEqual(['inviteCode']);
  });

  it('WorkspaceSnapshotMeta：快照目录行（workspace.snapshots 响应项）', () => {
    const meta: WorkspaceSnapshotMeta = {
      snapshotId: 'snap-1',
      workspaceName: 'ws',
      version: '3',
      byNode: 'node-a',
      at: 1_700_000_000_000,
    };
    expect(JSON.parse(JSON.stringify(meta))).toEqual(meta);
  });
});

describe('镜像类型 round-trip', () => {
  it('MirrorRequest：strategy=replace + confirm?（两段式）', () => {
    const base: MirrorRequest = {
      groupId: 'g1',
      targetNodeId: 'node-b',
      workspaceName: 'ws',
      snapshotId: 'snap-1',
      strategy: 'replace',
    };
    expect(base.confirm).toBeUndefined();
    const confirmed: MirrorRequest = { ...base, confirm: true };
    expect(JSON.parse(JSON.stringify(confirmed))).toEqual({ ...base, confirm: true });
  });

  it('MirrorResult：{ok, appliedResources}', () => {
    const res: MirrorResult = { ok: true, appliedResources: ['coder-pro'] };
    expect(JSON.parse(JSON.stringify(res))).toEqual(res);
  });
});

describe('委派类型 round-trip', () => {
  it('TaskRecord：status 七态枚举（账本状态机，M3.1 增 waiting）+ 代答留痕四可选字段', () => {
    const statuses: DelegationStatus[] = [
      'pending', 'running', 'waiting', 'done', 'failed', 'interrupted', 'cancelled',
    ];
    expect(statuses).toHaveLength(7);
    const rec: TaskRecord = {
      taskId: 't1',
      workspaceName: 'ws',
      targetNodeId: 'node-b',
      status: 'running',
      title: '修 bug',
      createdAt: 1_700_000_000_000,
      updatedAt: 1_700_000_000_001,
    };
    expect(JSON.parse(JSON.stringify(rec))).toEqual(rec);
    expect(Object.keys(rec).sort()).toEqual([
      'createdAt',
      'status',
      'targetNodeId',
      'taskId',
      'title',
      'updatedAt',
      'workspaceName',
    ]);
    // M3.1 代答留痕：四字段全可选（未代答不带键——0.1.6 载荷向后兼容）
    const answered: TaskRecord = {
      ...rec,
      status: 'waiting',
      answeredBy: 'node-a',
      answerDecision: 'approve',
      answerMessage: '准了',
      answeredAt: 1_700_000_000_002,
    };
    expect(JSON.parse(JSON.stringify(answered))).toEqual(answered);
    expect(Object.keys(answered).sort()).toEqual([
      'answerDecision', 'answerMessage', 'answeredAt', 'answeredBy',
      'createdAt', 'status', 'targetNodeId', 'taskId', 'title', 'updatedAt', 'workspaceName',
    ]);
    // M4.2 组归属：groupId 可选——在场时入键集（hub 侧 group_id 恒非空，task.list
    // 实际输出恒带该字段；类型层锁在场格，缺省格由上方基础记录锁覆盖）
    const grouped: TaskRecord = { ...rec, groupId: 'g-42' };
    expect(JSON.parse(JSON.stringify(grouped))).toEqual(grouped);
    expect(Object.keys(grouped).sort()).toEqual([
      'createdAt', 'groupId', 'status', 'targetNodeId', 'taskId', 'title', 'updatedAt', 'workspaceName',
    ]);
  });

  it('M3.1 代答三类型：TaskAnswerReq / TaskAnswerRes / TaskAnsweredEvent 形状', () => {
    const req: TaskAnswerReq = { taskId: 't1', decision: 'approve' };
    expect(JSON.parse(JSON.stringify(req))).toEqual(req);
    expect(Object.keys(req).sort()).toEqual(['decision', 'taskId']);
    const withMsg: TaskAnswerReq = { taskId: 't1', decision: 'deny', message: '不同意' };
    expect(Object.keys(withMsg).sort()).toEqual(['decision', 'message', 'taskId']);
    for (const disposition of ['granted', 'denied', 'stale'] as const) {
      const res: TaskAnswerRes = { disposition };
      expect(JSON.parse(JSON.stringify(res))).toEqual(res);
    }
    for (const decision of ['approve', 'deny'] as const) {
      const evt: TaskAnsweredEvent = {
        taskId: 't1', decision, by: 'node-a', disposition: 'granted',
      };
      expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
      expect(Object.keys(evt).sort()).toEqual(['by', 'decision', 'disposition', 'taskId']);
    }
  });

  it('TASK_WIRE.report 常量与 TaskReportPayload 形状（M2d T9 wire 约定锁）', () => {
    expect(TASK_WIRE).toEqual({ report: 'task.report' });
    const running: TaskReportPayload = {
      status: 'running',
      sessionId: 'sess-1',
      progress: { lastSeq: 42, excerpt: '正在编辑…' },
    };
    expect(JSON.parse(JSON.stringify(running))).toEqual(running);
    const done: TaskReportPayload = { status: 'done' };
    expect(Object.keys(done)).toEqual(['status']);
    const failed: TaskReportPayload = { status: 'failed', reason: 'declined' };
    expect(JSON.parse(JSON.stringify(failed))).toEqual(failed);
  });
});

describe('远程查看类型 round-trip', () => {
  it('ViewOverview：七字段（§3.1 view.workspace 响应）', () => {
    const overview: ViewOverview = {
      overview: { name: 'ws', description: '主力工作空间', sessionCount: 3 },
      agentMembers: [{ name: 'coder', agentSlug: 'coder-pro' }],
      teams: [{ name: '后端组', leaderName: 'pm', memberNames: ['pm', 'coder'] }],
      mcpEnabled: ['fetch'],
      skills: ['code-review'],
      pluginsEnabled: ['plugin-x'],
      resourcesSummary: { agent: 1, mcp: 1, skill: 1, plugin: 1 },
    };
    expect(JSON.parse(JSON.stringify(overview))).toEqual(overview);
    expect(Object.keys(overview).sort()).toEqual([
      'agentMembers',
      'mcpEnabled',
      'overview',
      'pluginsEnabled',
      'resourcesSummary',
      'skills',
      'teams',
    ]);
  });

  it('ViewSessionPage / ViewMessagePage：分页形状（messages 带 hasMore）', () => {
    const sessions: ViewSessionPage = {
      sessions: [{ sessionId: 's1', title: '会话 1', lastSeq: 30, updatedAt: 1_700_000_000_000 }],
    };
    const messages: ViewMessagePage = {
      messages: [{ seq: 1, at: 1_700_000_000_000, role: 'user', content: '开始' }],
      hasMore: true,
    };
    expect(JSON.parse(JSON.stringify(sessions))).toEqual(sessions);
    expect(JSON.parse(JSON.stringify(messages))).toEqual(messages);
  });

  it('ViewSubscribe 请求/响应', () => {
    const req: ViewSubscribeReq = { targetNodeId: 'node-b', sessionId: 's1' };
    const res: ViewSubscribeRes = { ok: true };
    expect(JSON.parse(JSON.stringify(req))).toEqual(req);
    expect(res.ok).toBe(true);
  });

  it('M3.2 既有六方法 req 可选 groupId：缺省不带键（旧形状零变化）；在场合法', () => {
    const reqs: ViewSubscribeReq[] = [
      { targetNodeId: 'node-b', sessionId: 's1' },
      { targetNodeId: 'node-b', sessionId: 's1', groupId: 'g1' },
    ];
    for (const req of reqs) expect(JSON.parse(JSON.stringify(req))).toEqual(req);
    // 缺省（1.2/1.3 旧调用形状）不得携带 groupId 键
    expect(reqs[0]).not.toHaveProperty('groupId');
    expect(reqs[1].groupId).toBe('g1');
  });

  it('M3.2 新类型：ViewWorkspacesReq/Res + ViewWorkspaceSummary 投影白名单三字段', () => {
    const req: ViewWorkspacesReq = { targetNodeId: 'node-b' };
    const withGroup: ViewWorkspacesReq = { targetNodeId: 'node-b', groupId: 'g1' };
    expect(JSON.parse(JSON.stringify(req))).toEqual(req);
    expect(req).not.toHaveProperty('groupId');
    expect(withGroup.groupId).toBe('g1');
    const summary: ViewWorkspaceSummary = { id: 'w1', name: 'ws-a', description: '主力' };
    // 白名单键集锁：id/name/description——多一个键即破坏投影契约（防本机路径泄漏）
    expect(Object.keys(summary).sort()).toEqual(['description', 'id', 'name']);
    const res: ViewWorkspacesRes = { workspaces: [summary] };
    expect(JSON.parse(JSON.stringify(res))).toEqual(res);
  });

  it('M3.2 新类型：ViewFilesReq（op 两态 + path?/groupId?）+ ViewFilesRes（list/read 两态）round-trip', () => {
    const listReq: ViewFilesReq = { targetNodeId: 'node-b', workspaceName: 'ws-a', op: 'list' };
    const readReq: ViewFilesReq = { targetNodeId: 'node-b', workspaceName: 'ws-a', op: 'read', path: 'docs/a.md', groupId: 'g1' };
    expect(JSON.parse(JSON.stringify(listReq))).toEqual(listReq);
    expect(listReq).not.toHaveProperty('path');
    expect(JSON.parse(JSON.stringify(readReq))).toEqual(readReq);

    const entry: ViewFileEntry = { name: 'docs', isDirectory: true, size: 0 };
    expect(Object.keys(entry).sort()).toEqual(['isDirectory', 'name', 'size']);
    const list: ViewFilesRes = { entries: [{ name: 'a.md', isDirectory: false, size: 12 }], truncated: false };
    const read: ViewFilesRes = { content: Buffer.from('hi', 'utf8').toString('base64'), size: 2, truncated: true };
    for (const res of [list, read]) expect(JSON.parse(JSON.stringify(res))).toEqual(res);
    // read.content 为 base64——解码回原文（wire 语义锁）
    expect(Buffer.from((read as { content: string }).content, 'base64').toString('utf8')).toBe('hi');
  });

  it('M3.2 限额常量锁：path 512 / entries 500 / read 256KB（与 TAIL_CONTENT_MAX 同值）', () => {
    expect(VIEW_FILES_PATH_MAX).toBe(512);
    expect(VIEW_FILES_ENTRIES_MAX).toBe(500);
    expect(VIEW_FILES_READ_MAX).toBe(256 * 1024);
  });
});

// ────────────────────────────────────────────────────────────────────────────
// validateSnapshotManifest（S1 同款：字段类型/长度/未知容忍）
// ────────────────────────────────────────────────────────────────────────────

function validManifest(): SnapshotManifest {
  return {
    workspaceName: 'ws',
    version: '3',
    resources: [
      { type: 'agent', slug: 'coder-pro' },
      { type: 'mcp', slug: 'mcp-fetch' },
    ],
    config: {
      agentMembers: [
        { name: 'coder', slug: 'coder-pro' },
        { name: 'custom-agent', customDefinition: 'frontmatter: yaml' },
      ],
      teams: [{ name: '后端组', leader: 'pm', members: ['pm', 'coder'] }],
      mcpEnabled: ['fetch'],
      skills: ['code-review'],
      pluginsEnabled: [{ pluginId: 'plugin-x', slug: 'plugin-x' }],
      providerRefs: [{ provider: 'zhipu', model: 'glm-5.3' }, { provider: 'openai' }],
      prefs: { maxToolCalls: 50, readonly: 'off' },
    },
  };
}

describe('validateSnapshotManifest 好例', () => {
  it('合法 manifest 归一化返回；未知字段容忍且剥除（顶层 + 引用项内）', () => {
    const expected = { ...validManifest(), resources: [validManifest().resources[0]] };
    const raw = {
      ...expected,
      resources: [{ ...expected.resources[0], extra: 1 }],
      futureField: true,
    };
    const out = validateSnapshotManifest(raw);
    expect(out).toEqual(expected);
    expect(out).not.toHaveProperty('futureField');
    expect(out?.resources[0]).not.toHaveProperty('extra');
  });

  it('空 resources / 空配置段各子清单合法', () => {
    const m = validManifest();
    const out = validateSnapshotManifest({
      ...m,
      resources: [],
      config: { ...m.config, agentMembers: [], teams: [], mcpEnabled: [], skills: [], pluginsEnabled: [] },
    });
    expect(out).not.toBeNull();
    expect(out?.resources).toEqual([]);
  });

  it('customDefinition 恰好 64KB 合法（readme 同款限）', () => {
    const m = validManifest();
    m.config.agentMembers[1] = { name: 'big', customDefinition: 'a'.repeat(64 * 1024) };
    expect(validateSnapshotManifest(m)).not.toBeNull();
  });

  it('modelBinding 在场 → 归一化保留；未知字段在段内剥除', () => {
    const m = validManifest();
    const out = validateSnapshotManifest({
      ...m,
      config: { ...m.config, modelBinding: { provider: 'zhipu', model: 'glm-5.3', extra: 1 } },
    });
    expect(out?.config.modelBinding).toEqual({ provider: 'zhipu', model: 'glm-5.3' });
  });

  it('modelBinding 缺省 → 合法且不落段（1.2.0 旧快照向后兼容）', () => {
    const out = validateSnapshotManifest(validManifest());
    expect(out).not.toBeNull();
    expect(out?.config).not.toHaveProperty('modelBinding');
  });
});

describe('validateSnapshotManifest resources[].version?（1.5.0 M3.4 B13）', () => {
  it('三格之缺省：引用项不带 version → 合法且不落字段（1.4 旧 manifest 零变化）', () => {
    const out = validateSnapshotManifest(validManifest());
    expect(out?.resources[0]).toEqual({ type: 'agent', slug: 'coder-pro' });
    expect(out?.resources[0]).not.toHaveProperty('version');
  });

  it('三格之在场：合法 version 透传保留（版本钉死——目标节点按该版本安装）', () => {
    const m = validManifest();
    m.resources[0] = { ...m.resources[0], version: '1.2.3' };
    const out = validateSnapshotManifest(m);
    expect(out?.resources[0]).toEqual({ type: 'agent', slug: 'coder-pro', version: '1.2.3' });
  });

  it('三格之非法：越字符集 / 空串 / 非字符串 → null（S1 白名单，version 拼下载路径同强度校验）', () => {
    const base = validManifest();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'a', version: '1.0 x' }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'a', version: '1.0.0/../x' }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'a', version: '' }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'a', version: 123 }] })).toBeNull();
  });

  it('剥除断言范围显式排除 version：已知透传字段不剥除，未知字段 extra 仍剥除', () => {
    const base = validManifest();
    const out = validateSnapshotManifest({
      ...base,
      resources: [{ type: 'agent', slug: 'coder-pro', version: '2.0.0', extra: 1 }],
    });
    expect(out?.resources[0]).toEqual({ type: 'agent', slug: 'coder-pro', version: '2.0.0' });
    expect(out?.resources[0]).not.toHaveProperty('extra');
  });

  it('METHOD_REQUIRED_MINOR 无 M3.4 门槛登记：全表无 minor≥5 条目（本组全部为可选字段/事件，无需能力协商）', () => {
    for (const [method, minor] of Object.entries(METHOD_REQUIRED_MINOR)) {
      expect(minor, `方法 ${method}`).toBeLessThan(5);
    }
  });
});

describe('validateSnapshotManifest 坏矩阵：顶层结构', () => {
  it('非对象 / null / 数组 → null', () => {
    expect(validateSnapshotManifest(null)).toBeNull();
    expect(validateSnapshotManifest('manifest')).toBeNull();
    expect(validateSnapshotManifest([validManifest()])).toBeNull();
  });

  it('缺 workspaceName / 空串 / 超长（>128）→ null', () => {
    const m = validManifest() as unknown as Record<string, unknown>;
    const { workspaceName: _omit, ...noName } = m;
    expect(validateSnapshotManifest(noName)).toBeNull();
    const base = validManifest();
    expect(validateSnapshotManifest({ ...base, workspaceName: '' })).toBeNull();
    expect(validateSnapshotManifest({ ...base, workspaceName: 'w'.repeat(129) })).toBeNull();
  });

  it('坏 version：缺 / 越字符集 / 空串 → null（S1 白名单）', () => {
    const base = validManifest();
    const { version: _omitVersion, ...noVersion } = base;
    expect(validateSnapshotManifest(noVersion)).toBeNull();
    expect(validateSnapshotManifest({ ...base, version: '1.0 x' })).toBeNull();
    expect(validateSnapshotManifest({ ...base, version: '' })).toBeNull();
  });

  it('resources 非数组 → null', () => {
    const base = validManifest();
    expect(validateSnapshotManifest({ ...base, resources: 'coder-pro' })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: {} })).toBeNull();
  });

  it('config 缺失 / 非对象 → null', () => {
    const base = validManifest() as unknown as Record<string, unknown>;
    const { config: _omitConfig, ...noConfig } = base;
    expect(validateSnapshotManifest(noConfig)).toBeNull();
    expect(validateSnapshotManifest({ ...base, config: 'cfg' })).toBeNull();
    expect(validateSnapshotManifest({ ...base, config: [] })).toBeNull();
  });
});

describe('validateSnapshotManifest 坏矩阵：resources 引用清单', () => {
  it('项非对象 / type 越枚举 / slug 坏字符集 / slug 超长 / slug 缺失 → null', () => {
    const base = validManifest();
    expect(validateSnapshotManifest({ ...base, resources: ['coder-pro'] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'app', slug: 'a' }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'Bad/Slug!' }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent', slug: 'a'.repeat(65) }] })).toBeNull();
    expect(validateSnapshotManifest({ ...base, resources: [{ type: 'agent' }] })).toBeNull();
  });

  it('resources 超量（>512）→ null（DoS 下限）', () => {
    const base = validManifest();
    const flood = Array.from({ length: 513 }, () => ({ type: 'skill', slug: 's' }));
    expect(validateSnapshotManifest({ ...base, resources: flood })).toBeNull();
    const edge = Array.from({ length: 512 }, (_, i) => ({ type: 'skill', slug: `s-${i}` }));
    expect(validateSnapshotManifest({ ...base, resources: edge })).not.toBeNull();
  });
});

describe('validateSnapshotManifest 坏矩阵：config 配置段', () => {
  it('agentMembers 非数组 / 项缺 name / name 超长 / slug 坏 → null', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ agentMembers: 'x' })).toBeNull();
    expect(bad({ agentMembers: [{ slug: 'coder-pro' }] })).toBeNull();
    expect(bad({ agentMembers: [{ name: 'n'.repeat(129) }] })).toBeNull();
    expect(bad({ agentMembers: [{ name: 'x', slug: 'Bad!' }] })).toBeNull();
  });

  it('customDefinition 超限（64KB+1，readme 超限类）→ null', () => {
    const base = validManifest();
    const over = {
      ...base,
      config: {
        ...base.config,
        agentMembers: [{ name: 'big', customDefinition: 'a'.repeat(64 * 1024 + 1) }],
      },
    };
    expect(validateSnapshotManifest(over)).toBeNull();
  });

  it('teams 非数组 / 项缺字段 / members 非字符串数组 → null', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ teams: 'x' })).toBeNull();
    expect(bad({ teams: [{ name: 't' }] })).toBeNull();
    expect(bad({ teams: [{ name: 't', leader: 'l', members: ['a', 1] }] })).toBeNull();
  });

  it('mcpEnabled / skills 非字符串数组 → null', () => {
    const base = validManifest();
    expect(validateSnapshotManifest({ ...base, config: { ...base.config, mcpEnabled: 'fetch' } })).toBeNull();
    expect(validateSnapshotManifest({ ...base, config: { ...base.config, skills: [1] } })).toBeNull();
  });

  // strListFrom 项长上限（M2c T2 携带票）：mcpEnabled/skills/team members 等字符串清单
  // 逐项 ≤128 字符——防单条超长字符串灌入（DoS 下限与 S1 纪律同源）。
  it('strList 项长 >128 → null（mcpEnabled / skills / team members）；边界 128 合法', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ mcpEnabled: ['x'.repeat(129)] })).toBeNull();
    expect(bad({ skills: ['x'.repeat(129)] })).toBeNull();
    expect(bad({ teams: [{ name: 't', leader: 'l', members: ['ok', 'x'.repeat(129)] }] })).toBeNull();
    // 边界：恰好 128 合法
    expect(bad({ mcpEnabled: ['x'.repeat(128)] })).not.toBeNull();
    expect(bad({ skills: ['x'.repeat(128)] })).not.toBeNull();
    expect(bad({ teams: [{ name: 't', leader: 'l', members: ['x'.repeat(128)] }] })).not.toBeNull();
  });

  it('pluginsEnabled 项缺 pluginId/slug → null', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ pluginsEnabled: [{ pluginId: 'x' }] })).toBeNull();
    expect(bad({ pluginsEnabled: [{ slug: 'x' }] })).toBeNull();
  });

  it('providerRefs 项 provider 缺失 / model 非字符串 → null', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ providerRefs: [{ model: 'm' }] })).toBeNull();
    expect(bad({ providerRefs: [{ provider: 'p', model: 1 }] })).toBeNull();
  });

  it('prefs 值类型越界（对象/数组/null/NaN）→ null', () => {
    const base = validManifest();
    const bad = (prefs: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, prefs } });
    expect(bad({ nested: { a: 1 } })).toBeNull();
    expect(bad({ arr: [1] })).toBeNull();
    expect(bad({ nil: null })).toBeNull();
    expect(bad({ nan: Number.NaN })).toBeNull();
    expect(bad({ num: 1, str: 's', bool: true })).not.toBeNull();
  });

  it('modelBinding 非对象 / provider 缺失或空 / model 空或非串 / 超长 → null', () => {
    const base = validManifest();
    const bad = (patch: Record<string, unknown>): unknown =>
      validateSnapshotManifest({ ...base, config: { ...base.config, ...patch } });
    expect(bad({ modelBinding: 'zhipu' })).toBeNull();
    expect(bad({ modelBinding: {} })).toBeNull();
    expect(bad({ modelBinding: { provider: '', model: 'm' } })).toBeNull();
    expect(bad({ modelBinding: { provider: 'p'.repeat(65), model: 'm' } })).toBeNull();
    expect(bad({ modelBinding: { provider: 'zhipu' } })).toBeNull();
    expect(bad({ modelBinding: { provider: 'zhipu', model: '' } })).toBeNull();
    expect(bad({ modelBinding: { provider: 'zhipu', model: 'm'.repeat(201) } })).toBeNull();
    // 边界：provider 恰 64 / model 恰 200 合法
    expect(bad({ modelBinding: { provider: 'p'.repeat(64), model: 'm' } })).not.toBeNull();
    expect(bad({ modelBinding: { provider: 'zhipu', model: 'm'.repeat(200) } })).not.toBeNull();
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 镜像私有 wire（M2c T6：MIRROR_WIRE 方法名 + 两段式载荷形状）
// ────────────────────────────────────────────────────────────────────────────

describe('镜像私有 wire', () => {
  it('MIRROR_WIRE 方法名常量锁定（hub 与 studio 双端消费的单源）', () => {
    expect(MIRROR_WIRE).toEqual({
      apply: 'mirror.apply',
      receipt: 'mirror.receipt',
      result: 'mirror.result',
    });
  });

  it('MirrorApplyPayload / MirrorAcceptRes / MirrorResultPayload 形状 round-trip', () => {
    const apply: MirrorApplyPayload = { mirrorId: 'm1', snapshotId: 's1', workspaceName: 'ws', strategy: 'replace', confirm: true };
    expect(JSON.parse(JSON.stringify(apply))).toEqual(apply);
    const applyNoConfirm: MirrorApplyPayload = { mirrorId: 'm2', snapshotId: 's1', workspaceName: 'ws', strategy: 'replace' };
    expect('confirm' in applyNoConfirm).toBe(false); // confirm 缺省不携带（wire 精确性）

    const accept: MirrorAcceptRes = { accepted: true, mirrorId: 'm1' };
    expect(accept.accepted).toBe(true);

    const okPayload: MirrorResultPayload = { mirrorId: 'm1', appliedResources: ['coder-pro', 'mcp-fetch'] };
    const errPayload: MirrorResultPayload = {
      mirrorId: 'm2', code: 'conflict', message: '目标工作空间已存在',
      diffSummary: { added: 1, removed: 2, changed: 3 },
    };
    expect(JSON.parse(JSON.stringify(okPayload))).toEqual(okPayload);
    expect(JSON.parse(JSON.stringify(errPayload))).toEqual(errPayload);

    const receiptOk: MirrorReceiptOk = { appliedResources: ['coder-pro'] };
    expect(receiptOk.appliedResources).toEqual(['coder-pro']);
  });
});
