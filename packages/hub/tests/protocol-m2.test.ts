// protocol 1.2.0 工作组契约测试（spec 2026-10-24 M2 §3.1/§3.2/§3.3 + 裁定 MC1-MC7）：
// RPC 目录常量 wire 稳定锁 / 载荷与响应类型 round-trip / 错误码只增 /
// validateSnapshotManifest 好-坏矩阵（S1 同款纪律）/ 1.1.0 目录零回退兼容。
import { describe, expect, it } from 'vitest';
import {
  EVT,
  MIRROR_WIRE,
  PROTOCOL_VERSION,
  RPC,
  isCompatible,
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
  type TaskAssignedEvent,
  type TaskProgressEvent,
  type TaskRecord,
  type TaskTerminalEvent,
  type ViewAppendedEvent,
  type ViewMessagePage,
  type ViewOverview,
  type ViewSessionPage,
  type ViewSubscribeReq,
  type ViewSubscribeRes,
  type WorkspaceMirroredEvent,
  type WorkspaceSnapshotMeta,
} from 'momo-hub-protocol';

// ────────────────────────────────────────────────────────────────────────────
// 版本与兼容（MC7：1.2.0 向后兼容，major 不变）
// ────────────────────────────────────────────────────────────────────────────

describe('版本 1.2.0 与 1.1.0 兼容线', () => {
  it('PROTOCOL_VERSION === 1.2.0', () => {
    expect(PROTOCOL_VERSION).toBe('1.2.0');
  });

  it('1.1.0 ↔ 1.2.0 major 相等即兼容；跨 major 不兼容', () => {
    expect(isCompatible('1.1.0', '1.2.0')).toBe(true);
    expect(isCompatible('1.2.0', '1.1.0')).toBe(true);
    expect(isCompatible('2.0.0', '1.2.0')).toBe(false);
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

  it('RPC.task 三方法', () => {
    expect(RPC.task).toEqual({ delegate: 'task.delegate', cancel: 'task.cancel', list: 'task.list' });
  });

  it('RPC.view 六方法', () => {
    expect(RPC.view).toEqual({
      workspace: 'view.workspace',
      sessions: 'view.sessions',
      messages: 'view.messages',
      tasks: 'view.tasks',
      subscribe: 'view.subscribe',
      unsubscribe: 'view.unsubscribe',
    });
  });
});

// ────────────────────────────────────────────────────────────────────────────
// 错误码（§3.3 只增不改义）
// ────────────────────────────────────────────────────────────────────────────

describe('RpcErrorCode 扩充', () => {
  it('新增四码入枚举；既有五码不回退', () => {
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

  it('TaskProgressEvent：{taskId, sessionId, lastSeq, excerpt}', () => {
    const evt: TaskProgressEvent = { taskId: 't1', sessionId: 's1', lastSeq: 42, excerpt: '正在编辑…' };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
    expect(Object.keys(evt).sort()).toEqual(['excerpt', 'lastSeq', 'sessionId', 'taskId']);
  });

  it('终态四事件共用形状：{taskId, reason?}', () => {
    const minimal: TaskTerminalEvent = { taskId: 't1' };
    const withReason: TaskTerminalEvent = { taskId: 't1', reason: 'declined' };
    expect(JSON.parse(JSON.stringify(minimal))).toEqual({ taskId: 't1' });
    expect(withReason.reason).toBe('declined');
  });

  it('ViewAppendedEvent：{sessionId, events[]}', () => {
    const evt: ViewAppendedEvent = {
      sessionId: 's1',
      events: [{ seq: 1, at: 1_700_000_000_000, role: 'assistant', content: 'hi' }],
    };
    expect(JSON.parse(JSON.stringify(evt))).toEqual(evt);
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
  it('TaskRecord：status 六态枚举（账本状态机）', () => {
    const statuses: DelegationStatus[] = ['pending', 'running', 'done', 'failed', 'interrupted', 'cancelled'];
    expect(statuses).toHaveLength(6);
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
