# momo-hub M0（连接基建）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 M0 连接基建——hub 常驻服务可部署、studio 可靠连接、协议有版本纪律、双设备互见上线/离线。

**Architecture:** 控制面 WebSocket（RPC 信封 + 推送事件）+ 数据面 HTTP（enroll、后续资源传输）。hub = Fastify + @fastify/websocket + better-sqlite3（hub_meta/nodes 两表起步）。协议单源在仓库根 `src/`（包名 `momo-hub-protocol`，studio 经 git tag 依赖消费）。studio 侧连接层落 `electron/src/main/hub/`（状态机/退避重连/心跳/keychain 凭证）。

**Tech Stack:** Node 24 / TypeScript strict / pnpm 9 workspace / Fastify 5 + @fastify/websocket 11 / better-sqlite3 12 / ws 8 / vitest 3（forks 池）。studio 侧沿用 momo-studio 既有栈（Electron 主进程 CommonJS、zustand、lucide-react）。

**Spec:** `docs/specs/2026-10-23-momo-hub-overall-design.md`（整体设计 v1，§2 核心裁定 / §6 M0 范围）

## Global Constraints

- **Node 24**（两仓 `.nvmrc` 钉 24）；pnpm 9（`packageManager: pnpm@9.0.0`）
- **TypeScript strict**：禁 `any` / `@ts-ignore` / `as any`；ESLint `no-explicit-any: error`（momo-hub 仓从 Task 1 起机械强制）
- **语言**：全部注释/文档用中文；标识符英文；Conventional Commits（`feat:`/`fix:`/`chore:`/`docs:`/`test:`/`refactor:`）
- **协议纪律**：根 `src/`（protocol）零运行时依赖（node 内建除外）、CJS 产物；不兼容变更 bump major 并重打 tag，studio 跟进 pin——协议变更永远 hub 仓先行
- **安全红线**：device token 只入 keychain，不落日志不落 DB 明文（DB 存 sha256）；enrollment token 不出现在日志 payload 行（启动横幅除外——它本来就是给操作者看的）
- **momo-studio 侧任务**（Task 8-12）必须遵守其 AGENTS.md：测试位置规范（`electron/tests/hub/` 镜像、renderer 贴源）、UI 只用语义 token + lucide 16px/1.75、版本号纪律（特性合入 alpha +1）；执行时加载 skills：`momo-test-rules`（写测试）、`momo-ui-preview-rules`（Task 11）、`momo-boundary-rules`（Task 10 IPC 面）
- **每个任务结束 commit**；vitest 非正常退出（SIGSEGV/SIGKILL）后必须检查并清理孤儿 worker

## Review Focus

spec 未逐条点名、但最可能咬人的五类输入/故障（每条已 pin 到所属任务的测试）：

1. **幽灵在线（半开连接）**：hub 侧 ws ping 两个周期无 pong → 强制下线并广播 `node.offline`——网络分区下不能永远显示在线 → Task 6 集成测试「心跳判死」用例（裸 TCP + socket.pause 模拟半开）
2. **token 时序攻击**：enrollment/device token 校验必须 timing-safe（长度不等直接 false，等长走 `crypto.timingSafeEqual`）→ Task 5 `timingSafeEqualStr` 单测 + enroll 拒绝路径
3. **协议版本漂移**：客户端 major 不匹配 → 显式 `proto_version_mismatch` 错误响应 + 关连接（4402），绝不静默挂起或降级 → Task 3 单测 + Task 6 集成测试双保险
4. **重连风暴**：hub 短暂重启 → studio 指数退避（1s 起、封顶 30s、成功即重置）自动恢复 online → Task 9 fake timers 逐拍断言退避序列
5. **重复 enroll（凭证轮换语义）**：同名节点再次 enroll → 复用 nodeId、替换 token（旧 token 立即失效）——防节点行膨胀 + 防旧凭证残留 → Task 5「同名重 enroll = 轮换」用例

---

## 第一部分：momo-hub 仓库（Task 1-7）

仓库根：`/Users/stbearangel/dev/AiProject/momo-hub`（已 git init、已有 spec 提交、remote origin = github.com/bearangel/momo-hub）

### Task 1: 工程脚手架与工程基线

**Files:**
- Create: `package.json`、`pnpm-workspace.yaml`、`.gitignore`、`.nvmrc`、`tsconfig.base.json`、`tsconfig.protocol.json`、`eslint.config.js`、`LICENSE`、`README.md`、`.github/workflows/ci.yml`、`src/index.ts`、`packages/hub/package.json`、`packages/hub/tsconfig.json`、`packages/hub/vitest.config.ts`

**Interfaces:**
- Produces: 仓库骨架 + `pnpm typecheck` / `pnpm test` / `pnpm build` 三命令空跑通过；后续所有任务的工程基线

**背景裁定（写进 README，执行者不必重新决策）：**

- 仓库根 `package.json` **就是** protocol 分发包（name `momo-hub-protocol`，源码在根 `src/`，`prepare` 构建出 `dist/`）。原因：npm/pnpm git 依赖只能安装仓库根清单，无法装子目录；把 protocol 放根上，studio `pnpm add github:bearangel/momo-hub#v0.1.0` 即得协议单源
- `packages/hub` 是唯一 workspace 成员，经 `"momo-hub-protocol": "file:../.."` 链接根包消费协议（workspace 根 install 时 `prepare` 已把 `dist/` 构建好）。**备选**（若 pnpm 对 `file:../..` 解析异常）：hub tsconfig 改用 `paths: { "momo-hub-protocol": ["../../src/index.ts"] }` + `"rootDir": "../.."`，main/bin 指向 `dist/packages/hub/src/index.js`
- LICENSE 落 **AGPL-3.0-only**（momo 系列开源惯例，与 momo-studio 同款）

- [ ] **Step 1: 写工程文件**

`package.json`（根 = protocol 分发包 + workspace 容器）：

```json
{
  "name": "momo-hub-protocol",
  "version": "0.1.0",
  "description": "momo-hub 与 momo-studio 之间的协议与类型单源（studio 经 git tag 依赖消费；spec 2026-10-23 M0）",
  "license": "AGPL-3.0-only",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "files": ["dist"],
  "scripts": {
    "prepare": "tsc -p tsconfig.protocol.json",
    "build": "tsc -p tsconfig.protocol.json && pnpm --filter momo-hub build",
    "typecheck": "tsc -p tsconfig.protocol.json --noEmit && pnpm --filter momo-hub typecheck",
    "test": "pnpm --filter momo-hub test",
    "lint": "eslint src packages/hub/src"
  },
  "devDependencies": {
    "@eslint/js": "^9.0.0",
    "eslint": "^9.0.0",
    "typescript": "^5.4.0",
    "typescript-eslint": "^8.0.0"
  },
  "engines": { "node": ">=24.0.0" },
  "packageManager": "pnpm@9.0.0"
}
```

`pnpm-workspace.yaml`：

```yaml
packages:
  - packages/hub
```

`.gitignore`：

```
node_modules/
dist/
packages/hub/dist/
*.tsbuildinfo
coverage/
data/
.omo/
```

`.nvmrc`：`24`

`tsconfig.base.json`：

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "moduleResolution": "Node",
    "strict": true,
    "noUnusedLocals": true,
    "noUncheckedIndexedAccess": true,
    "forceConsistentCasingInFileNames": true,
    "esModuleInterop": true,
    "declaration": true,
    "skipLibCheck": true
  }
}
```

`tsconfig.protocol.json`：

```json
{
  "extends": "./tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src/**/*.ts"]
}
```

`src/index.ts`（占位，Task 2/3 填实）：

```typescript
// momo-hub-protocol 出口——hub 与 studio 双端消费的协议单源。
export const PROTOCOL_PLACEHOLDER = true;
```

`eslint.config.js`：

```js
// ESLint flat config——与 momo-studio 同纪律：no-explicit-any error。
const js = require('@eslint/js');
const tseslint = require('typescript-eslint');

module.exports = tseslint.config(
  { ignores: ['**/dist/**', '**/node_modules/**'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/consistent-type-imports': 'error',
    },
  },
);
```

`LICENSE`：从 momo-studio 仓库 `LICENSE` 文件原样复制（AGPL-3.0 全文，`cp /Users/stbearangel/dev/AiProject/moo-studio/LICENSE ./LICENSE`）。

`README.md`：

```markdown
# momo-hub

Momo Studio 的常驻中心服务——资源分发（私有 marketplace，M1）与分布式任务节点（工作组/镜像/委派/远程查看，M2）。M0 交付连接基建。

- 设计：docs/specs/2026-10-23-momo-hub-overall-design.md
- 实施计划：docs/plans/
- 架构：控制面 WebSocket（RPC + 事件）+ 数据面 HTTP；Node 24 / TypeScript strict / Fastify + SQLite
- 协议包：仓库根即 momo-hub-protocol（momo-studio 经 git tag 依赖消费）

状态：M0 连接基建开发中。
```

`.github/workflows/ci.yml`：

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with: { version: 9.0.0 }
      - uses: actions/setup-node@v4
        with: { node-version: 24 }
      - run: pnpm install --no-frozen-lockfile
      - run: pnpm typecheck
      - run: pnpm lint
      - run: pnpm test
```

`packages/hub/package.json`：

```json
{
  "name": "momo-hub",
  "version": "0.1.0",
  "description": "momo-hub 中心服务（Fastify + WebSocket + SQLite）",
  "license": "AGPL-3.0-only",
  "type": "commonjs",
  "main": "dist/index.js",
  "bin": { "momo-hub": "dist/index.js" },
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc --noEmit",
    "test": "vitest run --config vitest.config.ts"
  },
  "dependencies": {
    "@fastify/websocket": "^11.0.0",
    "better-sqlite3": "^12.11.1",
    "fastify": "^5.2.0",
    "momo-hub-protocol": "file:../..",
    "ws": "^8.18.0"
  },
  "devDependencies": {
    "@types/better-sqlite3": "^7.6.0",
    "@types/node": "^24.0.0",
    "@types/ws": "^8.5.0",
    "typescript": "^5.4.0",
    "vitest": "^3.0.0"
  }
}
```

（版本以 install 实际解析为准，peer 冲突就近取兼容版。）

`packages/hub/tsconfig.json`：

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src/**/*.ts"]
}
```

`packages/hub/vitest.config.ts`：

```typescript
import { defineConfig } from 'vitest/config';

// forks 池：better-sqlite3 原生句柄 + 真实 socket 集成测试
// 在 worker 线程析构有 NAPI 竞态前科（momo-studio 同教训）。
export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    pool: 'forks',
    testTimeout: 15_000,
    passWithNoTests: true,
  },
});
```

- [ ] **Step 2: 安装并验证空跑**

Run: `npx pnpm@9.0.0 install && npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 lint && npx pnpm@9.0.0 test`
Expected: 四命令全部成功（hub 暂无测试文件，passWithNoTests 兜底）

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "chore: momo-hub 工程脚手架——pnpm workspace + TS strict 基线 + CI + AGPL LICENSE"
```

### Task 2: protocol——消息信封与编解码

**Files:**
- Create: `src/envelope.ts`（并改写 `src/index.ts` 出口）
- Test: `packages/hub/tests/protocol-envelope.test.ts`

**Interfaces:**
- Produces（后续所有任务的公共契约）:
  - `ENVELOPE_VERSION = 1`；`EnvelopeKind = 'req' | 'res' | 'evt'`
  - `RequestEnvelope<T>` `{ v: 1; kind: 'req'; id: string; method: string; payload: T }`
  - `ResponseEnvelope<T>` `{ v: 1; kind: 'res'; id: string; method: string; ok: boolean; payload: T }`（ok=false 时 payload 为 `RpcErrorPayload`）
  - `EventEnvelope<T>` `{ v: 1; kind: 'evt'; method: string; payload: T }`
  - `parseEnvelope(raw: unknown): Envelope | null`（严格结构校验；未知字段容忍）
  - `encodeEnvelope(e: Envelope): string`
  - `makeRequest(method: string, payload: unknown): RequestEnvelope`（id = randomUUID，单点生成）

- [ ] **Step 1: 写失败测试**（`packages/hub/tests/protocol-envelope.test.ts`——测试经 `momo-hub-protocol` 名导入，vitest 通过 hub 的 file: 链接解析到根 dist；跑测试前必须 `pnpm build`）

```typescript
// 信封编解码契约锁：round-trip + 全部畸形输入拒绝。
import { describe, expect, it } from 'vitest';
import {
  encodeEnvelope, parseEnvelope, makeRequest,
  type EventEnvelope, type ResponseEnvelope,
} from 'momo-hub-protocol';

describe('信封编解码', () => {
  it('req/res/evt round-trip 保持结构', () => {
    const req = makeRequest('hello', { protoVer: '1.0.0' });
    expect(parseEnvelope(encodeEnvelope(req))).toEqual(req);

    const res: ResponseEnvelope<{ ok2: true }> = { v: 1, kind: 'res', id: req.id, method: 'hello', ok: true, payload: { ok2: true } };
    expect(parseEnvelope(encodeEnvelope(res))).toEqual(res);

    const evt: EventEnvelope<{ a: 1 }> = { v: 1, kind: 'evt', method: 'node.online', payload: { a: 1 } };
    expect(parseEnvelope(encodeEnvelope(evt))).toEqual(evt);
  });

  it('畸形输入一律 null（非 JSON / 缺字段 / 错类型 / 错版本号 / evt 带 id / 未知 kind）', () => {
    expect(parseEnvelope('not json')).toBeNull();
    expect(parseEnvelope(null)).toBeNull();
    expect(parseEnvelope('{"v":1,"kind":"req","method":"hello","payload":{}}')).toBeNull(); // 缺 id
    expect(parseEnvelope('{"v":1,"kind":"req","id":"x","method":123,"payload":{}}')).toBeNull(); // method 非字符串
    expect(parseEnvelope('{"v":2,"kind":"req","id":"x","method":"m","payload":{}}')).toBeNull(); // v≠1
    expect(parseEnvelope('{"v":1,"kind":"res","id":"x","method":"m","payload":{}}')).toBeNull(); // res 缺 ok
    expect(parseEnvelope('{"v":1,"kind":"evt","id":"x","method":"m","payload":{}}')).toBeNull(); // evt 不允许 id
    expect(parseEnvelope('{"v":1,"kind":"other","id":"x","method":"m","payload":{}}')).toBeNull(); // 未知 kind
  });

  it('makeRequest 每次生成唯一 id', () => {
    expect(makeRequest('a', {}).id).not.toBe(makeRequest('a', {}).id);
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`
Expected: FAIL——`parseEnvelope`/`makeRequest` 不在导出中

- [ ] **Step 3: 实现**（`src/envelope.ts`）

```typescript
// src/envelope.ts
// 消息信封——hub 与 studio 之间全部 wire 格式的最小容器。
// 纪律：结构校验严格（字段缺失/类型不符/evt 带 id 一律 null），未知字段容忍（minor 向前兼容）。
import { randomUUID } from 'node:crypto';

export const ENVELOPE_VERSION = 1 as const;
export type EnvelopeKind = 'req' | 'res' | 'evt';

export interface RequestEnvelope<T = unknown> { v: 1; kind: 'req'; id: string; method: string; payload: T }
export interface ResponseEnvelope<T = unknown> { v: 1; kind: 'res'; id: string; method: string; ok: boolean; payload: T }
export interface EventEnvelope<T = unknown> { v: 1; kind: 'evt'; method: string; payload: T }
export type Envelope<T = unknown> = RequestEnvelope<T> | ResponseEnvelope<T> | EventEnvelope<T>;

const isStr = (x: unknown): x is string => typeof x === 'string';

/** 严格结构校验：任何已知字段缺失/类型不符/非法组合返回 null；未知字段忽略。 */
export function parseEnvelope(raw: unknown): Envelope | null {
  if (typeof raw !== 'string') return null;
  let obj: unknown;
  try { obj = JSON.parse(raw); } catch { return null; }
  if (typeof obj !== 'object' || obj === null) return null;
  const e = obj as Record<string, unknown>;
  if (e.v !== ENVELOPE_VERSION) return null;
  switch (e.kind) {
    case 'req':
      if (!isStr(e.id) || e.id.length === 0 || !isStr(e.method) || e.method.length === 0) return null;
      return { v: 1, kind: 'req', id: e.id, method: e.method, payload: e.payload ?? null };
    case 'res':
      if (!isStr(e.id) || e.id.length === 0 || !isStr(e.method) || typeof e.ok !== 'boolean') return null;
      return { v: 1, kind: 'res', id: e.id, method: e.method, ok: e.ok, payload: e.payload ?? null };
    case 'evt':
      if ('id' in e || !isStr(e.method) || e.method.length === 0) return null;
      return { v: 1, kind: 'evt', method: e.method, payload: e.payload ?? null };
    default:
      return null;
  }
}

export function encodeEnvelope(e: Envelope): string {
  return JSON.stringify(e);
}

/** 构造请求信封（id 单点生成——与 momo 系「跨模块 ID 单点生成」纪律一致）。 */
export function makeRequest(method: string, payload: unknown): RequestEnvelope {
  return { v: ENVELOPE_VERSION, kind: 'req', id: randomUUID(), method, payload };
}
```

`src/index.ts` 本任务先只导出信封（Task 3 再补其余三行）：

```typescript
// momo-hub-protocol 出口——hub 与 studio 双端消费的协议单源。
export * from './envelope';
```

- [ ] **Step 4: 跑测试确认通过**

Run: `npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`
Expected: PASS（3 用例）

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(protocol): 消息信封与编解码——req/res/evt 严格结构校验"
```

### Task 3: protocol——RPC/事件目录、错误码、版本协商

**Files:**
- Create: `src/methods.ts`、`src/events.ts`、`src/version.ts`、`src/error.ts`
- Modify: `src/index.ts`（补三行 export）
- Test: `packages/hub/tests/protocol-catalog.test.ts`

**Interfaces:**
- Consumes: Task 2 信封类型
- Produces:
  - `RPC = { hello: 'hello'; ping: 'ping' }`（enroll 走 HTTP，不在 ws RPC 目录）
  - `HelloReq { protoVer: string; nodeName?: string }` / `HelloRes { hubId: string; protoVer: string; serverTime: number; onlineNodes: Array<{ nodeId: string; nodeName: string }> }`
  - `PingReq { ts: number }` / `PongRes { ts: number; serverTime: number }`
  - `EnrollReq { enrollmentToken: string; nodeName: string }` / `EnrollRes { nodeId: string; deviceToken: string }`（HTTP POST /enroll 请求/响应体）
  - `EVT = { nodeOnline: 'node.online'; nodeOffline: 'node.offline' }`；`NodeOnlineEvent { nodeId: string; nodeName: string; at: number }`；`NodeOfflineEvent { nodeId: string; at: number }`
  - `PROTOCOL_VERSION = '1.0.0'`；`majorOf(v): number | null`；`isCompatible(client, server): boolean`（major 相等）
  - `RpcErrorCode = 'unauthorized' | 'bad_request' | 'proto_version_mismatch' | 'unknown_method' | 'internal'`；`RpcErrorPayload { code: RpcErrorCode; message: string }`

- [ ] **Step 1: 写失败测试**（`packages/hub/tests/protocol-catalog.test.ts`）

```typescript
// RPC/事件目录与版本协商契约锁。
import { describe, expect, it } from 'vitest';
import { PROTOCOL_VERSION, isCompatible, majorOf, RPC, EVT } from 'momo-hub-protocol';

describe('版本协商', () => {
  it('major 相等即兼容；不等不兼容；畸形版本号不兼容', () => {
    expect(isCompatible('1.0.0', '1.9.9')).toBe(true);
    expect(isCompatible('1.0.0', '2.0.0')).toBe(false);
    expect(isCompatible('abc', '1.0.0')).toBe(false);
    expect(majorOf('3.2.1')).toBe(3);
    expect(majorOf('x')).toBeNull();
  });
  it('PROTOCOL_VERSION 起点 1.x', () => {
    expect(majorOf(PROTOCOL_VERSION)).toBe(1);
  });
});

describe('目录常量', () => {
  it('RPC/EVT 方法名稳定（wire 契约，改名即破坏性）', () => {
    expect(RPC.hello).toBe('hello');
    expect(RPC.ping).toBe('ping');
    expect(EVT.nodeOnline).toBe('node.online');
    expect(EVT.nodeOffline).toBe('node.offline');
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`
Expected: FAIL——导出不存在

- [ ] **Step 3: 实现四个文件**

`src/methods.ts`：

```typescript
// RPC 方法目录与载荷类型（ws 控制面）。enroll 走 HTTP 数据面（Task 6 的 POST /enroll），
// 其 wire 类型放这里供两端共享。
export const RPC = { hello: 'hello', ping: 'ping' } as const;
export type RpcMethodName = (typeof RPC)[keyof typeof RPC];

export interface HelloReq { protoVer: string; nodeName?: string }
/** onlineNodes：hello 成功时的在线节点快照（后续增量走 node.online/offline 事件） */
export interface HelloRes { hubId: string; protoVer: string; serverTime: number; onlineNodes: Array<{ nodeId: string; nodeName: string }> }

export interface PingReq { ts: number }
export interface PongRes { ts: number; serverTime: number }

export interface EnrollReq { enrollmentToken: string; nodeName: string }
export interface EnrollRes { nodeId: string; deviceToken: string }
```

`src/events.ts`：

```typescript
// 服务端推送事件目录（ws 控制面，evt 信封）。
export const EVT = { nodeOnline: 'node.online', nodeOffline: 'node.offline' } as const;
export type EventMethodName = (typeof EVT)[keyof typeof EVT];

export interface NodeOnlineEvent { nodeId: string; nodeName: string; at: number }
export interface NodeOfflineEvent { nodeId: string; at: number }
```

`src/version.ts`：

```typescript
// 协议版本纪律：semver；不兼容变更 bump major（重打 tag、studio 跟进 pin）。
export const PROTOCOL_VERSION = '1.0.0';

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
```

`src/error.ts`：

```typescript
// res.ok=false 时的 payload 形状（错误码是 wire 契约，只增不改义）。
export type RpcErrorCode =
  | 'unauthorized'
  | 'bad_request'
  | 'proto_version_mismatch'
  | 'unknown_method'
  | 'internal';

export interface RpcErrorPayload { code: RpcErrorCode; message: string }
```

`src/index.ts` 补齐：

```typescript
// momo-hub-protocol 出口——hub 与 studio 双端消费的协议单源。
export * from './envelope';
export * from './methods';
export * from './events';
export * from './version';
export * from './error';
```

- [ ] **Step 4: 跑测试确认通过**

Run: `npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`
Expected: PASS（两文件全绿）

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(protocol): RPC/事件目录、错误码与 semver 版本协商"
```

### Task 4: hub——配置、日志、SQLite 迁移与 hub 身份

**Files:**
- Create: `packages/hub/src/config.ts`、`packages/hub/src/logger.ts`、`packages/hub/src/db.ts`、`packages/hub/src/identity.ts`
- Test: `packages/hub/tests/identity.test.ts`

**Interfaces:**
- Produces:
  - `loadHubConfig(env: NodeJS.ProcessEnv): HubConfig`，`HubConfig = { port: number; dataDir: string }`（`MOMO_HUB_PORT` 默认 8787；`MOMO_HUB_DATA_DIR` 默认 `~/.momo-hub`）
  - `openHubDatabase(dataDir: string): Database.Database`（建目录、`hub.db`、WAL、内联迁移 + `schema_version` 门控；事务包裹）
  - `ensureHubIdentity(db): HubIdentity`（幂等；`HubIdentity = { hubId: string; enrollmentToken: string }`）
  - `rotateEnrollmentToken(db): string`

- [ ] **Step 1: 写失败测试**（`packages/hub/tests/identity.test.ts`）

```typescript
// hub 身份与迁移：首次生成、重开稳定、可轮换、唯一约束生效。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, rotateEnrollmentToken } from '../src/identity';

describe('hub 身份', () => {
  it('首次初始化生成 hubId 与 enrollment token；重开同目录保持稳定', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    const a = ensureHubIdentity(db);
    expect(a.hubId).toMatch(/^[0-9a-f-]{36}$/);
    expect(a.enrollmentToken.length).toBeGreaterThanOrEqual(32);
    db.close();

    const db2 = openHubDatabase(dir);
    expect(ensureHubIdentity(db2)).toEqual(a);
    db2.close();
  });

  it('轮换 enrollment token 只换 token 不换 hubId', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    const before = ensureHubIdentity(db);
    const next = rotateEnrollmentToken(db);
    expect(next).not.toBe(before.enrollmentToken);
    expect(ensureHubIdentity(db)).toEqual({ hubId: before.hubId, enrollmentToken: next });
    db.close();
  });

  it('nodes 表 token_hash 唯一约束生效', () => {
    const dir = mkdtempSync(join(tmpdir(), 'momo-hub-'));
    const db = openHubDatabase(dir);
    db.prepare("INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES ('n1','a','h1',0,0)").run();
    expect(() =>
      db.prepare("INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES ('n2','b','h1',0,0)").run(),
    ).toThrow();
    db.close();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd packages/hub && npx pnpm@9.0.0 vitest run tests/identity.test.ts`
Expected: FAIL——`../src/db` 不存在

- [ ] **Step 3: 实现四文件**

`packages/hub/src/config.ts`：

```typescript
// 运行配置：环境变量 → HubConfig。
import os from 'node:os';
import path from 'node:path';

export interface HubConfig { port: number; dataDir: string }

export function loadHubConfig(env: NodeJS.ProcessEnv): HubConfig {
  const port = Number(env.MOMO_HUB_PORT ?? 8787);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`非法 MOMO_HUB_PORT: ${String(env.MOMO_HUB_PORT)}`);
  }
  return { port, dataDir: env.MOMO_HUB_DATA_DIR ?? path.join(os.homedir(), '.momo-hub') };
}
```

`packages/hub/src/logger.ts`：

```typescript
// 极简分级日志——零依赖（对齐 momo-studio 自研 logger 的取舍）。
const ts = (): string => new Date().toISOString();
export const logger = {
  info: (msg: string, meta?: unknown) => console.log(`[INFO ${ts()}] ${msg}`, meta ?? ''),
  warn: (msg: string, meta?: unknown) => console.warn(`[WARN ${ts()}] ${msg}`, meta ?? ''),
  error: (msg: string, meta?: unknown) => console.error(`[ERR ${ts()}] ${msg}`, meta ?? ''),
};
```

`packages/hub/src/db.ts`：

```typescript
// SQLite 打开 + 内联 SQL 迁移（momo-studio 纪律：迁移是 TS 常量不是 .sql 文件，
// 保证 tsc 产物自包含）。
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

/** 迁移序列：下标 +1 = schema_version。已应用迁移不可变，只追加。 */
const MIGRATIONS: readonly string[] = [
  // v1
  `CREATE TABLE IF NOT EXISTS hub_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS nodes (
     id TEXT PRIMARY KEY,
     name TEXT NOT NULL UNIQUE,
     token_hash TEXT NOT NULL UNIQUE,
     enrolled_at INTEGER NOT NULL,
     last_seen_at INTEGER NOT NULL
   )`,
];

export function openHubDatabase(dataDir: string): Database.Database {
  mkdirSync(dataDir, { recursive: true });
  const db = new Database(path.join(dataDir, 'hub.db'));
  db.pragma('journal_mode = WAL');
  db.exec('BEGIN');
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS hub_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
    const row = db.prepare(`SELECT value FROM hub_meta WHERE key = 'schema_version'`).get() as { value: string } | undefined;
    const current = row ? Number(row.value) : 0;
    for (let v = current; v < MIGRATIONS.length; v++) {
      db.exec(MIGRATIONS[v]!);
    }
    if (current < MIGRATIONS.length) {
      db.prepare(`INSERT INTO hub_meta (key, value) VALUES ('schema_version', ?)
                  ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
        .run(String(MIGRATIONS.length));
    }
    db.exec('COMMIT');
    return db;
  } catch (err) {
    db.exec('ROLLBACK');
    db.close();
    throw err;
  }
}
```

`packages/hub/src/identity.ts`：

```typescript
// hub 身份：hub_id（稳定标识）+ enrollment_token（信任根，可轮换）。
import { randomBytes, randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';

export interface HubIdentity { hubId: string; enrollmentToken: string }

function readMeta(db: Database.Database, key: string): string | null {
  const row = db.prepare('SELECT value FROM hub_meta WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value ?? null;
}

function writeMeta(db: Database.Database, key: string, value: string): void {
  db.prepare(`INSERT INTO hub_meta (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(key, value);
}

/** 幂等：首次生成持久化，之后原样返回。 */
export function ensureHubIdentity(db: Database.Database): HubIdentity {
  let hubId = readMeta(db, 'hub_id');
  if (!hubId) { hubId = randomUUID(); writeMeta(db, 'hub_id', hubId); }
  let token = readMeta(db, 'enrollment_token');
  if (!token) { token = randomBytes(24).toString('base64url'); writeMeta(db, 'enrollment_token', token); }
  return { hubId, enrollmentToken: token };
}

/** 轮换加入凭证（泄漏处置用）。 */
export function rotateEnrollmentToken(db: Database.Database): string {
  const next = randomBytes(24).toString('base64url');
  writeMeta(db, 'enrollment_token', next);
  return next;
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd packages/hub && npx pnpm@9.0.0 vitest run tests/identity.test.ts`
Expected: PASS（3 用例）

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(hub): 配置/日志/SQLite 迁移与 hub 身份（enrollment token 可轮换）"
```

### Task 5: hub——鉴权（enroll + device token 校验）

**Files:**
- Create: `packages/hub/src/timing.ts`、`packages/hub/src/auth.ts`
- Test: `packages/hub/tests/auth.test.ts`

**Interfaces:**
- Consumes: Task 4 `openHubDatabase`/`ensureHubIdentity`
- Produces:
  - `timingSafeEqualStr(a: string, b: string): boolean`
  - `enrollNode(db, enrollmentToken, nodeName): { nodeId: string; deviceToken: string } | { error: 'invalid_enrollment' } | { error: 'invalid_name' }`——同名重 enroll = 轮换语义（复用 nodeId、替换 token、旧 token 立即失效）
  - `verifyDeviceToken(db, deviceToken): { nodeId: string; name: string } | null`（sha256 查表；命中顺带 touch `last_seen_at`）

- [ ] **Step 1: 写失败测试**（`packages/hub/tests/auth.test.ts`）

```typescript
// 鉴权语义锁：时序安全比较、轮换语义、哈希落库、名字校验。
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity } from '../src/identity';
import { enrollNode, verifyDeviceToken } from '../src/auth';
import { timingSafeEqualStr } from '../src/timing';

function freshDb() {
  const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-auth-')));
  const identity = ensureHubIdentity(db);
  return { db, identity };
}

describe('timingSafeEqualStr', () => {
  it('等长相等 true；不等长 false；不等内容 false', () => {
    expect(timingSafeEqualStr('aaaa', 'aaaa')).toBe(true);
    expect(timingSafeEqualStr('aaaa', 'aaa')).toBe(false);
    expect(timingSafeEqualStr('aaaa', 'aaab')).toBe(false);
  });
});

describe('enrollNode / verifyDeviceToken', () => {
  it('错 enrollment token 拒绝', () => {
    const { db } = freshDb();
    expect(enrollNode(db, 'wrong-token', 'mac-main')).toEqual({ error: 'invalid_enrollment' });
    db.close();
  });

  it('空名/超长名/纯空白名拒绝', () => {
    const { db, identity } = freshDb();
    expect(enrollNode(db, identity.enrollmentToken, '')).toEqual({ error: 'invalid_name' });
    expect(enrollNode(db, identity.enrollmentToken, '   ')).toEqual({ error: 'invalid_name' });
    expect(enrollNode(db, identity.enrollmentToken, 'x'.repeat(65))).toEqual({ error: 'invalid_name' });
    db.close();
  });

  it('enroll → verify 闭环；DB 只存哈希不存明文', () => {
    const { db, identity } = freshDb();
    const r = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in r)) throw new Error('unreachable');
    expect(verifyDeviceToken(db, r.deviceToken)).toEqual({ nodeId: r.nodeId, name: 'mac-main' });
    const row = db.prepare('SELECT token_hash FROM nodes WHERE id = ?').get(r.nodeId) as { token_hash: string };
    expect(row.token_hash).not.toBe(r.deviceToken);
    db.close();
  });

  it('同名重 enroll = 轮换：nodeId 复用、旧 token 失效、新 token 有效、节点行不膨胀', () => {
    const { db, identity } = freshDb();
    const first = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in first)) throw new Error('unreachable');
    const second = enrollNode(db, identity.enrollmentToken, 'mac-main');
    if (!('nodeId' in second)) throw new Error('unreachable');
    expect(second.nodeId).toBe(first.nodeId);
    expect(second.deviceToken).not.toBe(first.deviceToken);
    expect(verifyDeviceToken(db, first.deviceToken)).toBeNull();
    expect(verifyDeviceToken(db, second.deviceToken)?.nodeId).toBe(first.nodeId);
    const count = db.prepare('SELECT COUNT(*) AS n FROM nodes').get() as { n: number };
    expect(count.n).toBe(1);
    db.close();
  });

  it('未知 token 查询返回 null', () => {
    const { db } = freshDb();
    expect(verifyDeviceToken(db, 'nope')).toBeNull();
    db.close();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd packages/hub && npx pnpm@9.0.0 vitest run tests/auth.test.ts`
Expected: FAIL——模块不存在

- [ ] **Step 3: 实现**

`packages/hub/src/timing.ts`：

```typescript
// 字符串时序安全比较：长度不等直接 false（不泄露比较时机差）。
import { timingSafeEqual } from 'node:crypto';

export function timingSafeEqualStr(a: string, b: string): boolean {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  return ba.length === bb.length && timingSafeEqual(ba, bb);
}
```

`packages/hub/src/auth.ts`：

```typescript
// 节点鉴权：enrollment（信任根换 device token）+ device token 校验（sha256 落库）。
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import { timingSafeEqualStr } from './timing';

export type EnrollResult =
  | { nodeId: string; deviceToken: string }
  | { error: 'invalid_enrollment' }
  | { error: 'invalid_name' };

function sha256(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

/** 节点名规范：trim 后 1..64 字符。 */
function validName(name: string): string | null {
  const n = name.trim();
  return n.length >= 1 && n.length <= 64 ? n : null;
}

export function enrollNode(db: Database.Database, enrollmentToken: string, nodeName: string): EnrollResult {
  const name = validName(nodeName);
  if (!name) return { error: 'invalid_name' };
  const row = db.prepare(`SELECT value FROM hub_meta WHERE key = 'enrollment_token'`).get() as { value: string } | undefined;
  if (!row || !timingSafeEqualStr(enrollmentToken, row.value)) {
    return { error: 'invalid_enrollment' };
  }
  const deviceToken = randomBytes(32).toString('base64url');
  const tokenHash = sha256(deviceToken);
  const existing = db.prepare('SELECT id FROM nodes WHERE name = ?').get(name) as { id: string } | undefined;
  const now = Date.now();
  if (existing) {
    // 轮换语义：复用 nodeId，替换 token（旧凭证立即失效）
    db.prepare('UPDATE nodes SET token_hash = ?, last_seen_at = ? WHERE id = ?').run(tokenHash, now, existing.id);
    return { nodeId: existing.id, deviceToken };
  }
  const nodeId = randomUUID();
  db.prepare('INSERT INTO nodes (id, name, token_hash, enrolled_at, last_seen_at) VALUES (?, ?, ?, ?, ?)')
    .run(nodeId, name, tokenHash, now, now);
  return { nodeId, deviceToken };
}

export function verifyDeviceToken(db: Database.Database, deviceToken: string): { nodeId: string; name: string } | null {
  const row = db.prepare('SELECT id, name FROM nodes WHERE token_hash = ?').get(sha256(deviceToken)) as
    | { id: string; name: string }
    | undefined;
  if (!row) return null;
  db.prepare('UPDATE nodes SET last_seen_at = ? WHERE id = ?').run(Date.now(), row.id);
  return { nodeId: row.id, name: row.name };
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd packages/hub && npx pnpm@9.0.0 vitest run tests/auth.test.ts`
Expected: PASS（6 用例）

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(hub): 节点鉴权——enrollment 换 device token（哈希落库/时序安全/同名轮换）"
```

### Task 6: hub——WS 服务（升级鉴权/hello/注册表/心跳判死/dispatcher）

**Files:**
- Create: `packages/hub/src/registry.ts`、`packages/hub/src/server.ts`
- Test: `packages/hub/tests/server.integration.test.ts`

**Interfaces:**
- Consumes: Task 3 protocol 全部、Task 4 `openHubDatabase`/`ensureHubIdentity`、Task 5 `enrollNode`/`verifyDeviceToken`
- Produces:
  - `createHubServer(opts: { db: Database.Database; identity: HubIdentity; helloTimeoutMs?: number; keepaliveIntervalMs?: number }): HubServer`
  - `HubServer = { listen(port: number, host?: string): Promise<string>; close(): Promise<void>; onlineNodes(): Array<{ nodeId: string; nodeName: string }> }`
  - wire 行为（测试按此断言）：
    - `GET /health` → `{ ok: true, hubId }`
    - `POST /enroll` body=EnrollReq → EnrollRes（200）或 `{ error: 'invalid_enrollment' | 'invalid_name' }`（400）
    - `GET /ws`：`Authorization: Bearer <deviceToken>` 升级前校验——失败 `close(4401)`；成功后 helloTimeoutMs（默认 5s）内必须收到 `hello` req——超时 `close(4403)`；hello 的 `protoVer` major 不符 → `res ok:false proto_version_mismatch` + `close(4402)`；成功 → `res HelloRes`（含 onlineNodes 快照）+ 全员广播 `node.online`；断开 → 注册表移除 + 广播 `node.offline`
    - 服务端每 keepaliveIntervalMs（默认 30s）ws ping；连续 2 周期无 pong → terminate（触发 close → node.offline）
    - `ping` req → `pong` res；未知方法 → `res ok:false unknown_method`；坏信封 → `close(4400)`

- [ ] **Step 1: 写失败集成测试**（真实端口、真实 socket；`helloTimeoutMs`/`keepaliveIntervalMs` 注入缩短等待）

```typescript
// WS 服务集成契约锁：鉴权、hello 协商、双节点互见、心跳判死（真实 socket，wire 语义全真）。
import WebSocket from 'ws';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { openHubDatabase } from '../src/db';
import { ensureHubIdentity, type HubIdentity } from '../src/identity';
import { enrollNode } from '../src/auth';
import { createHubServer, type HubServer } from '../src/server';
import {
  PROTOCOL_VERSION, RPC, EVT, encodeEnvelope, makeRequest, parseEnvelope, type Envelope,
} from 'momo-hub-protocol';

const db = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-srv-')));
const identity = ensureHubIdentity(db);
let server: HubServer;
let wsUrl: string;
let baseUrl: string;

function enroll(name: string): { nodeId: string; deviceToken: string } {
  const r = enrollNode(db, identity.enrollmentToken, name);
  if ('nodeId' in r) return r;
  throw new Error('enroll 失败');
}

/** 便捷客户端：自动过 hello，收集全部信封，支持条件等待。 */
function connect(name: string) {
  return new Promise<{ ws: WebSocket; nodeId: string; next(match: (e: Envelope) => boolean): Promise<Envelope> }>((resolve, reject) => {
    const cred = enroll(name);
    const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const inbox: Envelope[] = [];
    const waiters: Array<{ match: (e: Envelope) => boolean; resolve: (e: Envelope) => void }> = [];
    ws.on('message', (data) => {
      const e = parseEnvelope(String(data));
      if (!e) return;
      inbox.push(e);
      const i = waiters.findIndex((w) => w.match(e));
      if (i >= 0) waiters.splice(i, 1)[0]!.resolve(e);
    });
    ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: name }))));
    ws.on('error', reject);
    const next = (match: (e: Envelope) => boolean): Promise<Envelope> =>
      new Promise((res) => { const hit = inbox.find(match); if (hit) return res(hit); waiters.push({ match, resolve: res }); });
    void next((e) => e.kind === 'res' && e.method === RPC.hello).then((h) => {
      if (h.kind !== 'res' || !h.ok) reject(new Error('hello 失败'));
      else resolve({ ws, nodeId: cred.nodeId, next });
    });
  });
}

beforeAll(async () => {
  server = createHubServer({ db, identity });
  const url = await server.listen(0, '127.0.0.1');
  baseUrl = url;
  wsUrl = url.replace(/^http/, 'ws') + '/ws';
});
afterAll(async () => { await server.close(); db.close(); });

describe('hub ws 服务', () => {
  it('health 端点返回 hubId', async () => {
    const res = await fetch(baseUrl + '/health');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, hubId: identity.hubId });
  });

  it('HTTP enroll：正确凭证成功、错误凭证 400 invalid_enrollment', async () => {
    const ok = await fetch(baseUrl + '/enroll', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enrollmentToken: identity.enrollmentToken, nodeName: 'http-node' }),
    });
    expect(ok.status).toBe(200);
    const body = await ok.json() as { nodeId: string; deviceToken: string };
    expect(typeof body.nodeId).toBe('string');
    const bad = await fetch(baseUrl + '/enroll', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enrollmentToken: 'nope', nodeName: 'x' }),
    });
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: 'invalid_enrollment' });
  });

  it('无 token / 错 token 升级被拒（4401）', async () => {
    for (const headers of [{}, { authorization: 'Bearer nope' }]) {
      const code = await new Promise<number>((resolve) => {
        const ws = new WebSocket(wsUrl, { headers });
        ws.on('close', resolve);
        ws.on('error', () => undefined);
      });
      expect(code).toBe(4401);
    }
  });

  it('双节点互见：B 上线 A 收 node.online；B 断开 A 收 node.offline', async () => {
    const a = await connect('node-a');
    const b = await connect('node-b');
    const onlineEvt = await a.next((e) =>
      e.kind === 'evt' && e.method === EVT.nodeOnline && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    expect((onlineEvt.payload as { nodeName: string }).nodeName).toBe('node-b');
    b.ws.close();
    const offlineEvt = await a.next((e) =>
      e.kind === 'evt' && e.method === EVT.nodeOffline && (e.payload as { nodeId: string }).nodeId === b.nodeId);
    expect(offlineEvt.kind).toBe('evt');
    a.ws.close();
  });

  it('hello 携带不兼容版本 → proto_version_mismatch + 4402 关闭', async () => {
    const cred = enroll('node-c');
    await new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${cred.deviceToken}` } });
      ws.on('open', () => ws.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: '99.0.0' }))));
      ws.on('message', (data) => {
        const e = parseEnvelope(String(data));
        if (e?.kind === 'res' && !e.ok) {
          expect((e.payload as { code: string }).code).toBe('proto_version_mismatch');
        }
      });
      ws.on('close', (code) => { expect(code).toBe(4402); resolve(); });
      ws.on('error', () => undefined);
      setTimeout(() => reject(new Error('超时未关闭')), 5000);
    });
  });

  it('ping → pong；未知方法 → unknown_method；坏信封 → 4400', async () => {
    const a = await connect('node-d');
    a.ws.send(encodeEnvelope(makeRequest(RPC.ping, { ts: 123 })));
    const pong = await a.next((e) => e.kind === 'res' && e.method === RPC.ping);
    expect(pong.kind === 'res' && pong.ok && (pong.payload as { serverTime: number }).serverTime).toBeGreaterThan(0);
    a.ws.send(encodeEnvelope(makeRequest('bogus', {})));
    const err = await a.next((e) => e.kind === 'res' && e.method === 'bogus');
    expect(err.kind === 'res' && (err.payload as { code: string }).code).toBe('unknown_method');
    const closed = new Promise<number>((res) => { a.ws.on('close', res); a.ws.on('error', () => undefined); });
    a.ws.send('not-an-envelope');
    expect(await closed).toBe(4400);
  });

  it('心跳判死：半开连接（不再读 socket）两周期后被踢 → 他人收到 node.offline', async () => {
    // 用注入 keepaliveIntervalMs=100 的独立实例
    const db2 = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-ka-')));
    const id2 = ensureHubIdentity(db2);
    const s2 = createHubServer({ db: db2, identity: id2, keepaliveIntervalMs: 100 });
    const url2 = await s2.listen(0, '127.0.0.1');
    const watcherCred = enrollNode(db2, id2.enrollmentToken, 'watcher');
    const victimCred = enrollNode(db2, id2.enrollmentToken, 'victim');
    if (!('nodeId' in watcherCred) || !('nodeId' in victimCred)) throw new Error('unreachable');

    const watcher = new WebSocket(url2.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${watcherCred.deviceToken}` } });
    const victim = new WebSocket(url2.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${victimCred.deviceToken}` } });
    // victim 过 hello 后 pause socket——不再读 → 不回 pong（半开模拟）
    victim.on('open', () => {
      victim.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: 'victim' })));
      setTimeout(() => victim.pause(), 50);
    });
    await new Promise<void>((res) => {
      watcher.on('message', (data) => {
        const e = parseEnvelope(String(data));
        if (e?.kind === 'evt' && e.method === EVT.nodeOffline && (e.payload as { nodeId: string }).nodeId === victimCred.nodeId) res();
      });
      watcher.on('open', () => watcher.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION }))));
      setTimeout(() => res(), 5000); // 兜底防挂
    }).then(() => {
      watcher.close(); victim.close();
    });
    await s2.close(); db2.close();
  });

  it('hello 超时（注入 200ms）→ 4403', async () => {
    const db3 = openHubDatabase(mkdtempSync(join(tmpdir(), 'momo-hub-hello-')));
    const id3 = ensureHubIdentity(db3);
    const s3 = createHubServer({ db: db3, identity: id3, helloTimeoutMs: 200 });
    const url3 = await s3.listen(0, '127.0.0.1');
    const cred = enrollNode(db3, id3.enrollmentToken, 'silent');
    if (!('nodeId' in cred)) throw new Error('unreachable');
    const ws = new WebSocket(url3.replace(/^http/, 'ws') + '/ws', { headers: { authorization: `Bearer ${cred.deviceToken}` } });
    const code = await new Promise<number>((resolve) => { ws.on('close', resolve); ws.on('error', () => undefined); });
    expect(code).toBe(4403);
    await s3.close(); db3.close();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd packages/hub && npx pnpm@9.0.0 vitest run tests/server.integration.test.ts`
Expected: FAIL——`../src/server` 不存在

- [ ] **Step 3: 实现**

`packages/hub/src/registry.ts`：

```typescript
// 在线节点注册表：nodeId → 连接。广播 evt 信封。
import type { WebSocket } from 'ws';
import { encodeEnvelope, type EventEnvelope } from 'momo-hub-protocol';

export interface NodeConn { nodeId: string; nodeName: string; ws: WebSocket; alive: boolean }

export class NodeRegistry {
  private byId = new Map<string, NodeConn>();

  add(conn: NodeConn): void { this.byId.set(conn.nodeId, conn); }
  remove(nodeId: string): NodeConn | undefined { const c = this.byId.get(nodeId); this.byId.delete(nodeId); return c; }
  get(nodeId: string): NodeConn | undefined { return this.byId.get(nodeId); }
  onlineNodes(): Array<{ nodeId: string; nodeName: string }> {
    return [...this.byId.values()].map((c) => ({ nodeId: c.nodeId, nodeName: c.nodeName }));
  }
  broadcast(method: string, payload: unknown): void {
    const evt: EventEnvelope = { v: 1, kind: 'evt', method, payload };
    const raw = encodeEnvelope(evt);
    for (const c of this.byId.values()) c.ws.send(raw);
  }
}
```

`packages/hub/src/server.ts`：

```typescript
// hub 服务本体：Fastify(/health /enroll) + WebSocket(/ws)。
// 鉴权链：升级头 Bearer deviceToken → hello（版本协商）→ 注册广播。
// 判死：ws ping 帧 keepalive，两周期无 pong terminate → node.offline。
import fastify from 'fastify';
import websocket from '@fastify/websocket';
import type Database from 'better-sqlite3';
import {
  EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, isCompatible, parseEnvelope,
  type HelloReq, type RpcErrorPayload,
} from 'momo-hub-protocol';
import type { HubIdentity } from './identity';
import { enrollNode, verifyDeviceToken } from './auth';
import { NodeRegistry } from './registry';
import { logger } from './logger';

export interface HubServer {
  listen(port: number, host?: string): Promise<string>;
  close(): Promise<void>;
  onlineNodes(): Array<{ nodeId: string; nodeName: string }>;
}

export interface CreateHubServerOpts {
  db: Database.Database;
  identity: HubIdentity;
  helloTimeoutMs?: number;      // 默认 5000
  keepaliveIntervalMs?: number; // 默认 30000；判死 = 2 × interval
}

interface Session { nodeId: string; nodeName: string; helloDone: boolean }

export function createHubServer(opts: CreateHubServerOpts): HubServer {
  const { db, identity } = opts;
  const helloTimeoutMs = opts.helloTimeoutMs ?? 5000;
  const keepaliveIntervalMs = opts.keepaliveIntervalMs ?? 30_000;
  const registry = new NodeRegistry();
  const app = fastify({ logger: false });
  void app.register(websocket);

  app.get('/health', async () => ({ ok: true, hubId: identity.hubId }));

  app.post('/enroll', async (req, reply) => {
    const body = req.body as { enrollmentToken?: unknown; nodeName?: unknown };
    if (typeof body?.enrollmentToken !== 'string' || typeof body?.nodeName !== 'string') {
      return reply.code(400).send({ error: 'invalid_name' });
    }
    const r = enrollNode(db, body.enrollmentToken, body.nodeName);
    if ('error' in r) return reply.code(400).send({ error: r.error });
    logger.info('节点已注册', { nodeId: r.nodeId });
    return reply.send({ nodeId: r.nodeId, deviceToken: r.deviceToken });
  });

  app.get('/ws', { websocket: true }, (socket, req) => {
    const auth = req.headers.authorization ?? '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    const node = token ? verifyDeviceToken(db, token) : null;
    if (!node) { socket.close(4401, 'unauthorized'); return; }

    const session: Session = { nodeId: node.nodeId, nodeName: node.name, helloDone: false };
    const helloTimer = setTimeout(() => socket.close(4403, 'hello_timeout'), helloTimeoutMs);

    const sendRes = (id: string, method: string, ok: boolean, payload: unknown): void => {
      socket.send(encodeEnvelope({ v: 1, kind: 'res', id, method, ok, payload }));
    };
    const errPayload = (code: RpcErrorPayload['code'], message: string): RpcErrorPayload => ({ code, message });

    socket.on('message', (raw: unknown) => {
      const env = parseEnvelope(typeof raw === 'string' ? raw : String(raw));
      if (!env) { socket.close(4400, 'bad_envelope'); return; }
      if (env.kind !== 'req') return; // M0：客户端 res/evt 忽略
      if (!session.helloDone && env.method !== RPC.hello) {
        sendRes(env.id, env.method, false, errPayload('unauthorized', '必须先完成 hello'));
        return;
      }
      switch (env.method) {
        case RPC.hello: {
          const ver = (env.payload as HelloReq | null)?.protoVer ?? '';
          if (!isCompatible(ver, PROTOCOL_VERSION)) {
            sendRes(env.id, RPC.hello, false, errPayload('proto_version_mismatch', `hub=${PROTOCOL_VERSION} node=${ver}`));
            socket.close(4402, 'proto_version_mismatch');
            return;
          }
          clearTimeout(helloTimer);
          session.helloDone = true;
          registry.add({ nodeId: session.nodeId, nodeName: session.nodeName, ws: socket, alive: true });
          sendRes(env.id, RPC.hello, true, {
            hubId: identity.hubId, protoVer: PROTOCOL_VERSION,
            serverTime: Date.now(), onlineNodes: registry.onlineNodes(),
          });
          registry.broadcast(EVT.nodeOnline, { nodeId: session.nodeId, nodeName: session.nodeName, at: Date.now() });
          return;
        }
        case RPC.ping: {
          const ts = (env.payload as { ts: number } | null)?.ts ?? 0;
          sendRes(env.id, RPC.ping, true, { ts, serverTime: Date.now() });
          return;
        }
        default:
          sendRes(env.id, env.method, false, errPayload('unknown_method', env.method));
      }
    });

    // keepalive：服务端 ping，两周期无 pong → terminate（半开判死 → node.offline）
    const ka = setInterval(() => {
      if (!session.helloDone) return;
      const conn = registry.get(session.nodeId);
      if (!conn) return;
      if (!conn.alive) { socket.terminate(); return; }
      conn.alive = false;
      socket.ping();
    }, keepaliveIntervalMs);
    ka.unref?.();
    socket.on('pong', () => { const c = registry.get(session.nodeId); if (c) c.alive = true; });

    socket.on('close', () => {
      clearTimeout(helloTimer);
      clearInterval(ka);
      registry.remove(session.nodeId);
      if (session.helloDone) {
        registry.broadcast(EVT.nodeOffline, { nodeId: session.nodeId, at: Date.now() });
      }
    });
  });

  return {
    async listen(port, host = '0.0.0.0') {
      await app.listen({ port, host });
      const addr = app.server.address();
      const actualPort = typeof addr === 'object' && addr ? addr.port : port;
      return `http://${host === '0.0.0.0' ? '127.0.0.1' : host}:${actualPort}`;
    },
    async close() { await app.close(); },
    onlineNodes: () => registry.onlineNodes(),
  };
}
```

- [ ] **Step 4: 跑全量测试（含 Task 2-5 回归）**

Run: `npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(hub): ws 服务——升级鉴权/hello 协商/注册表广播/心跳判死/enroll HTTP 端点"
```

### Task 7: hub——bin 入口、Dockerfile、README、打 tag

**Files:**
- Create: `packages/hub/src/index.ts`、`Dockerfile`、`.dockerignore`
- Modify: `README.md`（部署章节）

**Interfaces:**
- Consumes: Task 4/6 全部
- Produces: `node packages/hub/dist/index.js` 可执行入口；git tag `v0.1.0`（studio Task 8 的 pin 锚点）

- [ ] **Step 1: 实现 bin 入口**（`packages/hub/src/index.ts`）

```typescript
#!/usr/bin/env node
// momo-hub 服务入口：env 配置 → db + 身份 → listen；SIGINT/SIGTERM 优雅退出。
import { loadHubConfig } from './config';
import { openHubDatabase } from './db';
import { ensureHubIdentity } from './identity';
import { createHubServer } from './server';
import { logger } from './logger';

async function main(): Promise<void> {
  const config = loadHubConfig(process.env);
  const db = openHubDatabase(config.dataDir);
  const identity = ensureHubIdentity(db);
  const server = createHubServer({ db, identity });
  const url = await server.listen(config.port);
  logger.info(`momo-hub 已启动 ${url}（数据目录 ${config.dataDir}）`);
  logger.info(`hub id: ${identity.hubId}`);
  logger.info(`加入凭证（studio 首次接入用，可轮换）：${identity.enrollmentToken}`);

  let closing = false;
  const shutdown = (): void => {
    if (closing) return;
    closing = true;
    logger.info('正在关闭…');
    void server.close().finally(() => { db.close(); process.exit(0); });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  void main().catch((err: unknown) => {
    logger.error('启动失败', err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
}

export { main };
```

- [ ] **Step 2: 手工冒烟（一次性，不入测试文件）**

Run: `npx pnpm@9.0.0 build && MOMO_HUB_PORT=18787 MOMO_HUB_DATA_DIR=$(mktemp -d) node packages/hub/dist/index.js & sleep 1 && curl -s localhost:18787/health; kill %1`
Expected: `{"ok":true,"hubId":"…"}`

- [ ] **Step 3: Dockerfile + .dockerignore + README 部署章节**

`Dockerfile`：

```dockerfile
# momo-hub 服务镜像——Node 24 Alpine，数据卷 /data。
FROM node:24-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml tsconfig.base.json tsconfig.protocol.json ./
COPY packages/hub/package.json packages/hub/
RUN pnpm install --no-frozen-lockfile
COPY src ./src
COPY packages/hub/tsconfig.json packages/hub/
COPY packages/hub/src packages/hub/src
RUN pnpm build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production MOMO_HUB_DATA_DIR=/data
COPY --from=build /app/package.json /app/pnpm-workspace.yaml ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages/hub/node_modules ./packages/hub/node_modules
COPY --from=build /app/packages/hub/dist ./packages/hub/dist
EXPOSE 8787
VOLUME /data
CMD ["node", "packages/hub/dist/index.js"]
```

`.dockerignore`：

```
node_modules
dist
packages/hub/dist
data
.git
```

README 追加「快速开始」章，内容包含：裸 Node 三步（clone → `npx pnpm@9.0.0 install && npx pnpm@9.0.0 build` → `MOMO_HUB_PORT=8787 MOMO_HUB_DATA_DIR=~/.momo-hub node packages/hub/dist/index.js`）；Docker 构建/运行（`docker build -t momo-hub . && docker run -p 8787:8787 -v momo-hub-data:/data momo-hub`）；Caddy TLS 反代最小配置（`example.com { reverse_proxy 127.0.0.1:8787 }`）；enrollment token 说明（启动横幅获取、泄漏时删 hub_meta 行或改库轮换——M0 轮换 API 是 identity.ts 内函数，CLI 化留 M1）。

- [ ] **Step 4: 全量验证 + 打 tag**

Run: `npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 lint && npx pnpm@9.0.0 test && npx pnpm@9.0.0 build`
Expected: 全绿。然后：

```bash
git add -A && git commit -m "feat(hub): bin 入口 + Dockerfile + 部署文档"
git tag v0.1.0 && git push origin main --tags
```

---

## 第二部分：momo-studio 仓库（Task 8-12）

仓库根：`/Users/stbearangel/dev/AiProject/moo-studio`。**执行 Task 10/11 前加载 skills：`momo-boundary-rules` / `momo-test-rules` / `momo-ui-preview-rules`。**

### Task 8: studio 接入 protocol 依赖

**Files:**
- Modify: `electron/package.json`（dependencies += momo-hub-protocol）、`pnpm-lock.yaml`
- Create: `electron/src/main/hub/protocol.ts`

**Interfaces:**
- Produces: studio 侧可 `import { ... } from './hub/protocol'`（或 `'../hub/protocol'`）消费全部协议类型/函数——后续任务唯一协议入口，禁直接写 `'momo-hub-protocol'` 字面量（收敛消费面，pin 升级只动一处）

- [ ] **Step 1: 加依赖并安装**

`electron/package.json` `dependencies` 增加：

```json
"momo-hub-protocol": "github:bearangel/momo-hub#v0.1.0"
```

Run: `npx pnpm@9.0.0 install`
Expected: 安装成功（git 依赖 prepare 构建执行）——验证 `ls electron/node_modules/momo-hub-protocol/dist/index.js` 存在

- [ ] **Step 2: 建转发出口 + 类型检查**

`electron/src/main/hub/protocol.ts`：

```typescript
// studio 侧协议唯一入口——全部协议引用经此转发（禁直接写 'momo-hub-protocol' 字面量）。
export * from 'momo-hub-protocol';
```

Run: `npx pnpm@9.0.0 typecheck`
Expected: 双 workspace 通过

- [ ] **Step 3: Commit**

```bash
git add electron/package.json pnpm-lock.yaml electron/src/main/hub/ && git commit -m "feat(hub): 接入 momo-hub-protocol v0.1.0（git tag 依赖）"
```

### Task 9: studio——连接层（状态机/退避/心跳/enroll 客户端/凭证）

**Files:**
- Create: `electron/src/main/hub/client.ts`、`electron/src/main/hub/config-store.ts`、`electron/src/main/hub/creds.ts`
- Test: `electron/tests/hub/client.test.ts`、`electron/tests/hub/config-store.test.ts`

**Interfaces:**
- Consumes: Task 8 `./protocol`
- Produces（Task 10/11 消费）:
  - `HubConnectionState = 'disconnected' | 'connecting' | 'online'`
  - `createHubClient(opts): HubClient`——`opts = { url; nodeName; deviceToken; onStateChange(state, lastError?); onEvent(evt); ws?; helloTimeoutMs?; heartbeatMs?; backoffBaseMs?; backoffMaxMs? }`，`HubClient = { start(): void; stop(): void; state(): HubConnectionState }`
  - `enrollOverHttp(hubUrl, enrollmentToken, nodeName, deps?): Promise<{ nodeId: string; deviceToken: string }>`——失败抛 `Error`（message：`加入凭证无效` / `节点名非法（1-64 个字符）` / `无法连接 hub` / `hub 响应异常`）
  - `createHubConfigStore(db): { read(): HubConfigRow | null; write(row): void; clear(): void }`，`HubConfigRow = { hubUrl: string; nodeName: string; nodeId: string | null }`（kv_store key `hub_config`）
  - creds：`getDeviceToken/setDeviceToken/clearDeviceToken`（keychain key `hub.deviceToken`）

- [ ] **Step 1: 写失败测试**（先加载 skill `momo-test-rules`）

`electron/tests/hub/client.test.ts`：

```typescript
// 连接层契约锁：状态机、退避序列、心跳判死重连、事件透传、enroll 错误映射。
// ws 经工厂注入（EventEmitter fake）——信封编解码用真 protocol（wire 语义不简化）。
import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createHubClient, enrollOverHttp, type WsFactory, type WebSocketLike } from '../../src/main/hub/client';
import { EVT, PROTOCOL_VERSION, RPC, encodeEnvelope, type EventEnvelope } from '../../src/main/hub/protocol';

class FakeSocket extends EventEmitter implements WebSocketLike {
  sent: string[] = [];
  send(data: string): void { this.sent.push(data); this.emit('to-server', data); }
  close(): void { this.emit('close', 1000, Buffer.from('')); }
  terminate(): void { this.emit('close', 1006, Buffer.from('')); }
}
class FakeServer extends EventEmitter {
  sockets: FakeSocket[] = [];
  /** 回放 hello 应答；mutate 为 true 时回放失败应答 */
  replyHello(fail = false): void {
    this.on('to-server', (raw: string, sock: FakeSocket) => {
      const env = JSON.parse(raw) as { kind: string; method: string; id: string };
      if (env.kind === 'req' && env.method === RPC.hello) {
        sock.emit('message', JSON.stringify(fail
          ? { v: 1, kind: 'res', id: env.id, method: RPC.hello, ok: false, payload: { code: 'proto_version_mismatch', message: 'hub=2.0.0' } }
          : { v: 1, kind: 'res', id: env.id, method: RPC.hello, ok: true, payload: { hubId: 'hub', protoVer: PROTOCOL_VERSION, serverTime: 0, onlineNodes: [] } }));
      }
    });
  }
}

function makeFactory(): { factory: WsFactory; server: FakeServer } {
  const server = new FakeServer();
  const factory: WsFactory = () => {
    const s = new FakeSocket();
    server.sockets.push(s);
    queueMicrotask(() => s.emit('open'));
    return s;
  };
  return { factory, server };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe('HubClient 状态机', () => {
  it('start → connecting → online；stop → disconnected', () => {
    const { factory, server } = makeFactory();
    server.replyHello();
    const states: string[] = [];
    const client = createHubClient({
      url: 'ws://x/ws', nodeName: 'a', deviceToken: 't',
      onStateChange: (s) => states.push(s), onEvent: () => undefined, ws: factory,
    });
    client.start();
    expect(states).toEqual(['connecting', 'online']);
    client.stop();
    expect(client.state()).toBe('disconnected');
  });

  it('退避序列 1s/2s/4s…封顶 30s（注入 base=100/max=400 观察 factory 调用节奏）', () => {
    const { factory } = makeFactory(); // 不回 hello → hello 超时 → 重连
    const client = createHubClient({
      url: 'ws://x/ws', nodeName: 'a', deviceToken: 't',
      onStateChange: () => undefined, onEvent: () => undefined, ws: factory,
      helloTimeoutMs: 100, backoffBaseMs: 100, backoffMaxMs: 400,
    });
    client.start();
    // 时间线：t0 connect；t100 hello 超时 terminate；backoff 100 → t200 connect；
    // t300 超时；backoff 200 → t500 connect；t600 超时；backoff 400 → t1000 connect；此后恒 400
    vi.advanceTimersByTime(60_000);
    // 重连次数 = server.sockets.length；60s 内 ≥ 10 次且封顶后节奏恒定（400ms 间隔 → 约 145 次上限，
    // 断言下界即可）；「封顶」由 advanceAndCount 分段推进断言两相邻连接间隔 ≤ 2 × backoffMaxMs
    expect(server.sockets.length).toBeGreaterThanOrEqual(10);
    client.stop();
  });
});
```

（**实现注记**：`advanceAndCount(ms)` 辅助函数分段推进 fake timers（每 400ms 一拍），记录每拍 `server.sockets.length` 增量，断言封顶后每 400ms 恰一次新连接。执行者把该断言补进用例，不许删除「封顶」断言。）

```typescript
describe('HubClient 心跳与事件', () => {
  it('心跳无 pong → terminate 重连（connecting 再现）', () => {
    const { factory, server } = makeFactory();
    server.replyHello(); // 只回 hello，不回 ping
    const states: string[] = [];
    const client = createHubClient({
      url: 'ws://x/ws', nodeName: 'a', deviceToken: 't',
      onStateChange: (s) => states.push(s), onEvent: () => undefined, ws: factory,
    });
    client.start();
    expect(client.state()).toBe('online');
    vi.advanceTimersByTime(25_000 + 25_000); // 第一拍发 ping、第二拍无 res → terminate
    expect(states.filter((s) => s === 'connecting').length).toBeGreaterThanOrEqual(2);
    client.stop();
  });

  it('node.online 事件透传到 onEvent', () => {
    const { factory, server } = makeFactory();
    server.replyHello();
    const got: EventEnvelope[] = [];
    const client = createHubClient({
      url: 'ws://x/ws', nodeName: 'a', deviceToken: 't',
      onStateChange: () => undefined, onEvent: (e) => got.push(e), ws: factory,
    });
    client.start();
    server.sockets[0]!.emit('message', encodeEnvelope({ v: 1, kind: 'evt', method: EVT.nodeOnline, payload: { nodeId: 'n2', nodeName: 'b', at: 1 } }));
    expect(got.map((e) => e.method)).toEqual([EVT.nodeOnline]);
    client.stop();
  });

  it('hello 被拒（版本不匹配）→ 记错误并按退避重连', () => {
    const { factory, server } = makeFactory();
    server.replyHello(true);
    const errors: Array<string | undefined> = [];
    const client = createHubClient({
      url: 'ws://x/ws', nodeName: 'a', deviceToken: 't',
      onStateChange: (_s, err) => errors.push(err), onEvent: () => undefined, ws: factory,
      backoffBaseMs: 100,
    });
    client.start();
    expect(errors.some((e) => e?.includes('hub=2.0.0'))).toBe(true);
    client.stop();
  });
});

describe('enrollOverHttp', () => {
  it('invalid_enrollment → 人话错误；成功返回凭证；网络失败 → 无法连接 hub', async () => {
    const fetchJson = vi.fn()
      .mockResolvedValueOnce({ error: 'invalid_enrollment' })
      .mockResolvedValueOnce({ nodeId: 'n1', deviceToken: 'dt' })
      .mockRejectedValueOnce(new Error('boom'));
    await expect(enrollOverHttp('http://h', 'tok', 'name', { fetchJson })).rejects.toThrow('加入凭证无效');
    await expect(enrollOverHttp('http://h', 'tok', 'name', { fetchJson })).resolves.toEqual({ nodeId: 'n1', deviceToken: 'dt' });
    await expect(enrollOverHttp('http://h', 'tok', 'name', { fetchJson })).rejects.toThrow('无法连接 hub');
  });
});
```

`electron/tests/hub/config-store.test.ts`：

```typescript
// hub 配置 kv 存储：读写清三态（:memory: db + kv_store 最小表结构——与主库 schema 对齐）。
import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import { createHubConfigStore } from '../../src/main/hub/config-store';

function freshDb(): Database.Database {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT)');
  return db;
}

describe('hub config store', () => {
  it('空库 read → null；write → read 回读；clear → null；脏 JSON 容错回 null', () => {
    const db = freshDb();
    const store = createHubConfigStore(db);
    expect(store.read()).toBeNull();
    store.write({ hubUrl: 'https://hub.lan:8787', nodeName: 'mac-main', nodeId: 'n1' });
    expect(store.read()).toEqual({ hubUrl: 'https://hub.lan:8787', nodeName: 'mac-main', nodeId: 'n1' });
    store.clear();
    expect(store.read()).toBeNull();
    db.prepare("INSERT INTO kv_store (key, value) VALUES ('hub_config', '{bad json')").run();
    expect(store.read()).toBeNull();
    db.close();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd electron && npx pnpm@9.0.0 vitest run tests/hub --pool=forks`
Expected: FAIL——模块不存在

- [ ] **Step 3: 实现三文件**

`electron/src/main/hub/client.ts`：

```typescript
// hub 连接层：状态机 + 指数退避重连 + 心跳判死 + enroll HTTP 客户端。
// ws 经工厂注入（测试 fake / 生产 'ws'）；信封语义全部走 protocol 包（不自制 wire 格式）。
import WebSocket from 'ws';
import {
  PROTOCOL_VERSION, RPC, encodeEnvelope, makeRequest, parseEnvelope,
  type EventEnvelope, type ResponseEnvelope,
} from './protocol';

export type HubConnectionState = 'disconnected' | 'connecting' | 'online';

export interface WebSocketLike {
  send(data: string): void;
  close(): void;
  terminate(): void;
  on(event: 'open', cb: () => void): void;
  on(event: 'close', cb: (code: number, reason: Buffer) => void): void;
  on(event: 'error', cb: (err: Error) => void): void;
  on(event: 'message', cb: (data: unknown) => void): void;
}
export type WsFactory = (url: string, headers: Record<string, string>) => WebSocketLike;

const defaultWsFactory: WsFactory = (url, headers) => new WebSocket(url, { headers }) as unknown as WebSocketLike;

export interface HubClientOpts {
  url: string;
  nodeName: string;
  deviceToken: string;
  onStateChange(state: HubConnectionState, lastError?: string): void;
  onEvent(evt: EventEnvelope): void;
  ws?: WsFactory;
  helloTimeoutMs?: number; // 默认 5000
  heartbeatMs?: number;    // 默认 25000（发 ping；下一拍无 res 即判死重连）
  backoffBaseMs?: number;  // 默认 1000
  backoffMaxMs?: number;   // 默认 30000
}

export interface HubClient { start(): void; stop(): void; state(): HubConnectionState }

export function createHubClient(opts: HubClientOpts): HubClient {
  const wsFactory = opts.ws ?? defaultWsFactory;
  const helloTimeoutMs = opts.helloTimeoutMs ?? 5000;
  const heartbeatMs = opts.heartbeatMs ?? 25_000;
  const base = opts.backoffBaseMs ?? 1000;
  const max = opts.backoffMaxMs ?? 30_000;

  let state: HubConnectionState = 'disconnected';
  let sock: WebSocketLike | null = null;
  let attempts = 0;
  let stopped = true;
  let helloTimer: NodeJS.Timeout | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let pongSincePing = true;
  let lastError: string | undefined;

  const setState = (s: HubConnectionState, err?: string): void => {
    state = s;
    opts.onStateChange(s, err);
  };

  const clearTimers = (): void => {
    if (helloTimer) { clearTimeout(helloTimer); helloTimer = null; }
    if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
  };

  const scheduleReconnect = (): void => {
    if (stopped) return;
    const delay = Math.min(base * 2 ** attempts, max);
    attempts += 1;
    setTimeout(() => connect(), delay);
  };

  const connect = (): void => {
    if (stopped) return;
    setState('connecting');
    const s = wsFactory(opts.url, { authorization: `Bearer ${opts.deviceToken}` });
    sock = s;
    s.on('error', (err) => { lastError = err.message; });
    s.on('open', () => {
      s.send(encodeEnvelope(makeRequest(RPC.hello, { protoVer: PROTOCOL_VERSION, nodeName: opts.nodeName })));
      helloTimer = setTimeout(() => s.terminate(), helloTimeoutMs);
    });
    s.on('message', (raw) => {
      const env = parseEnvelope(typeof raw === 'string' ? raw : String(raw));
      if (!env) return;
      if (env.kind === 'evt') { opts.onEvent(env); return; }
      if (env.kind !== 'res') return;
      const res = env as ResponseEnvelope;
      if (res.method === RPC.hello) {
        if (!helloTimer) return; // 迟到响应（已进入重连）——忽略
        clearTimeout(helloTimer); helloTimer = null;
        if (res.ok) {
          attempts = 0;
          pongSincePing = true;
          setState('online');
          heartbeatTimer = setInterval(() => {
            if (!pongSincePing) { s.terminate(); return; }
            pongSincePing = false;
            s.send(encodeEnvelope(makeRequest(RPC.ping, { ts: Date.now() })));
          }, heartbeatMs);
          return;
        }
        // hello 被拒（如版本不匹配）——记错误并按退避重连（hub 可能刚升级）
        lastError = (res.payload as { message?: string } | null)?.message ?? 'hello 被拒';
        clearTimers();
        setState('connecting', lastError);
        scheduleReconnect();
        return;
      }
      if (res.method === RPC.ping && res.ok) pongSincePing = true;
    });
    s.on('close', () => {
      clearTimers();
      if (sock === s) sock = null;
      if (!stopped) { setState('connecting', lastError); scheduleReconnect(); }
    });
  };

  return {
    start(): void {
      if (!stopped) return;
      stopped = false;
      attempts = 0;
      connect();
    },
    stop(): void {
      stopped = true;
      clearTimers();
      sock?.close();
      sock = null;
      setState('disconnected');
    },
    state(): HubConnectionState { return state; },
  };
}

export interface EnrollDeps {
  fetchJson(url: string, init: { method: string; body: string }): Promise<unknown>;
}

/** 首次接入：HTTP POST /enroll 换 device token。错误映射为人话（UI 直接展示）。 */
export async function enrollOverHttp(
  hubUrl: string, enrollmentToken: string, nodeName: string, deps?: EnrollDeps,
): Promise<{ nodeId: string; deviceToken: string }> {
  const fetchJson = deps?.fetchJson ?? (async (url, init) => {
    const res = await fetch(url, {
      method: init.method, body: init.body, headers: { 'content-type': 'application/json' },
    });
    return (await res.json().catch(() => ({ error: 'network' }))) as unknown;
  });
  let body: unknown;
  try {
    body = await fetchJson(`${hubUrl.replace(/\/$/, '')}/enroll`, {
      method: 'POST',
      body: JSON.stringify({ enrollmentToken, nodeName }),
    });
  } catch {
    throw new Error('无法连接 hub');
  }
  const r = body as { nodeId?: string; deviceToken?: string; error?: string };
  if (r.error === 'invalid_enrollment') throw new Error('加入凭证无效');
  if (r.error === 'invalid_name') throw new Error('节点名非法（1-64 个字符）');
  if (typeof r.nodeId !== 'string' || typeof r.deviceToken !== 'string') throw new Error('hub 响应异常');
  return { nodeId: r.nodeId, deviceToken: r.deviceToken };
}
```

`electron/src/main/hub/config-store.ts`：

```typescript
// hub 非密配置（hubUrl/nodeName/nodeId）——kv_store key 'hub_config'（密钥永远不进这里）。
import type { Database } from 'better-sqlite3';

export interface HubConfigRow { hubUrl: string; nodeName: string; nodeId: string | null }

const KEY = 'hub_config';

export interface HubConfigStore { read(): HubConfigRow | null; write(row: HubConfigRow): void; clear(): void }

export function createHubConfigStore(db: Database): HubConfigStore {
  const read = (): HubConfigRow | null => {
    const row = db.prepare('SELECT value FROM kv_store WHERE key = ?').get(KEY) as { value: string } | undefined;
    if (!row) return null;
    try {
      const parsed = JSON.parse(row.value) as Partial<HubConfigRow>;
      if (typeof parsed.hubUrl !== 'string' || typeof parsed.nodeName !== 'string') return null;
      return { hubUrl: parsed.hubUrl, nodeName: parsed.nodeName, nodeId: typeof parsed.nodeId === 'string' ? parsed.nodeId : null };
    } catch {
      return null; // 脏数据容错（browser settings-store 同款读侧纪律）
    }
  };
  return {
    read,
    write(row) {
      db.prepare(`INSERT INTO kv_store (key, value, updated_at) VALUES (?, ?, datetime('now'))
                  ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`)
        .run(KEY, JSON.stringify(row));
    },
    clear() {
      db.prepare('DELETE FROM kv_store WHERE key = ?').run(KEY);
    },
  };
}
```

`electron/src/main/hub/creds.ts`：

```typescript
// device token 凭证——keychain 专用通道（测试经 setKeychainImpl 注入，keychain.ts 既有钩子）。
import { getSecret, setSecret, deleteSecret } from '../storage/keychain';

const TOKEN_KEY = 'hub.deviceToken';

export async function getDeviceToken(): Promise<string | null> { return getSecret(TOKEN_KEY); }
export async function setDeviceToken(token: string): Promise<void> { await setSecret(TOKEN_KEY, token); }
export async function clearDeviceToken(): Promise<void> { await deleteSecret(TOKEN_KEY); }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd electron && npx pnpm@9.0.0 vitest run tests/hub --pool=forks`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add electron/src/main/hub electron/tests/hub && git commit -m "feat(hub): 连接层——状态机/退避重连/心跳判死/enroll 客户端/keychain 凭证"
```

### Task 10: studio——IPC 面与主进程生命周期接线

**Files:**
- Create: `electron/src/main/hub/index.ts`（HubManager 单例）、`electron/src/main/hub/ipc.handlers.ts`
- Modify: `electron/src/main/ipc/index.ts`（+registerHubIpc）、`electron/src/main/index.ts`（whenReady 后 initHub；before-quit stopHub）、`electron/src/preload/index.ts`（api.hub）、`renderer/src/ipc/types.d.ts`（HubApi/HubStatus/NodePresence）
- Test: `electron/tests/hub/ipc.test.ts`

**Interfaces:**
- Consumes: Task 9 全部
- Produces（Task 11 消费；通道名与 types.d.ts 逐一对齐）:
  - IPC invoke：`hub:getStatus` → `HubStatus`；`hub:configure` {hubUrl, nodeName} → `HubStatus`；`hub:enroll` {hubUrl, enrollmentToken, nodeName} → `HubStatus`（失败抛错给人话）；`hub:forget` → void
  - IPC 推送：`hub:stateChanged` {state: HubConnectionState, lastError: string | null}；`hub:nodePresence` {kind: 'online' | 'offline', nodeId: string, nodeName?: string}
  - `HubStatus = { configured: boolean; state: HubConnectionState; hubUrl: string | null; nodeName: string | null; nodeId: string | null; lastError: string | null }`

- [ ] **Step 1: 写失败测试**（先加载 `momo-boundary-rules` + `momo-test-rules`）

`electron/tests/hub/ipc.test.ts`：

```typescript
// hub IPC 面接线锁：通道注册与 payload 透传（manager 逻辑在 client.test 已锁，这里只锁接线）。
import { beforeEach, describe, expect, it, vi } from 'vitest';

const handlers = new Map<string, (...args: unknown[]) => unknown>();
vi.mock('electron', () => ({
  ipcMain: { handle: (ch: string, fn: (...args: unknown[]) => unknown) => handlers.set(ch, fn) },
  BrowserWindow: { getAllWindows: () => [] },
}));

vi.mock('../../src/main/hub/index', () => ({
  initHub: vi.fn(),
  stopHub: vi.fn(),
  hubGetStatus: vi.fn(() => ({ configured: false, state: 'disconnected', hubUrl: null, nodeName: null, nodeId: null, lastError: null })),
  hubConfigure: vi.fn(async (input: { hubUrl: string }) => ({ configured: true, state: 'connecting', hubUrl: input.hubUrl, nodeName: 'a', nodeId: null, lastError: null })),
  hubEnroll: vi.fn(async () => ({ configured: true, state: 'connecting', hubUrl: 'http://h', nodeName: 'a', nodeId: 'n1', lastError: null })),
  hubForget: vi.fn(async () => undefined),
}));

import { registerHubIpc } from '../../src/main/hub/ipc.handlers';
import * as manager from '../../src/main/hub/index';

beforeEach(() => { handlers.clear(); vi.clearAllMocks(); });

describe('hub IPC 接线', () => {
  it('注册四通道；getStatus 透传 manager', async () => {
    registerHubIpc();
    expect([...handlers.keys()].sort()).toEqual(['hub:configure', 'hub:enroll', 'hub:forget', 'hub:getStatus']);
    expect(await handlers.get('hub:getStatus')!()).toEqual(await manager.hubGetStatus());
  });

  it('configure/enroll 透传参数', async () => {
    registerHubIpc();
    await handlers.get('hub:configure')!(null, { hubUrl: 'http://h', nodeName: 'a' });
    expect(manager.hubConfigure).toHaveBeenCalledWith({ hubUrl: 'http://h', nodeName: 'a' });
    await handlers.get('hub:enroll')!(null, { hubUrl: 'http://h', enrollmentToken: 't', nodeName: 'a' });
    expect(manager.hubEnroll).toHaveBeenCalledWith({ hubUrl: 'http://h', enrollmentToken: 't', nodeName: 'a' });
  });

  it('forget 透传', async () => {
    registerHubIpc();
    await handlers.get('hub:forget')!();
    expect(manager.hubForget).toHaveBeenCalledTimes(1);
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd electron && npx pnpm@9.0.0 vitest run tests/hub/ipc.test.ts --pool=forks`
Expected: FAIL——handler 模块不存在

- [ ] **Step 3: 实现**

`electron/src/main/hub/index.ts`：

```typescript
// HubManager 单例——主进程生命周期接线（taskExecutor 同款模块级单例模式）。
// 职责：读配置+凭证 → 起停 HubClient；状态/事件转发全部窗口（webContents.send）。
import { BrowserWindow } from 'electron';
import { getDb } from '../storage/db';
import { logger } from '../logger';
import { createHubClient, enrollOverHttp, type HubClient, type HubConnectionState } from './client';
import { createHubConfigStore, type HubConfigRow } from './config-store';
import { clearDeviceToken, getDeviceToken, setDeviceToken } from './creds';
import { EVT, type EventEnvelope } from './protocol';

export interface HubStatus {
  configured: boolean;
  state: HubConnectionState;
  hubUrl: string | null;
  nodeName: string | null;
  nodeId: string | null;
  lastError: string | null;
}

let client: HubClient | null = null;
let lastError: string | null = null;

const configStore = () => createHubConfigStore(getDb());

function broadcast(channel: string, payload: unknown): void {
  for (const w of BrowserWindow.getAllWindows()) w.webContents.send(channel, payload);
}

export function hubGetStatus(): HubStatus {
  const cfg = configStore().read();
  return {
    configured: cfg !== null,
    state: client?.state() ?? 'disconnected',
    hubUrl: cfg?.hubUrl ?? null,
    nodeName: cfg?.nodeName ?? null,
    nodeId: cfg?.nodeId ?? null,
    lastError,
  };
}

function startClient(hubUrl: string, nodeName: string, deviceToken: string): void {
  client?.stop();
  client = createHubClient({
    url: wsUrlOf(hubUrl),
    nodeName,
    deviceToken,
    onStateChange(state, err) {
      if (err) lastError = err;
      if (state === 'online') lastError = null;
      broadcast('hub:stateChanged', { state, lastError });
    },
    onEvent(evt: EventEnvelope) {
      if (evt.method === EVT.nodeOnline) {
        broadcast('hub:nodePresence', { kind: 'online', ...(evt.payload as { nodeId: string; nodeName: string }) });
      } else if (evt.method === EVT.nodeOffline) {
        broadcast('hub:nodePresence', { kind: 'offline', ...(evt.payload as { nodeId: string }) });
      }
    },
  });
  client.start();
}

/** hubUrl（http/https origin）→ ws/wss URL + /ws 路径。 */
function wsUrlOf(hubUrl: string): string {
  const u = new URL(hubUrl);
  u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:';
  u.pathname = '/ws';
  return u.toString();
}

function normUrl(hubUrl: string): string { return hubUrl.trim().replace(/\/$/, ''); }

/** app ready 后调用：已配置且有凭证则自动连接。 */
export async function initHub(): Promise<void> {
  const cfg = configStore().read();
  if (!cfg) return;
  const token = await getDeviceToken();
  if (!token) { logger.warn('hub 已配置但 keychain 无 device token（跳过连接）'); return; }
  startClient(cfg.hubUrl, cfg.nodeName, token);
}

export async function hubConfigure(input: { hubUrl: string; nodeName: string }): Promise<HubStatus> {
  const store = configStore();
  const prev: HubConfigRow | null = store.read();
  store.write({ hubUrl: normUrl(input.hubUrl), nodeName: input.nodeName.trim(), nodeId: prev?.nodeId ?? null });
  await initHub();
  return hubGetStatus();
}

export async function hubEnroll(input: { hubUrl: string; enrollmentToken: string; nodeName: string }): Promise<HubStatus> {
  const hubUrl = normUrl(input.hubUrl);
  const nodeName = input.nodeName.trim();
  const r = await enrollOverHttp(hubUrl, input.enrollmentToken, nodeName);
  await setDeviceToken(r.deviceToken);
  configStore().write({ hubUrl, nodeName, nodeId: r.nodeId });
  startClient(hubUrl, nodeName, r.deviceToken);
  return hubGetStatus();
}

export async function hubForget(): Promise<void> {
  client?.stop();
  client = null;
  configStore().clear();
  await clearDeviceToken();
  broadcast('hub:stateChanged', { state: 'disconnected', lastError: null });
}

/** before-quit 调用。 */
export function stopHub(): void {
  client?.stop();
  client = null;
}
```

`electron/src/main/hub/ipc.handlers.ts`：

```typescript
// hub 命名空间 IPC——通道名与 types.d.ts / preload 逐一对齐（momo-boundary-rules）。
import { ipcMain } from 'electron';
import { hubConfigure, hubEnroll, hubForget, hubGetStatus } from './index';

export function registerHubIpc(): void {
  ipcMain.handle('hub:getStatus', () => hubGetStatus());
  ipcMain.handle('hub:configure', (_evt, input: { hubUrl: string; nodeName: string }) => hubConfigure(input));
  ipcMain.handle('hub:enroll', (_evt, input: { hubUrl: string; enrollmentToken: string; nodeName: string }) => hubEnroll(input));
  ipcMain.handle('hub:forget', () => hubForget());
}
```

`electron/src/main/ipc/index.ts`：import 区加 `import { registerHubIpc } from '../hub/ipc.handlers';`；`registerIpcHandlers` 内 `registerWindowIpc(...)` 之前加：

```typescript
  // M0 节点互联（spec 2026-10-23）：hub 连接管理四通道 + 两推送
  registerHubIpc();
```

`electron/src/main/index.ts`：`grep -n "registerIpcHandlers\|before-quit" electron/src/main/index.ts` 定位插入点——whenReady 序列 `registerIpcHandlers(...)` 调用之后加 `void initHub();`（`import { initHub } from './hub/index';`）；`before-quit` 处理器内加 `stopHub();`（同文件 import `stopHub`）。

`electron/src/preload/index.ts` api 对象加（对齐既有订阅模式——返回取消订阅函数）：

```typescript
  // M0 节点互联（spec 2026-10-23）：hub 连接状态/接入/忘记 + 状态与节点在线推送
  hub: {
    getStatus: () => invoke('hub:getStatus'),
    configure: (input) => invoke('hub:configure', input),
    enroll: (input) => invoke('hub:enroll', input),
    forget: () => invoke('hub:forget'),
    onStateChanged: (callback) => {
      const handler = (_e: IpcRendererEvent, payload: HubStateChangedEvent): void => callback(payload);
      ipcRenderer.on('hub:stateChanged', handler);
      return () => { ipcRenderer.off('hub:stateChanged', handler); };
    },
    onNodePresence: (callback) => {
      const handler = (_e: IpcRendererEvent, payload: HubNodePresenceEvent): void => callback(payload);
      ipcRenderer.on('hub:nodePresence', handler);
      return () => { ipcRenderer.off('hub:nodePresence', handler); };
    },
  },
```

`renderer/src/ipc/types.d.ts` 加（与 electron 端 `HubStatus` 结构对齐——跨进程独立定义纪律）：

```typescript
/** M0 节点互联（spec 2026-10-23）：hub 连接状态（与 electron/src/main/hub/index.ts HubStatus 对齐） */
export type HubConnectionState = 'disconnected' | 'connecting' | 'online';
export interface HubStatus {
  configured: boolean;
  state: HubConnectionState;
  hubUrl: string | null;
  nodeName: string | null;
  nodeId: string | null;
  lastError: string | null;
}
export interface HubStateChangedEvent { state: HubConnectionState; lastError: string | null }
export interface HubNodePresenceEvent { kind: 'online' | 'offline'; nodeId: string; nodeName?: string }
```

ApiSurface 增加 `hub: HubApi`（形状即上面 preload 六方法；`configure`/`enroll` 入参类型同 handler）。preload 的 `Api` 类型推导会自动收敛。

- [ ] **Step 4: 跑测试确认通过 + 双 workspace typecheck**

Run: `cd electron && npx pnpm@9.0.0 vitest run tests/hub --pool=forks && cd .. && npx pnpm@9.0.0 typecheck`
Expected: PASS + typecheck 双绿

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(hub): IPC 四通道 + 主进程生命周期接线 + preload/types 契约"
```

### Task 11: studio——设置 UI「节点互联」面板

**Files:**
- Create: `renderer/src/components/settings/HubSettingsPanel.tsx`、`renderer/src/stores/hub.store.ts`
- Modify: `renderer/src/stores/settings.store.ts`（union += 'hub'）、`renderer/src/components/settings/SettingsNav.tsx`（CATEGORIES += hub）、`renderer/src/components/settings/SettingsView.tsx`（渲染分支 + 头部注释更新）
- Test: `renderer/src/components/settings/HubSettingsPanel.test.tsx`、`renderer/src/components/settings/SettingsView.test.tsx`（分类锁 11→12）

**Interfaces:**
- Consumes: Task 10 的 `api.hub` 六方法 + `HubStatus`/推送类型
- Produces: 设置第 12 分类「节点互联」（key `'hub'`，icon `Network`，lucide 16px/1.75）；面板三态（未接入表单 / 已接入状态+节点列表 / 错误提示）

**P1 预览门禁（momo-ui-preview-rules，硬性步骤）**：实现前先产出静态预览（HTML 截图或组件树示意）交用户确认后动笔；本面板是 P1（新组件 + 新分类页）。

- [ ] **Step 1: 静态预览获批**（产出三态布局示意：状态徽章行 / 未接入表单〔hub 地址、节点名、加入凭证三输入 + 接入按钮〕/ 已接入〔节点列表：在线点 + 名称 + 本机标记、忘记节点按钮（两步确认——沿用 ProviderModelPicker 防误删交互）〕；全部语义 token，无硬编码色）。**用户确认前不进 Step 2。**

- [ ] **Step 2: 写失败测试**

`renderer/src/stores/hub.store.ts`：

```typescript
// 节点互联设置面板数据源：status 拉取 + 两推送订阅 + 在线节点表维护。
import { create } from 'zustand';
import type { HubNodePresenceEvent, HubStatus } from '../ipc/types.d';

export interface HubNodeRow { nodeId: string; nodeName: string }

interface HubState {
  status: HubStatus | null;
  nodes: HubNodeRow[];
  init(): void;
}

export const useHubStore = create<HubState>((set) => ({
  status: null,
  nodes: [],
  init() {
    void window.api.hub.getStatus().then((status) => set({ status }));
    window.api.hub.onStateChanged((e) => {
      set((s) => (s.status ? { status: { ...s.status, state: e.state, lastError: e.lastError } } : {}));
    });
    window.api.hub.onNodePresence((e: HubNodePresenceEvent) => {
      set((s) => {
        if (e.kind === 'online') {
          const nodes = [...s.nodes.filter((n) => n.nodeId !== e.nodeId), { nodeId: e.nodeId, nodeName: e.nodeName ?? '' }];
          return { nodes };
        }
        return { nodes: s.nodes.filter((n) => n.nodeId !== e.nodeId) };
      });
    });
  },
}));
```

（注：若 window.api 的既有类型别名为其他写法，以 preload/types.d.ts 现行推导为准调整访问路径；hello 快照里的 onlineNodes 初值在 M0 面板通过 state 上线后 hub 侧下一版本补拉——M0 UI 以事件增量维护，重启后列表冷启动为空属预期，面板文案注明「上线通知驱动」。**该缺口记入 Task 12 验收备注**。）

`renderer/src/components/settings/HubSettingsPanel.test.tsx` 骨架（完整断言按下述三态补全）：

```typescript
// 节点互联面板：三态渲染、表单提交接线、presence 增量更新、忘记两步确认。
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { HubSettingsPanel } from './HubSettingsPanel';

const api = {
  hub: {
    getStatus: vi.fn(async () => null),
    configure: vi.fn(),
    enroll: vi.fn(),
    forget: vi.fn(),
    onStateChanged: vi.fn(() => () => undefined),
    onNodePresence: vi.fn(() => () => undefined),
  },
};
vi.stubGlobal('window', { api });

// 用例清单（执行者补全实现）：
// 1. 未接入：渲染三输入（hub 地址/节点名/加入凭证）+ 接入按钮；提交调 api.hub.enroll，
//    enroll reject 时展示人话错误文案（异步断言）
// 2. 已接入：状态徽章（连接中/已连接）+ hub 地址 + 本机节点名 + 在线节点列表；
//    onStateChanged 回调置 online 后徽章切换
// 3. onNodePresence online → 列表新增；offline → 移除
// 4. 忘记节点：首击亮确认文案、再击才调 api.hub.forget（防误删两步确认纪律）
// 5. api.hub.enroll 抛「加入凭证无效」→ 错误行展示原文
beforeEach(() => vi.clearAllMocks());
describe('HubSettingsPanel', () => { it('占位——按上述清单补全', () => { expect(true).toBe(true); }); });
```

- [ ] **Step 3: 跑测试确认失败→实现面板与分类接线**

Run: `cd renderer && npx pnpm@9.0.0 vitest run src/components/settings/HubSettingsPanel.test.tsx`

实现 `HubSettingsPanel.tsx`（按已批预览；原子组件优先——`components/ui/` 的 Button/Input/Badge；语义 token；lucide `Network`/`Trash2` 16px 1.75；状态徽章：`已连接`（text-accent-600）/`连接中`（text-secondary，动效可有可无）/`未连接`（text-tertiary）+ lastError 行）。分类三件套：

`settings.store.ts` union 加 `| 'hub'`；`SettingsNav.tsx` CATEGORIES 在 browser 之后插 `{ key: 'hub', label: '节点互联', icon: Network }`；`SettingsView.tsx` 渲染分支 `{active === 'hub' && <HubSettingsPanel />}` + 头部注释「11 个分类」改「12 个分类」并把「节点互联」写回清单（原注释里它随 p2p 下架，本次以 hub 语境回归）。

`SettingsView.test.tsx:108` 分类锁同步：`渲染 11 个分类菜单项` → `渲染 12 个分类菜单项`，顺序断言在 browser 之后插入节点互联。

- [ ] **Step 4: 跑测试确认通过 + typecheck + lint**

Run: `cd renderer && npx pnpm@9.0.0 vitest run src/components/settings/ && cd .. && npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 lint`
Expected: PASS（含 SettingsView 12 分类锁）+ 双绿

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(hub): 设置「节点互联」面板——接入表单/状态徽章/在线节点列表/两步确认忘记"
```

### Task 12: M0 验收——全量验证、版本账本、真机验收

**Files:**
- Modify: momo-studio `CHANGELOG.md`（M0 条目）、三处 `package.json` alpha +1（`2.1.0-alpha.28`）；momo-hub `CHANGELOG.md`（新建，0.1.0 条目）

- [ ] **Step 1: momo-hub 仓终验**

Run: `npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 lint && npx pnpm@9.0.0 test && npx pnpm@9.0.0 build`
Expected: 全绿；新建 `CHANGELOG.md`（0.1.0——M0 连接基建交付面）；commit `docs: CHANGELOG 0.1.0`；若 protocol 自 v0.1.0 tag 后有改动 → bump 根 package version 至 0.2.0 并重打 tag、同步 studio pin（纪律：tag 即契约）

- [ ] **Step 2: momo-studio 仓终验**

Run: `npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 lint && npx pnpm@9.0.0 test && npx pnpm@9.0.0 build`
Expected: 全绿（含既有回归——资源库/设置面无破坏）

- [ ] **Step 3: 版本与账本**

三处 `package.json`（根/electron/renderer）`2.1.0-alpha.27` → `2.1.0-alpha.28`（特性合入纪律）；`CHANGELOG.md` 增 M0 条目（节点互联：hub 独立服务接入、协议 v0.1.0、设置面板、双设备互见；已知限制：节点列表为事件增量维护、重启后冷启动为空）；commit `chore: 节点互联 M0 特性合入 alpha 号 +1`。

- [ ] **Step 4: 真机验收（加载 skill `momo-acceptance`）**

按 spec M0 验收形态：hub 跑本机（`node packages/hub/dist/index.js`）+ 两台 studio（本机多实例隔离 userData 或两台真机）：A/B 分别 enroll → 双方设置页互见上线；断开一方 → 另一方 30s 内见离线（心跳判死实测）；enroll 错凭证 → 人话报错。产出 `.omo/qa-reports/` 报告。

- [ ] **Step 5: 收尾 commit + push**

```bash
git add -A && git commit -m "chore: M0 节点互联验收收尾"
git push origin main
```

---

## 附录：任务间接口速查

| 产出 | 定义处 | 消费处 |
|---|---|---|
| 信封/编解码 | Task 2 `src/envelope.ts` | Task 3/6/9 |
| RPC/EVT/版本/错误码 | Task 3 | Task 6/9 |
| `openHubDatabase`/`ensureHubIdentity` | Task 4 | Task 5/6/7 |
| `enrollNode`/`verifyDeviceToken`/`timingSafeEqualStr` | Task 5 | Task 6 |
| `createHubServer` | Task 6 | Task 7 |
| git tag `v0.1.0` | Task 7 | Task 8（studio pin） |
| `./hub/protocol` 出口 | Task 8 | Task 9/10 |
| `createHubClient`/`enrollOverHttp`/config-store/creds | Task 9 | Task 10 |
| IPC 四通道 + 两推送 + `HubStatus` | Task 10 | Task 11 |
| `useHubStore` + `HubSettingsPanel` + 12 分类 | Task 11 | 验收（Task 12） |
