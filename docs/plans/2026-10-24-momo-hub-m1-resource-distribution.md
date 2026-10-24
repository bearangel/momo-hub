# momo-hub M1（资源分发·私有 marketplace）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 私有 marketplace 交付——任意 studio 发布 custom 资源到 hub（agent/skill/mcp/plugin 四类），任意 studio 资源库 hub 页签浏览/安装/更新；折入 M0 终审补丁包。

**Architecture:** hub 新增资源仓（SQLite v2 两表 + 内容寻址 blobs + 三 HTTP 端点 + resource.published 事件）；studio 新增第五 ResourceSource（HubProvider，SWR/退避/degraded 复用 marketplace client 模式）+ installPackage 复用（https 白名单放宽）+ plugin 打包/安装新链 + 发布面 UI。

**Tech Stack:** 同 M0（Node 24 / TS strict / Fastify 5 + @fastify/multipart / better-sqlite3 / ws / vitest forks；studio Electron CJS + zustand + 语义 token）。

**Spec:** `docs/specs/2026-10-24-momo-hub-m1-resource-distribution.md`（C1-C7 裁定 + §3-§10 契约——本计划按 spec 逐节展开，冲突以 spec 为准）

## Global Constraints

- 两仓分支：momo-hub `feat/m1-resources`、momo-studio `feat/hub-m1`（从各自 main 切出）；每任务 commit；Conventional Commits
- TS strict 禁 any/@ts-ignore；注释中文；vitest 非正常退出查孤儿 worker；hub 仓测试 forks 池
- 协议纪律：protocol 变更 hub 仓先行；PROTOCOL_VERSION → 1.1.0；包版本与 tag **v0.1.1**（用户裁定 C7）；studio pin 随之升级
- 安全红线：服务端自算 checksum（C6）；S1 字符集原样（SLUG_PATTERN/VERSION_PATTERN）；插件包 symlink 拒绝；url 形态 mcp 不可发布；blob 不落日志
- studio 侧任务加载 skills：`momo-test-rules`（写测试）、`momo-boundary-rules`（IPC/协议面）、`momo-ui-preview-rules`（T14 UI）、`momo-feature-scope-rules`（T9/T14 消费面矩阵——新建×存量×builtin 全覆盖防只补单点）
- spec 裁定 C1-C7 为硬约束；plugin 面（T11/T12）若实施中发现插件注册链耦合超预期，按 spec §8 备选拆 M1.5——发现即报控制器裁定，不得自行降级
- 测试中 hub HTTP mock 用 node:http 本地服务（studio 测试不依赖 hub 运行时）

## Review Focus

1. **multipart 超限中断清理**：publish 流式收包 >200MB 中断连接 + 删临时件 + 不留半个 blob → T6 用例（注满 201MB 假流）
2. **checksum 双锚**：服务端自算入库（不信自报，manifest 不收 checksum 字段）+ 下载侧客户端复算拒绝 → T6/T7/T10 三层各锁一段
3. **409 并发竞态**：两连接并发发布同 slug+version——事务 + 唯一主键兜底，恰一个 201 一个 409 → T6 用例（Promise.all 并发）
4. **plugin 打包越界**：symlink / `..` 逃逸 / dataDir / node_modules 混入 → 全拒绝或全排除 → T11 用例矩阵
5. **hub 源降级正确性**：hub 离线时 SWR 返旧值 + degraded=true + 不阻塞切页；未接入时引导态非报错 → T9 用例（复用 marketplace client 测试模式）

---

## M1a · M0 补丁包（Task 1-3）

### Task 1: hub 仓收尾——README/CI Docker 构建/OBS-2 备案

**Files:** Modify `README.md`、`.github/workflows/ci.yml`、`CHANGELOG.md`

**Interfaces:** Produces: CI 含 docker build 验证步骤（无 daemon 环境的实构替代）；README 状态行更新

- [ ] Step 1: README「状态」改为「M0 已交付（tag v0.1.0）——M1 资源分发开发中」；快速开始章 `git clone <repo>` 占位换 `https://github.com/bearangel/momo-hub`
- [ ] Step 2: ci.yml `check` job 后加 `docker` job（ubuntu-latest 自带 daemon）：`uses: actions/checkout@v4` + `run: docker build -t momo-hub:ci .`（只验构建，不 run——run 需起服务，冒烟留 M1e）
- [ ] Step 3: CHANGELOG 0.1.0 条目追加备注行：「同名节点重复 enroll = 凭证轮换语义（设计即如此，OBS-2 备案）」
- [ ] Step 4: 验证 `npx pnpm@9.0.0 typecheck && npx pnpm@9.0.0 test`；yaml 语法本地 `node -e "require('js-yaml')"` 或 actionlint 不可用则目检
- [ ] Step 5: Commit `chore: M0 收尾——README 状态与 clone URL 落实、CI 增加 docker build 验证、OBS-2 备案`

### Task 2: studio OBS-1——hello 快照透出（presence 种子）

**Files:** Modify `electron/src/main/hub/client.ts`、`electron/src/main/hub/index.ts`、`renderer/src/stores/hub.store.ts`、`renderer/src/components/settings/HubSettingsPanel.tsx`；Test `electron/tests/hub/client.test.ts`、`electron/tests/hub/manager.test.ts`

**Interfaces:**
- Consumes: HelloRes.onlineNodes（hub 自 M0 T6 起已实发——归因修正：缺口在 client 丢弃）
- Produces: `HubClientOpts.onHelloSnapshot?: (nodes: Array<{nodeId: string; nodeName: string}>) => void`——hello res 成功时回调一次；manager 据此对每个非本机节点广播 `hub:nodePresence {kind:'online', nodeId, nodeName}`（本机除外，防与自合成行重复）；store 初始化列表由事件种子自然填充（现有 dedupe 按 nodeId 兜底）

- [ ] Step 1: 失败测试：client——hello 成功回调 onHelloSnapshot（payload.onlineNodes 原样）；manager——onHelloSnapshot 广播 presence 种子且**排除本机 nodeId**；既有用例回归
- [ ] Step 2: 跑红（`cd electron && npx pnpm@9.0.0 vitest run tests/hub --pool=forks`）
- [ ] Step 3: 实现三处（client 回调 / manager 过滤本机广播 / store 不动——事件链已覆盖；面板 M0 冷启动 helper 文案「上线通知驱动」改为「连接时同步」）
- [ ] Step 4: 全绿 + typecheck + lint（hub 范围）
- [ ] Step 5: Commit `fix(hub): hello 快照透出在线节点种子——修冷启动列表为空（OBS-1 归因修正：hub 已下发、client 曾丢弃）`

### Task 3: studio D1 换址入口 + 死目录清理 + types.d 统一

**Files:** Modify `renderer/src/components/settings/HubSettingsPanel.tsx` + 其测试、`renderer/src/stores/hub.store.ts`；Delete `momo-hub/`（studio 仓根死目录）；Modify 全仓 `'../ipc/types.d'` specifier → `'../ipc/types'`（仅 renderer 侧机械替换，preload 惯例不动）

**Interfaces:**
- Consumes: `hub:configure` 换址语义（M0 R14：换址→清 token+nodeId→强制重新接入）
- Produces: 已接入态身份区新增「修改 hub 地址」次级按钮 → 弹确认（文案：「修改地址后将断开并需要重新接入」）→ 调 configure(新地址) → 面板落到换址引导态（M0 已有）

- [ ] Step 1: 失败测试：入口按钮渲染 + 点击两步确认后调 api.hub.configure；死目录删除后 typecheck 仍绿（证明零引用）
- [ ] Step 2: 跑红
- [ ] Step 3: 实现 + `git rm -r momo-hub/` + specifier 机械替换（`grep -rl "ipc/types.d" renderer/src | xargs sed -i '' 's|ipc/types\.d|ipc/types|g'` 后逐文件复核）
- [ ] Step 4: settings/stores 套件 + typecheck + lint
- [ ] Step 5: Commit `feat(hub): 已接入态换址入口（D1）；清理 momo-hub 原型死目录；types specifier 统一`

---

## M1b · hub 资源仓（Task 4-7）

### Task 4: protocol 1.1.0——资源类型/事件/S1 常量

**Files:** Create `src/resources.ts`；Modify `src/events.ts`（EVT += resourcePublished）、`src/version.ts`（1.0.0→1.1.0）、`src/index.ts`；Test `packages/hub/tests/protocol-resources.test.ts`

**Interfaces:**
- Produces（T6/T7/T8+ 消费）:
  - `HubResourceType = 'agent' | 'mcp' | 'skill' | 'plugin'`
  - `HubResourceMeta`（spec §3.2 全字段——slug/type/name/version/author/description/readme/tags/category/iconEmoji/verificationStatus:'unverified'/downloadUrl/checksum/sizeBytes/publishedAt/versions）
  - `HubCatalog { version: string; updatedAt: string; items: HubResourceMeta[] }`
  - `EVT.resourcePublished = 'resource.published'`；`ResourcePublishedEvent { slug; type; name; version; author; at }`
  - S1 常量拷贝（注释指回 momo-studio marketplace/types.ts:60-70，跨仓不引依赖，两仓一致性由 T4 测试锁）：`SLUG_PATTERN`/`VERSION_PATTERN`/`SHA256_HEX_PATTERN` + `isValidHubSlug/isValidHubVersion/isValidHubChecksum`
  - `validateHubManifest(raw: unknown): HubPublishManifest | null`——manifest part 结构校验（type 枚举四类、slug/version/checksum-无此字段 过 S1、字符串字段、tags 数组、slug≤64/name≤128/readme≤64KB；返回归一化对象或 null）
  - `HubPublishManifest`：meta 字段减去 downloadUrl/checksum/sizeBytes/publishedAt/versions（发布时客户端不报这些——C6）

- [ ] Step 1: 失败测试：类型 round-trip；S1 字符集正反例（slug 好 `a-b.c_d` 坏 `A/b!`；version 好 `1.2.3+build` 坏 `1.2.3 x`）；validateHubManifest 好/坏矩阵（缺 type/type 越枚举/readme 超限/未知字段容忍）；EVT 常量名稳定断言
- [ ] Step 2: 跑红（`npx pnpm@9.0.0 build && npx pnpm@9.0.0 test`）
- [ ] Step 3: 实现四文件（version bump 使既有 protocol-catalog 测试 `majorOf(PROTOCOL_VERSION)===1` 仍绿）
- [ ] Step 4: 全绿 + typecheck + lint
- [ ] Step 5: Commit `feat(protocol): 资源分发契约——四类型/HubCatalog/发布事件/S1 常量拷贝与 manifest 校验（1.1.0）`

### Task 5: hub 迁移 v2 + blobs 存储

**Files:** Modify `packages/hub/src/db.ts`（MIGRATIONS 追加 v2 两表——spec §4 SQL 逐字）；Create `packages/hub/src/blobs.ts`；Test `packages/hub/tests/blobs.test.ts`

**Interfaces:**
- Produces: `saveBlobStream(source: NodeJS.ReadableStream, maxBytes: number, dataDir: string): Promise<{ checksum: string; sizeBytes: number }>`
  - 流式 sha256 + 写 `{dataDir}/blobs/.tmp-{uuid}` → 完成且 ≤maxBytes → rename 至 `blobs/{sha256}.tar.gz`（已存在则删临时件幂等返回）；超限/中断 → 删临时件 + throw（不留半个 blob——Review Focus #1）
  - `openBlob(dataDir, checksum): string`（路径合成，存在性调用方查）；禁止目录逃逸（checksum 必须匹配 SHA256_HEX_PATTERN）

- [ ] Step 1: 失败测试：正常流落盘+checksum 正确（与 node:crypto 手算对照）；同内容重存幂等去重；超限流中断 + 临时件清理 + 抛错；坏 checksum 路径参数拒绝
- [ ] Step 2: 跑红
- [ ] Step 3: 实现（db.ts 追加迁移后 identity 测试回归仍绿——迁移幂等）
- [ ] Step 4: 全绿 + typecheck + lint
- [ ] Step 5: Commit `feat(hub): 迁移 v2 资源两表 + 内容寻址 blobs 流式存储（超限中断清理/幂等去重）`

### Task 6: hub publish 端点（multipart 流式 + 校验矩阵 + 409）

**Files:** Modify `packages/hub/package.json`（+= `@fastify/multipart ^9`）；Create `packages/hub/src/resources.ts`（路由注册模块）；Modify `packages/hub/src/server.ts`（挂载）；Test `packages/hub/tests/resources-publish.test.ts`

**Interfaces:**
- Consumes: T4 validateHubManifest、T5 saveBlobStream、M0 auth/registry
- Produces（wire 契约，spec §3.1 逐字）:
  - `POST /resources/publish`（Bearer 鉴权）：multipart 两 part——`manifest`（JSON）+ `package`（binary 流）
  - 处理序：鉴权 → manifest part 收齐解析校验（400 invalid_manifest/invalid_slug/invalid_version）→ version_exists 预查（409）→ saveBlobStream 流式收包（服务端自算 checksum，413 超限中断）→ 事务（resources upsert latest_version + resource_versions INSERT；唯一键冲突回滚→409——Review Focus #3）→ 201 `{slug, version, checksum, sizeBytes}`
  - `registerResourceRoutes(app, {db, dataDir, registry})`——server.ts 挂载（registry 供 T7 事件用，本任务先注入不消费）

- [ ] Step 1: 失败集成测试（真实 listen + fetch FormData 上传）：正常发布 201 + DB 落行 + blob 落盘；坏 slug/version/type 400 矩阵；重复版本 409；并发同版本 Promise.all 恰一 201 一 409；>200MB 流 413 + 临时件清理；未鉴权 401；坏 multipart 400
- [ ] Step 2: 跑红
- [ ] Step 3: 实现（multipart 配置 limits fileSize 不设上限——手动流控以实际落盘字节为准；事务用 better-sqlite3 同步事务）
- [ ] Step 4: 全绿（含 M0 全量回归）+ typecheck + lint
- [ ] Step 5: Commit `feat(hub): 资源发布端点——multipart 流式收包/服务端自算校验和/校验矩阵/版本不可变(409)`

### Task 7: hub catalog/download 端点 + 发布事件 + 版本定档

**Files:** Modify `packages/hub/src/resources.ts`、根 `package.json` 与 `packages/hub/package.json`（0.1.0→0.1.1）；Test `packages/hub/tests/resources-catalog.test.ts`

**Interfaces:**
- Produces:
  - `GET /resources/catalog`（Bearer）：JOIN 组装 HubCatalog——items 按 latest_version、versions 数组新→旧、downloadUrl = `{请求Origin}/resources/{slug}/{version}/download`（用 req 的 host/协议——反代场景天然正确）
  - `GET /resources/{slug}/{version}/download`（Bearer）：blob 流 + `X-Checksum-Sha256` + `X-Published-By` 头；404 未知
  - publish 成功尾部：`registry.broadcast(EVT.resourcePublished, {slug, type, name, version, author, at})`（在线 studio 角标链）
  - 版本定档：两处 package.json → 0.1.1；`git tag v0.1.1` 推送（studio T8 pin 锚点）

- [ ] Step 1: 失败集成测试：publish 两个版本 → catalog 字段逐项断言（versions 序/latest/downloadUrl 组装）→ download 流 sha256 复算 == X-Checksum-Sha256 == DB 值（Review Focus #2 中段）；404；**事件链**——ws 客户端在线收到 resource.published（真实 socket，复用 M0 server.integration 模式）
- [ ] Step 2: 跑红
- [ ] Step 3: 实现 + 版本 bump + 全量验证阶梯（typecheck/lint/test/build）
- [ ] Step 4: Commit `feat(hub): 资源目录与下载端点 + 发布事件广播；定档 v0.1.1` + `git tag v0.1.1 && git push origin feat/m1-resources --tags`

---

## M1c · studio hub 源（Task 8-10）

### Task 8: studio pin v0.1.1 + installPackage origin 白名单

**Files:** Modify `electron/package.json`（pin → `github:bearangel/momo-hub#v0.1.1`）、`electron/src/main/marketplace/installer.ts`；Test `electron/tests/marketplace/installer-origin.test.ts`

**Interfaces:**
- Produces: `registerTrustedDownloadOrigin(origin: string): void`——installer 模块级信任集；downloadUrl 检查改为「https:// 开头 **或** 以任一信任 origin 开头」（C2）；信任集同时约束 `downloadFile()`（流式下载同规则）；hub origin 形态 `http(s)://host[:port]`（尾斜杠归一）

- [ ] Step 1: 失败测试：未注册 origin 的 http URL 拒绝（S1 原行为保持）；注册后同 URL 放行；https 恒放行不受影响；origin 尾斜杠归一
- [ ] Step 2: 跑红 → Step 3: 实现（含 pin 升级 + `npx pnpm@9.0.0 install` 验证 dist 新版 1.1.0 产物、PROTOCOL_VERSION 引用处无破坏）→ Step 4: 全绿 + typecheck → Step 5: Commit `feat(hub): protocol pin v0.1.1；installPackage 下载源白名单（hub origin，https 恒信）`

### Task 9: HubProvider——第五 ResourceSource

**Files:** Create `electron/src/main/resource/hub/momo-hub.ts`；Modify 资源域注册处（resource/hub/ provider 目录挂载点——以 smithery 挂载方式为模板）、`resource` IPC 状态面（getCatalogStatus 扩展 hub 源）；Test `electron/tests/resource/hub-provider.test.ts`

**Interfaces:**
- Consumes: T8 白名单（注册 hub origin）、M0 HubManager（hubGetStatus 取 hubUrl/连接态）
- Produces:
  - `HubProvider implements HubProviderImpl`——**以 `resource/hub/smithery.ts` 为结构模板**（接口形状、错误处理、provider 元信息注册以该文件现行为准），行为差异：
    1. 目录源：HTTP GET `{hubUrl}/resources/catalog`（Bearer deviceToken 头）；响应经 `validateHubCatalog`（HubCatalog 结构校验——T8 后 protocol 包提供，或 provider 本地实现同构校验，实施时择一并在测试锁）
    2. 缓存策略逐条对齐 marketplace/client.ts 模式：TTL 5min 成功缓存 + 60s 失败退避负缓存 + lastGood 直返 + 后台单飞刷新 + degraded 布尔透传（`resource:getCatalogStatus` 聚合返回 `{source:'hub', degraded}`）
    3. HubResourceMeta → MarketplaceItem 映射（type 'plugin' 项在 M1c **过滤不输出**——plugin 消费面 M1d 接通；versions/installCount 丢弃语义）
    4. **未接入引导态**：hubGetStatus().configured === false → list 返回空目录 + degraded=false + `notEnrolled: true` 标志（IPC 透传，renderer 显示「未接入 momo-hub」引导链到设置）
    5. hub 在线但 catalog 401（token 失效）→ degraded=true（不静默空目录）
  - hub origin 注册：provider 初始化/配置变更时 `registerTrustedDownloadOrigin(hubUrl)`

- [ ] Step 1: 失败测试（node:http 本地 mock hub——catalog 200/401/超时三态 + download 流）：SWR 行为（首拉阻塞/退避窗零网络/恢复后单飞刷新——复用 marketplace client.test 的时钟前移工具模式）；未接入引导态；plugin 过滤；401 degraded
- [ ] Step 2: 跑红 → Step 3: 实现 → Step 4: 全绿 + typecheck + lint + **消费面矩阵自查**（momo-feature-scope-rules：新建 hub 源 × 存量四源并存 × builtin 不受扰——资源库 UI 回归由 T10 锁）→ Step 5: Commit `feat(hub): 资源库第五源 HubProvider——SWR/退避/degraded/未接入引导/plugin 预过滤`

### Task 10: 三类安装端到端 + 五源回归锁

**Files:** Test `electron/tests/resource/hub-install.e2e.test.ts`、`renderer/src/components/library/`（或资源库页签测试所在处，以现行文件为准）回归更新

**Interfaces:**
- Consumes: T9 provider 输出 MarketplaceItem → `installPackage` 全链
- Produces: 端到端测试（进程内 node:http mock hub + 真打包产物）：
  1. agent：真 manifest.yaml（parseAgentManifest 可解析形态）打进 tar.gz → install → `listAgentDefinitions` 出现 + source 正确 + 越权 defaultTools 钳制生效（marketplace 同款语义保持）
  2. skill：真 SKILL.md 包 → install → skill_definitions 落行
  3. mcp：真 package.json（合法 npm 名）→ install → mcp_definitions 注册
  4. checksum 不匹配 → 拒绝安装 + cache 清理（Review Focus #2 末段）
  5. hub 项 downloadUrl 为 hub origin http（非 https）→ 白名单放行安装成功（C2 端到端）
- 资源库页签回归：五源并列渲染（builtin/marketplace/custom/smithery/hub）、hub 未接入引导态文案、degraded 提示条

- [ ] Step 1: 失败测试 → Step 2: 跑红 → Step 3: 修通（预期仅微调——链路各段已绿；若发现链路断点如实报告再修）→ Step 4: 全绿 + typecheck → Step 5: Commit `test(hub): 三类资源 hub→安装端到端 + 五源资源库回归锁`

---

## M1d · plugin 面（Task 11-12）

> ⚠️ 本块是 spec C1 的重面。实施前先做**探索步**（探明 plugin 域现有注册链入口）；若发现「插件注册链与目录形态强耦合、无法以等价方式落地归档安装」，**停：报控制器裁定是否拆 M1.5**（spec §8 备选），不得自行降级或大改 plugin 域。

### Task 11: plugin 发布打包

**Files:** Create `electron/src/main/plugin/publish-pack.ts`；Test `electron/tests/plugin/publish-pack.test.ts`

**Interfaces:**
- Consumes: plugin 域 `resolvePluginRoot`、`validateManifest`（plugin/manifest.ts）、installer 域 `computeChecksum` 形态（可复用或同构实现）
- Produces: `packPluginForPublish(pluginId: string): Promise<{ archivePath: string; checksum: string; sizeBytes: number; manifest: PluginManifest }>`
  - 解析插件根目录 → manifest 过 validateManifest（不过即 throw）
  - 打包白名单（Review Focus #4）：仅常规文件；**排除** `dataDir/`（插件运行数据）与 `node_modules/`（spec §6.4——插件包须自身可 require）；symlink / 设备文件 / 绝对路径条目 → 整包拒绝
  - `tar -czf`（execFile 数组参数，不经 shell——与 extractTarGz 对称）到临时目录，返回归档路径 + sha256 + size（≤200MB 超限抛错）
  - 归档内容 = 插件根相对路径树（顶层即 manifest.json 等，与 plugin 包目录形态一致——安装侧解压即得等价目录）

- [ ] Step 1: 失败测试矩阵：正常打包（文件清单断言——含 manifest.json/模块、不含 dataDir/node_modules）；symlink 混入拒绝；越界 `..` 条目拒绝；manifest 非法拒绝；超限拒绝；checksum 与手算对照
- [ ] Step 2: 跑红 → Step 3: 实现 → Step 4: 全绿 + typecheck + lint → Step 5: Commit `feat(plugin): 插件发布打包——白名单排除/越界拒绝/校验和锚定`

### Task 12: plugin hub 安装链

**Files:** 探明后定（预期 Modify installer 或 plugin 域装配处 + `electron/src/main/resource/hub/momo-hub.ts`〔T9 的 plugin 过滤在此解开——消费面矩阵联动：hub 页签/成员候选/插件设置三处〕）；Test `electron/tests/plugin/hub-install.test.ts`

**Interfaces:**
- 探索步（先行）：`grep -rn "PluginRecord" electron/src/main/plugin/ | grep -i "insert\|register\|install"` + PluginSettings IPC 面定位**现有本地插件安装/注册入口**（manifest→PluginRecord 落库→assembly→工具模块加载→mcp-sync 全链的现成函数）
- Produces: hub plugin 项安装 = 下载（白名单+checksum）→ 解压（tar + symlink 拒绝——installer 同款）→ 落插件包目录 → **复用现有注册入口**（目标：与本地安装完全等价——PluginRecord 落库、工具模块可加载、mcp-sync 副作用一致）
- T9 的 plugin 预过滤解开的消费面矩阵（momo-feature-scope-rules）：hub 页签可见 plugin 项 / 安装后插件设置出现并可启用 / 卸载级联（既有链）不受扰

- [ ] Step 1: 探索步产出入口结论写入任务报告（若无可复用入口→BLOCK 上报裁定）
- [ ] Step 2: 失败测试：真插件包（manifest+工具模块+mcp.json 最小样本）经 mock hub → 安装 → PluginRecord 落库 + 工具模块 require 成功 + mcp_definitions 出现 plugin 源条目；坏 checksum 拒绝；symlink 包拒绝；重复安装幂等
- [ ] Step 3: 跑红 → 实现（含 T9 过滤解除 + 页签矩阵回归）→ Step 4: 全绿 + typecheck + lint → Step 5: Commit `feat(plugin): hub 插件安装链——与本地安装等价（注册/工具/mcp 同步）+ 消费面三处接通`

---

## M1e · 发布面 + 更新角标 + 验收（Task 13-15）

### Task 13: 发布编排（主进程）——四类打包上传

**Files:** Create `electron/src/main/hub/publish.ts`、`electron/src/main/hub/ipc.handlers.ts` 扩展、preload/types.d.ts 契约扩展；Test `electron/tests/hub/publish.test.ts`

**Interfaces:**
- Consumes: T11 packPluginForPublish；agent 域 `parseAgentManifest` 逆（yamlDump 导出 manifest.yaml）、skill cachePath、mcp 定义导出（stdio 形态守卫——url 形态 throw「url 形态 MCP 不可发布」红线）；M0 enrollOverHttp 的 fetch 形态
- Produces:
  - `buildPublishPackage(type, ref, meta): Promise<{ manifest: HubPublishManifest; archivePath: string }>`——四类各自打包（agent：custom 定义→manifest.yaml+readme→tar.gz；skill：cachePath 目录→tar.gz；mcp：package.json stub→tar.gz；plugin：packPluginForPublish）
  - `publishToHub(input): Promise<{ slug; version; checksum; sizeBytes }>`——deviceToken 鉴权 multipart POST（fetch FormData），错误映射人话（409→「该版本已存在」/413→「超过 200MB 上限」/401→「连接已失效，请重新接入」/网络→「无法连接 hub」）
  - IPC `hub:publishResource {type, ref, meta} → PublishResult`；**推送 `hub:resourcePublished`**（manager onEvent 新增 EVT.resourcePublished 分支 → 全窗口 webContents.send + preload `hub:onResourcePublished` 订阅 + types.d 契约——boundary 三处对齐）；发布面守卫：hubGetStatus 非 online 时报「hub 未连接」；type/ref 非法 400 文案
- 发布范围守卫（C3）：agent 仅 source='custom'；mcp 仅自定义 stdio；skill/plugin 本地包天然合规——越界即 throw 人话

- [ ] Step 1: 失败测试：四类 buildPublishPackage 形态断言（agent tar 含 manifest.yaml 且 parseAgentManifest 可逆解析；skill tar 含 SKILL.md；mcp stub npm 名合法；plugin 委派 T11）；publishToHub 错误映射矩阵（mock fetch：409/413/401/网络）；C3 守卫（marketplace source agent 拒绝；url mcp 拒绝）；IPC 接线
- [ ] Step 2: 跑红 → Step 3: 实现（IPC + preload + types.d.ts 三处对齐——boundary 规则）→ Step 4: 全绿 + typecheck + lint → Step 5: Commit `feat(hub): 四类资源发布编排——打包/上传/人话错误/C3 范围守卫 + IPC 契约`

### Task 14: 发布 UI + 更新角标（P1 预览门禁）

**Files:** Create `renderer/src/components/library/PublishDialog.tsx`（或资源域现行弹窗模式同构处）；Modify 资源详情/卡片入口、`renderer/src/stores/`（hub.resourcePublished 事件订阅 + 更新角标态）、preload（`hub:onResourcePublished` 订阅）；Test 贴源

**Interfaces:**
- Consumes: T13 `hub:publishResource`；M0 `hub:onNodePresence` 同款订阅模式新增 `hub:onResourcePublished {slug,type,name,version,author,at}`
- Produces:
  - **P1 预览门禁（momo-ui-preview-rules）先行**：发布对话框静态预览（元数据确认表单〔name/description/tags/category/version 预填〕+ 目标 hub 状态行 + 提交/进度/结果三态）→ 用户批准后实现——**本任务 Step 1 即预览，未批不进实现**
  - 资源详情/卡片「发布到 hub」入口（custom 资源限定显示；hub 未接入禁用+引导文案）
  - 更新角标：`resource.published` 事件 → store 比对已装 slug/version → 资源库 hub 页签与已装列表「可更新」徽标 → 更新走安装链新版本
- 消费面矩阵（momo-feature-scope-rules）：入口 × 四类资源类型 × 未接入/已接入 × hub 离线 全组合自查表入报告

- [ ] Step 1: 静态预览产出 → **暂停等用户批准**（M0 T11 同款门禁）
- [ ] Step 2: 失败测试（贴源）：对话框三态渲染/提交调 IPC/错误人话展示；角标事件→store→徽标链；入口条件渲染矩阵
- [ ] Step 3: 跑红 → 实现 → Step 4: settings/library 相关套件 + typecheck + lint + 五源回归 → Step 5: Commit `feat(hub): 发布对话框与更新角标——事件链/入口矩阵/三态反馈`

### Task 15: M1 验收——全量/真机/账本

**Files:** Modify studio 三处 package.json（alpha.29）+ CHANGELOG；hub CHANGELOG 0.1.1

- [ ] Step 1: hub 终验阶梯（typecheck/lint/test/build 全绿——预期 24+M1 新增用例）
- [ ] Step 2: studio 终验（typecheck 门 + lint 改动面 + tests/hub + tests/resource + tests/plugin + tests/marketplace + renderer 相关套件）
- [ ] Step 3: 真机验收（momo-acceptance，spec §1 验收形态全走查）：A 发布自定义 agent + 插件包 → B hub 页签可见 → 安装 → 成员候选/插件设置可用 → A 发新版 → B 角标 → 更新成功；hub 断线时资源库 degraded 提示；错 checksum 包拒绝。产出 `.omo/qa-reports/`
- [ ] Step 4: 版本账本：studio alpha.29 + CHANGELOG M1 条目（含已知限制：资源不可删/无覆盖、mcp 仅 stdio 可发布、插件不含 node_modules）；hub CHANGELOG 0.1.1
- [ ] Step 5: 收尾 commits + push（两仓）→ 转全分支终审（oracle）→ 合并选项呈报用户

---

## 附录：SDD 派发批量建议

| 批次 | 任务 | 同面理由 |
|---|---|---|
| D1 | T1 | hub 仓独立小改 |
| D2 | T2+T3 | studio hub 面补丁同面 |
| D3 | T4+T5 | protocol+存储（hub 仓同批） |
| D4 | T6、T7 | 顺序依赖重，各自派发 |
| D5 | T8+T9 | studio 依赖面（pin+provider） |
| D6 | T10 | 独立验收面 |
| D7 | T11、T12 | plugin 链顺序依赖（T12 有探索步） |
| D8 | T13 | 主进程编排 |
| D9 | T14（含预览暂停点）、T15 | UI 门禁 + 收尾 |

任务间接口速查：T4 契约→T6/T7/T9；T5 blobs→T6/T7；T6/T7 端点→T9/T10/T13；T8 白名单→T9/T10/T12；T11 打包→T13；T13 IPC→T14；全部→T15。
