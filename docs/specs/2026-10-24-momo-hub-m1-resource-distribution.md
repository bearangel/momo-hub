# momo-hub M1（资源分发·私有 marketplace）设计 spec

- **日期**：2026-10-24
- **状态**：待用户审阅
- **上游**：`2026-10-23-momo-hub-overall-design.md` §7（M1 范围与三个内嵌裁定，已随整体设计批准）；M0 交付（tag v0.1.0 / studio 2.1.0-alpha.28）
- **本文职责**：把 §7 细化为可实施契约——wire 协议、hub 存储、studio 挂点、四类资源落地路径、里程碑拆分；并折入 M0 终审的 M1 待办

## 1. 目标与验收形态

**目标**：机器 A 把本机资源发布到 hub 资源仓 → 机器 B 资源库「hub」页签浏览 → 安装/更新 → 平台内可用（agent 可 spawn / skill 可注册 / mcp 可引用 / plugin 可启用）。

**验收形态**：A 发布一个自定义 agent 与一个插件包 → B 在 hub 页签看到两项 → 安装 → agent 出现在成员编辑候选、插件出现在插件设置并可启用；A 发新版 → B 收到更新角标 → 更新成功。

## 2. 关键裁定（本文新增，供审阅否决）

| # | 裁定 | 理由 | 若错的代价 |
|---|---|---|---|
| C1 | **plugin 完整进 M1**（整体设计钦定四类；hub 侧类型无关，studio 侧 plugin 打包/安装是最大新增面，见 §6.4） | 设计原文 | 里程碑拉长（可拆 M1.5，见 §8 备选） |
| C2 | **S1 的 downloadUrl 强制 https 改为「https 或已注册 hub origin」白名单**；hub 项 checksum **强制非空**（sha256） | hub 可能跑 LAN http（Caddy TLS 是主路径非唯一路径）；完整性由强制校验和补足，https 防的是公网中间人，hub origin 白名单防的是注入——S1 注入防线（slug/version 字符集）原样保留 | LAN http 下中间人可换包，但校验和仍锚定完整性（hub 自身计算 checksum，见 §4）|
| C3 | **发布范围 = source 为 custom 的资源**（custom agent 定义 / 本地 skill 目录 / custom mcp 定义 / 本地插件包）；marketplace/smithery 安装项**不可转推**（再分发许可风险） | 整体设计「自定义」语义 + 许可洁净 | 想转推已装资源需自行改 source（不做） |
| C4 | **同 slug+version 重复发布 → 409 拒绝**；版本只增不改（无覆盖/无删除——删除 M1 不做，YAGNI） | 资源仓不可变性 = 其他节点安装可复现 | 发布错版本只能升版本号绕过 |
| C5 | **大小上限 200MB/包**（沿用 MAX_DOWNLOAD_BYTES）；总配额不做（个人多设备） | 与 marketplace 一致 | 无 |
| C6 | **hub 侧发布即校验即算校验和**：客户端只传内容，服务端流式落盘后自算 sha256 写入 resource_versions——不信客户端自报 | 信任根在 hub | 无 |
| C7 | 协议 **PROTOCOL_VERSION → 1.1.0**（新增 resource.published 事件与 HTTP 端点，均向后兼容——envelope 未知 method 容忍 + 事件分发 fall-through 已验证；major 兼容门只看主版本号）；git tag 与包版本 **v0.1.1**（用户裁定：M1 维持 0.1.x 线）；studio pin 升至 `#v0.1.1` | wire 语义 minor 递增；包版本按用户裁定走 patch | 无 |

## 3. Wire 协议（momo-hub `src/`，协议单源）

### 3.1 HTTP 数据面（新增三个端点，沿用 device token Bearer 鉴权）

| 端点 | 方法 | 请求 | 响应 | 错误 |
|---|---|---|---|---|
| `/resources/catalog` | GET | — | `HubCatalog`（JSON，见 3.2） | 500 |
| `/resources/publish` | POST | multipart：`manifest`（JSON part）+ `package`（tar.gz part，≤200MB 流式） | 201 `{slug, version, checksum, sizeBytes}` | 400 `{error:'invalid_manifest'\|'invalid_slug'\|'invalid_version'}`；401；409 `{error:'version_exists'}`；413 超限 |
| `/resources/{slug}/{version}/download` | GET | — | tar.gz 流 + `X-Checksum-Sha256` 头 + `X-Published-By` 头 | 404；401 |

- 鉴权：`Authorization: Bearer <deviceToken>`（与 /ws 升级同源校验）
- 流式：publish 端点边收边算 sha256 边落临时文件，全部收完且 ≤200MB 才转正入库（C6）；超限回 413 + 排干剩余上传 + 删临时件

### 3.2 `HubCatalog` 与 `HubResourceMeta`（新类型，protocol 包）

```typescript
/** hub 资源类型：现有 marketplace 三类 + plugin（整体设计钦定四类） */
export type HubResourceType = 'agent' | 'mcp' | 'skill' | 'plugin';

/** 单条 hub 资源元数据——字段对齐 MarketplaceItem（marketplace/types.ts:14），
 *  差异：无 variants / installCount；新增 publishedBy / publishedAt；downloadUrl 为 hub 绝对地址 */
export interface HubResourceMeta {
  slug: string;
  type: HubResourceType;
  name: string;
  version: string;          // 最新版本
  author: string;           // 发布节点名
  description: string;
  readme: string;
  tags: string[];
  category: string;
  iconEmoji: string;        // 沿用既有字段（marketplace 同名同义），hub 源不新增图标体系
  verificationStatus: 'unverified'; // hub 恒为 unverified（私有仓无分级）
  downloadUrl: string;      // `${hubBase}/resources/{slug}/{version}/download`
  checksum: string;         // sha256 hex，强制非空（C2）
  sizeBytes: number;
  publishedAt: number;
  versions: string[];       // 全部版本号，新→旧
}

export interface HubCatalog { version: string; updatedAt: string; items: HubResourceMeta[] }
```

### 3.3 事件（EVT 目录新增）

- `resource.published`：`{ slug, type, name, version, author, at }` —— 发布成功后广播全部在线节点

### 3.4 服务端校验（S1 移植 + C4/C6）

- manifest part 解析后逐项校验（对齐 `validateCatalog` 强度）：type 枚举〔四类〕、slug/version 字符集（`SLUG_PATTERN`/`VERSION_PATTERN` 原样）、字符串字段类型、tags 数组、sizeBytes 数字
- slug 长度 ≤64；name ≤128；readme ≤64KB（DoS 下限）
- `version_exists`：resources 表已有同 slug+version → 409
- 下载侧：blob 文件名即 sha256（内容寻址），流式回放 + 头部校验和——客户端安装前仍独立复算（纵深）

## 4. hub 存储（SQLite 迁移 v2 + blobs 目录）

```sql
-- migration v2（追加，不可变纪律）
CREATE TABLE IF NOT EXISTS resources (
  slug TEXT PRIMARY KEY,
  type TEXT NOT NULL,            -- 'agent'|'mcp'|'skill'|'plugin'
  name TEXT NOT NULL,
  author TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  readme TEXT NOT NULL DEFAULT '',
  tags_json TEXT NOT NULL DEFAULT '[]',
  category TEXT NOT NULL DEFAULT '',
  icon_emoji TEXT NOT NULL DEFAULT '',
  latest_version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_versions (
  slug TEXT NOT NULL,
  version TEXT NOT NULL,
  checksum TEXT NOT NULL,        -- sha256 hex（服务端自算，C6）
  size_bytes INTEGER NOT NULL,
  published_by TEXT NOT NULL,    -- 节点名
  published_at INTEGER NOT NULL,
  PRIMARY KEY (slug, version)
);
```

- blobs：`{dataDir}/blobs/{sha256}.tar.gz`（内容寻址，同包重传天然幂等去重；孤儿 blob 不回收——M1 无删除，YAGNI）
- catalog 组装：resources JOIN 最新 version，毫秒级，无缓存层

## 5. 事件与更新通知链

hub：publish 入库成功 → `EVT.resource.published` 广播（registry.broadcast）→ 各在线 studio。

studio：HubManager onEvent 新增 `resource.published` 分支 → `webContents.send('hub:resourcePublished', payload)` → renderer 资源库 store 比对已装 slug+version → 角标提示。**冷启动不补拉历史**（catalog TTL 5min 自然刷新覆盖）。

## 6. studio 侧落地

### 6.1 第五 source + HubProvider

- `ResourceSource += 'hub'`；`resource/hub/` 新增 provider（与 smithery 并列）：`list()` → HTTP GET catalog（SWR/退避/degraded 全套复用 marketplace/client.ts 模式：TTL 5min + 失败退避 60s + lastGood 直返 + 后台单飞 + degraded 标志透传 `resource:getCatalogStatus`）
- hub 源未接入（M0 面板未 enroll）时：provider 返回空目录 + degraded=false，资源库 hub 页签显示「未接入 momo-hub」引导态（链到设置）

### 6.2 安装链复用（agent / skill / mcp 三类）

- HubProvider 输出 `MarketplaceItem` 形状（HubResourceMeta 字段映射，`verificationStatus:'unverified'`、`iconEmoji` 直传）→ **`installPackage()` 原样复用**（S1 校验 → cache/{type}s/{slug}/{version} → 下载〔C2：installer 的 https 检查改白名单——hub origin 注册入 installer 信任集〕→ checksum 复算 → extractTarGz〔symlink 拒绝〕→ installed_packages 表 → registerInstalledPackage 按类注册）
- installed_packages 增列 `source` 或复用 item_id 前缀区分 hub 来源（实施计划定，倾向 item_id 前缀 `hub:`——零 schema 变更）
- 更新：资源库已装项比对 catalog versions[0] → 「可更新」→ 走同链路新 version 落地（cache 按 version 分目录天然共存，registry 注册覆盖语义已有）

### 6.3 mcp 资源形态

- 发布：custom mcp 定义导出为 package.json stub 形态（对齐 marketplace mcp 包：`{name, version, description}`）——**仅 stdio/npx 形态可发布**；url 形态 mcp 定义不发布（含潜在凭证头，红线）
- 安装：复用 registerInstalledPackage 的 mcp 分支（npm 包名校验 + npx 注册）

### 6.4 plugin 资源（最大新增面）

- **发布**（本机插件目录 → tar.gz）：plugin 根目录整包打包（manifest.json + 工具模块 + mcp.json 等）；manifest 必须 `validateManifest` 通过（plugin/manifest.ts 既有校验，含 isValidVersion）；排除 dataDir（插件运行数据不入包）
- **安装**：下载解压到插件包目录 → 走既有插件注册链（assembly + PluginRecord + 工具模块加载 + mcp-sync）——目标 = 与本地安装插件完全等价
- 打包白名单：仅常规文件（symlink 拒绝，与 extractTarGz 对称）；模块文件 `.js/.cjs`，单包 ≤200MB（C5）
- 插件含原生依赖（node_modules native）时不打包 node_modules——README 声明「插件包须自身可 require（纯 JS 或 install 脚本自述）」，M1 不做依赖解析

### 6.5 发布面 UI（资源库 + 各资源详情）

- custom 资源详情/卡片新增「发布到 hub」入口（hub 未接入时禁用+引导）；点击 → 填/确认元数据（name/description/tags/category，version 预填资源当前版本可改）→ 打包上传 → 进度/结果反馈
- 发布成功后本机标记该资源 `hubPublished: {version}`（内存态即可，M1 不做发布状态持久化）

## 7. M0 终审待办折入（M1 首个任务包）

| 项 | 处置 |
|---|---|
| OBS-1 归因修正 | **studio 侧修**：client.ts hello res 载荷透出 `onlineNodes` 快照 → manager 广播 `hub:nodePresence` 种子 → store 初始化列表（hub 不动）|
| D1 换址 UI 入口 | 已接入态面板补「修改 hub 地址」次级入口（走 hub:configure 换址语义：提示将强制重新接入）|
| Docker 实构 | 有 daemon 环境首次 `docker build && run` + `/health` 冒烟（CI 加 build job 或手动验证记录）|
| studio `momo-hub/` 死目录 | 删除（chore）|
| hub README 状态行 | 「M0 已交付」更新 + clone URL 落实 |
| OBS-2 | spec 备案：同名重 enroll = 轮换语义（已实现即设计）|
| types.d 混用 / hub tests 不在 typecheck | 顺手统一/补 tsconfig.test.json |

## 8. 里程碑拆分（每块独立可验收）

- **M1a M0 补丁包**（§7 全部，studio+hub 小改；独立交付）
- **M1b hub 资源仓**（§3/§4：迁移 v2 + 三端点 + S1 服务端 + 事件 + 协议 1.1.0 + tag v0.1.1；hub 侧集成测试可独立验收）
- **M1c studio hub 源**（§6.1/6.2/6.3：第五 source + 安装链复用〔https 白名单〕+ degraded；agent/skill/mcp 三类端到端）
- **M1d plugin 面**（§6.4：发布打包 + 安装注册链；**备选：若实施中发现插件注册链耦合超预期，独立成 M1.5 推后，不阻塞其余三类验收**）
- **M1e 发布面 + 更新角标**（§6.5 + §5 事件链 + 验收形态全走查 + 版本账本 alpha.29 + 真机验收）

## 9. 测试策略

- hub：publish 校验拒绝矩阵（坏 slug/version/type/超限/重复版本/坏 multipart）、catalog 端点、下载流 + checksum 头、事件广播（集成，沿用真实 socket 模式）
- protocol：新类型 round-trip + S1 字符集单测（与 studio marketplace/types.ts 规则逐字对齐——两仓正则一致性用测试锁〔hub 侧拷贝常量 + 注释指回源，跨仓不引依赖〕）
- studio：HubProvider SWR/退避/degraded 行为（复用 marketplace client 测试模式）、installPackage 白名单分支、plugin 打包白名单（symlink/越界路径拒绝）、发布流程单测（打包+上传 mock）+ 三类安装端到端（进程内真链路）
- 回归：资源库四源 → 五源 UI 回归锁（现有 marketplace 测试基线不动）

## 10. 明确不做（M1 YAGNI）

- 资源删除 / 下架 / 版本覆盖（C4 不可变）
- 资源评分/安装计数（installCount 字段弃用语义）
- hub 仓按工作空间隔离（整体设计已裁定全局）
- url 形态 mcp 发布（凭证红线）
- 插件依赖解析/node_modules 打包
- 发布审批流（any-node-trusted 已裁定）
