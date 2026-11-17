# momo-hub 便携包打包规范（Windows x64）

标准化构建入口（**不要手工往旧包里替换文件**）：

```powershell
pnpm package:portable          # = node scripts/build-portable.mjs
```

脚本流程：重建协议 `dist` + hub `dist` → 干净暂存目录里重装生产依赖（pnpm hoisted）→
裁剪原生编译中间产物 → 跑门禁 → 打 zip → 解压落地包再验一遍 → 输出 `.sha256`。
**任一门禁失败即退出非零，且不产出 zip。**

| 门禁 | 内容 |
|---|---|
| G1 | `app/momo-hub-protocol` 与 `app/node_modules/momo-hub-protocol` 都是**实体目录**（非 symlink/junction） |
| G2 | 两份副本的包版本 = 根 `package.json` 版本；`PROTOCOL_VERSION` = 源码 `dist/version.js` |
| G3 | `better-sqlite3` 在**内置 node.exe** 下实载 `new Database(':memory:')`（ABI 匹配硬证据） |
| G4 | 暂存包真机引导：`/health` 200 + `/enroll` + 真实 WS `Rpc.hello` 断言 hub 宣告的 `protoVer` |
| G5 | 落地包复验：解压刚生成的 zip，重跑 G1/G2/G4（验「真正会发出去的那份」） |

产物：`dist-release/momo-hub-<version>-win-x64.zip`（含 `BUILD-INFO.json` 与 `.sha256`）。

## 目录约定

便携包静态资产是本仓库**唯一真相源**，位于 `packaging/portable/`：

| 文件 | 作用 |
|---|---|
| `start.cmd` | 启动器（`lan` / IP / `0.0.0.0` 三种参数形态） |
| `lan-ip.js` | 局域网 IPv4 探测 |
| `README.md` | 包内使用说明模板（`__VERSION__` / `__PROTOCOL__` 占位符由脚本替换） |
| `app-package.json` | `app/` 的依赖清单模板（钉版生产依赖 + `file:./momo-hub-protocol`） |

改启动器或包内说明请改 `packaging/portable/`，不要只改已生成的 zip。

## 历史事故（2026-11-17）——门禁存在的原因

`app/package.json` 用 `file:./momo-hub-protocol` 声明协议依赖，包管理器会把它装成
`app/node_modules/momo-hub-protocol` 的**独立副本**。手工改包时只替换了 `app/momo-hub-protocol`
（file: 源目录），而 Node 解析 `require('momo-hub-protocol')` 优先命中 `node_modules` 那份，
于是「源码协议 1.12.0、发布包协议 1.10.0」静默错配 —— 跨节点两段式验收 / 干预代答 / `task.steer`
全被版本门拒，且从源码 grep 完全看不出问题（源码全对）。

教训固化为三条纪律：

1. **发布物必须被验证，而不是被相信**——门禁跑的是解压后的落地包，不是暂存目录；
2. **协议版本只有一处真相源**（`PROTOCOL_VERSION`），任何副本都要与之比对；
3. **不做「在上一版包上打补丁」**——每次从源码重建，避免继承旧副本。
