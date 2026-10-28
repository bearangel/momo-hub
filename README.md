# momo-hub

Momo Studio 的常驻中心服务——资源分发（私有 marketplace，M1）与分布式任务节点（工作组/镜像/委派/远程查看，M2）。M0 交付连接基建。

- 设计：docs/specs/2026-10-23-momo-hub-overall-design.md
- 实施计划：docs/plans/
- 架构：控制面 WebSocket（RPC + 事件）+ 数据面 HTTP；Node 24 / TypeScript strict / Fastify + SQLite
- 协议包：仓库根即 momo-hub-protocol（momo-studio 经 git tag 依赖消费）

状态：M0 已交付（tag v0.1.0）——M1 资源分发开发中。

## 快速开始

### 裸 Node 运行（三步）

```bash
git clone https://github.com/bearangel/momo-hub && cd momo-hub
npx pnpm@9.0.0 install && npx pnpm@9.0.0 build
MOMO_HUB_PORT=8787 MOMO_HUB_DATA_DIR=~/.momo-hub node packages/hub/dist/index.js
```

启动成功后横幅会打印三行：监听地址、hub id（节点标识）与加入凭证（enrollment token）。`GET /health` 可探活，返回 `{"ok":true,"hubId":"…"}`。

默认只监听回环地址 `127.0.0.1`（仅本机可接入）。需要跨主机接入时显式配置监听地址：

```bash
MOMO_HUB_HOST=0.0.0.0   # 监听全部网卡（启动日志会 WARN 提示跨主机接入风险）
MOMO_HUB_HOST=192.168.1.10  # 或指定网卡 IP / 主机名；非法值回退 127.0.0.1 并 WARN
```

端口由 `MOMO_HUB_PORT` 配置（默认 8787），行为不变。

### Docker 运行

```bash
docker build -t momo-hub .
docker run -p 8787:8787 -v momo-hub-data:/data momo-hub
```

数据（SQLite 库 + hub 身份）落在 `momo-hub-data` 卷的 `/data`，容器重建不丢。

### Caddy TLS 反代（最小配置）

公网部署时建议前置 Caddy 自动签发 TLS：

```
example.com {
    reverse_proxy 127.0.0.1:8787
}
```

momo-hub 自身只监听 HTTP（默认 `127.0.0.1:8787`；反代部署时设 `MOMO_HUB_HOST=0.0.0.0` 让宿主网卡可达），TLS 终结交给反代。

反代后 hub 看到的仍是内网 HTTP，catalog 里的下载地址默认按请求的 Host 拼——要让 studio 拿到公网 https 地址，配置公网 origin：

```bash
MOMO_HUB_PUBLIC_ORIGIN=https://hub.example.com
```

设了它，catalog 的 `downloadUrl` 一律用该 origin 拼装（非法值启动即报错）；不设则用请求自身的协议与 Host 头。

### 加入凭证（enrollment token）

新节点首次接入 hub 时用 enrollment token 换取自己的 device token（`POST /enroll`）。token 在 hub 启动横幅中打印；**泄露或需要吊销旧节点时轮换**：直接改库即可——`hub_meta` 表中 `enrollment_token` 行删掉后重启 hub 会自动生成新 token（或调 `packages/hub/src/identity.ts` 内的 `rotateEnrollmentToken(db)`）。M0 未提供轮换 CLI，命令行化留 M1。
