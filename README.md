# momo-hub

Momo Studio 的常驻中心服务——资源分发（私有 marketplace，M1）与分布式任务节点（工作组/镜像/委派/远程查看，M2）。M0 交付连接基建。

- 设计：docs/specs/2026-10-23-momo-hub-overall-design.md
- 实施计划：docs/plans/
- 架构：控制面 WebSocket（RPC + 事件）+ 数据面 HTTP；Node 24 / TypeScript strict / Fastify + SQLite
- 协议包：仓库根即 momo-hub-protocol（momo-studio 经 git tag 依赖消费）

状态：M0 连接基建开发中。
