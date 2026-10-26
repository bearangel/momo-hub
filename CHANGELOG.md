# 更新日志

## 0.1.3（2026-10-26）

委派载荷 permissionTier 可选字段透传（M2.1 权限档位 HITL，评审 P1 裁定 (a)）：任务书 `DelegationTask.permissionTier` 增三值枚举 `readonly`/`sandbox`/`confirm`（缺省合法，0.1.2 载荷向后兼容）；hub `parseDelegationTask` 白名单收该字段 + 枚举校验——合法值随 `task.assigned` 定向透传目标节点，非法值剥除不抛（未知容忍先例 modelBinding，任务书其余字段照常受理）。修复 studio T10 委派链路档位被 hub 白名单剥除的断点。

## 0.1.2（2026-10-25）

M2 分布式工作组定档（协议 1.2.0）：工作组内核——组 RPC（group.create/join/leave/list）+ leader 制 + `group.changed` 组内定向投递 + 成员离线广播收敛；快照存档与镜像两段式——workspace.snapshot HTTP 数据面上传（manifest 结构校验 + tar.gz ≤200MB + 校验和核对）与 `workspace.mirror` 受理即答 / `mirror.apply`→`mirror.receipt`→`mirror.result` 私有 wire 对账链（conflict 差异摘要确认，回执健壮性：伪造来源忽略 / 重复回执单次结算）；委派账本——delegate/cancel/list RPC + 状态机单点校验（`task.report` 私有 wire，pending/running/interrupted/done/failed/cancelled 迁移矩阵）+ 目标断连自动迁 interrupted + 续传窗口（interrupted→running 收）+ 审计日志；远程查看转发——view.workspace/files/messages/subscribe/unsubscribe 转发面 + `view.consent` 开关（默认 true / 断线清除回默认 / per-node 隔离）+ `view.tail`→`view.appended` 尾部定向（扇出 / 会话隔离 / 订阅随断开注销 / 存续期复检同组）；快照模型绑定可选段（`WorkspaceSnapshotConfig.modelBinding`——1.2.0 旧快照向后兼容，缺省合法）。已知限制（spec §9）：不做远端代发消息/远程写操作、文件树远程浏览、子节点反向配置推送（MC2）、委派编排/依赖图（单任务委派）、跨组路由/组间可见性、镜像增量合并（只做整体替换+差异摘要）、委派结果自动回收为本地产物（结果以会话消息流形态在查看面消费，拉取结果文件 M3+ 议）。

## 0.1.1（2026-10-24）

M1 资源仓数据面：资源仓三端点——发布端点（multipart 流式收包 / manifest part 定序校验 / 服务端自算校验和 / 版本不可变 409 / 200MB 上限 413）、资源目录端点（HubCatalog 组装 / downloadUrl origin 拼装）、下载端点（blob 流 + `X-Checksum-Sha256` + `X-Published-By` 节点名字节级清洗）；发布事件广播（`resource.published` → 全部在线节点）；协议定档 1.1.0（资源域字段与事件入协议单源）；catalog origin 信任收敛——`MOMO_HUB_PUBLIC_ORIGIN` 可选配置替代 X-Forwarded 采信（设置时独占拼装 downloadUrl，非法值启动即报错）。已知限制：http hub 无内容真实性——checksum 仅防损坏不防篡改，公网暴露场景应使用 TLS + publicOrigin（spec §10 补记）。

## 0.1.0（2026-10-24）

M0 连接基建首发：协议包（信封/RPC/事件/semver 协商）、hub 服务（升级鉴权/hello 协商/注册表广播/心跳判死/enroll HTTP 端点/bin 入口/Dockerfile）、SQLite 迁移与身份（enrollment token 可轮换）、registry 身份校验删除。

备注：同名节点重复 enroll = 凭证轮换语义（设计即如此，OBS-2 备案）。
