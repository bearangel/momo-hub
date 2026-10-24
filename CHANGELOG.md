# 更新日志

## 0.1.1（2026-10-24）

M1 资源仓数据面：资源仓三端点——发布端点（multipart 流式收包 / manifest part 定序校验 / 服务端自算校验和 / 版本不可变 409 / 200MB 上限 413）、资源目录端点（HubCatalog 组装 / downloadUrl origin 拼装）、下载端点（blob 流 + `X-Checksum-Sha256` + `X-Published-By` 节点名字节级清洗）；发布事件广播（`resource.published` → 全部在线节点）；协议定档 1.1.0（资源域字段与事件入协议单源）；catalog origin 信任收敛——`MOMO_HUB_PUBLIC_ORIGIN` 可选配置替代 X-Forwarded 采信（设置时独占拼装 downloadUrl，非法值启动即报错）。已知限制：http hub 无内容真实性——checksum 仅防损坏不防篡改，公网暴露场景应使用 TLS + publicOrigin（spec §10 补记）。

## 0.1.0（2026-10-24）

M0 连接基建首发：协议包（信封/RPC/事件/semver 协商）、hub 服务（升级鉴权/hello 协商/注册表广播/心跳判死/enroll HTTP 端点/bin 入口/Dockerfile）、SQLite 迁移与身份（enrollment token 可轮换）、registry 身份校验删除。

备注：同名节点重复 enroll = 凭证轮换语义（设计即如此，OBS-2 备案）。
