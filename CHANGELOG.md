# 更新日志

## 0.1.1（2026-10-24）

M1 资源仓数据面：资源发布端点（multipart 流式收包 / 服务端自算校验和 / 版本不可变 409）、资源目录端点（HubCatalog 组装 / downloadUrl 按请求 origin 拼装）、下载端点（blob 流 + `X-Checksum-Sha256` + `X-Published-By`）、发布事件广播（`resource.published` → 全部在线节点）。

## 0.1.0（2026-10-24）

M0 连接基建首发：协议包（信封/RPC/事件/semver 协商）、hub 服务（升级鉴权/hello 协商/注册表广播/心跳判死/enroll HTTP 端点/bin 入口/Dockerfile）、SQLite 迁移与身份（enrollment token 可轮换）、registry 身份校验删除。

备注：同名节点重复 enroll = 凭证轮换语义（设计即如此，OBS-2 备案）。
