# 更新日志

## 0.1.0（2026-10-24）

M0 连接基建首发：协议包（信封/RPC/事件/semver 协商）、hub 服务（升级鉴权/hello 协商/注册表广播/心跳判死/enroll HTTP 端点/bin 入口/Dockerfile）、SQLite 迁移与身份（enrollment token 可轮换）、registry 身份校验删除。

备注：同名节点重复 enroll = 凭证轮换语义（设计即如此，OBS-2 备案）。
