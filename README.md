# Farm Game：完整项目阅读归档

Go 农场游戏后端，以及较早版本的 TypeScript H5 和面试归档。保留实现、测试、设计文档与历史版本，方便沿调用链阅读、理解和讲解。

| 内容 | 入口 |
|---|---|
| 较新后端 | [backend/](backend/) · [原 README](backend/README.md) |
| 较早完整归档 | [archives/interview/](archives/interview/) |
| H5 前端 | [classic-farm-h5/](archives/interview/classic-farm-h5/) |
| 后端阅读指南 | [种植、Actor、持久化与恢复](docs/reading-backend.md) |
| 前端阅读指南 | [登录、协议、状态与断线恢复](docs/reading-h5.md) |

## 建议从哪里开始

先读后端指南，再沿登录和种植请求追踪入口、Player/Farm Actor、Redis 持久化及 Worker 恢复。较新后端的种植链路以 SourceOrder 为主；旧归档中的 Workflow/WorkRecord 设计需要结合版本阅读。

H5 来自较早的面试归档。两个后端快照分别保留，没有覆盖合并；尚未验证旧 H5 与较新后端的所有协议兼容性。

## 运行与材料边界

依赖和启动方式见各子项目 README。原工程使用 Go 1.26.3、Redis、Protobuf 及部分内部 SDK；公开地址替换后需要配置自己的可用依赖，不能据此认为直接克隆就能启动。Actor 仅保证同进程内的串行，跨实例写权限与持久化语义请结合指南阅读。

本次未重新验证完整 Go 构建、Docker 部署或端到端业务。前期派生示例中的测试结果不作为本完整归档的验收结果。

人员、地址与凭据的公开替换及留在本地的附件范围见 [PUBLICATION.md](PUBLICATION.md)。原始完整文件保留在本地工作区。
