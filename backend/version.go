// Package farmgame 是经典农场小游戏后台的根包,仅承载模块级元信息。
//
// 本项目按 3000w DAU 目标做后端架构,前端 H5 仅用于演示后端能力。
// 总体架构与冻结结论见 docs/design/00-overview.md 与项目交接文档
// study/supernova/classic-farm-agent-handoff.md。
package farmgame

// Version 是当前构建的语义版本。由构建流程通过 -ldflags "-X farmgame.Version=..."
// 注入(Makefile 从 VERSION 文件读取),未注入时回退到占位值。
//
// 版本语义见 docs/engineering/VERSIONING.md:主版本冻结在 S2 阶段为 0.y.z。
var Version = "0.0.0-dev"

// GitCommit 是构建时的源码 commit,由 -ldflags 注入;未注入为空。
var GitCommit = ""

// ImageRevision 是构建流水线绑定到 OCI image revision label/environment
// 的不可变 revision。源码构建可与 GitCommit 相同;预编译封装必须显式传入。
var ImageRevision = ""

// FullVersion returns the build and image identities used by startup logs.
func FullVersion() string {
	value := Version
	if GitCommit != "" {
		value += " (" + GitCommit + ")"
	}
	if ImageRevision != "" && ImageRevision != GitCommit {
		value += " [image=" + ImageRevision + "]"
	}
	return value
}
