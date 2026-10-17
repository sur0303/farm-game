package fault

import (
	"context"

	logicrpcpb "farmgame/api/gen/logicrpc"
)

// LogicClient 适配 internal/gateway/command.LogicClient 接缝,
// 用于 Gate→Logic 的 RPC 错误/延迟/断连注入,不复制任何生产逻辑。
type LogicClient struct {
	Inner interface {
		ExecuteCommand(context.Context, *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error)
	}
	Injector *Injector
	// Call 是注入脚本键名,默认 "LogicClient.ExecuteCommand"。
	Call string
}

var _ interface {
	ExecuteCommand(context.Context, *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error)
} = (*LogicClient)(nil)

func (c *LogicClient) callName() string {
	if c.Call != "" {
		return c.Call
	}
	return "LogicClient.ExecuteCommand"
}

// ExecuteCommand 按脚本放行/失败/挂起/崩溃。
func (c *LogicClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	return Do(ctx, c.Injector, c.callName(), req, c.Inner.ExecuteCommand)
}
