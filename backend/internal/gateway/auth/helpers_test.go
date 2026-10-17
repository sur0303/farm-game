package auth

import (
	"context"
	"strings"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth/ticket"
)

type testBootstrapper struct{}

func (testBootstrapper) EnsureOwnerInitialized(context.Context, uint64, string, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return nil, nil
}

func (testBootstrapper) EnsureOwnerInitializedWithProfile(context.Context, uint64, string, string, uint32, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return nil, nil
}

func ticketSvc() *ticket.Service {
	return ticket.New(nil)
}

func ticketValid(tok string) bool {
	return ticket.ValidTokenFormat(tok)
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
