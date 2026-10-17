package gatewayapp

import (
	"context"
	"fmt"
	"log"
	"strings"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/session"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type staticGateControlKicker struct {
	clients map[string]logicrpcpb.GateControlServiceClient
}

func newStaticGateControlKicker(targets map[string]string) (*staticGateControlKicker, []*grpc.ClientConn, error) {
	kicker := &staticGateControlKicker{clients: make(map[string]logicrpcpb.GateControlServiceClient)}
	connections := make([]*grpc.ClientConn, 0, len(targets))
	for gateID, endpoint := range targets {
		connection, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			for _, opened := range connections {
				_ = opened.Close()
			}
			return nil, nil, fmt.Errorf("dial GateControl %s=%s: %w", gateID, endpoint, err)
		}
		// grpc.Dial is intentionally non-blocking. Start resolving/connecting
		// now so the first takeover is less likely to pay the cold dial cost.
		connection.Connect()
		connections = append(connections, connection)
		kicker.clients[gateID] = logicrpcpb.NewGateControlServiceClient(connection)
	}
	return kicker, connections, nil
}

func (k *staticGateControlKicker) KickOldSession(ctx context.Context, route session.ActiveRoute) error {
	if k == nil {
		return fmt.Errorf("GateControl target %q unavailable", route.GatewayID)
	}
	client := k.clients[route.GatewayID]
	if client == nil {
		return fmt.Errorf("GateControl target %q unavailable", route.GatewayID)
	}
	response, err := client.KickSession(ctx, &logicrpcpb.KickSessionRequest{
		PlayerId: route.PlayerID, ActiveSessionEpoch: route.SessionEpoch,
		GatewayId: route.GatewayID, GatewayBootId: route.GatewayBootID,
		ConnectionId: route.ConnectionID, ConnectionGeneration: route.ConnectionGeneration,
		RouteRevision: route.RouteRevision,
	}, grpc.WaitForReady(true))
	if err != nil {
		log.Printf("GateControl KickSession player=%d target_gate=%s conn=%s rpc error: %v",
			route.PlayerID, route.GatewayID, route.ConnectionID, err)
		return err
	}
	log.Printf("GateControl KickSession player=%d target_gate=%s conn=%s -> %s",
		route.PlayerID, route.GatewayID, route.ConnectionID, response.GetStatus())
	switch response.GetStatus() {
	case logicrpcpb.GateKickStatus_KICKED,
		logicrpcpb.GateKickStatus_STALE_TARGET,
		logicrpcpb.GateKickStatus_TARGET_NOT_FOUND:
		return nil
	default:
		return fmt.Errorf("GateControl target %q returned %s", route.GatewayID, response.GetStatus())
	}
}

// parseGateControlTargets accepts "gate-a=gate-a:9093,gate-b=gate-b:9093".
// It deliberately does not derive network addresses from gate_id.
func parseGateControlTargets(value string) map[string]string {
	result := make(map[string]string)
	for _, item := range strings.Split(value, ",") {
		gateID, endpoint, ok := strings.Cut(strings.TrimSpace(item), "=")
		gateID, endpoint = strings.TrimSpace(gateID), strings.TrimSpace(endpoint)
		if ok && gateID != "" && endpoint != "" {
			result[gateID] = endpoint
		}
	}
	return result
}
