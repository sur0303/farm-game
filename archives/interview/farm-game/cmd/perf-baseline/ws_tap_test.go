package main

import (
	"testing"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

func TestDecodeWSTapTheftRetainsIdentityWithoutRawPayload(t *testing.T) {
	payload, err := proto.Marshal(&publicpb.TheftRequest{TargetFarmId: 42, PlotId: 3, CropCycleId: "cycle-9"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&commonpb.FarmFrame{ProtocolVersion: 4, Sequence: 7,
		FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
			OperationId: "operation-f3", RequestId: "request-f3", CommandType: commonpb.CommandType_THEFT, Payload: payload,
		}}})
	if err != nil {
		t.Fatal(err)
	}
	event := decodeWSTapFrame("CLIENT_TO_SERVER", "tap-1", raw)
	if event["event"] != "CLIENT_COMMAND" || event["command_type"] != "THEFT" || event["operation_id"] != "operation-f3" {
		t.Fatalf("unexpected event: %#v", event)
	}
	if event["target_farm_id"] != uint64(42) || event["plot_id"] != uint32(3) || event["crop_cycle_id"] != "cycle-9" {
		t.Fatalf("missing Theft identity: %#v", event)
	}
	if _, exists := event["payload"]; exists {
		t.Fatalf("raw payload must not be retained: %#v", event)
	}
	if _, exists := event["connection_ticket"]; exists {
		t.Fatalf("ticket must not be retained: %#v", event)
	}
}
