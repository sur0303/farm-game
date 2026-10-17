package holdercontrol

import (
	"testing"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestHolderControlProtoIsExactlySevenUnaryRPCsWithoutCallerCredentials(t *testing.T) {
	service := holdercontrolpb.File_holdercontrol_holder_control_proto.Services().ByName("HolderControlService")
	if service == nil {
		t.Fatal("HolderControlService descriptor missing")
	}
	if service.Methods().Len() != 7 {
		t.Fatalf("method count=%d, want 7", service.Methods().Len())
	}
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			t.Fatalf("method %s is streaming", method.Name())
		}
	}
	forbidden := map[protoreflect.Name]bool{
		"caller_role":        true,
		"writer_claim":       true,
		"writer_claim_id":    true,
		"secret":             true,
		"action_token":       true,
		"writer_epoch_token": true,
	}
	generation := service.ParentFile().Messages().ByName("AuthorityGeneration")
	if generation == nil {
		t.Fatal("AuthorityGeneration descriptor missing")
	}
	for i := 0; i < generation.Fields().Len(); i++ {
		if forbidden[generation.Fields().Get(i).Name()] {
			t.Fatalf("forbidden generation field %q", generation.Fields().Get(i).Name())
		}
	}
	status := service.ParentFile().Messages().ByName("ShardStatus")
	if status == nil {
		t.Fatal("ShardStatus descriptor missing")
	}
	for _, field := range []protoreflect.Name{"holder_id", "holder_boot_id"} {
		if status.Fields().ByName(field) == nil {
			t.Fatalf("ShardStatus field %q missing", field)
		}
	}
	runtimeIdentity := service.ParentFile().Messages().ByName("RuntimeIdentity")
	if runtimeIdentity == nil {
		t.Fatal("RuntimeIdentity message missing")
	}
	for _, field := range []protoreflect.Name{
		"domain", "holder_id", "boot_id", "workload_uid", "pod_uid", "ordinal",
		"process_ready", "draining", "serving_shard_count", "observed_route_revision",
		"lifecycle_state", "observed_at_ms", "workload_identity_token",
	} {
		if runtimeIdentity.Fields().ByName(field) == nil {
			t.Fatalf("RuntimeIdentity field %q missing", field)
		}
	}
}
