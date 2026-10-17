package holdercontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeHolderControlRPC struct {
	mu               sync.Mutex
	holderID         string
	bootID           string
	status           holdercontrolpb.ShardStatus
	stop             int
	wait             int
	facts            int
	prepare          int
	prepareExtension time.Duration
}

func (f *fakeHolderControlRPC) bindStatus(reqDomain string, reqShard uint32) {
	f.status.Domain, f.status.Shard = reqDomain, reqShard
	f.status.HolderId, f.status.HolderBootId = f.holderID, f.bootID
}

func (f *fakeHolderControlRPC) StopNewActivationAndTurns(_ context.Context, req *holdercontrolpb.StopNewActivationAndTurnsRequest, _ ...grpc.CallOption) (*holdercontrolpb.HolderControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stop++
	f.bindStatus(req.Domain, req.Shard)
	f.status.Phase, f.status.Admitting = holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED, false
	return &holdercontrolpb.HolderControlResponse{Status: cloneStatus(&f.status)}, nil
}

func (f *fakeHolderControlRPC) WaitCurrentTurns(_ context.Context, req *holdercontrolpb.WaitCurrentTurnsRequest, _ ...grpc.CallOption) (*holdercontrolpb.HolderControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wait++
	f.bindStatus(req.Domain, req.Shard)
	f.status.Phase, f.status.WaitComplete, f.status.AcceptedUserJobs = holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED, true, 0
	return &holdercontrolpb.HolderControlResponse{Status: cloneStatus(&f.status)}, nil
}

func (f *fakeHolderControlRPC) ConfirmFactsCommitted(_ context.Context, req *holdercontrolpb.ConfirmFactsCommittedRequest, _ ...grpc.CallOption) (*holdercontrolpb.HolderControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.facts++
	f.bindStatus(req.Domain, req.Shard)
	f.status.Phase, f.status.WaitComplete, f.status.AcceptedUserJobs = holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED, true, 0
	return &holdercontrolpb.HolderControlResponse{Status: cloneStatus(&f.status)}, nil
}

func (f *fakeHolderControlRPC) PrepareShardGeneration(_ context.Context, req *holdercontrolpb.PrepareShardGenerationRequest, _ ...grpc.CallOption) (*holdercontrolpb.HolderControlResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepare++
	f.bindStatus(req.Domain, req.Shard)
	prepared := proto.Clone(req.Generation).(*holdercontrolpb.AuthorityGeneration)
	prepared.LeaseUntilMs += f.prepareExtension.Milliseconds()
	f.status.Prepared, f.status.PreparedGeneration = true, prepared
	f.status.Phase = holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN
	f.status.Admitting = true
	return &holdercontrolpb.HolderControlResponse{Status: cloneStatus(&f.status)}, nil
}

func (f *fakeHolderControlRPC) GetShardStatus(_ context.Context, req *holdercontrolpb.GetShardStatusRequest, _ ...grpc.CallOption) (*holdercontrolpb.GetShardStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bindStatus(req.Domain, req.Shard)
	return &holdercontrolpb.GetShardStatusResponse{Status: cloneStatus(&f.status)}, nil
}

func (f *fakeHolderControlRPC) GetRuntimeIdentity(context.Context, *holdercontrolpb.GetRuntimeIdentityRequest, ...grpc.CallOption) (*holdercontrolpb.RuntimeIdentity, error) {
	return nil, errors.New("test runtime identity RPC not implemented")
}

func (f *fakeHolderControlRPC) ConfirmZeroShard(context.Context, *holdercontrolpb.ConfirmZeroShardRequest, ...grpc.CallOption) (*holdercontrolpb.RuntimeIdentity, error) {
	return nil, errors.New("test zero-shard RPC not implemented")
}

func newTestHolderControlClient(rpc holdercontrolpb.HolderControlServiceClient) *HolderControlClient {
	return &HolderControlClient{
		rpc:                    rpc,
		verifiedServerIdentity: "holder.internal",
		rpcTimeout:             time.Second,
	}
}

func cloneStatus(value *holdercontrolpb.ShardStatus) *holdercontrolpb.ShardStatus {
	return proto.Clone(value).(*holdercontrolpb.ShardStatus)
}

func TestRemoteDrainControllerFreezesPeersAndPreparesOnly(t *testing.T) {
	source := &fakeHolderControlRPC{holderID: "source", bootID: "source-boot", status: holdercontrolpb.ShardStatus{Domain: "PLAYER", Shard: 3}}
	target := &fakeHolderControlRPC{holderID: "target", bootID: "target-boot", prepareExtension: 5 * time.Second, status: holdercontrolpb.ShardStatus{Domain: "PLAYER", Shard: 3}}
	controller, err := NewRemoteDrainController(RemoteDrainControllerOptions{
		Source: HolderPeer{Client: newTestHolderControlClient(source), Domain: actorrouting.DomainPlayer, HolderID: "source", BootID: "source-boot", LogicEndpoint: "source:9000", HolderControlEndpoint: "source:9000", TLSDNSIdentity: "holder.internal"},
		Target: HolderPeer{Client: newTestHolderControlClient(target), Domain: actorrouting.DomainPlayer, HolderID: "target", BootID: "target-boot", LogicEndpoint: "target:9000", HolderControlEndpoint: "target:9000", TLSDNSIdentity: "holder.internal"},
		Domain: actorrouting.DomainPlayer, VirtualShard: 3, VerifyStatus: true,
		OperationTimeout: time.Second, Now: func() time.Time { return time.UnixMilli(1_800_000_000_000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.StopNewActivationAndTurns(context.Background(), actorrouting.DomainFarm, 3); !errors.Is(err, ErrDrainControllerCallbackInput) {
		t.Fatalf("callback target swap err=%v", err)
	}
	if err := controller.StopNewActivationAndTurns(context.Background(), actorrouting.DomainPlayer, 3); err != nil {
		t.Fatal(err)
	}
	if err := controller.WaitCurrentTurns(context.Background(), actorrouting.DomainPlayer, 3); err != nil {
		t.Fatal(err)
	}
	if err := controller.ConfirmFactsCommitted(context.Background(), actorrouting.DomainPlayer, 3); err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		Domain: actorrouting.DomainPlayer, VirtualShard: 3,
		HolderID: "target", HolderBootID: "target-boot",
		AssignmentRevision: 4, WriterEpoch: 5, LeaseEpoch: 6,
		State:        authoritystore.ShardAuthorityActive,
		LeaseUntilMs: 1_800_000_060_000,
	}
	if err := controller.ColdLoadNewHolder(context.Background(), actorrouting.DomainPlayer, 3, authority); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	sourceCalls := source.stop + source.wait + source.facts
	source.mu.Unlock()
	target.mu.Lock()
	targetCalls := target.prepare
	target.mu.Unlock()
	if sourceCalls != 3 || targetCalls != 1 {
		t.Fatalf("source calls=%d target prepares=%d", sourceCalls, targetCalls)
	}
	authority.HolderBootID = "wrong-boot"
	if err := controller.ColdLoadNewHolder(context.Background(), actorrouting.DomainPlayer, 3, authority); !errors.Is(err, ErrDrainControllerGeneration) {
		t.Fatalf("wrong target generation err=%v", err)
	}
}

func TestRemoteStatusValidatorIsExactAndComplete(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	peer := HolderPeer{
		Domain:         actorrouting.DomainPlayer,
		HolderID:       "holder-a",
		BootID:         "boot-a",
		TLSDNSIdentity: "holder.internal",
	}
	generation := &holdercontrolpb.AuthorityGeneration{
		HolderId:           peer.HolderID,
		HolderBootId:       peer.BootID,
		AssignmentRevision: 1,
		WriterEpoch:        2,
		LeaseEpoch:         3,
		LeaseUntilMs:       now.Add(time.Minute).UnixMilli(),
	}
	base := func(phase holdercontrolpb.DrainPhase) *holdercontrolpb.ShardStatus {
		status := &holdercontrolpb.ShardStatus{
			Domain:       string(peer.Domain),
			Shard:        4,
			Phase:        phase,
			HolderId:     peer.HolderID,
			HolderBootId: peer.BootID,
		}
		switch phase {
		case holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN:
			status.Admitting = true
		case holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED:
		case holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
			holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED:
			status.WaitComplete = true
		}
		return status
	}
	for _, phase := range []holdercontrolpb.DrainPhase{
		holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED,
	} {
		if err := validateShardStatus(base(phase), peer, 4, now); err != nil {
			t.Fatalf("phase=%v err=%v", phase, err)
		}
	}

	tests := []struct {
		name   string
		mutate func(*holdercontrolpb.ShardStatus)
		want   error
	}{
		{"unknown phase", func(status *holdercontrolpb.ShardStatus) {
			status.Phase = holdercontrolpb.DrainPhase(99)
		}, ErrDrainControllerStatus},
		{"open not admitting", func(status *holdercontrolpb.ShardStatus) {
			status.Admitting = false
		}, ErrDrainControllerStatus},
		{"waited has jobs", func(status *holdercontrolpb.ShardStatus) {
			status.Phase = holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED
			status.WaitComplete = true
			status.AcceptedUserJobs = 1
		}, ErrDrainControllerStatus},
		{"prepared false with generation", func(status *holdercontrolpb.ShardStatus) {
			status.PreparedGeneration = generation
		}, ErrDrainControllerGeneration},
		{"prepared true without generation", func(status *holdercontrolpb.ShardStatus) {
			status.Prepared = true
		}, ErrDrainControllerGeneration},
		{"prepared expired", func(status *holdercontrolpb.ShardStatus) {
			status.Prepared = true
			status.PreparedGeneration = proto.Clone(generation).(*holdercontrolpb.AuthorityGeneration)
			status.PreparedGeneration.LeaseUntilMs = now.UnixMilli()
		}, ErrDrainControllerGeneration},
		{"foreign holder", func(status *holdercontrolpb.ShardStatus) {
			status.HolderId = "foreign"
		}, ErrDrainControllerStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := base(holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN)
			test.mutate(status)
			if err := validateShardStatus(status, peer, 4, now); !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}

func TestRemoteDrainControllerRejectsIdentityAndTimeoutDrift(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	sourceRPC := &fakeHolderControlRPC{holderID: "source", bootID: "boot-source"}
	targetRPC := &fakeHolderControlRPC{holderID: "target", bootID: "boot-target"}
	source := HolderPeer{
		Client: newTestHolderControlClient(sourceRPC), Domain: actorrouting.DomainPlayer,
		HolderID: "source", BootID: "boot-source", LogicEndpoint: "source:9000", HolderControlEndpoint: "source:9000", TLSDNSIdentity: "holder.internal",
	}
	target := HolderPeer{
		Client: newTestHolderControlClient(targetRPC), Domain: actorrouting.DomainPlayer,
		HolderID: "target", BootID: "boot-target", LogicEndpoint: "target:9000", HolderControlEndpoint: "target:9000", TLSDNSIdentity: "holder.internal",
	}
	if _, err := NewRemoteDrainController(RemoteDrainControllerOptions{
		Source: source, Target: target, Domain: actorrouting.DomainPlayer,
		OperationTimeout: 31 * time.Second, Now: func() time.Time { return now },
	}); !errors.Is(err, ErrDrainControllerInvalid) {
		t.Fatalf("timeout err=%v", err)
	}
	source.TLSDNSIdentity = "other.internal"
	if _, err := NewRemoteDrainController(RemoteDrainControllerOptions{
		Source: source, Target: target, Domain: actorrouting.DomainPlayer,
		Now: func() time.Time { return now },
	}); !errors.Is(err, ErrDrainControllerIdentity) {
		t.Fatalf("identity err=%v", err)
	}
}
