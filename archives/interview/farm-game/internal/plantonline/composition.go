package plantonline

import (
	"context"
	"errors"
	"fmt"
	"time"

	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/scenario/plant"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidConfig         = errors.New("plantonline: invalid composition")
	ErrTerminalResultState   = errors.New("plantonline: terminal Plant result state unavailable")
	ErrOnlineCoordinatorDown = errors.New("plantonline: online Plant coordinator unavailable")
)

// PlayerStateReader is the existing PlayerParticipant read seam used only to
// populate the public FarmCommandResult after the generic workflow is
// terminal. It is not a second result store.
type PlayerStateReader interface {
	GetPrivateState(context.Context, *logicrpcpb.GetPrivateStateRequest) (*logicrpcpb.GetPrivateStateResponse, error)
}

// FarmSnapshotReader reads the existing authoritative Farm projection after
// a terminal typed CommitPlant. The read is deliberately outside the
// Participant mailbox and does not persist a public operation receipt.
type FarmSnapshotReader func(context.Context, uint64, time.Time) (*farmpb.FarmSnapshot, error)

// Config is the smallest online SC1 composition. Direct invokers are useful
// for hermetic tests; production supplies the existing typed RPC client maps
// and the existing route snapshot. No legacy PlayerParticipant is accepted as
// a Plant effect port.
type Config struct {
	Repository     workflow.WorkRepository
	BucketResolver workidentity.BucketResolver
	BucketVersion  uint32
	Now            func() time.Time
	Deadline       time.Duration

	RouteResolver actorrouting.Resolver
	Realm         string
	PlayerClients map[string]plant.CoordinatorParticipantClient
	FarmClients   map[string]plant.CoordinatorParticipantClient
	Player        workflow.ParticipantInvoker
	Farm          workflow.ParticipantInvoker

	PlayerState  PlayerStateReader
	FarmSnapshot FarmSnapshotReader
}

// CommandResult is the online adapter result. Record is always returned when
// the generic WorkRepository has created or loaded durable work, including
// when a participant call is interrupted.
type CommandResult struct {
	Record     *workflow.WorkRecord
	Created    bool
	FarmResult *farmpb.FarmCommandResult
}

// Coordinator owns only the process-local composition around the frozen
// scenario Coordinator. It does not replace workflow.Engine or create a
// second runtime.
type Coordinator struct {
	scenario *plant.Coordinator
	now      func() time.Time
	snapshot FarmSnapshotReader
	player   PlayerStateReader
}

func New(config Config) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 || config.Now == nil {
		return nil, fmt.Errorf("%w: repository, placement, bucket version, and injected clock are required", ErrInvalidConfig)
	}
	if config.PlayerState == nil || config.FarmSnapshot == nil {
		return nil, fmt.Errorf("%w: terminal public result readers are required", ErrInvalidConfig)
	}
	playerInvoker := config.Player
	farmInvoker := config.Farm
	if playerInvoker == nil || farmInvoker == nil {
		if config.RouteResolver == nil || config.Realm == "" {
			return nil, fmt.Errorf("%w: route resolver and realm are required for typed RPC composition", ErrInvalidConfig)
		}
		if len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 {
			return nil, fmt.Errorf("%w: typed Player and Farm client maps are required", ErrInvalidConfig)
		}
		playerClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainPlayer, config.PlayerClients)
		if err != nil {
			return nil, err
		}
		farmClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainFarm, config.FarmClients)
		if err != nil {
			return nil, err
		}
		playerRoute := CoordinatorRouteProvider(config.RouteResolver, config.Realm)
		farmRoute := CoordinatorRouteProvider(config.RouteResolver, config.Realm)
		playerInvoker, err = plant.NewCoordinatorInvoker(playerClient, playerRoute)
		if err != nil {
			return nil, err
		}
		farmInvoker, err = plant.NewCoordinatorInvoker(farmClient, farmRoute)
		if err != nil {
			return nil, err
		}
	}
	scenario, err := plant.NewCoordinator(plant.CoordinatorConfig{
		Repository:     config.Repository,
		Player:         playerInvoker,
		Farm:           farmInvoker,
		BucketResolver: config.BucketResolver,
		BucketVersion:  config.BucketVersion,
		Deadline:       config.Deadline,
		Now:            config.Now,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{scenario: scenario, now: config.Now, snapshot: config.FarmSnapshot, player: config.PlayerState}, nil
}

// Start performs one online Coordinator pass. The scenario creates the
// generic PLANT WorkRecord before it invokes a Participant. Errors that leave
// a durable non-terminal record are returned together with that record so the
// ingress can map them to PROCESSING without losing the operation identity.
func (c *Coordinator) Start(ctx context.Context, input plant.PlantInput) (CommandResult, error) {
	if c == nil || c.scenario == nil {
		return CommandResult{}, ErrOnlineCoordinatorDown
	}
	record, created, err := c.scenario.Start(ctx, input)
	result := CommandResult{Record: record, Created: created}
	if err != nil {
		return result, err
	}
	if record == nil || !record.Terminal() || record.State != workflow.WorkStateCompleted {
		return result, nil
	}
	snapshot, err := c.snapshot(ctx, input.TargetFarmID, c.now())
	if err != nil || snapshot == nil {
		if err == nil {
			err = ErrTerminalResultState
		}
		return result, errors.Join(ErrTerminalResultState, err)
	}
	private, err := c.player.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: input.SubjectPlayerID})
	if err != nil || private == nil || private.PrivateState == nil {
		if err == nil {
			err = ErrTerminalResultState
		}
		return result, errors.Join(ErrTerminalResultState, err)
	}
	privateBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(private.PrivateState)
	if err != nil {
		return result, errors.Join(ErrTerminalResultState, err)
	}
	result.FarmResult = &farmpb.FarmCommandResult{Snapshot: snapshot, PrivateState: privateBytes}
	return result, nil
}
