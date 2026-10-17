// Package unlockplotonline is the online composition for the durable
// UnlockPlot workflow. It owns only route/client selection and the one
// coordinator pass; workflow state and participant effects remain in their
// existing authorities.
package unlockplotonline

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/plantonline"
	"farmgame/internal/scenario/plant"
	"farmgame/internal/scenario/unlockplot"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"
)

var (
	ErrInvalidConfig         = errors.New("unlockplotonline: invalid composition")
	ErrOnlineCoordinatorDown = errors.New("unlockplotonline: online coordinator unavailable")
)

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
}

type CommandResult struct {
	Record  *workflow.WorkRecord
	Created bool
	Status  commonpb.CommandStatus
	Result  *publicpb.UnlockPlotResult
}

type Coordinator struct {
	scenario *unlockplot.Coordinator
}

func New(config Config) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 {
		return nil, fmt.Errorf("%w: repository, placement and bucket version are required", ErrInvalidConfig)
	}
	clock := config.Now
	if clock == nil {
		clock = time.Now
	}
	playerInvoker := config.Player
	farmInvoker := config.Farm
	if playerInvoker == nil || farmInvoker == nil {
		if config.RouteResolver == nil || config.Realm == "" || len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 {
			return nil, fmt.Errorf("%w: typed route/client composition is incomplete", ErrInvalidConfig)
		}
		playerClient, err := plantonline.NewRoutedCoordinatorParticipantClient(config.RouteResolver, config.Realm, actorrouting.DomainPlayer, config.PlayerClients)
		if err != nil {
			return nil, err
		}
		farmClient, err := plantonline.NewRoutedCoordinatorParticipantClient(config.RouteResolver, config.Realm, actorrouting.DomainFarm, config.FarmClients)
		if err != nil {
			return nil, err
		}
		route := plantonline.CoordinatorRouteProvider(config.RouteResolver, config.Realm)
		playerInvoker, err = plant.NewCoordinatorInvoker(playerClient, route)
		if err != nil {
			return nil, err
		}
		farmInvoker, err = plant.NewCoordinatorInvoker(farmClient, route)
		if err != nil {
			return nil, err
		}
	}
	scenario, err := unlockplot.NewCoordinator(unlockplot.CoordinatorConfig{
		Repository: config.Repository, Player: playerInvoker, Farm: farmInvoker,
		BucketResolver: config.BucketResolver, BucketVersion: config.BucketVersion,
		Deadline: config.Deadline, Now: clock,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{scenario: scenario}, nil
}

func (c *Coordinator) Scenario() *unlockplot.Coordinator {
	if c == nil {
		return nil
	}
	return c.scenario
}

func (c *Coordinator) Start(ctx context.Context, input unlockplot.UnlockInput) (CommandResult, error) {
	if c == nil || c.scenario == nil {
		return CommandResult{}, ErrOnlineCoordinatorDown
	}
	record, created, err := c.scenario.Start(ctx, input)
	return CommandResult{Record: record, Created: created}, err
}

// ProjectPublicResult folds only durable WorkRecord step receipts into the
// frozen public UnlockPlotResult. In particular, normal_coin_balance comes
// from the ReserveUnlockCoin receipt, not from a later wallet read.
func ProjectPublicResult(record *workflow.WorkRecord) (*publicpb.UnlockPlotResult, commonpb.CommandStatus, commonpb.ErrorCode, error) {
	if record == nil {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, errors.New("unlockplotonline: nil WorkRecord")
	}
	terminal, err := unlockplot.ReadTerminalResults(record)
	if err != nil {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, err
	}
	if record.State == workflow.WorkStateCreated || record.State == workflow.WorkStateRunning || record.State == workflow.WorkStateStepUnknown {
		return nil, commonpb.CommandStatus_PROCESSING, commonpb.ErrorCode_OK, nil
	}
	if record.State == workflow.WorkStateRejected {
		return nil, commonpb.CommandStatus_REJECTED, codeFromClass(record.LastErrorClass), nil
	}
	if record.State == workflow.WorkStateFailed {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, nil
	}
	if terminal.Root == nil || terminal.Reserve == nil || terminal.Farm == nil {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, errors.New("unlockplotonline: terminal typed receipts are incomplete")
	}
	if record.State == workflow.WorkStateCompleted && terminal.Commit == nil {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, errors.New("unlockplotonline: commit coin receipt is missing")
	}
	if record.State == workflow.WorkStateCompensated && terminal.Release == nil {
		return nil, commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL, errors.New("unlockplotonline: release coin receipt is missing")
	}
	result := &publicpb.UnlockPlotResult{
		FarmId: terminal.Root.GetFarmId(), PlotId: terminal.Root.GetPlotId() - 1,
		AppliedUnlockRuleVersion: terminal.Farm.GetAppliedUnlockRuleVersion(),
		PaidNormalCoin:           terminal.Root.GetAmountNormalCoin(), NormalCoinBalance: terminal.Reserve.GetAvailableNormalCoinAfter(),
		CommittedPlotVersion: terminal.Farm.GetCommittedPlotVersion(), CommittedFarmVersion: terminal.Farm.GetCommittedFarmVersion(),
	}
	if record.State == workflow.WorkStateCompensated {
		if terminal.FarmStatus == workflow.StepNoChange {
			return result, commonpb.CommandStatus_NO_CHANGE, commonpb.ErrorCode_OK, nil
		}
		return nil, commonpb.CommandStatus_REJECTED, codeFromClass(terminal.FarmErrorClass), nil
	}
	return result, commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, nil
}

func codeFromClass(class string) commonpb.ErrorCode {
	if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
		return commonpb.ErrorCode(value)
	}
	return commonpb.ErrorCode_ERR_INTERNAL
}
