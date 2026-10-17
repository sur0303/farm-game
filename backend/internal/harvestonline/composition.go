// Package harvestonline is the Harvest-specific online composition boundary.
// It creates/loads the generic HARVEST WorkRecord, runs one Coordinator pass,
// and maps only durable typed results to public status/result values.
package harvestonline

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"
)

var (
	ErrInvalidConfig         = errors.New("harvestonline: invalid composition")
	ErrOnlineCoordinatorDown = errors.New("harvestonline: online Harvest coordinator unavailable")
	ErrTerminalResultState   = errors.New("harvestonline: terminal Harvest result state unavailable")
)

// Config contains only generic persistence, physical placement, typed
// Participant ports, and optional route-selected clients. It has no Gate,
// Actor, legacy workflow, or public ingress dependency.
type Config struct {
	Repository     workflow.WorkRepository
	BucketResolver workidentity.BucketResolver
	BucketVersion  uint32
	Now            func() time.Time
	Deadline       time.Duration

	RouteResolver actorrouting.Resolver
	Realm         string
	PlayerClients map[string]harvest.CoordinatorParticipantClient
	FarmClients   map[string]harvest.CoordinatorParticipantClient
	Player        workflow.ParticipantInvoker
	Farm          workflow.ParticipantInvoker
}

// CommandResult is returned even when a participant response is interrupted;
// Record remains the only durable operation anchor.
type CommandResult struct {
	Record  *workflow.WorkRecord
	Created bool
	Status  commonpb.CommandStatus
	Outcome harvest.Outcome
	Result  *publicpb.HarvestResult
	Error   *commonpb.ErrorDetail
}

type Coordinator struct {
	scenario *harvest.Coordinator
}

func New(config Config) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 {
		return nil, fmt.Errorf("%w: repository, placement, and bucket version are required", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	playerInvoker, farmInvoker := config.Player, config.Farm
	if playerInvoker == nil || farmInvoker == nil {
		if config.RouteResolver == nil || config.Realm == "" || len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 {
			return nil, fmt.Errorf("%w: direct invokers or route-selected typed clients are required", ErrInvalidConfig)
		}
		playerClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainPlayer, config.PlayerClients)
		if err != nil {
			return nil, err
		}
		farmClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainFarm, config.FarmClients)
		if err != nil {
			return nil, err
		}
		playerInvoker, err = harvest.NewCoordinatorInvoker(playerClient, CoordinatorRouteProvider(config.RouteResolver, config.Realm))
		if err != nil {
			return nil, err
		}
		farmInvoker, err = harvest.NewCoordinatorInvoker(farmClient, CoordinatorRouteProvider(config.RouteResolver, config.Realm))
		if err != nil {
			return nil, err
		}
	}
	scenario, err := harvest.NewCoordinator(harvest.CoordinatorConfig{
		Repository: config.Repository, Player: playerInvoker, Farm: farmInvoker,
		BucketResolver: config.BucketResolver, BucketVersion: config.BucketVersion,
		Deadline: config.Deadline, Now: config.Now,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{scenario: scenario}, nil
}

func (c *Coordinator) Scenario() *harvest.Coordinator {
	if c == nil {
		return nil
	}
	return c.scenario
}

// CreateOrLoad is the no-side-effect ingress seam. The returned WorkRecord is
// created before any subsequent Start/Resume participant call.
func (c *Coordinator) CreateOrLoad(ctx context.Context, input harvest.HarvestInput) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.scenario == nil {
		return nil, false, ErrOnlineCoordinatorDown
	}
	return c.scenario.Create(ctx, input)
}

// Start performs one online Coordinator pass and maps the durable record. A
// retry-scheduled error is returned together with PROCESSING and its record.
func (c *Coordinator) Start(ctx context.Context, input harvest.HarvestInput) (CommandResult, error) {
	if c == nil || c.scenario == nil {
		return CommandResult{}, ErrOnlineCoordinatorDown
	}
	record, created, err := c.scenario.Start(ctx, input)
	result := CommandResult{Record: record, Created: created}
	if record != nil {
		result.Status, result.Outcome = statusForRecord(record)
		result.Error = errorForRecord(record)
	}
	if err != nil {
		if errors.Is(err, workflow.ErrIdempotencyConflict) {
			// CreateIfAbsent rejected a same-operation/different-payload intent.
			// It is a public terminal rejection, not a record to Resume.
			result.Status = commonpb.CommandStatus_REJECTED
			result.Outcome = harvest.OutcomeRejected
			result.Error = &commonpb.ErrorDetail{
				ErrorCode:    commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT,
				ErrorMessage: "Harvest operation payload conflicts with the existing operation",
			}
			return result, err
		}
		if record != nil && !record.Terminal() {
			return result, err
		}
		return result, err
	}
	if record == nil {
		return result, ErrTerminalResultState
	}
	if record.State == workflow.WorkStateCompleted {
		publicResult, decodeErr := PublicResult(record)
		if decodeErr != nil {
			result.Status = commonpb.CommandStatus_UNAVAILABLE
			result.Outcome = harvest.OutcomeFailed
			return result, errors.Join(ErrTerminalResultState, decodeErr)
		}
		result.Result = publicResult
	}
	return result, nil
}

// Resume is exposed for integration tests and a caller that explicitly owns
// the unleased online pass. Worker recovery uses the separate recovery
// package and supplies its lease fence to the generic Engine.
func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.scenario == nil {
		return ErrOnlineCoordinatorDown
	}
	return c.scenario.Resume(ctx, record)
}

func statusForRecord(record *workflow.WorkRecord) (commonpb.CommandStatus, harvest.Outcome) {
	outcome := harvest.OutcomeFor(record)
	switch outcome {
	case harvest.OutcomeSucceeded:
		return commonpb.CommandStatus_SUCCEEDED, outcome
	case harvest.OutcomeRejected:
		return commonpb.CommandStatus_REJECTED, outcome
	case harvest.OutcomeNoChange:
		return commonpb.CommandStatus_NO_CHANGE, outcome
	case harvest.OutcomeFailed:
		return commonpb.CommandStatus_UNAVAILABLE, outcome
	default:
		return commonpb.CommandStatus_PROCESSING, outcome
	}
}

// StatusForRecord maps durable state without reading or mutating a second
// result store. It is useful to an operation-result reader composed later.
func StatusForRecord(record *workflow.WorkRecord) commonpb.CommandStatus {
	status, _ := statusForRecord(record)
	return status
}

func errorForRecord(record *workflow.WorkRecord) *commonpb.ErrorDetail {
	if record == nil {
		return nil
	}
	classes := make([]string, 0, len(record.Steps)+1)
	if strings.TrimSpace(record.LastErrorClass) != "" {
		classes = append(classes, record.LastErrorClass)
	}
	for _, step := range record.Steps {
		if strings.TrimSpace(step.ErrorClass) != "" {
			classes = append(classes, step.ErrorClass)
		}
	}
	for _, class := range classes {
		switch strings.ToUpper(strings.TrimSpace(class)) {
		case harvest.ErrorClassParticipantIdempotencyConflict, harvest.ErrorClassIdempotencyConflict, "IDEMPOTENCY_CONFLICT":
			return &commonpb.ErrorDetail{
				ErrorCode:    commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT,
				ErrorMessage: "Harvest Participant invocation conflicts with the existing receipt",
			}
		}
	}
	return nil
}

// PublicResult decodes and verifies both typed Participant results, then
// projects the frozen Farm Decision and Player CompendiumUnlockDelta. It does
// not calculate any reward quantity, experience, or unlock decision.
func PublicResult(record *workflow.WorkRecord) (*publicpb.HarvestResult, error) {
	farmResult, grantResult, err := harvest.DecodeResults(record)
	if err != nil {
		return nil, err
	}
	decision := farmResult.GetHarvestDecision()
	delta := grantResult.GetCompendiumUnlockDelta()
	if decision == nil || delta == nil || decision.GetRewardQuantity() > math.MaxUint32 {
		return nil, fmt.Errorf("%w: frozen result is outside public Harvest shape", ErrTerminalResultState)
	}
	result := &publicpb.HarvestResult{
		TargetFarmId:            decision.GetFarmId(),
		PlotId:                  decision.GetPlotId() - 1,
		CropCycleId:             decision.GetCropCycleId(),
		CommittedFarmVersion:    farmResult.GetCommittedFarmVersion(),
		HarvestedQuantity:       uint32(decision.GetRewardQuantity()),
		CompendiumNewlyUnlocked: delta.GetNewlyUnlocked(),
		CompendiumRevision:      delta.GetCompendiumRevisionAfter(),
	}
	if delta.GetNewlyUnlocked() {
		cropID := delta.GetCropDefinitionId()
		result.UnlockedCropDefinitionId = &cropID
	}
	return result, nil
}
