package ingress

import (
	"context"
	"errors"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	playercompendium "farmgame/internal/player/compendium"

	"google.golang.org/protobuf/proto"
)

func (s *Service) executeCompendiumGet(ctx context.Context, playerID uint64, request *publicpb.GetCompendiumRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.registry == nil {
		return nil, errors.New("player actor registry unavailable")
	}
	if s.compendiumCatalog == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "Compendium catalog is not composed")
		return businessResponse(outcome), nil
	}
	// Freeze one immutable catalog generation before the Actor turn. A hot
	// publication after this point affects only the next request.
	catalog, err := s.compendiumCatalog.Snapshot()
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	value, err := s.registry.Submit(ctx, playerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
		state, err := playercompendium.StateFromPlayerRecord(record, s.compendiumLimits)
		return state, false, err
	})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	state, ok := value.(playercompendium.State)
	if !ok {
		applyError(outcome, errors.New("compendium read returned invalid state"))
		return businessResponse(outcome), nil
	}
	view, domainErr := playercompendium.BuildPageView(state, catalog, s.compendiumLimits, request.GetArchiveCursor())
	if domainErr != nil {
		applyCompendiumError(outcome, domainErr)
		return businessResponse(outcome), nil
	}
	result := &publicpb.GetCompendiumResult{Page: publicCompendiumPage(view)}
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

func (s *Service) executeCompendiumAck(ctx context.Context, playerID uint64, request *publicpb.AckCompendiumSeenRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.registry == nil {
		return nil, errors.New("player actor registry unavailable")
	}
	type ackExecution struct {
		result *publicpb.AckCompendiumSeenResult
		status commonpb.CommandStatus
	}
	value, err := s.registry.Submit(ctx, playerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
		state, err := playercompendium.StateFromPlayerRecord(record, s.compendiumLimits)
		if err != nil {
			return nil, false, err
		}
		decision := playercompendium.AckSeen(state, request.GetObservedRevision())
		result := &publicpb.AckCompendiumSeenResult{
			CompendiumSeenRevisionAfter: decision.SeenAfter,
			CurrentCompendiumRevision:   decision.CurrentRevision,
		}
		if decision.Status == playercompendium.StatusNoChange {
			return ackExecution{result: result, status: commonpb.CommandStatus_NO_CHANGE}, false, nil
		}
		if decision.Status != playercompendium.StatusSucceeded {
			return nil, false, decision.Err
		}
		if err := playercompendium.ApplyStateToPlayerRecord(record, decision.Candidate, "", s.trustedNowMs()); err != nil {
			return nil, false, err
		}
		return ackExecution{result: result, status: commonpb.CommandStatus_SUCCEEDED}, true, nil
	})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	execution, ok := value.(ackExecution)
	if !ok {
		applyError(outcome, errors.New("compendium ack returned invalid result"))
		return businessResponse(outcome), nil
	}
	outcome.Status = execution.status
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(execution.result)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

func publicCompendiumSummary(state playercompendium.State) *publicpb.CompendiumSummaryView {
	return &publicpb.CompendiumSummaryView{
		CompendiumRevision:     state.Revision,
		CompendiumSeenRevision: state.SeenRevision,
		HasNewUnlock:           state.HasNewUnlock(),
		UnlockedCount:          state.UnlockedCount(),
	}
}

func publicCompendiumPage(view playercompendium.PageView) *publicpb.CompendiumPageView {
	out := &publicpb.CompendiumPageView{
		CatalogVersion:         view.CatalogVersion,
		CompendiumRevision:     view.Revision,
		CompendiumSeenRevision: view.SeenRevision,
		HasNewUnlock:           view.HasNewUnlock,
		Categories:             make([]*publicpb.CompendiumCategoryView, 0, len(view.Categories)),
	}
	if view.NextArchiveCursor != "" {
		out.NextArchiveCursor = proto.String(view.NextArchiveCursor)
	}
	for _, category := range view.Categories {
		publicCategory := &publicpb.CompendiumCategoryView{
			Kind:       publicCompendiumCategoryKind(category.Kind),
			CategoryId: category.CategoryID,
			Title:      category.Title,
			Entries:    make([]*publicpb.CompendiumEntryView, 0, len(category.Entries)),
		}
		for _, entry := range category.Entries {
			publicEntry := &publicpb.CompendiumEntryView{
				CropDefinitionId:    entry.CropDefinitionID,
				DisplayState:        publicCompendiumDisplayState(entry.DisplayState),
				SilhouetteAssetId:   entry.SilhouetteAssetID,
				LegacySeedPlantable: entry.LegacySeedPlantable,
			}
			if entry.DisplayDefinitionID != "" {
				publicEntry.DisplayDefinitionId = proto.String(entry.DisplayDefinitionID)
			}
			if entry.ActivityID != "" {
				publicEntry.ActivityId = proto.String(entry.ActivityID)
			}
			publicCategory.Entries = append(publicCategory.Entries, publicEntry)
		}
		out.Categories = append(out.Categories, publicCategory)
	}
	return out
}

func publicCompendiumDisplayState(state playercompendium.DisplayState) publicpb.CompendiumDisplayState {
	switch state {
	case playercompendium.DisplayStateUnlockedFull:
		return publicpb.CompendiumDisplayState_COMPENDIUM_UNLOCKED_FULL
	case playercompendium.DisplayStateLockedSilhouette:
		return publicpb.CompendiumDisplayState_COMPENDIUM_LOCKED_SILHOUETTE
	default:
		return publicpb.CompendiumDisplayState_COMPENDIUM_DISPLAY_STATE_UNSPECIFIED
	}
}

func publicCompendiumCategoryKind(kind playercompendium.CategoryKind) publicpb.CompendiumCategoryKind {
	switch kind {
	case playercompendium.CategoryKindNormal:
		return publicpb.CompendiumCategoryKind_COMPENDIUM_NORMAL
	case playercompendium.CategoryKindActivityActive:
		return publicpb.CompendiumCategoryKind_COMPENDIUM_ACTIVITY_ACTIVE
	case playercompendium.CategoryKindActivityArchive:
		return publicpb.CompendiumCategoryKind_COMPENDIUM_ACTIVITY_ARCHIVE
	case playercompendium.CategoryKindCatalogMissing:
		return publicpb.CompendiumCategoryKind_COMPENDIUM_CATALOG_MISSING
	default:
		return publicpb.CompendiumCategoryKind_COMPENDIUM_CATEGORY_KIND_UNSPECIFIED
	}
}

func applyCompendiumError(outcome *logicrpcpb.BusinessCommandOutcome, err *playercompendium.Error) {
	if err == nil {
		return
	}
	switch err.Code {
	case playercompendium.ErrInvalidArgument:
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, err.Error())
	case playercompendium.ErrWorkCapacityExceeded:
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_SERVER_BUSY, err.Error())
	default:
		applyError(outcome, err)
	}
}
