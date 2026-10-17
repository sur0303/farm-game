package ingress

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	actorauthority "farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/taskcatalog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) ReserveItem(ctx context.Context, req *logicrpcpb.ReserveItemRequest) (*logicrpcpb.ReservationResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.ReservationId) == "" || req.ItemId == 0 || req.Count <= 0 || len(req.PayloadHash) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid item reservation")
	}
	value, err := s.registry.Submit(ctx, req.PlayerId, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if existing := findReservation(rec, req.ReservationId); existing != nil {
			if existing.ItemId != req.ItemId || existing.Count != req.Count || !bytes.Equal(existing.PayloadHash, req.PayloadHash) {
				return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, rec), false, nil
			}
			switch existing.State {
			case playerpb.ReservationState_PREPARED, playerpb.ReservationState_COMMITTED:
				return reservationResponse(commonpb.CommandStatus_NO_CHANGE, commonpb.ErrorCode_OK, rec), false, nil
			default:
				return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, rec), false, nil
			}
		}
		nowMs := playeractor.NowMs()
		if err := ensureReservationCapacity(rec, nowMs); err != nil {
			return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_RATE_LIMITED, rec), false, nil
		}
		if !playeractor.AddItem(rec, req.ItemId, -req.Count) {
			return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, rec), false, nil
		}
		pruneExpiredReservations(rec, nowMs)
		rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
			ReservationId: req.ReservationId,
			ItemId:        req.ItemId,
			Count:         req.Count,
			State:         playerpb.ReservationState_PREPARED,
			UpdatedAtMs:   nowMs,
			PayloadHash:   append([]byte(nil), req.PayloadHash...),
		})
		rec.EconomyVersion++
		return reservationResponse(commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, rec), true, nil
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*logicrpcpb.ReservationResponse), nil
}

func (s *Service) CommitReservation(ctx context.Context, req *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.ReservationId) == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation commit")
	}
	return s.changeReservation(ctx, req.PlayerId, req.ReservationId, true)
}

func (s *Service) ReleaseReservation(ctx context.Context, req *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.ReservationId) == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation release")
	}
	return s.changeReservation(ctx, req.PlayerId, req.ReservationId, false)
}

func (s *Service) changeReservation(ctx context.Context, playerID uint64, reservationID string, commit bool) (*logicrpcpb.ReservationResponse, error) {
	value, err := s.registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		reservation := findReservation(rec, reservationID)
		if reservation == nil {
			return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, rec), false, nil
		}
		if commit {
			switch reservation.State {
			case playerpb.ReservationState_COMMITTED:
				return reservationResponse(commonpb.CommandStatus_NO_CHANGE, commonpb.ErrorCode_OK, rec), false, nil
			case playerpb.ReservationState_RELEASED:
				return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, rec), false, nil
			}
			reservation.State = playerpb.ReservationState_COMMITTED
		} else {
			switch reservation.State {
			case playerpb.ReservationState_RELEASED:
				return reservationResponse(commonpb.CommandStatus_NO_CHANGE, commonpb.ErrorCode_OK, rec), false, nil
			case playerpb.ReservationState_COMMITTED:
				return reservationResponse(commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INTERNAL, rec), false, nil
			}
			if !playeractor.AddItem(rec, reservation.ItemId, reservation.Count) {
				return nil, false, errors.New("failed to restore released reservation")
			}
			reservation.State = playerpb.ReservationState_RELEASED
		}
		reservation.UpdatedAtMs = playeractor.NowMs()
		rec.EconomyVersion++
		return reservationResponse(commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, rec), true, nil
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*logicrpcpb.ReservationResponse), nil
}

func (s *Service) GrantItem(ctx context.Context, req *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.OperationId) == "" || req.ItemId == 0 || req.Count <= 0 || len(req.PayloadHash) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid grant item request")
	}
	value, err := s.registry.Submit(ctx, req.PlayerId, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if receipt := findReceipt(rec, req.OperationId); receipt != nil {
			if !bytes.Equal(receipt.PayloadHash, req.PayloadHash) {
				return &logicrpcpb.GrantItemResponse{Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT}, false, nil
			}
			var state playerpb.PrivateState
			if err := proto.Unmarshal(receipt.ResultPayload, &state); err != nil {
				return nil, false, err
			}
			return &logicrpcpb.GrantItemResponse{Status: commonpb.CommandStatus_NO_CHANGE, ErrorCode: receipt.ErrorCode, PrivateState: &state}, false, nil
		}
		if err := ensureReceiptCapacity(rec, playeractor.NowMs()); err != nil {
			return &logicrpcpb.GrantItemResponse{
				Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_RATE_LIMITED,
			}, false, nil
		}
		if !playeractor.AddItem(rec, req.ItemId, req.Count) {
			return nil, false, errors.New("failed to grant item")
		}
		if req.Exp > 0 {
			rec.Progression.Exp += req.Exp
			rec.Progression.Level = int32(rec.Progression.Exp/20) + 1
		}
		rec.EconomyVersion++
		state := playeractor.PrivateState(rec)
		appendReceipt(rec, req.OperationId, req.PayloadHash, state)
		return &logicrpcpb.GrantItemResponse{Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK, PrivateState: state}, true, nil
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*logicrpcpb.GrantItemResponse), nil
}

func (s *Service) ApplyReward(ctx context.Context, req *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.OperationId) == "" ||
		len(req.PayloadHash) == 0 || req.ExpDelta < 0 ||
		req.CodexCropDefinitionId < 0 ||
		(req.ItemCount != 0 && req.ItemId == 0) ||
		(req.ItemCount == 0 && req.CoinDelta == 0 && req.ExpDelta == 0 && req.CodexCropDefinitionId == 0) {
		return nil, status.Error(codes.InvalidArgument, "invalid apply reward request")
	}
	value, err := s.registry.Submit(ctx, req.PlayerId, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if receipt := findReceipt(rec, req.OperationId); receipt != nil {
			if !bytes.Equal(receipt.PayloadHash, req.PayloadHash) {
				return &logicrpcpb.ApplyRewardResponse{
					Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT,
				}, false, nil
			}
			var state playerpb.PrivateState
			if err := proto.Unmarshal(receipt.ResultPayload, &state); err != nil {
				return nil, false, err
			}
			return &logicrpcpb.ApplyRewardResponse{
				Status: commonpb.CommandStatus_NO_CHANGE, ErrorCode: receipt.ErrorCode, PrivateState: &state,
			}, false, nil
		}
		if err := ensureReceiptCapacity(rec, playeractor.NowMs()); err != nil {
			return &logicrpcpb.ApplyRewardResponse{
				Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_RATE_LIMITED,
			}, false, nil
		}
		if req.CoinDelta < 0 && rec.Wallet.Coin < -req.CoinDelta {
			return &logicrpcpb.ApplyRewardResponse{
				Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_INSUFFICIENT_COIN,
			}, false, nil
		}
		if req.ItemCount != 0 && !playeractor.AddItem(rec, req.ItemId, req.ItemCount) {
			return &logicrpcpb.ApplyRewardResponse{
				Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM,
			}, false, nil
		}
		rec.Wallet.Coin += req.CoinDelta
		rec.Progression.Exp += req.ExpDelta
		rec.Progression.Level = int32(rec.Progression.Exp/20) + 1
		nowMs := playeractor.NowMs()
		playeractor.UnlockCodex(rec, req.CodexCropDefinitionId, req.OperationId, nowMs)
		if req.CodexCropDefinitionId > 0 {
			playeractor.AdvanceTask(rec, taskcatalog.EventHarvest, 1, nowMs)
		}
		rec.EconomyVersion++
		state := playeractor.PrivateState(rec)
		appendReceipt(rec, req.OperationId, req.PayloadHash, state)
		return &logicrpcpb.ApplyRewardResponse{
			Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK, PrivateState: state,
		}, true, nil
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*logicrpcpb.ApplyRewardResponse), nil
}

func (s *Service) GetPrivateState(ctx context.Context, req *logicrpcpb.GetPrivateStateRequest) (*logicrpcpb.GetPrivateStateResponse, error) {
	if req == nil || req.PlayerId == 0 {
		return nil, status.Error(codes.InvalidArgument, "player_id required")
	}
	rec, _, err := s.registry.Read(ctx, req.PlayerId)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &logicrpcpb.GetPrivateStateResponse{PrivateState: playeractor.PrivateState(rec)}, nil
}

// GetPublicProfile is the narrow internal profile reader used by Gateway and
// Farm composition. It deliberately returns no wallet, inventory, receipts,
// task state, or other PlayerRecord fields.
func (s *Service) GetPublicProfile(ctx context.Context, req *logicrpcpb.GetPublicProfileRequest) (*logicrpcpb.GetPublicProfileResponse, error) {
	if req == nil || req.PlayerId == 0 {
		return nil, status.Error(codes.InvalidArgument, "player_id required")
	}
	if s == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	// Public profile reads are placement-independent. In authority mode the
	// compact model reader addresses the durable projection directly, whereas
	// Registry.Read would try to claim an actor and fail on every non-owner
	// Player pod. Reuse the batch path for one ID so remote FriendshipService
	// calls remain valid regardless of the Kubernetes endpoint selected.
	if s.publicProfileModels != nil {
		response, err := s.BatchGetPublicProfiles(
			ctx,
			&logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{req.PlayerId}},
		)
		if err != nil {
			return nil, err
		}
		if len(response.GetPublicProfiles()) != 1 {
			return nil, status.Error(codes.DataLoss, "public profile cardinality mismatch")
		}
		return &logicrpcpb.GetPublicProfileResponse{
			PublicProfile: response.GetPublicProfiles()[0],
		}, nil
	}
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	rec, _, err := s.registry.Read(ctx, req.PlayerId)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if rec == nil || rec.GetPlayerId() != req.PlayerId {
		return nil, status.Error(codes.DataLoss, "public profile player_id mismatch")
	}
	displayName, avatarID := playeractor.PublicProfileValues(rec)
	level := rec.GetProgression().GetLevel()
	if level < 0 {
		return nil, status.Error(codes.DataLoss, "public profile level is malformed")
	}
	if level == 0 {
		level = 1
	}
	view := &publicpb.PublicProfileView{
		PlayerId: req.PlayerId, DisplayName: displayName, AvatarId: avatarID, Level: uint32(level),
	}
	if err := publicprofile.Validate(view, req.PlayerId); err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}
	return &logicrpcpb.GetPublicProfileResponse{PublicProfile: view}, nil
}

func (s *Service) BatchGetPublicProfiles(ctx context.Context, req *logicrpcpb.BatchGetPublicProfilesRequest) (*logicrpcpb.BatchGetPublicProfilesResponse, error) {
	if req == nil || len(req.PlayerIds) == 0 || len(req.PlayerIds) > publicprofile.MaxBatchProfiles {
		return nil, status.Errorf(codes.InvalidArgument, "public profile batch must contain 1..%d ids", publicprofile.MaxBatchProfiles)
	}
	seen := make(map[uint64]struct{}, len(req.PlayerIds))
	for _, playerID := range req.PlayerIds {
		if playerID == 0 {
			return nil, status.Error(codes.InvalidArgument, "public profile batch player_id required")
		}
		if _, duplicate := seen[playerID]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "public profile batch duplicate player_id")
		}
		seen[playerID] = struct{}{}
	}
	if s.publicProfileModels == nil {
		return s.batchGetPublicProfilesFallback(ctx, req.PlayerIds)
	}
	rows, err := s.publicProfileModels.BatchReadModelFields(ctx, actorrouting.DomainPlayer, req.PlayerIds,
		[]string{publicprofile.ModelField, "profile", "progression"})
	if err != nil || len(rows) != len(req.PlayerIds) {
		if err == nil {
			err = errors.New("public profile model batch cardinality mismatch")
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	profiles := make([]*publicpb.PublicProfileView, len(req.PlayerIds))
	missingIDs := make([]uint64, 0)
	missingIndexes := make([]int, 0)
	for index, row := range rows {
		if row.ActorID != req.PlayerIds[index] {
			return nil, status.Error(codes.DataLoss, "public profile model batch order mismatch")
		}
		if row.Err != nil {
			return nil, status.Error(codes.Unavailable, row.Err.Error())
		}
		if row.Found && row.ModelLayout == authoritystore.ModelLayoutV2 {
			profileField, profileOK := row.ModelFields["profile"]
			progressionField, progressionOK := row.ModelFields["progression"]
			if !profileOK || !progressionOK {
				return nil, status.Error(codes.DataLoss, "v2 public profile fields are incomplete")
			}
			record, decodeErr := playeractormodelv2.DecodePublicProfileFields(profileField, progressionField)
			if decodeErr != nil {
				return nil, status.Error(codes.DataLoss, decodeErr.Error())
			}
			profile, projectErr := playeractor.PublicProfileProjection(record)
			if projectErr != nil || profile.GetPlayerId() != row.ActorID {
				return nil, status.Error(codes.DataLoss, "v2 public profile identity mismatch")
			}
			profiles[index] = profile
			continue
		}
		if row.Found && row.ModelLayout != authoritystore.ModelLayoutV1 {
			return nil, status.Error(codes.DataLoss, "public profile model layout is unknown")
		}
		data, projected := row.ModelFields[publicprofile.ModelField]
		if !row.Found || !projected {
			missingIDs = append(missingIDs, row.ActorID)
			missingIndexes = append(missingIndexes, index)
			continue
		}
		profile, decodeErr := publicprofile.UnmarshalProjection(data, row.ActorID)
		if decodeErr != nil {
			return nil, status.Error(codes.DataLoss, decodeErr.Error())
		}
		profiles[index] = profile
	}
	if len(missingIDs) > 0 {
		// Legacy Player authorities predate model:public_profile. Read their
		// complete record directly through the same physical batch interface;
		// never enter Registry.Read or the per-player serial executor from the
		// friend-list path. A later Player mutation writes the compact projection.
		legacyRows, legacyErr := s.publicProfileModels.BatchReadModelFields(ctx, actorrouting.DomainPlayer, missingIDs, []string{actorauthority.RecordModelField})
		if legacyErr != nil || len(legacyRows) != len(missingIDs) {
			if legacyErr == nil {
				legacyErr = errors.New("legacy public profile model batch cardinality mismatch")
			}
			return nil, status.Error(codes.Unavailable, legacyErr.Error())
		}
		for index, row := range legacyRows {
			if row.ActorID != missingIDs[index] {
				return nil, status.Error(codes.DataLoss, "legacy public profile model batch order mismatch")
			}
			if row.Err != nil {
				return nil, status.Error(codes.Unavailable, row.Err.Error())
			}
			if row.Found && row.ModelLayout != authoritystore.ModelLayoutV1 {
				return nil, status.Error(codes.DataLoss, "legacy public profile fallback observed non-v1 layout")
			}
			recordBytes, ok := row.ModelFields[actorauthority.RecordModelField]
			if !row.Found || !ok || len(recordBytes) == 0 {
				return nil, status.Error(codes.DataLoss, "legacy public profile record is unavailable")
			}
			var record playerpb.PlayerRecord
			if decodeErr := proto.Unmarshal(recordBytes, &record); decodeErr != nil {
				return nil, status.Error(codes.DataLoss, "legacy public profile record is malformed")
			}
			profile, projectErr := playeractor.PublicProfileProjection(&record)
			if projectErr != nil || profile.GetPlayerId() != row.ActorID {
				return nil, status.Error(codes.DataLoss, "legacy public profile record identity mismatch")
			}
			profiles[missingIndexes[index]] = profile
		}
	}
	return &logicrpcpb.BatchGetPublicProfilesResponse{PublicProfiles: profiles}, nil
}

func (s *Service) batchGetPublicProfilesFallback(ctx context.Context, playerIDs []uint64) (*logicrpcpb.BatchGetPublicProfilesResponse, error) {
	profiles := make([]*publicpb.PublicProfileView, len(playerIDs))
	jobs := make(chan int)
	workers := 8
	if workers > len(playerIDs) {
		workers = len(playerIDs)
	}
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				response, err := s.GetPublicProfile(ctx, &logicrpcpb.GetPublicProfileRequest{PlayerId: playerIDs[index]})
				if err != nil {
					once.Do(func() { firstErr = err })
					continue
				}
				profiles[index] = response.GetPublicProfile()
			}
		}()
	}
	for index := range playerIDs {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return &logicrpcpb.BatchGetPublicProfilesResponse{PublicProfiles: profiles}, nil
}

func findReservation(rec *playerpb.PlayerRecord, id string) *playerpb.ItemReservation {
	for _, reservation := range rec.ItemReservations {
		if reservation != nil && reservation.ReservationId == id {
			return reservation
		}
	}
	return nil
}

// FindReservation exposes the existing bounded PlayerRecord reservation
// helper to the typed Plant Participant adapter. It deliberately returns the
// same physical ItemReservation used by legacy PlayerParticipant RPCs; no
// second reservation collection is introduced.
func FindReservation(rec *playerpb.PlayerRecord, id string) *playerpb.ItemReservation {
	return findReservation(rec, id)
}

// EnsureReservationCapacity is the shared retention/capacity guard for both
// legacy item reservations and typed Plant reservations.
func EnsureReservationCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	return ensureReservationCapacity(rec, nowMs)
}

// PruneExpiredReservations applies the existing bounded retention policy to
// the shared PlayerRecord carrier.
func PruneExpiredReservations(rec *playerpb.PlayerRecord, nowMs int64) {
	pruneExpiredReservations(rec, nowMs)
}

func reservationResponse(statusValue commonpb.CommandStatus, code commonpb.ErrorCode, rec *playerpb.PlayerRecord) *logicrpcpb.ReservationResponse {
	return &logicrpcpb.ReservationResponse{Status: statusValue, ErrorCode: code, PrivateState: playeractor.PrivateState(rec)}
}
