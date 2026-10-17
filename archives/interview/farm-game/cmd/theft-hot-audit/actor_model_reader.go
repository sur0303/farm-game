package main

import (
	"context"
	"errors"
	"fmt"

	farmpb "farmgame/api/gen/farm"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	farmdomain "farmgame/internal/farm/domain"
	logicauthority "farmgame/internal/logic/actor/authority"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

type auditActorRecord struct {
	RecordVersion uint64
	ModelLayout   authoritystore.ModelLayoutVersion
	Player        *playerpb.PlayerRecord
	Farm          *farmpb.FarmRecord
}

// auditActorReader is constructed once with commonConfig. Its production
// source owns key construction and the complete v2 metadata/manifest/hash/
// digest/byte validation; this adapter only selects one logical truth layout
// and invokes the domain codec.
type auditActorReader struct {
	source authoritystore.ModelFieldBatchReader
	fields map[actorrouting.RouteDomain][]string
}

func newAuditActorReader(source authoritystore.ModelFieldBatchReader) (*auditActorReader, error) {
	if source == nil {
		return nil, errors.New("theft-hot audit: model reader is required")
	}
	fields := make(map[actorrouting.RouteDomain][]string, 2)
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		names, err := fieldv2.ExactNames(domain)
		if err != nil {
			return nil, fmt.Errorf("theft-hot audit: %s model schema: %w", domain, err)
		}
		fields[domain] = append([]string{logicauthority.RecordModelField}, names...)
	}
	return &auditActorReader{source: source, fields: fields}, nil
}

func (r *auditActorReader) Load(ctx context.Context, domain actorrouting.RouteDomain, actorID uint64) (auditActorRecord, error) {
	if r == nil || r.source == nil || actorID == 0 {
		return auditActorRecord{}, errors.New("theft-hot audit: invalid actor read")
	}
	fields, ok := r.fields[domain]
	if !ok {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: unknown actor domain %s", domain)
	}
	rows, err := r.source.BatchReadModelFields(ctx, domain, []uint64{actorID}, fields)
	if err != nil {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: read %s actor %d: %w", domain, actorID, err)
	}
	if len(rows) != 1 {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: %s actor %d row unavailable", domain, actorID)
	}
	if rows[0].Err != nil {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: %s actor %d row invalid: %w", domain, actorID, rows[0].Err)
	}
	if !rows[0].Found {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: %s actor %d row missing", domain, actorID)
	}
	return decodeTheftAuditActorRow(domain, rows[0], actorID)
}

func decodeTheftAuditActorRow(domain actorrouting.RouteDomain, row authoritystore.ModelFieldReadResult, actorID uint64) (auditActorRecord, error) {
	if !row.Found || row.ActorID != actorID || actorID == 0 || row.RecordVersion == 0 {
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: %s actor identity/version invalid", domain)
	}
	result := auditActorRecord{RecordVersion: row.RecordVersion, ModelLayout: row.ModelLayout}
	switch domain {
	case actorrouting.DomainPlayer:
		record, err := decodeTheftAuditPlayer(row)
		if err != nil {
			return auditActorRecord{}, err
		}
		if record.GetPlayerId() != actorID {
			return auditActorRecord{}, errors.New("theft-hot audit: player id mismatch")
		}
		result.Player = record
	case actorrouting.DomainFarm:
		record, err := decodeTheftAuditFarm(row)
		if err != nil {
			return auditActorRecord{}, err
		}
		if record.GetFarmId() != actorID || record.GetCore() == nil || record.GetCore().GetFarmId() != actorID {
			return auditActorRecord{}, errors.New("theft-hot audit: farm id mismatch")
		}
		result.Farm = record
	default:
		return auditActorRecord{}, fmt.Errorf("theft-hot audit: unknown actor domain %s", domain)
	}
	return result, nil
}

func decodeTheftAuditPlayer(row authoritystore.ModelFieldReadResult) (*playerpb.PlayerRecord, error) {
	switch row.ModelLayout {
	case authoritystore.ModelLayoutV1:
		if err := rejectTheftAuditV1Mixed(actorrouting.DomainPlayer, row.ModelFields); err != nil {
			return nil, err
		}
		data, ok := row.ModelFields[logicauthority.RecordModelField]
		if !ok || len(data) == 0 {
			return nil, errors.New("theft-hot audit: v1 player record missing")
		}
		record := &playerpb.PlayerRecord{}
		if err := proto.Unmarshal(data, record); err != nil {
			return nil, fmt.Errorf("theft-hot audit: decode v1 player record: %w", err)
		}
		return record, nil
	case authoritystore.ModelLayoutV2:
		record, err := playeractormodelv2.Decode(row.ModelFields)
		if err != nil {
			return nil, fmt.Errorf("theft-hot audit: decode v2 player fields: %w", err)
		}
		return record, nil
	default:
		return nil, fmt.Errorf("theft-hot audit: unknown player model layout %s", row.ModelLayout)
	}
}

func decodeTheftAuditFarm(row authoritystore.ModelFieldReadResult) (*farmpb.FarmRecord, error) {
	switch row.ModelLayout {
	case authoritystore.ModelLayoutV1:
		if err := rejectTheftAuditV1Mixed(actorrouting.DomainFarm, row.ModelFields); err != nil {
			return nil, err
		}
		data, ok := row.ModelFields[logicauthority.RecordModelField]
		if !ok || len(data) == 0 {
			return nil, errors.New("theft-hot audit: v1 farm record missing")
		}
		record := &farmpb.FarmRecord{}
		if err := proto.Unmarshal(data, record); err != nil {
			return nil, fmt.Errorf("theft-hot audit: decode v1 farm record: %w", err)
		}
		return record, nil
	case authoritystore.ModelLayoutV2:
		record, _, _, err := farmactormodelv2.Decode(row.ModelFields)
		if err != nil {
			return nil, fmt.Errorf("theft-hot audit: decode v2 farm fields: %w", err)
		}
		return record, nil
	default:
		return nil, fmt.Errorf("theft-hot audit: unknown farm model layout %s", row.ModelLayout)
	}
}

func rejectTheftAuditV1Mixed(domain actorrouting.RouteDomain, state authoritystore.ModelState) error {
	names, err := fieldv2.ExactNames(domain)
	if err != nil {
		return err
	}
	for _, name := range names {
		// These two compact Farm projections predate Model Field V2 and remain
		// valid beside the v1 record. Every other v2 core name is mixed-invalid.
		if domain == actorrouting.DomainFarm &&
			(name == farmdomain.FarmOpportunityHeaderModelField || name == farmdomain.FarmOpportunityFactsModelField) {
			continue
		}
		if _, mixed := state[name]; mixed {
			return fmt.Errorf("theft-hot audit: mixed-invalid v1/v2 %s fields", domain)
		}
	}
	return nil
}
