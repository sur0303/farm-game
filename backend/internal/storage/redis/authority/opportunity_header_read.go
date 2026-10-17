package authority

import (
	"context"
	"fmt"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"

	"github.com/redis/go-redis/v9"
)

const maxOpportunityHeaderBatch = 64

var opportunityHeaderRedisFields = []string{
	"_meta:record_version",
	"_meta:model_layout_version",
	"model:opportunity_header",
}

// BatchReadOpportunityHeaders is the deliberately narrow coarse-candidate
// reader. Unlike BatchReadModelFields it does not validate the complete V2
// manifest/digest and therefore must never be used for final authorization or
// mutation. Each farm is still one Redis Hash and one HMGET; the commands are
// sent in one bounded pipeline and go-redis groups Cluster work by node.
func (s *Store) BatchReadOpportunityHeaders(ctx context.Context, farmIDs []uint64) ([]authoritystore.ModelFieldReadResult, error) {
	if s == nil || len(farmIDs) == 0 || len(farmIDs) > maxOpportunityHeaderBatch {
		return nil, fmt.Errorf("%w: opportunity header batch shape", authoritystore.ErrInvalidRequest)
	}

	type queuedRead struct {
		farmID uint64
		cmd    *redis.SliceCmd
		err    error
	}
	queued := make([]queuedRead, len(farmIDs))
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, farmID := range farmIDs {
			queued[index].farmID = farmID
			if farmID == 0 {
				queued[index].err = fmt.Errorf("%w: farm id", authoritystore.ErrInvalidRequest)
				continue
			}
			key, keyErr := s.keys.ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
			if keyErr != nil {
				queued[index].err = keyErr
				continue
			}
			queued[index].cmd = pipe.HMGet(ctx, key.String(), opportunityHeaderRedisFields...)
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redisauthority: batch opportunity header read unavailable: %w", err)
	}

	results := make([]authoritystore.ModelFieldReadResult, 0, len(queued))
	for _, item := range queued {
		result := authoritystore.ModelFieldReadResult{
			ActorID:     item.farmID,
			ModelFields: authoritystore.ModelState{},
			Err:         item.err,
		}
		if item.err != nil || item.cmd == nil {
			results = append(results, result)
			continue
		}
		values, cmdErr := item.cmd.Result()
		if cmdErr != nil && cmdErr != redis.Nil {
			result.Err = cmdErr
			results = append(results, result)
			continue
		}
		if len(values) != len(opportunityHeaderRedisFields) {
			result.Err = fmt.Errorf("%w: short opportunity header read", errMalformedBlob)
			results = append(results, result)
			continue
		}
		if values[0] == nil {
			results = append(results, result)
			continue
		}

		versionText, ok := asString(values[0])
		if !ok {
			result.Err = fmt.Errorf("%w: record version type", errMalformedBlob)
			results = append(results, result)
			continue
		}
		version, parseErr := parsePositiveUint(versionText, "record version")
		if parseErr != nil {
			result.Err = parseErr
			results = append(results, result)
			continue
		}
		result.Found = true
		result.RecordVersion = version

		layoutText := ""
		if values[1] != nil {
			layoutText, ok = asString(values[1])
			if !ok {
				result.Err = fmt.Errorf("%w: model layout type", errMalformedBlob)
				results = append(results, result)
				continue
			}
		}
		layout, _, layoutErr := parseStoredModelLayout(layoutText)
		if layoutErr != nil {
			result.Err = layoutErr
			results = append(results, result)
			continue
		}
		result.ModelLayout = layout

		if values[2] == nil {
			if layout == authoritystore.ModelLayoutV2 {
				result.Err = fmt.Errorf("%w: v2 opportunity header missing", errMalformedBlob)
			}
			results = append(results, result)
			continue
		}
		header, ok := asString(values[2])
		if !ok {
			result.Err = fmt.Errorf("%w: opportunity header type", errMalformedBlob)
			results = append(results, result)
			continue
		}
		result.ModelFields["opportunity_header"] = []byte(header)
		results = append(results, result)
	}
	return results, nil
}
