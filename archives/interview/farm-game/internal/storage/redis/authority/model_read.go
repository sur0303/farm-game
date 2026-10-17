package authority

import (
	"context"
	"fmt"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"

	"github.com/redis/go-redis/v9"
)

var _ authoritystore.ModelFieldBatchReader = (*Store)(nil)

// BatchReadModelFields pipelines bounded HMGETs. go-redis splits a cluster
// pipeline by node/slot internally, so callers do not perform one round trip
// per friend and never need to know bucket placement.
func (s *Store) BatchReadModelFields(ctx context.Context, domain actorrouting.RouteDomain, actorIDs []uint64, fieldNames []string) ([]authoritystore.ModelFieldReadResult, error) {
	if s == nil || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) ||
		len(actorIDs) == 0 || len(actorIDs) > 512 || len(fieldNames) == 0 || len(fieldNames) > 16 {
		return nil, fmt.Errorf("%w: model field batch shape", authoritystore.ErrInvalidRequest)
	}
	v2Names, err := fieldv2.ExactNames(domain)
	if err != nil {
		return nil, fmt.Errorf("%w: v2 model schema", authoritystore.ErrInvalidRequest)
	}
	// A v2 batch read fetches and validates the complete bounded model set in
	// the same pipeline, even when its caller asks for a projection. This keeps
	// a partial/corrupt v2 record from becoming readable through a narrow path.
	redisFields := make([]string, 8, 8+len(fieldNames)+len(v2Names))
	redisFields[0] = "_meta:record_version"
	redisFields[1] = "_meta:model_layout_version"
	redisFields[2] = "_meta:model_manifest"
	redisFields[3] = "_meta:model_field_count"
	redisFields[4] = "_meta:model_total_bytes"
	redisFields[5] = "_meta:business_state_digest"
	redisFields[6] = "_meta:receipt_count"
	redisFields[7] = "_meta:resolution_count"
	fieldIndexes := make(map[string]int, len(fieldNames)+len(v2Names))
	for _, name := range fieldNames {
		if !validModelFieldName(name) {
			return nil, fmt.Errorf("%w: model field name", authoritystore.ErrInvalidRequest)
		}
		if _, duplicate := fieldIndexes[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate model field name", authoritystore.ErrInvalidRequest)
		}
		fieldIndexes[name] = len(redisFields)
		redisFields = append(redisFields, "model:"+name)
	}
	for _, name := range v2Names {
		if _, exists := fieldIndexes[name]; exists {
			continue
		}
		fieldIndexes[name] = len(redisFields)
		redisFields = append(redisFields, "model:"+name)
	}

	type queuedRead struct {
		actorID uint64
		cmd     *redis.SliceCmd
		hashLen *redis.IntCmd
		err     error
	}
	queued := make([]queuedRead, len(actorIDs))
	_, err = s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, actorID := range actorIDs {
			queued[index].actorID = actorID
			if actorID == 0 {
				queued[index].err = fmt.Errorf("%w: actor id", authoritystore.ErrInvalidRequest)
				continue
			}
			key, keyErr := s.keys.ActorRecordFor(actorrouting.ActorAddress{Domain: domain, ActorID: actorID})
			if keyErr != nil {
				queued[index].err = keyErr
				continue
			}
			queued[index].cmd = pipe.HMGet(ctx, key.String(), redisFields...)
			queued[index].hashLen = pipe.HLen(ctx, key.String())
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redisauthority: batch model read unavailable: %w", err)
	}

	results := make([]authoritystore.ModelFieldReadResult, 0, len(queued))
	for _, item := range queued {
		result := authoritystore.ModelFieldReadResult{ActorID: item.actorID, ModelFields: authoritystore.ModelState{}, Err: item.err}
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
		if len(values) != len(redisFields) {
			result.Err = fmt.Errorf("%w: short model field read", errMalformedBlob)
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
		layoutValue := ""
		if values[1] != nil {
			layoutValue, ok = asString(values[1])
			if !ok {
				result.Err = fmt.Errorf("%w: model layout type", errMalformedBlob)
				results = append(results, result)
				continue
			}
		}
		layout, _, layoutErr := parseStoredModelLayout(layoutValue)
		if layoutErr != nil {
			result.Err = layoutErr
			results = append(results, result)
			continue
		}
		result.ModelLayout = layout
		if layout == authoritystore.ModelLayoutV2 {
			if item.hashLen == nil {
				result.Err = fmt.Errorf("%w: missing v2 hash length", errMalformedBlob)
				results = append(results, result)
				continue
			}
			manifest, manifestOK := asString(values[2])
			modelCountText, countOK := asString(values[3])
			modelBytesText, bytesOK := asString(values[4])
			digestText, digestOK := asString(values[5])
			receiptCountText, receiptOK := asString(values[6])
			resolutionCountText, resolutionOK := asString(values[7])
			manifestNames, manifestErr := parseManifest(manifest)
			modelCount, modelCountErr := parseStoredCount(modelCountText, s.opts.Limits.MaxModelFields, "model item usage")
			modelBytes, modelBytesErr := parseStoredCount(modelBytesText, s.opts.Limits.MaxModelBytes, "model byte usage")
			receiptCount, receiptCountErr := parseStoredCount(receiptCountText, s.opts.Limits.MaxReceipts, "receipt item usage")
			resolutionCount, resolutionCountErr := parseStoredCount(resolutionCountText, s.opts.Limits.MaxResolutionReceipts, "resolution item usage")
			digest, digestErr := parseDigestHex(digestText)
			if !manifestOK || len(manifest) > s.opts.Limits.MaxModelFields*(4+64) || !countOK || !bytesOK || !digestOK || !receiptOK || !resolutionOK ||
				manifestErr != nil || modelCountErr != nil || modelBytesErr != nil || receiptCountErr != nil || resolutionCountErr != nil || digestErr != nil ||
				modelCount != len(v2Names) || fieldv2.ValidateExactNames(domain, manifestNames) != nil ||
				item.hashLen.Val() != int64(actorRecordFixedMetaFields+modelCount+receiptCount+resolutionCount) {
				result.Err = fmt.Errorf("%w: incomplete v2 model metadata", errMalformedBlob)
				results = append(results, result)
				continue
			}
			complete := make(authoritystore.ModelState, len(v2Names))
			calculatedBytes := 0
			for _, name := range v2Names {
				value, valueOK := asString(values[fieldIndexes[name]])
				if !valueOK {
					result.Err = fmt.Errorf("%w: v2 model field %s missing", errMalformedBlob, name)
					break
				}
				complete[name] = []byte(value)
				calculatedBytes += len(name) + len(value)
			}
			if result.Err == nil && (calculatedBytes != modelBytes || businessStateDigest(complete) != digest || fieldv2.ValidateExactFields(domain, complete) != nil) {
				result.Err = fmt.Errorf("%w: malformed v2 model fields", errMalformedBlob)
			}
			if result.Err != nil {
				results = append(results, result)
				continue
			}
			for _, name := range fieldNames {
				value, exists := complete[name]
				if !exists {
					// A caller may request layout alternatives in one bounded
					// pipeline (for example v1 public_profile plus v2
					// profile/progression). Complete v2 was validated above;
					// legacy-only names are omitted, never used as fallback.
					continue
				}
				result.ModelFields[name] = append([]byte(nil), value...)
			}
			results = append(results, result)
			continue
		}
		for _, name := range fieldNames {
			valueIndex := fieldIndexes[name]
			if values[valueIndex] == nil {
				continue
			}
			value, ok := asString(values[valueIndex])
			if !ok {
				result.Err = fmt.Errorf("%w: model field %s type", errMalformedBlob, name)
				break
			}
			result.ModelFields[name] = []byte(value)
		}
		results = append(results, result)
	}
	return results, nil
}
