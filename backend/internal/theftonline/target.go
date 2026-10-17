package theftonline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

var (
	ErrTargetBindingUnavailable = errors.New("theft online: target binding unavailable")
	ErrTargetBindingConflict    = errors.New("theft online: target binding conflict")
	ErrTargetBindingInvalid     = errors.New("theft online: invalid target binding")
)

// TargetBindingLookup is the immutable lookup tuple available in the Pair
// dispatch manifest. The target sidecar is keyed by this tuple and therefore
// survives response loss and process restart without trusting a request-local
// map.
type TargetBindingLookup struct {
	PairID            string
	VisitorPlayerID   uint64
	OwnerPlayerID     uint64
	TargetFarmID      uint64
	OperationID       string
	PayloadHash       []byte
	TargetBindingHash []byte
}

// TargetBindingStore is the durable sidecar port used by public admission and
// Pair-order recovery. Production uses Redis; tests may inject an explicit
// deterministic fake, but no runtime composition creates an in-memory store.
type TargetBindingStore interface {
	Put(context.Context, farmingress.TheftTargetBinding) error
	Lookup(context.Context, TargetBindingLookup) (farmingress.TheftTargetBinding, error)
}

// DispatchTargetBindingStore is the production fast-path locator.  The Pair
// source order is physically bucketed by relation id, while public admission
// deliberately hands the dispatcher only the opaque dispatch order id.  This
// reverse lookup avoids probing every Pair bucket merely to recover the
// already-persisted relation id.  It is a rebuildable accelerator: Pair and
// its DueIndex remain the recovery authority if the locator is unavailable.
type DispatchTargetBindingStore interface {
	LookupByDispatchOrderID(context.Context, string) (farmingress.TheftTargetBinding, error)
}

type RedisTargetBindingStore struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration
}

func NewRedisTargetBindingStore(client redis.UniversalClient, prefix string, ttl time.Duration) (*RedisTargetBindingStore, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	if client == nil || prefix == "" {
		return nil, fmt.Errorf("%w: Redis client and prefix are required", ErrTargetBindingUnavailable)
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &RedisTargetBindingStore{client: client, prefix: prefix, ttl: ttl}, nil
}

func (s *RedisTargetBindingStore) Put(ctx context.Context, binding farmingress.TheftTargetBinding) error {
	if s == nil || s.client == nil {
		return ErrTargetBindingUnavailable
	}
	normalized, err := normalizeTargetBinding(binding)
	if err != nil {
		return err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrTargetBindingUnavailable, err)
	}
	key, err := s.key(normalized)
	if err != nil {
		return err
	}
	set, err := s.client.SetNX(ctx, key, data, s.ttl).Result()
	if err != nil {
		return fmt.Errorf("%w: Redis SETNX: %v", ErrTargetBindingUnavailable, err)
	}
	if set {
		// The reverse locator is only an online accelerator. Pair DueIndex is
		// already durable, so locator loss must not turn an accepted operation
		// into a false public failure.
		_ = s.putDispatchLocator(ctx, normalized, data)
		return nil
	}
	prior, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return fmt.Errorf("%w: Redis GET existing binding: %v", ErrTargetBindingUnavailable, err)
	}
	var existing farmingress.TheftTargetBinding
	if err := json.Unmarshal(prior, &existing); err != nil {
		return fmt.Errorf("%w: malformed existing binding", ErrTargetBindingUnavailable)
	}
	existing, err = normalizeTargetBinding(existing)
	if err != nil {
		return err
	}
	if !sameTargetBinding(existing, normalized) {
		return ErrTargetBindingConflict
	}
	// Pair admission first records the immutable target tuple, then records
	// the generated Pair dispatch ID after commit. Allow only that one-way
	// enrichment; all other changes remain a conflict.
	if existing.DispatchOrderID == "" && normalized.DispatchOrderID != "" {
		if _, err := s.client.Set(ctx, key, data, s.ttl).Result(); err != nil {
			return fmt.Errorf("%w: Redis target binding enrichment: %v", ErrTargetBindingUnavailable, err)
		}
	}
	_ = s.putDispatchLocator(ctx, normalized, data)
	return nil
}

func (s *RedisTargetBindingStore) Lookup(ctx context.Context, lookup TargetBindingLookup) (farmingress.TheftTargetBinding, error) {
	if s == nil || s.client == nil {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingUnavailable
	}
	key, err := s.keyFromLookup(lookup)
	if err != nil {
		return farmingress.TheftTargetBinding{}, err
	}
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: not found", ErrTargetBindingUnavailable)
		}
		return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: Redis GET: %v", ErrTargetBindingUnavailable, err)
	}
	var binding farmingress.TheftTargetBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: malformed stored binding", ErrTargetBindingUnavailable)
	}
	normalized, err := normalizeTargetBinding(binding)
	if err != nil {
		return farmingress.TheftTargetBinding{}, err
	}
	if !lookupMatches(lookup, normalized) {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingConflict
	}
	return normalized, nil
}

// LookupByDispatchOrderID resolves the non-authoritative reverse locator used
// only by the online fast path.  Missing or conflicting data fails the fast
// path; it never falls back to a 4096-bucket probe because the durable Pair
// DueIndex already guarantees recovery.
func (s *RedisTargetBindingStore) LookupByDispatchOrderID(ctx context.Context, orderID string) (farmingress.TheftTargetBinding, error) {
	if s == nil || s.client == nil {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingUnavailable
	}
	key, err := s.dispatchKey(orderID)
	if err != nil {
		return farmingress.TheftTargetBinding{}, err
	}
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: dispatch locator not found", ErrTargetBindingUnavailable)
		}
		return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: Redis GET dispatch locator: %v", ErrTargetBindingUnavailable, err)
	}
	var binding farmingress.TheftTargetBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: malformed dispatch locator", ErrTargetBindingUnavailable)
	}
	normalized, err := normalizeTargetBinding(binding)
	if err != nil {
		return farmingress.TheftTargetBinding{}, err
	}
	if normalized.DispatchOrderID != strings.TrimSpace(orderID) {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingConflict
	}
	return normalized, nil
}

func (s *RedisTargetBindingStore) putDispatchLocator(ctx context.Context, binding farmingress.TheftTargetBinding, data []byte) error {
	if binding.DispatchOrderID == "" {
		return nil
	}
	key, err := s.dispatchKey(binding.DispatchOrderID)
	if err != nil {
		return err
	}
	set, err := s.client.SetNX(ctx, key, data, s.ttl).Result()
	if err != nil {
		return fmt.Errorf("%w: Redis SETNX dispatch locator: %v", ErrTargetBindingUnavailable, err)
	}
	if set {
		return nil
	}
	prior, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return fmt.Errorf("%w: Redis GET existing dispatch locator: %v", ErrTargetBindingUnavailable, err)
	}
	var existing farmingress.TheftTargetBinding
	if err := json.Unmarshal(prior, &existing); err != nil {
		return fmt.Errorf("%w: malformed existing dispatch locator", ErrTargetBindingUnavailable)
	}
	normalized, err := normalizeTargetBinding(existing)
	if err != nil {
		return err
	}
	if !sameTargetBinding(normalized, binding) || normalized.DispatchOrderID != binding.DispatchOrderID {
		return ErrTargetBindingConflict
	}
	return nil
}

func (s *RedisTargetBindingStore) dispatchKey(orderID string) (string, error) {
	orderID = strings.TrimSpace(orderID)
	if s == nil || s.prefix == "" || workidentity.ValidateStableOpaqueID(orderID) != nil {
		return "", ErrTargetBindingInvalid
	}
	digest := sha256.Sum256([]byte(orderID))
	return s.prefix + ":theft-dispatch:v1:" + hex.EncodeToString(digest[:]), nil
}

func (s *RedisTargetBindingStore) key(binding farmingress.TheftTargetBinding) (string, error) {
	return s.keyFromLookup(TargetBindingLookup{
		PairID: binding.PairID, VisitorPlayerID: binding.VisitorPlayerID,
		OwnerPlayerID: binding.OwnerPlayerID, TargetFarmID: binding.TargetFarmID,
		OperationID: binding.OperationID, PayloadHash: binding.PayloadHash,
		TargetBindingHash: binding.TargetBindingHash,
	})
}

func (s *RedisTargetBindingStore) keyFromLookup(lookup TargetBindingLookup) (string, error) {
	if s == nil || s.prefix == "" {
		return "", ErrTargetBindingUnavailable
	}
	normalized, err := normalizeLookup(lookup)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	write := func(value string) {
		var length [4]byte
		length[0] = byte(len(value) >> 24)
		length[1] = byte(len(value) >> 16)
		length[2] = byte(len(value) >> 8)
		length[3] = byte(len(value))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	write(normalized.PairID)
	write(strconv.FormatUint(normalized.VisitorPlayerID, 10))
	write(strconv.FormatUint(normalized.OwnerPlayerID, 10))
	write(strconv.FormatUint(normalized.TargetFarmID, 10))
	write(normalized.OperationID)
	write(hex.EncodeToString(normalized.PayloadHash))
	write(hex.EncodeToString(normalized.TargetBindingHash))
	return s.prefix + ":theft-target:v1:" + hex.EncodeToString(h.Sum(nil)), nil
}

func normalizeTargetBinding(binding farmingress.TheftTargetBinding) (farmingress.TheftTargetBinding, error) {
	pairID := strings.TrimSpace(binding.PairID)
	derivedPair, err := identity.CanonicalFriendRelationID(binding.VisitorPlayerID, binding.OwnerPlayerID)
	if err != nil {
		return farmingress.TheftTargetBinding{}, fmt.Errorf("%w: pair: %v", ErrTargetBindingInvalid, err)
	}
	if pairID == "" {
		pairID = derivedPair
	}
	if pairID != derivedPair || binding.TargetFarmID != binding.OwnerPlayerID ||
		binding.VisitorPlayerID == 0 || binding.OwnerPlayerID == 0 || binding.VisitorPlayerID == binding.OwnerPlayerID ||
		binding.PlotID == 0 || binding.PlotID > 24 || strings.TrimSpace(binding.OperationID) == "" || len(binding.OperationID) > 64 ||
		workidentity.ValidateStableOpaqueID(pairID) != nil || workidentity.ValidateStableOpaqueID(binding.CropCycleID) != nil ||
		len(binding.PayloadHash) != sha256.Size || len(binding.TargetBindingHash) != sha256.Size ||
		!bytes.Equal(binding.TargetBindingHash, farmingress.TheftTargetBindingHash(binding.VisitorPlayerID, binding.OwnerPlayerID, binding.PlotID, binding.CropCycleID)) {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingInvalid
	}
	if binding.DispatchOrderID != "" && workidentity.ValidateStableOpaqueID(binding.DispatchOrderID) != nil {
		return farmingress.TheftTargetBinding{}, ErrTargetBindingInvalid
	}
	binding.PairID = pairID
	binding.PayloadHash = append([]byte(nil), binding.PayloadHash...)
	binding.TargetBindingHash = append([]byte(nil), binding.TargetBindingHash...)
	return binding, nil
}

func normalizeLookup(lookup TargetBindingLookup) (TargetBindingLookup, error) {
	pairID, err := identity.CanonicalFriendRelationID(lookup.VisitorPlayerID, lookup.OwnerPlayerID)
	if err != nil {
		return TargetBindingLookup{}, fmt.Errorf("%w: pair: %v", ErrTargetBindingInvalid, err)
	}
	if lookup.PairID == "" {
		lookup.PairID = pairID
	} else if lookup.PairID != pairID {
		return TargetBindingLookup{}, ErrTargetBindingInvalid
	}
	if lookup.TargetFarmID != lookup.OwnerPlayerID || lookup.OperationID == "" || len(lookup.PayloadHash) != sha256.Size || len(lookup.TargetBindingHash) != sha256.Size {
		return TargetBindingLookup{}, ErrTargetBindingInvalid
	}
	return lookup, nil
}

func sameTargetBinding(left, right farmingress.TheftTargetBinding) bool {
	return left.PairID == right.PairID && left.VisitorPlayerID == right.VisitorPlayerID &&
		left.OwnerPlayerID == right.OwnerPlayerID && left.TargetFarmID == right.TargetFarmID &&
		left.PlotID == right.PlotID && left.CropCycleID == right.CropCycleID &&
		left.OperationID == right.OperationID && bytes.Equal(left.PayloadHash, right.PayloadHash) &&
		bytes.Equal(left.TargetBindingHash, right.TargetBindingHash) &&
		(left.DispatchOrderID == right.DispatchOrderID || left.DispatchOrderID == "" || right.DispatchOrderID == "")
}

func lookupMatches(lookup TargetBindingLookup, binding farmingress.TheftTargetBinding) bool {
	normalized, err := normalizeLookup(lookup)
	if err != nil {
		return false
	}
	return normalized.PairID == binding.PairID && normalized.VisitorPlayerID == binding.VisitorPlayerID &&
		normalized.OwnerPlayerID == binding.OwnerPlayerID && normalized.TargetFarmID == binding.TargetFarmID &&
		normalized.OperationID == binding.OperationID && bytes.Equal(normalized.PayloadHash, binding.PayloadHash) &&
		bytes.Equal(normalized.TargetBindingHash, binding.TargetBindingHash)
}
