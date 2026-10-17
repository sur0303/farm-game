package mail

import (
	"errors"
	"fmt"
	"sort"
)

// ErrInvalidRewardBundle rejects reward content outside the P0 mail
// contract (35 §2.3): only plain coin, seed, ordinary item and experience
// grants; never PetUnlockGrant, land unlocks, paid currency or cross-farm
// rewards.
var ErrInvalidRewardBundle = errors.New("mail: invalid reward bundle")

// ItemGrant is a single inventory delta (seeds and ordinary items share the
// inventory namespace, as in contract 34's task RewardBundle).
type ItemGrant struct {
	ItemID int32
	Count  int64
}

// RewardBundle is the frozen, indivisible attachment snapshot of one mail
// (35 Q35-03). P0 claims always grant the whole bundle at once.
type RewardBundle struct {
	Coin  int64
	Exp   int64
	Items []ItemGrant
}

// IsZero reports whether the bundle carries no grant (pure text mail).
func (r RewardBundle) IsZero() bool {
	return r.Coin == 0 && r.Exp == 0 && len(r.Items) == 0
}

// KindCount counts distinct attachment kinds: coin, exp and each item
// entry each count once, capped by MaxRewardKinds (35 Q35-07).
func (r RewardBundle) KindCount() int {
	n := len(r.Items)
	if r.Coin > 0 {
		n++
	}
	if r.Exp > 0 {
		n++
	}
	return n
}

// Validate enforces the P0 reward bounds.
func (r RewardBundle) Validate(l Limits) error {
	if r.Coin < 0 || r.Exp < 0 {
		return fmt.Errorf("%w: negative grant", ErrInvalidRewardBundle)
	}
	if r.KindCount() > l.MaxRewardKinds {
		return fmt.Errorf("%w: too many kinds", ErrInvalidRewardBundle)
	}
	seen := make(map[int32]bool, len(r.Items))
	for _, it := range r.Items {
		if it.ItemID <= 0 || it.Count <= 0 {
			return fmt.Errorf("%w: bad item grant", ErrInvalidRewardBundle)
		}
		if seen[it.ItemID] {
			return fmt.Errorf("%w: duplicate item id", ErrInvalidRewardBundle)
		}
		seen[it.ItemID] = true
	}
	return nil
}

// canonical returns the bundle's canonical LP field sequence used by the
// claim material hash (35 §2.3): fixed order, items sorted by ItemID, each
// grant as LP(kind) || LP(item_id) || LP(quantity). It never hashes JSON,
// log text or nondeterministic protobuf bytes.
func (r RewardBundle) canonical(dst []byte) []byte {
	if r.Coin > 0 {
		dst, _ = appendLP(dst, "COIN")
		dst, _ = appendLP(dst, "0")
		dst, _ = appendLP(dst, itoaSigned(r.Coin))
	}
	if r.Exp > 0 {
		dst, _ = appendLP(dst, "EXP")
		dst, _ = appendLP(dst, "0")
		dst, _ = appendLP(dst, itoaSigned(r.Exp))
	}
	items := make([]ItemGrant, len(r.Items))
	copy(items, r.Items)
	sort.Slice(items, func(i, j int) bool { return items[i].ItemID < items[j].ItemID })
	for _, it := range items {
		dst, _ = appendLP(dst, "ITEM")
		dst, _ = appendLP(dst, itoaSigned(int64(it.ItemID)))
		dst, _ = appendLP(dst, itoaSigned(it.Count))
	}
	return dst
}
