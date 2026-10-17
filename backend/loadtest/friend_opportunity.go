package loadtest

import (
	"errors"
	"fmt"

	"farmgame/internal/socialprofile"
)

// FriendOpportunityABProfile freezes the matrix used to compare the P0
// authority Header/Facts read path (A) with an optional Summary projection
// coarse-filter path (B). Business-write-only (C) is the isolation baseline.
// The profile is consumed by the existing ws-stress/Prometheus evidence flow;
// it does not introduce a second load-test harness.
type FriendOpportunityABProfile struct {
	FriendCounts        []int
	OpportunityPercents []int
	PageSize            int
	ScanBatch           int
	CandidateWindow     int
	SameFarmViewers     int
	LoadTiers           []int
}

func DefaultFriendOpportunityABProfile() (FriendOpportunityABProfile, EvidenceRule) {
	maxFriends := socialprofile.Current().EffectiveMaxFriends
	return FriendOpportunityABProfile{
			FriendCounts:        []int{20, 100, maxFriends},
			OpportunityPercents: []int{0, 5, 20, 60},
			PageSize:            20,
			ScanBatch:           64,
			CandidateWindow:     8,
			SameFarmViewers:     200,
			LoadTiers:           []int{1, 5, 10},
		}, EvidenceRule{
			Tool: "cmd/ws-stress + existing Prometheus endpoints; go test benchmark is preliminary only",
			RequiredEvidence: []string{
				"A/B/C use the same seed, friend graph, opportunity facts, binary and load trace",
				"query P50/P95/P99, returned items/bytes and UNKNOWN/fallback counts",
				"Farm authority commit P95/P99 plus Redis CPU/network/pool wait",
				"B-only Summary refresh writes, lag, backlog and false-positive/false-negative counts",
				"raw ws-stress JSON and Prometheus snapshots for every matrix cell",
			},
			PassCriteria: "keep A by default; enable B only when A misses query SLO or raises Farm write P99 >10% versus C, and B has a stable net gain with bounded lag/write cost",
		}
}

func (p FriendOpportunityABProfile) Validate() error {
	maxFriends := socialprofile.Current().EffectiveMaxFriends
	if len(p.FriendCounts) == 0 || len(p.OpportunityPercents) == 0 || len(p.LoadTiers) == 0 ||
		p.PageSize <= 0 || p.ScanBatch <= 0 || p.ScanBatch > 64 || p.CandidateWindow <= 0 ||
		p.SameFarmViewers != 200 {
		return errors.New("loadtest: invalid friend opportunity AB profile shape")
	}
	for _, friends := range p.FriendCounts {
		if friends <= 0 || friends > maxFriends {
			return fmt.Errorf("loadtest: friend count %d is outside 1..%d (%s)", friends, maxFriends, socialprofile.Current().FriendLimitProfile)
		}
	}
	for _, percent := range p.OpportunityPercents {
		if percent < 0 || percent > 100 {
			return fmt.Errorf("loadtest: opportunity percent %d is outside 0..100", percent)
		}
	}
	for _, tier := range p.LoadTiers {
		if tier <= 0 {
			return errors.New("loadtest: load tiers must be positive")
		}
	}
	return nil
}
