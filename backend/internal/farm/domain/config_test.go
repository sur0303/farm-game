package domain

import (
	"errors"
	"testing"
)

func TestValidatePlotUnlockRule(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*PlotUnlockRule)
		wantErr error
	}{
		{name: "valid P0 rule", mutate: func(*PlotUnlockRule) {}, wantErr: nil},
		{name: "max plots not 24", mutate: func(r *PlotUnlockRule) { r.MaxPlots = 25 }, wantErr: ErrConfigMaxPlots},
		{name: "initial not 5", mutate: func(r *PlotUnlockRule) { r.InitialUnlockedPlots = 4 }, wantErr: ErrConfigInitialPlots},
		{name: "missing entry", mutate: func(r *PlotUnlockRule) { r.Entries = r.Entries[:len(r.Entries)-1] }, wantErr: ErrConfigUnlockSequence},
		{name: "order swap", mutate: func(r *PlotUnlockRule) {
			r.Entries[0].PlotID, r.Entries[1].PlotID = r.Entries[1].PlotID, r.Entries[0].PlotID
		}, wantErr: ErrConfigUnlockSequence},
		{name: "plot id diverges from order", mutate: func(r *PlotUnlockRule) { r.Entries[3].UnlockOrder = 99 }, wantErr: ErrConfigUnlockSequence},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := validP0UnlockRule()
			tc.mutate(&rule)
			err := ValidatePlotUnlockRule(rule)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidatePlotUnlockRule: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateCropRuleStealPlan(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CropRule)
		wantErr error
	}{
		{name: "valid P0 curve", mutate: func(*CropRule) {}, wantErr: nil},
		{name: "zero grow duration", mutate: func(r *CropRule) { r.GrowDurationMs = 0 }, wantErr: ErrConfigGrowDuration},
		{name: "zero base yield", mutate: func(r *CropRule) { r.BaseYield = 0 }, wantErr: ErrConfigBaseYield},
		// Q32-12: base_yield must be a multiple of 50 so every 2% slot is
		// an exact positive integer; 51 is rejected at publish time, never
		// rounded at runtime.
		{name: "base yield 51 not exact", mutate: func(r *CropRule) { r.BaseYield = 51 }, wantErr: ErrConfigStealSlotRounding},
		{name: "base yield 49 not exact", mutate: func(r *CropRule) { r.BaseYield = 49 }, wantErr: ErrConfigStealSlotRounding},
		{name: "slot ratio sum mismatch", mutate: func(r *CropRule) { r.StealPlan.SlotRatioBP[0] = 300 }, wantErr: ErrConfigStealCapExceeded},
		{name: "cap above 20 percent", mutate: func(r *CropRule) {
			r.StealPlan.TotalCapRatioBP = 2100
			r.StealPlan.SlotRatioBP[0] = 300
		}, wantErr: ErrConfigStealCapExceeded},
		{name: "zero-ratio slot rejected", mutate: func(r *CropRule) {
			r.StealPlan.SlotRatioBP[0] = 0
			r.StealPlan.SlotRatioBP[1] = 400
		}, wantErr: ErrConfigStealSlotRounding},
		{name: "declining curve still bounded", mutate: func(r *CropRule) {
			r.BaseYield = 100
			r.StealPlan.SlotRatioBP = []uint32{400, 400, 300, 300, 200, 200, 100, 100}
		}, wantErr: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := validCropRule(250)
			tc.mutate(&rule)
			err := ValidateCropRule(rule)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateCropRule: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestStealQuantitiesIntegerBoundaries(t *testing.T) {
	plan := validCropRule(50).StealPlan
	tests := []struct {
		baseYield uint32
		wantSlot  uint32
		wantLimit uint32
		wantErr   error
	}{
		{baseYield: 50, wantSlot: 1, wantLimit: 10},
		{baseYield: 100, wantSlot: 2, wantLimit: 20},
		{baseYield: 250, wantSlot: 5, wantLimit: 50},
		{baseYield: 5000, wantSlot: 100, wantLimit: 1000},
		{baseYield: 51, wantErr: ErrConfigStealSlotRounding},
	}
	for _, tc := range tests {
		slots, limit, err := stealQuantities(tc.baseYield, plan)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("base_yield=%d err=%v want %v", tc.baseYield, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("base_yield=%d err=%v", tc.baseYield, err)
		}
		var sum uint32
		for _, q := range slots {
			if q != tc.wantSlot {
				t.Fatalf("base_yield=%d slot=%d want %d", tc.baseYield, q, tc.wantSlot)
			}
			sum += q
		}
		// Invariant: ten slots sum exactly to 20%, never above.
		if sum != limit || sum != tc.wantLimit || sum*5 != tc.baseYield {
			t.Fatalf("base_yield=%d limit=%d want %d (20%%=%d)", tc.baseYield, limit, tc.wantLimit, tc.baseYield/5)
		}
	}
}

func TestValidateGuardRule(t *testing.T) {
	tests := []struct {
		name    string
		rule    GuardRule
		wantErr error
	}{
		{name: "valid", rule: GuardRule{GuardRuleVersion: 1, TriggerProbabilityBP: 3000, InterceptPolicy: InterceptPolicyFullGross}},
		{name: "probability zero", rule: GuardRule{GuardRuleVersion: 1, TriggerProbabilityBP: 0, InterceptPolicy: InterceptPolicyFullGross}},
		{name: "probability max", rule: GuardRule{GuardRuleVersion: 1, TriggerProbabilityBP: 10000, InterceptPolicy: InterceptPolicyFullGross}},
		{name: "probability overflow", rule: GuardRule{GuardRuleVersion: 1, TriggerProbabilityBP: 10001, InterceptPolicy: InterceptPolicyFullGross}, wantErr: ErrConfigGuardProbability},
		{name: "policy not full gross", rule: GuardRule{GuardRuleVersion: 1, TriggerProbabilityBP: 1, InterceptPolicy: InterceptPolicyUnspecified}, wantErr: ErrConfigInterceptPolicy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateGuardRule(tc.rule)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("ValidateGuardRule: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
