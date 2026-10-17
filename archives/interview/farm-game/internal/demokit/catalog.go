// Package demokit contains deterministic, side-effect-free building blocks for
// the resilience demonstration. It deliberately does not talk to Redis,
// Kubernetes, HTTP endpoints, or game services.
package demokit

import "fmt"

// Step describes one observable point in a demonstration scenario. The runner
// created in this package emits plans only; an operator must explicitly map a
// step to a fault-injection action in a separately approved environment.
type Step struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Expected    string `json:"expected"`
}

// Scenario is the contract for one resilience story shown in the defense.
// It is intentionally operational-language neutral so the same story can be
// run on kind, a staging namespace, or a recorded simulation later.
type Scenario struct {
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Goal          string   `json:"goal"`
	Prerequisites []string `json:"prerequisites"`
	Steps         []Step   `json:"steps"`
	Assertions    []string `json:"assertions"`
}

var catalog = []Scenario{
	{
		ID:    "F1",
		Title: "Lost response and idempotent recovery",
		Goal:  "Prove that a lost response can be recovered without duplicating the operation.",
		Prerequisites: []string{
			"an isolated non-production test environment",
			"the operation_id is retained across retries",
		},
		Steps: []Step{
			{ID: "submit", Description: "Submit an operation with a stable operation_id.", Expected: "The operation is accepted once."},
			{ID: "drop-response", Description: "Simulate a lost client response in the approved test harness.", Expected: "The operation remains observable by operation_id."},
			{ID: "recover", Description: "Retry using the same operation_id.", Expected: "Recovery returns the original result without a duplicate write."},
		},
		Assertions: []string{"The operation_id remains stable across retries.", "At most one operation result is committed."},
	},
	{
		ID:    "F2",
		Title: "Gate drain and fresh join",
		Goal:  "Prove that a draining Gate can recover clients through ticket renewal and a fresh join.",
		Prerequisites: []string{
			"the H5 client is in the InFarm state",
			"the approved test harness can observe Gate drain state",
		},
		Steps: []Step{
			{ID: "drain", Description: "Observe a Gate entering drain mode.", Expected: "Existing clients enter a recoverable state."},
			{ID: "renew", Description: "Renew the Cookie-backed ConnectionTicket.", Expected: "A fresh ticket is issued."},
			{ID: "fresh-join", Description: "Connect through another Gate and perform ClientHello plus Fresh Join.", Expected: "A snapshot and any required delta return the client to InFarm."},
		},
		Assertions: []string{"The client establishes a new WSS connection.", "The client returns to the ready state.", "The operation identity is preserved."},
	},
	{
		ID:    "F3",
		Title: "Holder loss and writer fencing",
		Goal:  "Prove that Holder loss and a new writer epoch prevent stale writes.",
		Prerequisites: []string{
			"a shard with an observable Holder lease",
			"an approved test fixture for lease-state observation",
		},
		Steps: []Step{
			{ID: "loss", Description: "Observe the current Holder lease becoming unavailable.", Expected: "The lease expires and the old Holder stops writing."},
			{ID: "claim", Description: "Claim the Holder with a higher writer_epoch and a new actor.", Expected: "The new Holder becomes authoritative."},
			{ID: "fence", Description: "Attempt a write using the stale epoch.", Expected: "The write is rejected with WRITER_CLAIM_MISMATCH."},
		},
		Assertions: []string{"Stale writes are rejected.", "The new Holder can complete a fresh join."},
	},
	{
		ID:    "F5",
		Title: "Slow-consumer isolation",
		Goal:  "Prove that a slow consumer does not block healthy FarmActor updates.",
		Prerequisites: []string{
			"a hot farm with an observable subscriber queue",
			"a healthy comparison client",
		},
		Steps: []Step{
			{ID: "broadcast", Description: "Publish a FarmChange while one client is slow.", Expected: "FarmActor continues processing the change."},
			{ID: "enqueue", Description: "Observe Gate queue behavior under FIFO pressure.", Expected: "The slow client is isolated or disconnected within the bound."},
			{ID: "observe", Description: "Observe the healthy client after the slow-consumer event.", Expected: "The healthy client receives the expected delta."},
		},
		Assertions: []string{"The healthy client remains responsive.", "The slow client cannot block FarmActor progress."},
	},
	{
		ID:    "F6",
		Title: "Opportunity hint recovery",
		Goal:  "Prove that a missed hint is recovered by a bounded Pull.",
		Prerequisites: []string{
			"an opportunity worker with a durable cursor",
			"the GET_NEXT_FRIEND_OPPORTUNITY read path",
		},
		Steps: []Step{
			{ID: "wakeup", Description: "Observe the worker receive an OpportunityChangedHint.", Expected: "The worker schedules a bounded refresh."},
			{ID: "drop-hint", Description: "Record a missed hint without changing durable state.", Expected: "The worker remains recoverable."},
			{ID: "pull", Description: "Run the normal bounded Pull path.", Expected: "The Navigator discovers the current opportunity."},
		},
		Assertions: []string{"Hint loss does not lose durable state.", "The Pull path restores the expected view."},
	},
}

// ListScenarios returns a defensive copy suitable for callers and tests.
func ListScenarios() []Scenario {
	result := make([]Scenario, 0, len(catalog))
	for _, scenario := range catalog {
		clone := scenario
		clone.Prerequisites = append([]string(nil), scenario.Prerequisites...)
		clone.Assertions = append([]string(nil), scenario.Assertions...)
		clone.Steps = append([]Step(nil), scenario.Steps...)
		result = append(result, clone)
	}
	return result
}

// ScenarioByID returns the named scenario or an operator-facing error.
func ScenarioByID(id string) (Scenario, error) {
	for _, scenario := range catalog {
		if scenario.ID == id {
			return scenario, nil
		}
	}
	return Scenario{}, fmt.Errorf("unknown resilience scenario %q", id)
}
