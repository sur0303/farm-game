// Package opportunityrecovery owns the bounded, farm-global opportunity
// wakeup worker. It only evaluates committed Farm Header/Facts and publishes
// invalidation metadata; Farm mutation and viewer-specific authorization stay
// outside this package.
//
// WP2 keeps one-second polling and the existing generation/lease/claim/
// recovery fences, while restricting discovery to OwnedShards from a
// WorkerShardAssignmentProvider (Static by default), capping each round with
// RoundTimeBudget, ClaimLimit, and MaxConcurrent, and exporting aggregate due
// lag / backlog / empty-shard observability without farm or player labels.
package opportunityrecovery
