package scaleplan

// Blocked termination tests use scaleplan.BlockedTerminationOwner with
// FakeQuiescence. No alternate owner implementation is provided: Executor is
// intentionally sealed to the concrete core owner.
