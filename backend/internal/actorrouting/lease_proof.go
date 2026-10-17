package actorrouting

// CompatibleLeaseProof reports whether a route header and a locally resolved
// assignment describe the same lease generation.
//
// LeaseUntilMs is deliberately not ordered here. Deadline renewal is projected
// to every process asynchronously, so a trusted caller can briefly observe a
// newer deadline than the receiver (or vice versa) while Holder/Boot,
// ShardRevision, WriterEpoch, and LeaseEpoch are still identical. Resolve and
// the holder-side LeaseGuard remain responsible for rejecting a locally expired
// lease; a caller's later deadline can never extend the receiver's authority.
func CompatibleLeaseProof(expectedLeaseEpoch uint64, expectedLeaseUntilMs *uint64, assignment Assignment, allowLegacy bool) bool {
	hasEpoch := expectedLeaseEpoch != 0
	hasDeadline := expectedLeaseUntilMs != nil
	if !hasEpoch && !hasDeadline {
		return allowLegacy
	}
	return hasEpoch && hasDeadline && *expectedLeaseUntilMs != 0 &&
		assignment.LeaseEpoch != 0 && assignment.LeaseUntilMs > 0 &&
		expectedLeaseEpoch == assignment.LeaseEpoch
}
