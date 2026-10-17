// Package identity is the single shared authority for cross-process stable
// identity in the farm backend (SCB-1): EffectiveIdempotencyKey,
// OperationRootRef, WorkType/ParticipantStep stable names and every
// StableOpaqueID v1 derivation frozen by design contracts 27/28/29/31/33A.
//
// The byte-level algorithm (LP + SHA-256 + Base64URL_NoPadding, "sid1_"
// prefix) is implemented once in internal/workidentity; this package only
// composes it with the frozen namespaces and canonical field encodings.
// It must never grow a second hashing algorithm, use routing hash seeds,
// or accept client-supplied root/authority identities.
package identity
