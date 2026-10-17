// Package mail implements the contract-35 PlayerMail domain: the non-Actor
// MailEntry authority owned by PlayerMailDomain (35 Q35-01), the bounded
// MailEntry/MailUserState/DeliveryDedup model (35 Q35-03), the three
// separated hashes (35 M5/Q35-03), stable delivery/mutation/work identities
// (35A §5), the six-per-page exclusive cursor model (35A §3) and the
// TrustedMailClaimMaterial trusted port (35 Q35-06).
//
// Authority split (35 §2.1): this package owns mail existence, snapshots,
// read/deleted flags, pagination indexes, mailbox summary and delivery
// dedup. It never owns asset grants, MailClaimState or MailClaimReceipt —
// those stay with PlayerActor (31 §11.8). The claimed projection handled
// here is a rebuildable display copy only (35A 35A-07).
//
// This package is storage-agnostic. The Redis physical mapping (keys, hash
// tags, Lua) lives in internal/storage/redis/playermail per contract 35A.
package mail
