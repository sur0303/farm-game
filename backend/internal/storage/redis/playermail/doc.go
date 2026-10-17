// Package playermail implements the contract-35A PlayerMail non-Actor
// authority on company Redis: the versioned PlayerMailKeyBuilderV1, the
// fixed small Lua script family (InsertMailIfAbsentV1, MarkMailReadV1,
// DeleteMailV1, ApplyMailClaimProjectionV1, ExpireMailVisibilityV1,
// GcMailV1 plus the bounded scan lease) and the Repository adapters behind
// internal/player/mail's ports.
//
// Physical invariants (35A §0.3): every key of one Lua call shares one
// verbatim hash tag; Entry+State+Dedup+Index+Summary+DueInfo commit in one
// script; item and byte bounds fail closed; Redis timeouts are typed
// Unknown, never fake success; no KEYS/HGETALL/unbounded loops anywhere.
package playermail
