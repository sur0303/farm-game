// Package maildelivery provides the trusted, directed system-mail delivery
// entry point. It deliberately has no player-facing transport or fan-out
// behavior; callers submit one frozen delivery for one recipient.
package maildelivery
