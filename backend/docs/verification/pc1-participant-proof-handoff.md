# PC1 Participant-proof handoff

The production proof adapter is `internal/workflow/participantproof`. It is
stateless between calls and reloads the requested `WorkRecord` through the
injected `workflow.WorkRepository` before every first-execution proof. A
repository error, missing record, malformed record, address mismatch, version
mismatch, identity mismatch, lease mismatch, or dependency mismatch is a
rejection; none is an implicit allow.

## Composition seams still required

The composition root still has to perform these exact steps:

1. Construct one `participantproof.Verifier` with the authoritative
   `workflow.WorkRepository`, the W31-A `workidentity.BucketResolver`, and the
   existing physical route/holder fence implementation:

   ```go
   proof, err := participantproof.New(workRepository,
       participantproof.WithBucketResolver(bucketResolver),
       participantproof.WithRouteFenceVerifier(routeFenceVerifier),
       participantproof.WithClock(now),
   )
   ```

   `WithClock(now)` is mandatory. It must be the repository-authoritative
   shared time source; there is no local `time.Now` fallback. A deterministic
   repository clock must be passed to both the repository and the proof
   adapter, and a nil or invalid clock fails closed.

2. Pass that same `proof` instance to the Player Participant options:

   ```go
   WorkVersionVerifier:     proof,
   WorkLeaseVerifier:        proof,
   RouteFenceVerifier:       proof,
   WorkAddressVerifier:      proof,
   IdentityBindingVerifier:  proof,
   PlantDependencyVerifier:  proof,
   ```

   Keep the existing PlayerActor `Registry` and its durable receipt-capable
   handler in the options, then call `player/participant.NewServerChecked`.

3. Pass the same `proof` instance to the Farm Participant options:

   ```go
   WorkVersionVerifier:    proof,
   WorkLeaseVerifier:      proof,
   RouteFenceVerifier:     proof,
   WorkAddressVerifier:    proof,
   IdentityBindingVerifier: proof,
   CommitPlantDependency:  proof,
   ```

   Keep the existing FarmActor `Registry` and durable receipt-capable handler
   in the options, then call `farm/ingress.NewServerChecked`.

4. Register those two already-existing servers in the current gRPC/service
   composition. No participant server, Actor, proto, command, routing, Redis
   key, or generic workflow interface change is needed for this handoff.

The server continues to resolve a terminal participant receipt before calling
the first-execution WorkVersion/lease/dependency proofs. The proof package is
therefore only the pre-mailbox authority check; it is not a receipt store and
does not replace the Actor-local durable receipt resolver.

## What the adapter proves

- `VerifyWorkAddress` loads the record by the wire WorkType/WorkID, compares
  the complete persisted `workidentity.WorkAddress`, and validates its
  versioned physical bucket through the injected resolver.
- `VerifyWorkVersion` requires the request version to equal the persisted
  `WorkVersion` and binds the current step, invocation, registered command,
  and persisted deterministic payload hash.
- `VerifyIdentityBinding` binds coordinator subject/operation and the typed
  payload subject to the persisted record. Worker recovery has no subject or
  operation field in the frozen protocol, so it is bound through the durable
  WorkAddress, invocation, and typed payload subject. Because shared ingress
  runs before receipt lookup, an exact invocation may match its terminal
  historical `StepRecord` after `WorkRecord.CurrentStep` advances; the later
  WorkVersion, lease, and dependency proofs remain current-step-only for first
  execution.
- `VerifyWorkLease` requires the persisted non-terminal lease owner, epoch,
  token hash, and unexpired lease. A takeover changes the persisted fence, so
  the previous owner/token cannot pass.
- The Plant methods decode the persisted `StepRecord` payloads and require
  every referenced dependency to have an allowed terminal status, a non-empty
  deterministic result, and a matching canonical result hash. A missing
  receipt is never treated as success. The rejected Farm branch is checked as
  `RELEASE_SEED_RESERVATION`'s explicit `FARM_REJECTED` dependency.
- `VerifyRoute` only delegates to the injected route-fence owner. This package
  has no Redis client, route calculator, holder lookup, or second routing
  system, and it must run outside the Actor turn.

## Exact limitations of the frozen seams

The existing `workflow.WorkRecord` persists `LeaseOwner`, `LeaseEpoch`,
`LeaseTokenHash`, and `LeaseExpireAtMs`, but it does not persist a Worker boot
ID. The adapter can require the frozen request's non-empty `CallerBootID`
through the shared ingress kernel, but it cannot independently compare that
value with durable authority. If boot identity must be a separate persisted
fence, the composition must bind it to the existing authoritative lease owner
or provide an already-authoritative lease record; this package must not invent
a second lease system or weaken the owner/epoch/token checks.

The `WorkRepository` port also does not expose its trusted clock. Lease expiry
is therefore checked only with the required injected clock. Production
composition must use the repository's time source (or a shared time source);
the adapter rejects a missing or zero clock result. Changing this generic
interface is outside PC1.

Finally, the internal `WorkAddress` representation has no general
`FRIEND_PAIR` source-authority field or `AuthorityLayoutVersion`. The adapter
fails closed when a source request carries a non-empty layout version because
it cannot compare that value with persisted authority. Composition must supply
an authoritative source-layout mapping before registering such source work.
It also fails closed for an unrepresentable source authority. Adding a new
source placement requires an existing authoritative mapping or a separate
contract change; it must not be inferred from request fields here.
