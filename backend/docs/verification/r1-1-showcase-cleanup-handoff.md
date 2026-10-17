# R1-1 showcase release cleanup handoff

## Scope and frozen inputs

- Backend worktree: `farm-game-r1-1-showcase-cleanup-20260809`, frozen input `ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c`.
- H5 worktree: `classic-farm-h5-r1-1-showcase-cleanup-20260809`, frozen input `b83cae607245391df6e2d605cffba85eb194cb09`.
- No API/proto, friend, authoritystore, logic/actor, workflow, Farm/Player rule, or Route Runtime files were changed.
- Backend and H5 remain separate commits; no push, merge, or tag was performed.

## Delivered boundary

`FARM_APP_PROFILE` is parsed as `local|test|showcase`; an unknown value fails FarmGate startup before Redis or business initialization. Local/test retain the explicit platform-token/dev-session and fault test paths. Showcase:

- has no default Demo platform identity;
- rejects Demo identity, load-secret, dev-session, fault, pprof, unsafe provider, and relaxed session-fencing configuration;
- requires secure HTTP session cookies and an explicit `wss://` endpoint;
- does not register `/v1/auth/login`, `/api/v1/auth/login`, `/v1/dev/session`, or `/api/v1/dev/session` test routes; the latter two therefore return the standard 404;
- does not register the repository's WeChat development verifier or the public command-result fault endpoint;
- keeps account registration/login on the real Cookie → connection ticket → WSS → Redis-backed authority path.

The H5 `web` build is the showcase profile. It uses `WebAuthProvider`; Demo/Staging tokens and mock provider settings fail the build. The staging provider now has no default token and remains available only to the explicit `test` profile. The showcase bundle boundary removes the staging-auth dynamic chunk. The committed build contract is [`r1-1-showcase-build-manifest.json`](../../../classic-farm-h5-r1-1-showcase-cleanup-20260809/docs/verification/r1-1-showcase-build-manifest.json).

Runtime/tool separation is documented in [`r1-1-runtime-image-contents.md`](r1-1-runtime-image-contents.md).

## Verification

Passed:

- `GOCACHE=/tmp/farm-game-go-cache go test ./internal/gatewayapp ./internal/gateway/auth ./cmd/farm-gate`
- `GOCACHE=/tmp/farm-game-go-cache go vet ./internal/gatewayapp ./internal/gateway/auth ./cmd/farm-gate`
- `GOCACHE=/tmp/farm-game-go-cache CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /tmp/farm-gate-r1-1-showcase ./cmd/farm-gate`
- Backend profile rejection startup checks for invalid profile and `DEMO_PLATFORM_TOKEN`.
- Backend showcase 404 route unit test and local/test route retention test.
- `node scripts/audit-runtime-images.mjs`.
- H5 `npm test -- --run`: 23 files, 204 tests passed.
- H5 `npx tsc --noEmit`.
- H5 `npm run audit:build`: `build:web`/showcase, `build:staging`/test, and `build:wechat`/local all passed.
- H5 invalid-profile and injected-Demo-Token showcase build checks failed closed as expected.

Recorded failures and limits:

- Before installing the lockfile dependencies, H5 `npm test -- --run` and `npm run build:web` returned 127 because `vitest`/`tsc` were absent. `npm ci --ignore-scripts --no-audit --no-fund` then succeeded; the final runs above were executed afterward.
- The first post-install H5 unit run had four failures because existing staging test fixtures relied on the removed provider default token. Those fixtures were changed to pass their explicit test token; the final unit run is 204/204.
- A direct Go build without `-buildvcs=false` returned 128 from VCS stamping in this worktree. The build was rerun with the repository's existing `-buildvcs=false` convention and passed.
- `scripts/build-images.sh` was attempted but Docker could not access `/var/run/docker.sock` (`permission denied`). Static image-content verification passed; Docker image contents, Redis production connectivity, WSS production connectivity, and production deployment were not verified.
- Hero/Task/F1-F3 end-to-end execution remains the Controller composition-gate responsibility and was not claimed here.
- Commit handoff is blocked by the execution sandbox: both worktree `.git` administrative directories are read-only, so `git add` cannot create `index.lock`. No alternate repository or temporary clone was substituted, and no commit hash is claimed. The implementation remains in the two requested worktrees for a host-side `git add`/`git commit` once their Git administrative paths are writable.

Final worktree status must be clean after the two commits are created.
