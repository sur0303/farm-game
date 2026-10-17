# CP-MVP-BEFORE-HARDENING evidence

Status: PASS

## Frozen product

- Backend product head: `260babcd93cfb6a4a58a481cc3c7f8c1f95e455a`
- H5 product parent: `df379b5fcc5eb710fc1efa5406c752ba18c3ab30`
- H5 frozen candidate: `48991e6a9ca724941b54202603605948b169eed1`
- Current Truth / contract baseline: `16858bcbde81520a133272f035f838b07e056c33`
- Actor Authority: Redis
- Actor Route: static snapshot

The H5 frozen candidate contains one test-only alignment: the F3 Playwright gate
now performs the explicit two-step Theft confirmation already shipped in the
product. There is no H5 runtime tree change from the parent product commit.

## Gates

Evidence root:

`.agentctl-cp-route-state/evidence/cp-mvp-before-hardening-20260809T091148Z`

- Backend `go test -buildvcs=false ./...`: PASS
- Backend `go vet -buildvcs=false ./...`: PASS
- Backend `go build -buildvcs=false ./cmd/...`: PASS
- Key gateway/friend/farm/player/workflow packages under `-race`: PASS
- H5: 22 files / 202 tests PASS
- H5 `npx tsc --noEmit`: PASS
- H5 web/staging/wechat audited builds: PASS
- `hero-task-f1f2f3-aligned`: Browser + Protocol Bot + Authority Oracle PASS
  - real login and economy/plant/harvest
  - HTTPS share and canonical friendship
  - WATER/THEFT/home
  - Task list/growth/claim
  - F1/F2/F3 recovery
- 200-WSS Theft baseline: 200/200 ready, 199/199 terminal, no gaps or disconnects
- 200-WSS slow reader: 200/200 ready, 199/199 terminal; slow reader isolated

The baseline Theft audit proves exactly ten successful visitor shares, 189
NO_CHANGE terminals, 100 total stolen, unique receipts, monotonic FarmChange,
queryable operation results, no pending settlement and zero compensation.

## Invalid attempts retained

Invalid runs are not release evidence:

1. `hero-task-f1f2f3`: missing linked-worktree Redis secret assembly
   (`INCONCLUSIVE`).
2. `hero-task-f1f2f3-assembled`: H5 port 5194 collided with the preserved manual
   review stack (`FAIL`, test environment assembly).
3. `hero-task-f1f2f3-valid`: exposed the stale F3 test expectation: the shipped
   product requires explicit second confirmation, while the old gate clicked
   once. This was a test-contract defect, not a business failure.

After the test-only alignment, the isolated fresh run passed all Browser,
Protocol and Authority witnesses.

## Boundary

This checkpoint freezes the working MVP. It does not claim FriendshipDomain
migration, legacy roster removal, Model Field V2 migration, SoftSave, dynamic
Route Runtime, Kubernetes capacity or 30-million-DAU readiness.
