#!/usr/bin/env python3
"""Widen ReleasePlacementExclusionForOwner to the frozen contract.

SPENT. Do not re-run. It rewrites whole method bodies by regex against the exact
text they had before this ran; the current implementations no longer match, and
a partial match would silently replace working logic with a sentinel. Kept
because it is the repair for CF1-F6 and shows which implementers were given real
behavior versus which were left deliberately unimplemented.

CF-1 threaded a five-parameter shape that omitted the expected revision and
the coordinator token, both of which section 8.3 requires. This corrects the
shape everywhere and gives the A0.5-owned implementers real behavior; only the
two A1R-owned stores keep a sentinel.
"""
import pathlib
import re

SENTINEL_BODY = "\treturn {q}PlacementExclusionResult{{}}, {q}ErrNotImplementedUntilA1R\n"

# path -> (qualifier, receiver-pattern, replacement body builder)
SENTINEL_KEEP = {
    "internal/storage/redis/routecontrol/holder_store.go": "core.",
}

ALLOW_ALL = {
    "internal/testkit/capstaticroute/placement_permit.go": "routecontrol.",
    "internal/actorrouting/authority/placement_permit_test.go": "routecontrol.",
    "internal/routeplan/placement_permit_test.go": "routecontrol.",
}

OLD_SIG = re.compile(
    r"func \((?P<recv>[^)]*)\) ReleasePlacementExclusionForOwner\("
    r"context\.Context, actorrouting\.RouteDomain, string, string, string\) "
    r"\((?P<ret>[\w.]*PlacementExclusionResult), error\) \{\n"
    r"(?P<body>(?:\t.*\n)*?)\}\n"
)

ALLOW_ALL_BODY = """\tif !token.Issued() {{
\t\treturn {q}PlacementExclusionResult{{}}, {q}ErrPlacementExclusionInvalid
\t}}
\tif release.OwnerPlanID == "" || release.ExpectedRevision == 0 {{
\t\treturn {q}PlacementExclusionResult{{}}, {q}ErrPlacementExclusionInvalid
\t}}
\t// Nothing was ever excluded here, so the release is vacuously already done.
\treturn {q}PlacementExclusionResult{{
\t\tCommitted: false,
\t\tOutcome:   {q}PlacementReleaseAlreadyReleased,
\t\tState: {q}PlacementExclusion{{Domain: domain, HolderID: holderID, BootID: bootID,
\t\t\tExcluded: false, Revision: release.ExpectedRevision}},
\t}}, nil
"""

NEW_SIG_NAMED = ("func ({recv}) ReleasePlacementExclusionForOwner(_ context.Context, "
                 "token {q}NestedControlOperationToken, domain actorrouting.RouteDomain, "
                 "holderID, bootID string, release {q}PlacementExclusionRelease) "
                 "({ret}, error) {{\n")
NEW_SIG_BLANK = ("func ({recv}) ReleasePlacementExclusionForOwner(context.Context, "
                 "{q}NestedControlOperationToken, actorrouting.RouteDomain, string, string, "
                 "{q}PlacementExclusionRelease) ({ret}, error) {{\n")

changed = []
for path, qual in list(SENTINEL_KEEP.items()) + list(ALLOW_ALL.items()):
    p = pathlib.Path(path)
    src = p.read_text()
    match = OLD_SIG.search(src)
    if not match:
        print("MISS", path)
        continue
    recv, ret = match.group("recv"), match.group("ret")
    if path in SENTINEL_KEEP:
        sig = NEW_SIG_BLANK.format(recv=recv, q=qual, ret=ret)
        body = SENTINEL_BODY.format(q=qual)
    else:
        sig = NEW_SIG_NAMED.format(recv=recv, q=qual, ret=ret)
        body = ALLOW_ALL_BODY.format(q=qual)
    src = src[:match.start()] + sig + body + "}\n" + src[match.end():]
    p.write_text(src)
    changed.append(path)

for path in changed:
    print("patched", path)
