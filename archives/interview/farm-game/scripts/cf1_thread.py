#!/usr/bin/env python3
"""CF-1: generic token/context threading for the remaining call sites.

SPENT. Do not re-run. This patched a tree that no longer exists; its output is
committed in the source files and re-running it would corrupt them. It is kept
only as the audit trail for how the CF-1 edits were made, because CF-1 produced
a methodology finding rather than just a result.

That finding, CF1-F6: matching by method name cannot separate unrelated
interfaces that happen to share one. This script edited call sites of
`ReleasePlacementExclusionForOwner` across two interfaces that were never meant
to move together, and the mistake survived `go vet` because the mismatch only
surfaced at a runtime type assertion. Any future mechanical edit of this kind
must key on the receiver type, not the method name.

Takes files on the command line, works out the right package qualifier from
their imports, and inserts the token argument. Guards against double insertion
so it is safe to re-run over a partially patched file.
"""
import pathlib
import re
import sys

SNAP_CALLS = ("Bootstrap", "CompareAndSwap", "Publish", "PublishAuthoritySnapshot")
CTX = r"(ctx|context\.Background\(\)|context\.TODO\(\)|t\.Context\(\)|[a-z][A-Za-z0-9_]*Ctx)"
C18_NOARG = ("LoadTransitionTombstones", "LoadTransitionRecoveries")
C18_ARG = ("LookupTransitionTombstone", "SaveTransitionTombstone",
           "LookupTransitionRecovery", "ReserveTransitionRecovery",
           "CommitTransitionRecovery", "DeleteTransitionRecovery")


def qualifier(src: str) -> str:
    if src.startswith("package routecontrol\n"):
        return ""
    m = re.search(r'(\w+)\s+"farmgame/internal/routecontrol"', src)
    if m:
        return m.group(1) + "."
    if '"farmgame/internal/routecontrol"' in src:
        return "routecontrol."
    return ""


def c18_ctx(src: str) -> str:
    """Inside the authority package the startup helper is available."""
    return ("transitionStoreStartupContext()"
            if src.startswith("package authority\n") else "context.Background()")


for rel in sys.argv[1:]:
    p = pathlib.Path(rel)
    src = p.read_text()
    tok = qualifier(src) + "NestedControlOperationToken{}"
    out = src
    n = 0

    for name in SNAP_CALLS:
        pat = re.compile(rf"\.{name}\({CTX}\s*,\s*(?!{re.escape(tok)})(?!\w*\.?NestedControlOperationToken)")
        out, k = pat.subn(lambda m: f".{name}({m.group(1)}, {tok}, ", out)
        n += k

    fallback = c18_ctx(src)
    for name in C18_NOARG:
        out, k = re.subn(rf"\.{name}\(\)", f".{name}({fallback})", out)
        n += k
    for name in C18_ARG:
        pat = re.compile(rf"\.{name}\((?!context\.|ctx|transitionStoreStartup)")
        out, k = pat.subn(f".{name}({fallback}, ", out)
        n += k

    if out != src:
        p.write_text(out)
        print(f"{rel}: {n} sites  (token={tok})")
    else:
        print(f"{rel}: no change")
