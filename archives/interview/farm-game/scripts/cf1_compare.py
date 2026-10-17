#!/usr/bin/env python3
"""CF-1 step 4: compare the measured true set against the section 8.1 write set.

SPENT. Do not re-run for a verdict. It parses the write set out of section 8.1,
which amendment W1 has since rewritten, so it would now compare against a table
that no longer describes what it did when the measurement was taken. Kept as the
audit trail for how the eight-package, twenty-file true set was derived.

The gate requires the prior table and the write set to come out of one
mechanical action rather than being transcribed by hand, so the write set is
parsed straight from the document.
"""
import fnmatch
import pathlib
import re
import subprocess

DOC = pathlib.Path("docs/verification/scaleplan-sp3-production-adapter-implementation-plan.md")

text = DOC.read_text().splitlines()
start = next(i for i, l in enumerate(text) if l.startswith("internal/scaleplan/repository.go"))
end = next(i for i, l in enumerate(text[start:], start) if l.startswith("```"))
block = text[start:end]

owned, conformance = [], []
in_conformance = False
for line in block:
    if line.startswith("#"):
        if "conformance" in line:
            in_conformance = True
        continue
    entry = line.split("#")[0].strip()
    if not entry:
        continue
    (conformance if in_conformance else owned).append(entry)

measured = subprocess.run(
    ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
).stdout.splitlines()
measured = sorted(
    l[3:] for l in measured
    if not l.startswith("??") and l[3:].endswith(".go")
)


def covered(path: str, patterns: list[str]) -> str | None:
    for pat in patterns:
        if pat.endswith("/**"):
            if path.startswith(pat[:-2]):
                return pat
        elif "*" in pat:
            # A glob does not recurse: it must match within one directory.
            if fnmatch.fnmatch(path, pat) and path.count("/") == pat.count("/"):
                return pat
        elif path == pat:
            return pat
    return None


print(f"write set: {len(owned)} owned entries, {len(conformance)} conformance entries")
print(f"measured true set: {len(measured)} files\n")

inside, conf, outside = [], [], []
for f in measured:
    if (hit := covered(f, owned)):
        inside.append((f, hit))
    elif (hit := covered(f, conformance)):
        conf.append((f, hit))
    else:
        outside.append(f)

print(f"--- inside the A0.5 owned write set: {len(inside)} ---")
for f, hit in inside:
    print(f"  {f:<62} <- {hit}")
print(f"\n--- covered by the conformance edit right: {len(conf)} ---")
for f, hit in conf:
    print(f"  {f:<62} <- {hit}")
print(f"\n--- OUTSIDE BOTH (stop condition): {len(outside)} ---")
for f in outside:
    print(f"  {f}")

print("\n--- write-set entries the measurement never touched ---")
for pat in owned + conformance:
    if not any(covered(f, [pat]) for f in measured):
        print(f"  {pat}")
