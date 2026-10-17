package controlfence

import "errors"

// ErrNotImplementedUntilA1R marks a method that A0.5 added only so the tree
// compiles at the seam, and that A1R must fill in.
//
// It lives in this leaf package rather than next to any one consumer because
// the seam crosses several trees that have no business importing each other
// just to name a staging error. A1R's exit gate greps the whole repository for
// this identifier and requires zero hits, counting the declaration as well as
// every call site, so keeping it to a single declaration keeps that check a
// single grep.
//
// Callers treat it as fail-closed: a routing decision that cannot be made is
// not a routing decision that defaults to permit.
var ErrNotImplementedUntilA1R = errors.New("conformance: not implemented until A1R")
