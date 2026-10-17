// Package controlfence holds the vocabulary shared by the control plane and
// the data plane for proving that a routing mutation runs under an acquired
// gate.
//
// It exists as a leaf with no internal dependencies for a reason the compiler
// found rather than a reviewer: publishing a RouteSnapshot is a control-plane
// action performed by a data-plane object. internal/routecontrol already
// imports internal/actorrouting, so a token declared in routecontrol and
// required by actorrouting's publisher closes an import cycle. Both layers
// import this package instead, and neither imports the other.
package controlfence

// NestedControlOperationToken is the inheritable proof that a control-plane
// mutation runs under an acquired ControlMutationCoordinator gate. A top-level
// writer acquires it once and passes the same token down the
// RoutePlan -> Manager -> Publisher chain rather than re-acquiring a
// non-reentrant gate at each layer.
//
// The zero value is not a valid token: it carries neither an operation identity
// nor an issuer, so a provider cannot satisfy the contract by handing back a
// forgeable bool (section 5.4, item 6).
type NestedControlOperationToken struct {
	issuer      *Issuer
	operationID string
	epoch       uint64
}

// OperationID reports the durable operation identity this token was issued
// against. An empty result means the token was never issued by a coordinator.
func (t NestedControlOperationToken) OperationID() string { return t.operationID }

// Epoch reports the coordinator epoch the token was issued under. Holders
// revalidate against it so a token cannot outlive its gate.
func (t NestedControlOperationToken) Epoch() uint64 { return t.epoch }

// Issued reports whether the token came from some issuer. It is the cheap check
// a nested callee performs before doing work; proving which gate issued it is
// IssuedBy's job.
func (t NestedControlOperationToken) Issued() bool {
	return t.issuer != nil && t.operationID != ""
}

// Issuer mints tokens for one coordinator instance.
//
// Minting is a capability rather than a function so that importing this package
// is not by itself enough to forge a credential: a token is only accepted by
// the issuer that produced it, and a coordinator does not hand its issuer out.
// Without this, every package that must merely *carry* a token -- which after
// the seam work is most of the routing tree -- could also create one.
type Issuer struct{ identity *byte }

// NewIssuer returns an issuer distinct from every other. Two issuers never
// accept each other's tokens even when both mint the same operation ID.
func NewIssuer() *Issuer { return &Issuer{identity: new(byte)} }

// Issue mints a token bound to this issuer. An empty operation ID yields the
// zero token, because an operation with no durable identity is not one a
// crashed controller could find again on restart.
func (i *Issuer) Issue(operationID string, epoch uint64) NestedControlOperationToken {
	if i == nil || operationID == "" {
		return NestedControlOperationToken{}
	}
	return NestedControlOperationToken{issuer: i, operationID: operationID, epoch: epoch}
}

// IssuedBy reports whether this issuer minted the token. Verification compares
// issuer identity rather than field values, so a token assembled by any other
// means fails even if every visible field matches.
func (i *Issuer) IssuedBy(token NestedControlOperationToken) bool {
	return i != nil && token.issuer == i && token.operationID != ""
}
