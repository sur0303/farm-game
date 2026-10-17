# RT2Q routectl VerifyConnection HandshakeComplete fix (2026-08-10)

## Defect

Production `cmd/routectl` `verifyRouteControllerConnection` required
`tls.ConnectionState.HandshakeComplete` inside `tls.Config.VerifyConnection`.

On Go 1.26, `crypto/tls` invokes that callback with `HandshakeComplete=false`
after verified chains are already available. Every live `routectl` dial therefore
failed closed (exit 1, redacted/empty stderr) while the same TLS 1.3 exact
single-DNS-SAN mTLS chain succeeded without the callback-phase flag check.

Unit coverage previously forced `HandshakeComplete=true` on a handcrafted
`ConnectionState`, so it did not observe the live callback.

## Correction

- Drop reliance on callback-phase `HandshakeComplete`.
- Keep fail-closed checks: TLS 1.3+, non-empty verified chain/leaf, empty CN,
  exactly one canonical DNS SAN (parsed + raw SAN), no IP/email/URI SAN, and
  peer leaf consistency when `PeerCertificates` is present.
- Add a real local TLS/gRPC integration test that records the live callback
  state and proves exact identity success plus wrong/extra identity, foreign CA,
  and TLS 1.2 rejection. Errors stay redacted.

## Scope

Authorized production paths for this fix only:

- `cmd/routectl/main.go`
- `cmd/routectl/*_test.go`
- this focused RT2Q verification note
