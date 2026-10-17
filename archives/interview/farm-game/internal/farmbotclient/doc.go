// Package farmbotclient is a small, synchronous client for the FarmGate
// platform login and v4 FarmFrame WebSocket protocol.
//
// A Session deliberately serializes every operation that reads from its
// WebSocket. It therefore has no multiplexing or background reader: callers
// get a simple request/response API with bounded deadlines and fail-closed
// protocol validation.
package farmbotclient
