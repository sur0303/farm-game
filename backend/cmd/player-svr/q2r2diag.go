package main

// Q2R-2 diagnostics: a TEST-ONLY local read-only endpoint that exposes the
// authority runtime's per-shard admission state so real-process smokes can
// DIRECTLY observe the local-deadline fail-closed gate.
//
// Hard constraints:
//   - enabled only when BOTH Q2R2_LIVE=1 and Q2R2_DIAGNOSTICS=1 (a single
//     misconfigured variable can never open it);
//   - loopback only: any non-loopback bind value is rejected;
//   - read-only and DERIVED from Runtime/LeaseGuardRegistry getters
//     (never caches a second truth);
//   - lifecycle follows the process: Shutdown when run() returns.
//
// It is NOT a public management API: no gRPC/proto registration, no CLB
// exposure, no state mutation.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
)

func q2r2DiagnosticsEnabled() bool {
	return strings.TrimSpace(os.Getenv("Q2R2_LIVE")) == "1" &&
		strings.TrimSpace(os.Getenv("Q2R2_DIAGNOSTICS")) == "1"
}

// startQ2R2Diagnostics starts the loopback-only diagnostics HTTP server.  bind
// defaults to 127.0.0.1:0; a non-loopback bind is rejected (fail closed).  It
// returns the bound address and a shutdown func.
func startQ2R2Diagnostics(runtime *authoritycomposition.Runtime, domain actorrouting.RouteDomain, bind string) (string, func(), error) {
	if !q2r2DiagnosticsEnabled() {
		return "", nil, nil
	}
	host := ""
	if bind == "" {
		host = "127.0.0.1"
		bind = "127.0.0.1:0"
	} else if h, _, err := net.SplitHostPort(bind); err == nil {
		host = h
	} else {
		// Allow a bare loopback host to be promoted to ":0".
		host = bind
		bind = bind + ":0"
	}
	if host == "" || !isLoopback(host) {
		return "", nil, fmt.Errorf("q2r2 diagnostics must bind an explicit loopback host, got %q", host)
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return "", nil, fmt.Errorf("q2r2 diagnostics listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/q2r2z/diag", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		serving, canServe := 0, false
		fenceReason, runtimeFatal := 0, false
		if runtime != nil {
			// Directly derived from existing read-only runtime/guard methods;
			// never a cached second truth.
			serving = runtime.ServingShardCount()
			canServe = runtime.CanServeShard(domain, 0)
			runtimeFatal = runtime.FatalErr() != nil
			if runtime.Guards != nil {
				if handle, ok := runtime.Guards.Guard(domain, 0); ok && handle != nil {
					fenceReason = int(handle.FenceReason())
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"serving_shard_count": serving,
			"can_serve_shard":     canServe,
			"lease_fence_reason":  fenceReason,
			"runtime_fatal":       runtimeFatal,
		})
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

func isLoopback(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return host == "localhost"
	}
	return ip.IsLoopback()
}
