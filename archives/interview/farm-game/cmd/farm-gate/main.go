// Command farm-gate is the canonical S2 application gateway process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"farmgame/internal/authoritycomposition"
	"farmgame/internal/gatewayapp"
	"farmgame/internal/grpcmetrics"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/socialprofile"
	"farmgame/internal/storage/redis/rediskey"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

func main() {
	if _, err := socialprofile.BindProcessEnvironment(); err != nil {
		log.Fatalf("bind social product profile: %v", err)
	}
	// Block profiling has measurable runtime cost. Keep it available for an
	// explicit load-test run, but never enable it merely by starting FarmGate.
	profile := strings.ToLower(strings.TrimSpace(os.Getenv("FARM_APP_PROFILE")))
	if profile != "showcase" {
		if raw := os.Getenv("GATE_BLOCK_PROFILE_RATE"); raw != "" {
			if rate, err := strconv.Atoi(raw); err == nil && rate > 0 {
				runtime.SetBlockProfileRate(rate)
			}
		}
	}
	options, closeRemote, err := assignmentOptions()
	if err != nil {
		log.Fatal(err)
	}
	defer closeRemote()
	gatewayapp.RunWithOptions(options)
}

func assignmentOptions() (gatewayapp.RunOptions, func(), error) {
	assignmentMode, err := authoritycomposition.AssignmentModeFromEnvironment()
	if err != nil {
		return gatewayapp.RunOptions{}, func() {}, err
	}
	membershipMode, err := authoritycomposition.MembershipModeFromEnvironment()
	if err != nil {
		return gatewayapp.RunOptions{}, func() {}, err
	}
	if err := authoritycomposition.ValidateModeCombination(membershipMode, assignmentMode); err != nil {
		return gatewayapp.RunOptions{}, func() {}, err
	}
	if assignmentMode == authoritycomposition.AssignmentModeStatic {
		return gatewayapp.RunOptions{}, func() {}, nil
	}
	keyspaceTuple, err := requiredRemoteKeyspaceTuple(membershipMode, assignmentMode)
	if err != nil {
		return gatewayapp.RunOptions{}, func() {}, err
	}
	_ = keyspaceTuple

	dialTimeout := authoritycomposition.RemoteDialTimeoutFromEnvironment()
	dialCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	client, err := authoritycomposition.NewRemoteSnapshotClientFromEnvironment(dialCtx)
	cancel()
	if err != nil {
		return gatewayapp.RunOptions{}, func() {}, fmt.Errorf("dial authenticated route-control snapshot source: %w", err)
	}
	initialTimeout := authoritycomposition.RemoteInitialFetchTimeoutFromEnvironment()
	initialCtx, initialCancel := context.WithTimeout(context.Background(), initialTimeout)
	composition, err := authoritycomposition.NewRemoteAssignmentComposition(initialCtx, client, authoritycomposition.RemoteAssignmentOptionsFromEnvironment())
	initialCancel()
	if err != nil {
		_ = client.Close()
		return gatewayapp.RunOptions{}, func() {}, err
	}
	rpcMetrics := grpcmetrics.New(prometheus.DefaultRegisterer)
	endpointOptions := authoritycomposition.EndpointConnectionOptionsFromEnvironment()
	endpointOptions.DialOptions = []grpc.DialOption{
		grpc.WithUnaryInterceptor(rpcMetrics.UnaryClientInterceptor()),
	}
	if err := composition.ConfigureEndpointConnections(endpointOptions); err != nil {
		_ = composition.Close()
		_ = client.Close()
		return gatewayapp.RunOptions{}, func() {}, err
	}
	verifier, err := newGateVerifierRuntime(composition)
	if err != nil {
		_ = composition.Close()
		_ = client.Close()
		return gatewayapp.RunOptions{}, func() {}, err
	}
	return gatewayapp.RunOptions{Assignment: composition, RPCMetrics: rpcMetrics}, func() {
		verifier.Close()
		_ = composition.Close()
		_ = client.Close()
	}, nil
}

type gateVerifierRuntime struct {
	listener net.Listener
	server   *gateverification.Server
	done     chan struct{}
	once     sync.Once
}

func newGateVerifierRuntime(composition *authoritycomposition.AssignmentComposition) (*gateVerifierRuntime, error) {
	listen := strings.TrimSpace(os.Getenv("GATE_VERIFIER_LISTEN"))
	cert := strings.TrimSpace(os.Getenv("GATE_VERIFIER_TLS_CERT"))
	key := strings.TrimSpace(os.Getenv("GATE_VERIFIER_TLS_KEY"))
	clientCA := strings.TrimSpace(os.Getenv("GATE_VERIFIER_CLIENT_CA"))
	identity := strings.TrimSpace(os.Getenv("GATE_VERIFIER_ALLOWED_CLIENT_IDENTITY"))
	if composition == nil || listen == "" || cert == "" || key == "" || clientCA == "" || identity == "" {
		return nil, errors.New("farm-gate: complete separate Gate verifier TLS configuration is required")
	}
	tlsConfig, err := transport.LoadServerTLSConfig(cert, key, clientCA)
	if err != nil {
		return nil, errors.New("farm-gate: Gate verifier TLS configuration failed")
	}
	server, err := gateverification.NewServer(composition, tlsConfig, []string{identity}, 64<<10)
	if err != nil {
		return nil, errors.New("farm-gate: Gate verifier service configuration failed")
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, errors.New("farm-gate: Gate verifier listener failed")
	}
	runtime := &gateVerifierRuntime{listener: listener, server: server, done: make(chan struct{})}
	go func() {
		defer close(runtime.done)
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			log.Printf("farm-gate verifier terminal")
		}
	}()
	return runtime, nil
}

func (r *gateVerifierRuntime) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		done := make(chan struct{})
		go func() {
			r.server.GracefulStop()
			close(done)
		}()
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-done:
		case <-timer.C:
			r.server.Stop()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		_ = r.listener.Close()
		<-r.done
	})
}

func requiredRemoteKeyspaceTuple(membership authoritycomposition.MembershipMode, assignment authoritycomposition.AssignmentMode) (*rediskey.KeyspaceTuple, error) {
	if !authoritycomposition.HolderCompositionEnabled(membership, assignment) {
		return nil, nil
	}
	tuple, err := rediskey.ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("farm-gate remote Redis keyspace tuple: %w", err)
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("farm-gate remote Redis keyspace fingerprint: %w", err)
	}
	log.Printf("farm-gate Redis keyspace fingerprint=%s", fingerprint)
	return &tuple, nil
}
