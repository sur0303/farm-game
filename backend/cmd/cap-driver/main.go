package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"farmgame/internal/testkit/capdriver"
	"farmgame/internal/testkit/gateclient"
	"farmgame/internal/testkit/gatedriver"
)

// adapterDesignBlocker keeps an unreviewed service fail-closed; every other
// adapter must be named explicitly by a reviewed config.
const (
	adapterDesignBlocker = "design_blocker"
	adapterGateSession   = "gate_session_v1"
)

type fileConfig struct {
	SchemaVersion   string            `json:"schema_version"`
	Listen          string            `json:"listen"`
	Service         capdriver.Service `json:"service"`
	Profile         string            `json:"profile"`
	MaximumInflight int               `json:"maximum_inflight"`
	Adapter         string            `json:"adapter"`
	// Gate carries the loopback deployment the gate_session_v1 adapter drives.
	Gate *gateConfig `json:"gate,omitempty"`
}

type gateConfig struct {
	HTTPURL string `json:"http_url"`
	WSURL   string `json:"ws_url"`
	TLSCA   string `json:"tls_ca,omitempty"`
	// LoadSecretEnv names the environment variable holding the load-test
	// signing secret, so no reviewed config file ever contains it.
	LoadSecretEnv string `json:"load_secret_env"`
	PlayerBase    uint64 `json:"player_base"`
	RunID         string `json:"run_id"`
}

func main() {
	configPath := flag.String("config", "", "path to reviewed cap-driver config")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("--config is required")
	}
	if err := run(*configPath); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string) error {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(configData))
	decoder.DisallowUnknownFields()
	var file fileConfig
	if err := decoder.Decode(&file); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	if file.SchemaVersion != "cap-l2-driver-config.v1" {
		return errors.New("unsupported cap-driver config schema")
	}
	if err := validateLoopback(file.Listen); err != nil {
		return err
	}
	adapter, err := buildAdapter(file)
	if err != nil {
		return err
	}
	binarySHA, err := executableSHA256()
	if err != nil {
		return err
	}
	blocker := "DESIGN_BLOCKER"
	if adapter != nil {
		blocker = ""
	}
	configDigest := sha256.Sum256(configData)
	server, err := capdriver.NewServer(capdriver.Config{
		BinarySHA256: binarySHA, ConfigSHA256: hex.EncodeToString(configDigest[:]), Service: file.Service,
		Profile: file.Profile, MaximumInflight: file.MaximumInflight,
		Ready: adapter != nil, Blocker: blocker,
	}, adapter)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr: file.Listen, Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	listener, err := net.Listen("tcp", file.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	state := "blocked"
	if adapter != nil {
		state = "ready adapter=" + file.Adapter
	}
	log.Printf("cap-driver %s service=%s profile=%s listen=%s", state, file.Service, file.Profile, file.Listen)
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.Serve(listener) }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdown)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// buildAdapter returns nil for a service that is still fail-closed. Any
// adapter other than the blocker must be named by the config and must supply
// the deployment it drives.
func buildAdapter(file fileConfig) (capdriver.Adapter, error) {
	switch file.Adapter {
	case adapterDesignBlocker:
		if file.Gate != nil {
			return nil, errors.New("the design_blocker adapter must not carry a gate deployment")
		}
		return nil, nil
	case adapterGateSession:
		if file.Service != capdriver.ServiceGate {
			return nil, errors.New("the gate_session_v1 adapter only serves the gate service")
		}
		if file.Gate == nil {
			return nil, errors.New("the gate_session_v1 adapter requires a gate deployment")
		}
		secret, err := gatedriver.LoadSecretFromEnv(file.Gate.LoadSecretEnv)
		if err != nil {
			return nil, err
		}
		return gatedriver.New(gatedriver.Config{
			Gate: gateclient.Config{
				HTTPBaseURL:     file.Gate.HTTPURL,
				WSURL:           file.Gate.WSURL,
				TLSCAPath:       file.Gate.TLSCA,
				LoadSecret:      secret,
				ClientBuild:     "cap-driver/1",
				BootstrapPrefix: "cap-driver",
				RunID:           file.Gate.RunID,
			},
			PlayerBase: file.Gate.PlayerBase,
		})
	default:
		return nil, errors.New("adapter is not implemented; legal service adapters remain required")
	}
}

func executableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read executable: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validateLoopback(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen must be host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("cap-driver must listen on an explicit loopback address")
	}
	return nil
}
