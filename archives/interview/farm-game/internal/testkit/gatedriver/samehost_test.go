package gatedriver

import (
	"net"
	"strings"
	"testing"
)

func mustNetwork(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("parse %s: %v", cidr, err)
	}
	return network
}

func TestAContainerBridgeAddressIsThisMachine(t *testing.T) {
	attached := []*net.IPNet{mustNetwork(t, "198.18.0.201/24")}
	for _, endpoint := range []string{"http://198.18.0.202:8080", "ws://198.18.0.202:8080/ws"} {
		if err := requireSameHost(endpoint, attached); err != nil {
			t.Fatalf("a pod on this host's bridge was rejected: %v", err)
		}
	}
}

func TestLoopbackNeedsNoAttachedNetwork(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080"} {
		if err := requireSameHost(endpoint, nil); err != nil {
			t.Fatalf("%s was rejected: %v", endpoint, err)
		}
	}
}

func TestAnAddressBeyondThisMachineIsRefused(t *testing.T) {
	attached := []*net.IPNet{mustNetwork(t, "198.18.0.201/24")}
	for _, endpoint := range []string{"http://198.18.0.203:8080", "http://198.18.0.204:80", "http://gate.example.com:8080"} {
		if err := requireSameHost(endpoint, attached); err == nil {
			t.Fatalf("%s was accepted as this host", endpoint)
		}
	}
}

// The uplink is excluded even though its own subnet needs no gateway, because
// the neighbours reachable there are other machines.
func TestTheDefaultRouteInterfaceIsNotThisMachine(t *testing.T) {
	table := strings.Join([]string{
		"Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT",
		"eth0\t00000000\t0100A8C0\t0003\t0\t0\t0\t00000000\t0\t0\t0",
		"eth0\t0000A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0",
		"cni0\t00002A0A\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0",
	}, "\n")
	uplinks := defaultRouteInterfaces(strings.NewReader(table))
	if !uplinks["eth0"] {
		t.Fatal("the interface carrying the default route was not recognised as the uplink")
	}
	if uplinks["cni0"] {
		t.Fatal("a bridge with only an on-link route was treated as the uplink")
	}
}

func TestAnUnreadableRouteTableLeavesOnlyLoopback(t *testing.T) {
	original := routeTablePath
	routeTablePath = t.TempDir() + "/absent"
	defer func() { routeTablePath = original }()

	networks, err := sameHostNetworks()
	if err != nil {
		t.Fatalf("an absent route table should not fail the driver: %v", err)
	}
	if len(networks) != 0 {
		t.Fatalf("without a route table nothing but loopback may qualify, got %d networks", len(networks))
	}
	if err := requireSameHost("http://198.18.0.202:8080", networks); err == nil {
		t.Fatal("a bridge address was accepted with no evidence that it is local")
	}
}
