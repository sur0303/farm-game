package gatedriver

import (
	"bufio"
	"io"
	"net"
	"os"
	"strings"
)

// routeTablePath is the kernel IPv4 route table. A test points this elsewhere.
var routeTablePath = "/proc/net/route"

// sameHostNetworks reports the networks an arrival can reach without passing
// through a gateway, excluding whichever interface carries the default route.
// A container bridge qualifies: its peers live in this machine's namespaces. A
// physical uplink does not, even for its own subnet, because the neighbours
// there are other machines.
//
// The set is empty when the route table cannot be read, which leaves loopback
// as the only endpoint the driver will accept.
func sameHostNetworks() ([]*net.IPNet, error) {
	file, err := os.Open(routeTablePath)
	if err != nil {
		return nil, nil
	}
	defer file.Close()
	uplinks := defaultRouteInterfaces(file)

	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	networks := []*net.IPNet{}
	for _, item := range interfaces {
		if item.Flags&net.FlagUp == 0 || uplinks[item.Name] {
			continue
		}
		addresses, err := item.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			if network, ok := address.(*net.IPNet); ok {
				networks = append(networks, network)
			}
		}
	}
	return networks, nil
}

// defaultRouteInterfaces names the interfaces holding a route to everywhere,
// which is exactly the path that leaves this machine.
func defaultRouteInterfaces(table io.Reader) map[string]bool {
	names := map[string]bool{}
	scanner := bufio.NewScanner(table)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 8 {
			continue
		}
		if fields[1] == "00000000" && fields[7] == "00000000" {
			names[fields[0]] = true
		}
	}
	return names
}
