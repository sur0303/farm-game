// Package netendpoint owns the canonical socket-address grammar shared by
// holder registration, route plans, endpoint pools, and direct Gate peers.
package netendpoint

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

var ErrInvalid = errors.New("netendpoint: invalid canonical host:port")

// Canonical accepts only an already-canonical host:port value. Hosts are
// lowercase ASCII DNS labels or exact unzoned netip literals. Ports are
// decimal 1..65535 without a leading zero. The returned value is byte-for-byte
// equal to input; it exists so callers can deduplicate the validated form.
func Canonical(value string, maximumBytes int) (string, error) {
	if maximumBytes <= 0 || value == "" || len(value) > maximumBytes || strings.TrimSpace(value) != value {
		return "", ErrInvalid
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" || !canonicalPort(port) {
		return "", ErrInvalid
	}
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		if address.Zone() != "" || address.Is4In6() || address.String() != host || net.JoinHostPort(host, port) != value {
			return "", ErrInvalid
		}
		return value, nil
	}
	if strings.Contains(host, ":") || looksLikeIPv4(host) || !canonicalASCIIHostname(host) || net.JoinHostPort(host, port) != value {
		return "", ErrInvalid
	}
	return value, nil
}

func Validate(value string, maximumBytes int) error {
	_, err := Canonical(value, maximumBytes)
	return err
}

func canonicalPort(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port >= 1 && strconv.FormatUint(port, 10) == value
}

func canonicalASCIIHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 || strings.ToLower(host) != host || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := 0; index < len(label); index++ {
			value := label[index]
			if (value < 'a' || value > 'z') && (value < '0' || value > '9') && value != '-' {
				return false
			}
		}
	}
	return true
}

// A dotted all-numeric value is an IP literal candidate, never a DNS alias.
// This prevents malformed/alternate IPv4 text from entering as a hostname.
func looksLikeIPv4(host string) bool {
	if !strings.Contains(host, ".") {
		return false
	}
	for index := 0; index < len(host); index++ {
		if (host[index] < '0' || host[index] > '9') && host[index] != '.' {
			return false
		}
	}
	return true
}
