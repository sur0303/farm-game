package netendpoint

import (
	"errors"
	"strings"
	"testing"
)

func TestCanonicalEndpointGrammar(t *testing.T) {
	for _, value := range []string{
		"holder-a.internal:1",
		"holder-a.internal:65535",
		"127.0.0.1:443",
		"[::1]:443",
		"[2001:db8::1]:9443",
	} {
		t.Run("valid_"+value, func(t *testing.T) {
			got, err := Canonical(value, 512)
			if err != nil || got != value {
				t.Fatalf("Canonical(%q)=%q err=%v", value, got, err)
			}
		})
	}

	invalid := []string{
		"not-a-socket", "holder:0", "holder:00", "holder:01", "holder:65536", "holder:+1",
		"Holder:443", " holder:443", "holder:443 ", "holder_name:443", "holder..internal:443",
		"-holder:443", "holder-:443", "holder.internal.:443", "holder/internal:443", "h\u00f6lder:443",
		"127.0.0.01:443", "999.0.0.1:443", "127.0.0.1 :443", "127.0.0.1:0443",
		"::1:443", "[0:0:0:0:0:0:0:1]:443", "[2001:DB8::1]:443", "[fe80::1%eth0]:443",
		"[::ffff:192.0.2.1]:443", "[::1]:443\n", "holder:\uff11",
		strings.Repeat("a", 64) + ".internal:443",
	}
	for _, value := range invalid {
		t.Run("invalid_"+value, func(t *testing.T) {
			if _, err := Canonical(value, 512); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Canonical(%q) err=%v", value, err)
			}
		})
	}
	if _, err := Canonical("holder:443", len("holder:443")-1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("maximum bound err=%v", err)
	}
}
