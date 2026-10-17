//go:build !linux

package routecontrol

import "fmt"

func openSecureRoot(string) (secureRoot, error) {
	return nil, fmt.Errorf("%w: platform has no rename-stable filesystem root capability", ErrDurableEvidenceUnavailable)
}

func createRunDirectory(string) (secureRunDir, error) {
	return nil, fmt.Errorf("%w: platform has no rename-stable filesystem root capability", ErrDurableEvidenceUnavailable)
}
