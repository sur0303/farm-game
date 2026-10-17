//go:build !linux

package routecontrol

import (
	"context"
	"fmt"
	"os/exec"
)

type processGroupCapability interface {
	interrupt() error
	kill() error
	cleanup(context.Context) error
	close() error
	ownsCapability() bool
}

type processGroupStarter interface {
	configure(*exec.Cmd)
	afterStart(*exec.Cmd) (processGroupCapability, error)
	cleanupAfterStartFailure(*exec.Cmd) error
}

type unsupportedProcessGroupStarter struct{}

func newProcessGroupStarter() (processGroupStarter, error) {
	return nil, fmt.Errorf("%w: %w", ErrProcessGroupUnavailable, ErrCapability)
}

func (unsupportedProcessGroupStarter) configure(*exec.Cmd) {}

func (unsupportedProcessGroupStarter) afterStart(*exec.Cmd) (processGroupCapability, error) {
	return nil, fmt.Errorf("%w: %w", ErrProcessGroupUnavailable, ErrCapability)
}

func (unsupportedProcessGroupStarter) cleanupAfterStartFailure(*exec.Cmd) error {
	return nil
}
