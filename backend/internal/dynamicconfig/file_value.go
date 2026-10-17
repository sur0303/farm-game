// Package dynamicconfig provides bounded process-local views of projected
// configuration files. It never writes configuration or watches remote APIs.
package dynamicconfig

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var ErrFileValueInvalid = errors.New("dynamicconfig: invalid file value")

type ApplyValueFunc func(string) error

type FileValue struct {
	path     string
	interval time.Duration
	apply    ApplyValueFunc

	current atomic.Value
	healthy atomic.Bool
}

func NewFileValue(
	path string,
	interval time.Duration,
	apply ApplyValueFunc,
) (*FileValue, error) {
	if strings.TrimSpace(path) == "" || interval < 100*time.Millisecond ||
		interval > time.Minute || apply == nil {
		return nil, ErrFileValueInvalid
	}
	value := &FileValue{
		path:     path,
		interval: interval,
		apply:    apply,
	}
	value.current.Store("")
	if err := value.Reload(); err != nil {
		return nil, err
	}
	return value, nil
}

func (v *FileValue) Reload() error {
	if v == nil || v.apply == nil {
		return ErrFileValueInvalid
	}
	raw, err := os.ReadFile(v.path)
	if err != nil {
		v.healthy.Store(false)
		return errors.Join(ErrFileValueInvalid, err)
	}
	next := strings.TrimSpace(string(raw))
	if next == "" {
		v.healthy.Store(false)
		return ErrFileValueInvalid
	}
	if next == v.Current() {
		v.healthy.Store(true)
		return nil
	}
	if err := v.apply(next); err != nil {
		v.healthy.Store(false)
		return errors.Join(ErrFileValueInvalid, err)
	}
	v.current.Store(next)
	v.healthy.Store(true)
	return nil
}

func (v *FileValue) Run(ctx context.Context, observeError func(error)) error {
	if v == nil || ctx == nil {
		return ErrFileValueInvalid
	}
	ticker := time.NewTicker(v.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := v.Reload(); err != nil && observeError != nil {
				observeError(err)
			}
		}
	}
}

func (v *FileValue) Current() string {
	if v == nil {
		return ""
	}
	current, _ := v.current.Load().(string)
	return current
}

func (v *FileValue) Healthy() bool {
	return v != nil && v.healthy.Load()
}
