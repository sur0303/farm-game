package dynamicconfig

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileValueReloadsValidChangesAndFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mode")
	if err := os.WriteFile(path, []byte("full\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	applied := make([]string, 0, 2)
	value, err := NewFileValue(path, time.Second, func(next string) error {
		if next == "invalid" {
			return errors.New("invalid mode")
		}
		applied = append(applied, next)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !value.Healthy() || value.Current() != "full" {
		t.Fatalf("initial value=%q healthy=%t", value.Current(), value.Healthy())
	}
	if err := os.WriteFile(path, []byte("narrow-shadow\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := value.Reload(); err != nil {
		t.Fatal(err)
	}
	if !value.Healthy() || value.Current() != "narrow-shadow" {
		t.Fatalf("reloaded value=%q healthy=%t", value.Current(), value.Healthy())
	}
	if err := os.WriteFile(path, []byte("invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := value.Reload(); !errors.Is(err, ErrFileValueInvalid) {
		t.Fatalf("invalid reload err=%v", err)
	}
	if value.Healthy() || value.Current() != "narrow-shadow" {
		t.Fatalf("failed value=%q healthy=%t", value.Current(), value.Healthy())
	}
	if len(applied) != 2 {
		t.Fatalf("applied=%v", applied)
	}
}
