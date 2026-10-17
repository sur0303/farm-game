package routecontrol

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// secureRoot and secureRunDir are implemented per OS. Linux is the only target
// with the descriptor-relative, rename-stable capability required here.
// Other targets fail closed instead of falling back to string-based checks.
type secureRoot interface {
	path() string
	openRegular(string) (*os.File, error)
	createManifest(string, []byte) error
	close() error
}

type secureRunDir interface {
	path() string
	createFile(string) (*os.File, error)
	createManifest(string, []byte) error
	cleanup([]string) error
	close() error
}

func relativePathWithinRoot(root, path string) (string, error) {
	if strings.TrimSpace(root) == "" || strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%w: root and path are required", ErrUnsafePath)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("%w: resolve root: %v", ErrUnsafePath, err)
	}

	var relative string
	if filepath.IsAbs(path) {
		targetAbs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("%w: resolve path: %v", ErrUnsafePath, err)
		}
		relative, err = filepath.Rel(rootAbs, targetAbs)
		if err != nil {
			return "", fmt.Errorf("%w: relate path to root: %v", ErrUnsafePath, err)
		}
	} else {
		relative = filepath.Clean(filepath.FromSlash(path))
	}
	if err := validateRelativePath(relative); err != nil {
		return "", err
	}
	return filepath.ToSlash(relative), nil
}

func validateRelativePath(value string) error {
	if strings.TrimSpace(value) == "" || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: relative path is required and must be NUL-free", ErrUnsafePath)
	}
	clean := filepath.Clean(filepath.FromSlash(value))
	if filepath.IsAbs(clean) || clean == "." || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: path must stay below the root", ErrUnsafePath)
	}
	return nil
}
