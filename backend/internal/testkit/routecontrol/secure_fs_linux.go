//go:build linux

package routecontrol

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const linuxSecureResolve = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV

type linuxSecureRoot struct {
	rootPath string
	dir      *os.File
}

type linuxSecureRunDir struct {
	runPath string
	parent  *os.File
	dir     *os.File
	name    string
}

func openSecureRoot(path string) (secureRoot, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve artifact root: %v", ErrUnsafePath, err)
	}
	fd, err := unix.Openat2(unix.AT_FDCWD, abs, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC),
		Resolve: unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: open artifact root: %v", ErrUnsafePath, err)
	}
	root := &linuxSecureRoot{rootPath: abs, dir: os.NewFile(uintptr(fd), abs)}
	if root.dir == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: open artifact root returned nil descriptor", ErrUnsafePath)
	}
	info, err := root.dir.Stat()
	if err != nil {
		_ = root.close()
		return nil, fmt.Errorf("%w: fstat artifact root: %v", ErrUnsafePath, err)
	}
	if !info.IsDir() {
		_ = root.close()
		return nil, fmt.Errorf("%w: artifact root is not a directory", ErrUnsafePath)
	}
	return root, nil
}

func (r *linuxSecureRoot) path() string {
	return r.rootPath
}

func (r *linuxSecureRoot) close() error {
	if r == nil || r.dir == nil {
		return nil
	}
	file := r.dir
	r.dir = nil
	return file.Close()
}

func (r *linuxSecureRoot) openRegular(relative string) (*os.File, error) {
	if err := validateRelativePath(relative); err != nil {
		return nil, err
	}
	fd, err := unix.Openat2(int(r.dir.Fd()), filepath.FromSlash(relative), &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_NONBLOCK | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: linuxSecureResolve,
	})
	if err != nil {
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return nil, fmt.Errorf("%w: artifact path resolution: %v", ErrUnsafePath, err)
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(r.rootPath, filepath.FromSlash(relative)))
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: open artifact returned nil descriptor", ErrUnsafePath)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%w: fstat opened artifact: %v", ErrArtifactMismatch, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("%w: opened artifact is not regular", ErrArtifactMismatch)
	}
	return file, nil
}

func (r *linuxSecureRoot) createManifest(relative string, data []byte) error {
	if err := validateRelativePath(relative); err != nil {
		return err
	}
	parent, name, err := r.openParent(relative)
	if err != nil {
		return err
	}
	defer parent.Close()
	return createLinuxManifestAt(parent, name, data)
}

func createLinuxManifestAt(parent *os.File, name string, data []byte) error {
	tempName, temp, err := createLinuxTemp(parent)
	if err != nil {
		return fmt.Errorf("create manifest temp file: %w", err)
	}
	tempRemoved := false
	cleanup := func() {
		if temp != nil {
			_ = temp.Close()
			temp = nil
		}
		if !tempRemoved {
			_ = unix.Unlinkat(int(parent.Fd()), tempName, 0)
		}
	}
	defer cleanup()

	written, err := temp.Write(data)
	if err != nil {
		return fmt.Errorf("write manifest temp file: %w", err)
	}
	if written != len(data) {
		return fmt.Errorf("write manifest temp file: %w (%d/%d bytes)", io.ErrShortWrite, written, len(data))
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync manifest temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		temp = nil
		return fmt.Errorf("close manifest temp file: %w", err)
	}
	temp = nil

	if err := unix.Linkat(int(parent.Fd()), tempName, int(parent.Fd()), name, 0); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return ErrRunAlreadyExists
		}
		return fmt.Errorf("install manifest: %w", err)
	}
	if err := unix.Unlinkat(int(parent.Fd()), tempName, 0); err != nil {
		return fmt.Errorf("remove manifest temp file: %w", err)
	}
	tempRemoved = true
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync manifest directory: %w", err)
	}
	return nil
}

func (r *linuxSecureRoot) openParent(relative string) (*os.File, string, error) {
	clean := filepath.Clean(filepath.FromSlash(relative))
	parentPath := filepath.ToSlash(filepath.Dir(clean))
	name := filepath.Base(clean)
	parent, err := r.openDirectory(parentPath)
	if err != nil {
		return nil, "", err
	}
	return parent, name, nil
}

func (r *linuxSecureRoot) openDirectory(relative string) (*os.File, error) {
	if relative != "." {
		if err := validateRelativePath(relative); err != nil {
			return nil, err
		}
	}
	fd, err := unix.Openat2(int(r.dir.Fd()), filepath.FromSlash(relative), &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: linuxSecureResolve,
	})
	if err != nil {
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return nil, fmt.Errorf("%w: directory path resolution: %v", ErrUnsafePath, err)
		}
		return nil, fmt.Errorf("%w: open manifest directory: %v", ErrUnsafePath, err)
	}
	dir := os.NewFile(uintptr(fd), filepath.Join(r.rootPath, filepath.FromSlash(relative)))
	if dir == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: open manifest directory returned nil descriptor", ErrUnsafePath)
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("%w: fstat manifest directory: %v", ErrUnsafePath, err)
	}
	if !info.IsDir() {
		_ = dir.Close()
		return nil, fmt.Errorf("%w: manifest parent is not a directory", ErrUnsafePath)
	}
	return dir, nil
}

func createRunDirectory(path string) (secureRunDir, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve run directory: %v", ErrUnsafePath, err)
	}
	parentPath := filepath.Dir(abs)
	name := filepath.Base(abs)
	parent, err := openLinuxDirectoryPath(parentPath)
	if err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err != nil {
		_ = parent.Close()
		if errors.Is(err, unix.EEXIST) {
			return nil, ErrRunAlreadyExists
		}
		return nil, fmt.Errorf("create run directory: %w", err)
	}
	fd, err := unix.Openat2(int(parent.Fd()), name, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: linuxSecureResolve,
	})
	if err != nil {
		_ = parent.Close()
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return nil, fmt.Errorf("%w: run directory path resolution: %v", ErrUnsafePath, err)
		}
		return nil, fmt.Errorf("%w: open run directory: %v", ErrUnsafePath, err)
	}
	dir := os.NewFile(uintptr(fd), abs)
	if dir == nil {
		_ = unix.Close(fd)
		_ = parent.Close()
		return nil, fmt.Errorf("%w: run directory returned nil descriptor", ErrUnsafePath)
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		_ = parent.Close()
		return nil, fmt.Errorf("%w: fstat run directory: %v", ErrUnsafePath, err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		_ = dir.Close()
		_ = parent.Close()
		return nil, fmt.Errorf("%w: run directory is not private", ErrUnsafePath)
	}
	return &linuxSecureRunDir{runPath: abs, parent: parent, dir: dir, name: name}, nil
}

func openLinuxDirectoryPath(path string) (*os.File, error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return nil, fmt.Errorf("%w: directory path resolution: %v", ErrUnsafePath, err)
		}
		return nil, fmt.Errorf("%w: open directory: %v", ErrUnsafePath, err)
	}
	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: directory returned nil descriptor", ErrUnsafePath)
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("%w: fstat directory: %v", ErrUnsafePath, err)
	}
	if !info.IsDir() {
		_ = dir.Close()
		return nil, fmt.Errorf("%w: path is not a directory", ErrUnsafePath)
	}
	return dir, nil
}

func (r *linuxSecureRunDir) path() string {
	return r.runPath
}

func (r *linuxSecureRunDir) createFile(name string) (*os.File, error) {
	if name == "" || filepath.Base(name) != name || strings.ContainsRune(name, 0) {
		return nil, fmt.Errorf("%w: invalid run file name", ErrUnsafePath)
	}
	fd, err := unix.Openat2(int(r.dir.Fd()), name, &unix.OpenHow{
		Flags:   uint64(unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:    0o600,
		Resolve: linuxSecureResolve,
	})
	if err != nil {
		if errors.Is(err, unix.EEXIST) {
			return nil, ErrRunAlreadyExists
		}
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return nil, fmt.Errorf("%w: run file path resolution: %v", ErrUnsafePath, err)
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(r.runPath, name))
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: run file returned nil descriptor", ErrUnsafePath)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%w: fstat run file: %v", ErrUnsafePath, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("%w: run file is not regular", ErrUnsafePath)
	}
	return file, nil
}

func (r *linuxSecureRunDir) createManifest(relative string, data []byte) error {
	if r == nil || r.dir == nil {
		return errors.New("run directory capability is closed")
	}
	if err := validateRelativePath(relative); err != nil {
		return err
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if filepath.Dir(clean) != "." {
		return fmt.Errorf("%w: run manifest must be a direct child", ErrUnsafePath)
	}
	return createLinuxManifestAt(r.dir, filepath.Base(clean), data)
}

func (r *linuxSecureRunDir) cleanup(names []string) error {
	var joined error
	for _, name := range names {
		if err := unix.Unlinkat(int(r.dir.Fd()), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			joined = errors.Join(joined, err)
		}
	}
	// Linux has no unlink/rmdir operation that consumes an already-held
	// directory fd. A parent/name removal would be vulnerable to a
	// rename-and-recreate between identity validation and unlink, so an empty
	// run directory is deliberately retained as a controlled abandoned run.
	// Callers can retry with a fresh run path; deleting this path is left to an
	// operator that can verify its inode out of band.
	if err := r.close(); err != nil {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (r *linuxSecureRunDir) close() error {
	var joined error
	if r.dir != nil {
		dir := r.dir
		r.dir = nil
		if err := dir.Close(); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if r.parent != nil {
		parent := r.parent
		r.parent = nil
		if err := parent.Close(); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func createLinuxTemp(parent *os.File) (string, *os.File, error) {
	var random [12]byte
	for attempt := 0; attempt < 32; attempt++ {
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, err
		}
		name := ".run-manifest.tmp-" + hex.EncodeToString(random[:])
		fd, err := unix.Openat2(int(parent.Fd()), name, &unix.OpenHow{
			Flags:   uint64(unix.O_WRONLY | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
			Mode:    0o600,
			Resolve: linuxSecureResolve,
		})
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		file := os.NewFile(uintptr(fd), name)
		if file == nil {
			_ = unix.Close(fd)
			_ = unix.Unlinkat(int(parent.Fd()), name, 0)
			return "", nil, fmt.Errorf("%w: temp manifest returned nil descriptor", ErrUnsafePath)
		}
		return name, file, nil
	}
	return "", nil, errors.New("could not allocate a private manifest temp name")
}
