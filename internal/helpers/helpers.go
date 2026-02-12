package helpers

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	MiB          int64 = 1024 * 1024
	MinPartSize  int64 = 5 * MiB
	MaxPartSize  int64 = 5 * 1024 * MiB
	MaxPartCount int64 = 10_000
)

func ComputePartSize(fileSize int64) (int64, error) {
	if fileSize < 0 {
		return 0, fmt.Errorf("file size cannot be negative")
	}
	if fileSize == 0 {
		return MinPartSize, nil
	}

	required := ceilDiv(fileSize, MaxPartCount)
	if required < MinPartSize {
		required = MinPartSize
	}

	partSize := roundUp(required, MiB)
	if partSize > MaxPartSize {
		return 0, fmt.Errorf("required part size %d exceeds max allowed %d", partSize, MaxPartSize)
	}
	return partSize, nil
}

func NormalizeS3Prefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "" || prefix == "." {
		return ""
	}
	return prefix
}

func JoinS3Key(prefix, key string) string {
	prefix = NormalizeS3Prefix(prefix)
	key = normalizeKeyComponent(key)
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + "/" + key
}

func RelativeKeyFromRoot(root, target string) (string, error) {
	root = path.Clean(root)
	target = path.Clean(target)

	if target == root {
		return "", fmt.Errorf("target %q resolves to root directory", target)
	}
	if root == "/" {
		return normalizeKeyComponent(strings.TrimPrefix(target, "/")), nil
	}

	prefix := root + "/"
	if !strings.HasPrefix(target, prefix) {
		return "", fmt.Errorf("target %q is outside root %q", target, root)
	}

	rel := strings.TrimPrefix(target, prefix)
	return normalizeKeyComponent(rel), nil
}

func KeyForExplicitSource(homeDir, remotePath string) string {
	rel, err := RelativeKeyFromRoot(homeDir, remotePath)
	if err == nil {
		return rel
	}
	return normalizeKeyComponent(strings.TrimLeft(path.Clean(remotePath), "/"))
}

func DefaultKnownHostsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".ssh", "known_hosts"), nil
}

func ExpandHome(filePath string) (string, error) {
	if filePath == "" {
		return "", nil
	}
	if filePath == "~" {
		return os.UserHomeDir()
	}
	if !strings.HasPrefix(filePath, "~/") {
		return filePath, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, strings.TrimPrefix(filePath, "~/")), nil
}

func ceilDiv(a, b int64) int64 {
	return int64(math.Ceil(float64(a) / float64(b)))
}

func roundUp(value, unit int64) int64 {
	if value%unit == 0 {
		return value
	}
	return ((value / unit) + 1) * unit
}

func normalizeKeyComponent(value string) string {
	clean := path.Clean(strings.ReplaceAll(value, "\\", "/"))
	clean = strings.TrimLeft(clean, "/")
	if clean == "." {
		return ""
	}
	return clean
}
