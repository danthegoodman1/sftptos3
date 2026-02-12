package helpers

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestComputePartSize(t *testing.T) {
	tests := []struct {
		name        string
		size        int64
		expect      int64
		expectError bool
	}{
		{name: "tiny file", size: 1024, expect: MinPartSize},
		{name: "exact minimum", size: MinPartSize, expect: MinPartSize},
		{name: "forces larger part size", size: (MaxPartCount * MinPartSize) + 1, expect: MinPartSize + MiB},
		{name: "max single part over limit", size: MaxPartSize*MaxPartCount + 1, expectError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ComputePartSize(tc.size)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got part size %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expect {
				t.Fatalf("expected part size %d, got %d", tc.expect, got)
			}
		})
	}
}

func TestRelativeKeyFromRoot(t *testing.T) {
	rel, err := RelativeKeyFromRoot("/home/alice", "/home/alice/data/file.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rel != "data/file.txt" {
		t.Fatalf("unexpected relative path: %s", rel)
	}
}

func TestKeyForExplicitSource(t *testing.T) {
	inside := KeyForExplicitSource("/home/alice", "/home/alice/a/b.txt")
	if inside != "a/b.txt" {
		t.Fatalf("expected home-relative key, got %s", inside)
	}

	outside := KeyForExplicitSource("/home/alice", "/opt/shared/b.txt")
	if outside != "opt/shared/b.txt" {
		t.Fatalf("expected absolute-derived key, got %s", outside)
	}
}

func TestJoinS3Key(t *testing.T) {
	key := JoinS3Key("backup", "a/b.txt")
	if key != "backup/a/b.txt" {
		t.Fatalf("unexpected key: %s", key)
	}
}

func TestExpandHome(t *testing.T) {
	got, err := ExpandHome("~/foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(got, filepath.Join(string(filepath.Separator), "foo")) && !strings.HasSuffix(got, "foo") {
		t.Fatalf("expected expanded path, got %s", got)
	}
}
