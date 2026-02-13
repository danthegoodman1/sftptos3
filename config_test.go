package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadOrCreateConfigCreatesTemplate(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")

	_, created, err := LoadOrCreateConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error creating template: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true")
	}

	content, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("expected template config file: %v", err)
	}
	if len(content) == 0 {
		t.Fatalf("expected non-empty template config")
	}
	if !strings.Contains(string(content), "multipart_concurrency: 1") {
		t.Fatalf("expected template to include multipart_concurrency option")
	}
	if !strings.Contains(string(content), "sftp_read_buffer_bytes: 33554432") {
		t.Fatalf("expected template to include sftp_read_buffer_bytes option")
	}
	if !strings.Contains(string(content), "resumability_dir: \"./resumability\"") {
		t.Fatalf("expected template to include resumability_dir option")
	}
	if !strings.Contains(string(content), "# Optional (defaults to ~/.ssh/known_hosts)") {
		t.Fatalf("expected template to include known_hosts optional comment")
	}
	if !strings.Contains(string(content), "sftp_known_hosts_file: \"") {
		t.Fatalf("expected template to include sftp_known_hosts_file value")
	}
	if !strings.Contains(string(content), "s3_use_path_style: false") {
		t.Fatalf("expected template to include s3_use_path_style option")
	}
	if !strings.Contains(string(content), "s3_retry_max_attempts: 20") {
		t.Fatalf("expected template to include s3_retry_max_attempts option")
	}
}

func TestConfigNormalizeDefaultsKnownHosts(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.SFTPKnownHostsFile == "" {
		t.Fatalf("expected known_hosts default path to be set")
	}
}

func TestConfigValidateRequiresRegion(t *testing.T) {
	cfg := Config{
		SFTPPrivateKeyFile:   "/tmp/key",
		SFTPServer:           "example.com:22",
		SFTPUsername:         "user",
		SFTPKnownHostsFile:   "/tmp/known_hosts",
		MultipartConcurrency: 1,
		S3Bucket:             "bucket",
		S3AccessKeyID:        "abc",
		S3SecretAccessKey:    "def",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure for missing s3_region")
	}
}

func TestConfigOverwriteDefaultsFalse(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.Overwrite {
		t.Fatalf("expected overwrite default false")
	}
}

func TestConfigMultipartConcurrencyDefaultsOne(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.MultipartConcurrency != 1 {
		t.Fatalf("expected multipart_concurrency default 1, got %d", cfg.MultipartConcurrency)
	}
}

func TestConfigSFTPReadBufferDefaults(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.SFTPReadBufferBytes != 32*1024*1024 {
		t.Fatalf("expected sftp_read_buffer_bytes default 33554432, got %d", cfg.SFTPReadBufferBytes)
	}
}

func TestConfigResumabilityDirDefaults(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.ResumabilityDir != "./resumability" {
		t.Fatalf("expected resumability_dir default ./resumability, got %q", cfg.ResumabilityDir)
	}
}

func TestConfigS3RetryMaxAttemptsDefaults(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()
	if cfg.S3RetryMaxAttempts != 20 {
		t.Fatalf("expected s3_retry_max_attempts default 20, got %d", cfg.S3RetryMaxAttempts)
	}
}

func TestConfigValidateMultipartConcurrencyMinimum(t *testing.T) {
	cfg := Config{
		SFTPPrivateKeyFile:   "/tmp/key",
		SFTPServer:           "example.com:22",
		SFTPUsername:         "user",
		SFTPKnownHostsFile:   "/tmp/known_hosts",
		MultipartConcurrency: -1,
		S3Region:             "us-east-1",
		S3Bucket:             "bucket",
		S3AccessKeyID:        "abc",
		S3SecretAccessKey:    "def",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure for multipart_concurrency < 1")
	}
}

func TestConfigValidateSFTPReadBufferMinimum(t *testing.T) {
	cfg := Config{
		SFTPPrivateKeyFile:   "/tmp/key",
		SFTPServer:           "example.com:22",
		SFTPUsername:         "user",
		SFTPKnownHostsFile:   "/tmp/known_hosts",
		MultipartConcurrency: 1,
		SFTPReadBufferBytes:  0,
		S3Region:             "us-east-1",
		S3Bucket:             "bucket",
		S3AccessKeyID:        "abc",
		S3SecretAccessKey:    "def",
	}
	cfg.Normalize()
	cfg.SFTPReadBufferBytes = -1
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure for sftp_read_buffer_bytes < 1")
	}
}

func TestConfigValidateResumabilityDirNotEmpty(t *testing.T) {
	cfg := Config{
		SFTPPrivateKeyFile:   "/tmp/key",
		SFTPServer:           "example.com:22",
		SFTPUsername:         "user",
		SFTPKnownHostsFile:   "/tmp/known_hosts",
		MultipartConcurrency: 1,
		SFTPReadBufferBytes:  1,
		ResumabilityDir:      "",
		S3Region:             "us-east-1",
		S3Bucket:             "bucket",
		S3AccessKeyID:        "abc",
		S3SecretAccessKey:    "def",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure for empty resumability_dir")
	}
}

func TestConfigValidateS3RetryMaxAttemptsMinimum(t *testing.T) {
	cfg := Config{
		SFTPPrivateKeyFile:   "/tmp/key",
		SFTPServer:           "example.com:22",
		SFTPUsername:         "user",
		SFTPKnownHostsFile:   "/tmp/known_hosts",
		MultipartConcurrency: 1,
		SFTPReadBufferBytes:  1,
		ResumabilityDir:      "./resumability",
		S3RetryMaxAttempts:   -1,
		S3Region:             "us-east-1",
		S3Bucket:             "bucket",
		S3AccessKeyID:        "abc",
		S3SecretAccessKey:    "def",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure for s3_retry_max_attempts < 1")
	}
}
