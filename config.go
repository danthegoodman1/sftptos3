package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"sftptos3/internal/helpers"
)

type Config struct {
	SFTPPrivateKeyFile   string   `yaml:"sftp_private_key_file"`
	SFTPServer           string   `yaml:"sftp_server"`
	SFTPUsername         string   `yaml:"sftp_username"`
	SFTPKnownHostsFile   string   `yaml:"sftp_known_hosts_file"`
	MultipartConcurrency int      `yaml:"multipart_concurrency"`
	SFTPReadBufferBytes  int      `yaml:"sftp_read_buffer_bytes"`
	ResumabilityDir      string   `yaml:"resumability_dir"`
	S3Region             string   `yaml:"s3_region"`
	S3Endpoint           string   `yaml:"s3_endpoint"`
	S3RetryMaxAttempts   int      `yaml:"s3_retry_max_attempts"`
	S3UsePathStyle       bool     `yaml:"s3_use_path_style"`
	S3Bucket             string   `yaml:"s3_bucket"`
	S3AccessKeyID        string   `yaml:"s3_access_key_id"`
	S3SecretAccessKey    string   `yaml:"s3_secret_access_key"`
	S3Prefix             string   `yaml:"s3_prefix"`
	SFTPSources          []string `yaml:"sftp_sources"`
	Overwrite            bool     `yaml:"overwrite"`
}

func LoadOrCreateConfig(configPath string) (Config, bool, error) {
	_, err := os.Stat(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if writeErr := writeTemplateConfig(configPath); writeErr != nil {
				return Config{}, false, fmt.Errorf("write template config: %w", writeErr)
			}
			return Config{}, true, nil
		}
		return Config{}, false, fmt.Errorf("stat config file: %w", err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, false, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(content))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, false, fmt.Errorf("decode yaml: %w", err)
	}

	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return Config{}, false, err
	}

	return cfg, false, nil
}

func (c *Config) Normalize() {
	c.SFTPPrivateKeyFile = strings.TrimSpace(c.SFTPPrivateKeyFile)
	c.SFTPServer = strings.TrimSpace(c.SFTPServer)
	c.SFTPUsername = strings.TrimSpace(c.SFTPUsername)
	c.SFTPKnownHostsFile = strings.TrimSpace(c.SFTPKnownHostsFile)
	if c.MultipartConcurrency == 0 {
		c.MultipartConcurrency = 1
	}
	if c.SFTPReadBufferBytes == 0 {
		c.SFTPReadBufferBytes = 32 * 1024 * 1024
	}
	c.ResumabilityDir = strings.TrimSpace(c.ResumabilityDir)
	if c.ResumabilityDir == "" {
		c.ResumabilityDir = "./resumability"
	}
	c.S3Region = strings.TrimSpace(c.S3Region)
	c.S3Endpoint = strings.TrimSpace(c.S3Endpoint)
	if c.S3RetryMaxAttempts == 0 {
		c.S3RetryMaxAttempts = 20
	}
	c.S3Bucket = strings.TrimSpace(c.S3Bucket)
	c.S3AccessKeyID = strings.TrimSpace(c.S3AccessKeyID)
	c.S3SecretAccessKey = strings.TrimSpace(c.S3SecretAccessKey)
	c.S3Prefix = helpers.NormalizeS3Prefix(c.S3Prefix)

	if c.SFTPKnownHostsFile == "" {
		if p, err := helpers.DefaultKnownHostsPath(); err == nil {
			c.SFTPKnownHostsFile = p
		}
	}
}

func (c Config) Validate() error {
	required := map[string]string{
		"sftp_private_key_file": c.SFTPPrivateKeyFile,
		"sftp_server":           c.SFTPServer,
		"sftp_username":         c.SFTPUsername,
		"sftp_known_hosts_file": c.SFTPKnownHostsFile,
		"s3_region":             c.S3Region,
		"s3_bucket":             c.S3Bucket,
		"s3_access_key_id":      c.S3AccessKeyID,
		"s3_secret_access_key":  c.S3SecretAccessKey,
	}
	for field, value := range required {
		if value == "" {
			return fmt.Errorf("config field %q is required", field)
		}
	}

	if _, _, err := net.SplitHostPort(c.SFTPServer); err != nil {
		return fmt.Errorf("config field %q must be host:port: %w", "sftp_server", err)
	}
	if c.MultipartConcurrency < 1 {
		return fmt.Errorf("config field %q must be >= 1", "multipart_concurrency")
	}
	if c.SFTPReadBufferBytes < 1 {
		return fmt.Errorf("config field %q must be >= 1", "sftp_read_buffer_bytes")
	}
	if c.ResumabilityDir == "" {
		return fmt.Errorf("config field %q must not be empty", "resumability_dir")
	}
	if c.S3RetryMaxAttempts < 1 {
		return fmt.Errorf("config field %q must be >= 1", "s3_retry_max_attempts")
	}

	return nil
}

func writeTemplateConfig(configPath string) error {
	dir := filepath.Dir(configPath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create config directory: %w", err)
		}
	}

	defaultKnownHosts := "~/.ssh/known_hosts"
	if p, err := helpers.DefaultKnownHostsPath(); err == nil && p != "" {
		defaultKnownHosts = p
	}

	template := fmt.Sprintf(`sftp_private_key_file: ""
sftp_server: ""              # host:port
sftp_username: ""
# Optional (defaults to ~/.ssh/known_hosts)
sftp_known_hosts_file: "%s"
multipart_concurrency: 1     # parallel multipart parts per file
sftp_read_buffer_bytes: 33554432 # optional; 32 MiB
resumability_dir: "./resumability" # optional; per-file multipart state

s3_region: ""                # required; e.g. us-east-1
s3_endpoint: ""              # optional; set for S3-compatible APIs
s3_retry_max_attempts: 20    # optional; per-request attempts (includes first try)
s3_use_path_style: false     # optional; enable path-style addressing
s3_bucket: ""
s3_access_key_id: ""
s3_secret_access_key: ""
s3_prefix: ""                # optional

sftp_sources: []              # optional exact file paths
overwrite: false              # optional; skip existing objects by default
`, defaultKnownHosts)

	if err := os.WriteFile(configPath, []byte(template), 0o600); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	return nil
}
