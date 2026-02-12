package main

import (
	"fmt"
	"os"
	"path"
	"sort"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"sftptos3/internal/helpers"
)

type SFTPConnection struct {
	SSHClient *ssh.Client
	Client    *sftp.Client
	HomeDir   string
}

type TransferFile struct {
	RemotePath string
	S3Key      string
	Size       int64
}

func ConnectSFTP(cfg Config) (*SFTPConnection, error) {
	keyPath, err := helpers.ExpandHome(cfg.SFTPPrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("expand private key path: %w", err)
	}
	privateKey, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read private key file: %w", err)
	}

	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	knownHostsPath, err := helpers.ExpandHome(cfg.SFTPKnownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("expand known_hosts path: %w", err)
	}
	hostKeyCallback, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return nil, fmt.Errorf("load known_hosts file %q: %w", knownHostsPath, err)
	}

	sshClient, err := ssh.Dial("tcp", cfg.SFTPServer, &ssh.ClientConfig{
		User:            cfg.SFTPUsername,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
	})
	if err != nil {
		return nil, fmt.Errorf("dial ssh: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, fmt.Errorf("create sftp client: %w", err)
	}

	homeDir, err := sftpClient.Getwd()
	if err != nil {
		_ = sftpClient.Close()
		_ = sshClient.Close()
		return nil, fmt.Errorf("resolve remote home directory: %w", err)
	}

	return &SFTPConnection{
		SSHClient: sshClient,
		Client:    sftpClient,
		HomeDir:   homeDir,
	}, nil
}

func (c *SFTPConnection) Close() error {
	var closeErr error
	if c.Client != nil {
		if err := c.Client.Close(); err != nil {
			closeErr = err
		}
	}
	if c.SSHClient != nil {
		if err := c.SSHClient.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func DiscoverTransferFiles(client *sftp.Client, homeDir string, cfg Config) ([]TransferFile, error) {
	if len(cfg.SFTPSources) == 0 {
		return discoverAllFiles(client, homeDir, cfg.S3Prefix)
	}
	return discoverExplicitFiles(client, homeDir, cfg)
}

func discoverAllFiles(client *sftp.Client, homeDir, prefix string) ([]TransferFile, error) {
	walk := client.Walk(homeDir)
	files := make([]TransferFile, 0)

	for walk.Step() {
		if err := walk.Err(); err != nil {
			return nil, fmt.Errorf("walk remote path %q: %w", walk.Path(), err)
		}

		stat := walk.Stat()
		if stat == nil || !stat.Mode().IsRegular() {
			continue
		}

		relPath, err := helpers.RelativeKeyFromRoot(homeDir, walk.Path())
		if err != nil {
			return nil, fmt.Errorf("derive relative path for %q: %w", walk.Path(), err)
		}

		files = append(files, TransferFile{
			RemotePath: walk.Path(),
			S3Key:      helpers.JoinS3Key(prefix, relPath),
			Size:       stat.Size(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].RemotePath < files[j].RemotePath
	})
	return files, nil
}

func discoverExplicitFiles(client *sftp.Client, homeDir string, cfg Config) ([]TransferFile, error) {
	files := make([]TransferFile, 0, len(cfg.SFTPSources))
	for _, src := range cfg.SFTPSources {
		cleanSource := path.Clean(src)
		if cleanSource == "." || cleanSource == "/" {
			return nil, fmt.Errorf("invalid sftp source path %q", src)
		}

		remotePath := cleanSource
		if !path.IsAbs(cleanSource) {
			remotePath = path.Join(homeDir, cleanSource)
		}

		stat, err := client.Stat(remotePath)
		if err != nil {
			return nil, fmt.Errorf("stat remote source %q: %w", remotePath, err)
		}
		if !stat.Mode().IsRegular() {
			return nil, fmt.Errorf("sftp source %q is not a file", src)
		}

		keyPart := helpers.KeyForExplicitSource(homeDir, remotePath)
		files = append(files, TransferFile{
			RemotePath: remotePath,
			S3Key:      helpers.JoinS3Key(cfg.S3Prefix, keyPart),
			Size:       stat.Size(),
		})
	}
	return files, nil
}
