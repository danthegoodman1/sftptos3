package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	if err := run(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("c", "config.yml", "Path to YAML config file")
	flag.Parse()

	cfg, created, err := LoadOrCreateConfig(*configPath)
	if err != nil {
		return err
	}
	if created {
		log.Printf("created template config at %s", *configPath)
		return nil
	}

	ctx := context.Background()
	conn, err := ConnectSFTP(cfg)
	if err != nil {
		return fmt.Errorf("connect sftp: %w", err)
	}
	defer conn.Close()

	files, err := DiscoverTransferFiles(conn.Client, conn.HomeDir, cfg)
	if err != nil {
		return fmt.Errorf("discover sources: %w", err)
	}
	if len(files) == 0 {
		log.Printf("no files discovered, nothing to upload")
		return nil
	}
	log.Printf("discovered %d file(s) for upload", len(files))

	uploader, err := NewS3Uploader(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init s3 uploader: %w", err)
	}

	uploaded := 0
	skipped := 0
	for idx, file := range files {
		log.Printf("[%d/%d] preparing remote=%s key=%s size=%d", idx+1, len(files), file.RemotePath, file.S3Key, file.Size)

		if !cfg.Overwrite {
			exists, headErr := uploader.ObjectExists(ctx, file.S3Key)
			if headErr != nil {
				return fmt.Errorf("check existing object %q: %w", file.S3Key, headErr)
			}
			if exists {
				skipped++
				log.Printf("[%d/%d] skipping existing key=%s", idx+1, len(files), file.S3Key)
				continue
			}
		}

		remoteFile, openErr := conn.Client.Open(file.RemotePath)
		if openErr != nil {
			return fmt.Errorf("open remote file %q: %w", file.RemotePath, openErr)
		}

		uploadErr := uploader.UploadFile(ctx, file, remoteFile)
		closeErr := remoteFile.Close()
		if uploadErr != nil {
			return fmt.Errorf("upload file %q: %w", file.RemotePath, uploadErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close remote file %q: %w", file.RemotePath, closeErr)
		}

		uploaded++
	}

	if uploaded == 0 && skipped > 0 {
		log.Printf("completed: uploaded=%d skipped=%d", uploaded, skipped)
		return nil
	}
	log.Printf("completed: uploaded=%d skipped=%d", uploaded, skipped)
	return nil
}
