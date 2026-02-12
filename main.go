package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type TransferSummary struct {
	TotalFiles    int
	UploadedFiles int
	SkippedFiles  int
	FailedFiles   int
	UploadedBytes int64
	Duration      time.Duration
}

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	startedAt := time.Now()
	summary := TransferSummary{
		TotalFiles: len(files),
	}

	for idx, file := range files {
		select {
		case <-ctx.Done():
			summary.Duration = time.Since(startedAt)
			logTransferSummary(summary)
			return fmt.Errorf("transfer interrupted: %w", ctx.Err())
		default:
		}

		log.Printf("[%d/%d] preparing remote=%s key=%s size=%d", idx+1, len(files), file.RemotePath, file.S3Key, file.Size)

		if !cfg.Overwrite {
			exists, headErr := uploader.ObjectExists(ctx, file.S3Key)
			if headErr != nil {
				summary.FailedFiles++
				log.Printf("[%d/%d] failed remote=%s key=%s reason=head-check error=%v", idx+1, len(files), file.RemotePath, file.S3Key, headErr)
				continue
			}
			if exists {
				summary.SkippedFiles++
				log.Printf("[%d/%d] skipping existing key=%s", idx+1, len(files), file.S3Key)
				continue
			}
		}

		remoteFile, openErr := conn.Client.Open(file.RemotePath)
		if openErr != nil {
			summary.FailedFiles++
			log.Printf("[%d/%d] failed remote=%s key=%s reason=open-remote-file error=%v", idx+1, len(files), file.RemotePath, file.S3Key, openErr)
			continue
		}

		uploadErr := uploader.UploadFile(ctx, file, remoteFile)
		closeErr := remoteFile.Close()
		if uploadErr != nil {
			summary.FailedFiles++
			log.Printf("[%d/%d] failed remote=%s key=%s reason=upload error=%v", idx+1, len(files), file.RemotePath, file.S3Key, uploadErr)
			continue
		}
		if closeErr != nil {
			log.Printf("[%d/%d] warning remote=%s close-remote-file error=%v", idx+1, len(files), file.RemotePath, closeErr)
		}

		summary.UploadedFiles++
		summary.UploadedBytes += file.Size
	}

	summary.Duration = time.Since(startedAt)
	logTransferSummary(summary)

	if summary.FailedFiles > 0 {
		return fmt.Errorf("transfer completed with %d failed file(s)", summary.FailedFiles)
	}

	return nil
}

func logTransferSummary(summary TransferSummary) {
	log.Printf(
		"transfer summary total=%d uploaded=%d skipped=%d failed=%d uploaded_bytes=%d duration=%s",
		summary.TotalFiles,
		summary.UploadedFiles,
		summary.SkippedFiles,
		summary.FailedFiles,
		summary.UploadedBytes,
		summary.Duration.Round(time.Millisecond),
	)
}
