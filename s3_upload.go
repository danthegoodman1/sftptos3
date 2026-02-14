package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pkg/sftp"

	"sftptos3/internal/helpers"
)

const resumeStateVersion = 1

type S3Uploader struct {
	client *s3.Client
	bucket string
	cfg    Config
}

type resumeState struct {
	Version    int       `json:"version"`
	Bucket     string    `json:"bucket"`
	RemotePath string    `json:"remote_path"`
	S3Key      string    `json:"s3_key"`
	FileSize   int64     `json:"file_size"`
	PartSize   int64     `json:"part_size"`
	UploadID   string    `json:"upload_id"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s resumeState) Matches(file TransferFile, bucket string, partSize int64) bool {
	return s.Version == resumeStateVersion &&
		s.Bucket == bucket &&
		s.RemotePath == file.RemotePath &&
		s.S3Key == file.S3Key &&
		s.FileSize == file.Size &&
		s.PartSize == partSize &&
		s.UploadID != ""
}

type multipartPartJob struct {
	PartNumber int32
	Offset     int64
	Length     int64
}

type multipartPartResult struct {
	Part s3types.CompletedPart
	Err  error
}

func NewS3Uploader(ctx context.Context, cfg Config) (*S3Uploader, error) {
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.S3Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, "")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = cfg.S3RetryMaxAttempts
			})
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	resumabilityDir, err := helpers.ExpandHome(cfg.ResumabilityDir)
	if err != nil {
		return nil, fmt.Errorf("expand resumability_dir: %w", err)
	}
	cfg.ResumabilityDir = resumabilityDir
	if err := os.MkdirAll(cfg.ResumabilityDir, 0o755); err != nil {
		return nil, fmt.Errorf("create resumability_dir %q: %w", cfg.ResumabilityDir, err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.S3UsePathStyle
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
	})

	return &S3Uploader{
		client: client,
		bucket: cfg.S3Bucket,
		cfg:    cfg,
	}, nil
}

func (u *S3Uploader) ObjectExists(ctx context.Context, key string) (bool, error) {
	_, err := u.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}

	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 404 {
		return false, nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return false, nil
		}
	}

	return false, err
}

func (u *S3Uploader) UploadFile(ctx context.Context, file TransferFile, remoteFile *sftp.File) error {
	resumePath, err := u.resumeStatePath(file)
	if err != nil {
		return fmt.Errorf("build resume state path: %w", err)
	}

	if file.Size == 0 {
		if err := u.removeResumeState(resumePath); err != nil {
			log.Printf("warning: failed to remove stale resume state for %s: %v", file.RemotePath, err)
		}
		return u.uploadEmptyFile(ctx, file)
	}

	partSize, err := helpers.ComputePartSize(file.Size)
	if err != nil {
		return fmt.Errorf("compute part size: %w", err)
	}

	allJobs := buildMultipartPartJobs(file.Size, partSize)
	if len(allJobs) == 0 {
		return fmt.Errorf("could not build multipart jobs for file size %d", file.Size)
	}

	workers := u.cfg.MultipartConcurrency
	if workers > len(allJobs) {
		workers = len(allJobs)
	}
	if workers < 1 {
		workers = 1
	}

	uploadID := ""
	existingParts := make(map[int32]s3types.CompletedPart)

	state, err := u.loadResumeState(resumePath)
	if err != nil {
		return fmt.Errorf("load resume state: %w", err)
	}

	if state != nil {
		if state.Matches(file, u.bucket, partSize) {
			uploadID = state.UploadID
			parts, listErr := u.listUploadedParts(ctx, file.S3Key, uploadID)
			if listErr != nil {
				if isNoSuchUploadError(listErr) {
					log.Printf("resume upload missing on S3, starting new upload remote=%s key=%s", file.RemotePath, file.S3Key)
					uploadID = ""
					existingParts = make(map[int32]s3types.CompletedPart)
					if err := u.removeResumeState(resumePath); err != nil {
						log.Printf("warning: failed to remove stale resume state %s: %v", resumePath, err)
					}
				} else {
					return fmt.Errorf("list uploaded parts for resume: %w", listErr)
				}
			} else {
				existingParts = parts
				log.Printf("resuming multipart upload remote=%s key=%s upload_id=%s existing_parts=%d",
					file.RemotePath,
					file.S3Key,
					uploadID,
					len(existingParts),
				)
			}
		} else {
			log.Printf("ignoring stale resume metadata for remote=%s key=%s", file.RemotePath, file.S3Key)
			if err := u.removeResumeState(resumePath); err != nil {
				log.Printf("warning: failed to remove stale resume state %s: %v", resumePath, err)
			}
		}
	}

	if uploadID == "" {
		createOut, createErr := u.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(u.bucket),
			Key:    aws.String(file.S3Key),
		})
		if createErr != nil {
			return fmt.Errorf("create multipart upload: %w", createErr)
		}
		if createOut.UploadId == nil || *createOut.UploadId == "" {
			return fmt.Errorf("create multipart upload returned empty upload id")
		}
		uploadID = *createOut.UploadId

		newState := resumeState{
			Version:    resumeStateVersion,
			Bucket:     u.bucket,
			RemotePath: file.RemotePath,
			S3Key:      file.S3Key,
			FileSize:   file.Size,
			PartSize:   partSize,
			UploadID:   uploadID,
			CreatedAt:  time.Now().UTC(),
		}
		if saveErr := u.saveResumeState(resumePath, newState); saveErr != nil {
			u.abortMultipartUpload(file.S3Key, uploadID)
			return fmt.Errorf("persist resume state: %w", saveErr)
		}
	}

	pendingJobs := make([]multipartPartJob, 0, len(allJobs))
	resumedBytes := int64(0)
	for _, job := range allJobs {
		if _, ok := existingParts[job.PartNumber]; ok {
			resumedBytes += job.Length
			continue
		}
		pendingJobs = append(pendingJobs, job)
	}

	counter := &atomicProgressCounter{}
	counter.Add(resumedBytes)
	reporter := progressReporter{
		remotePath: file.RemotePath,
		s3Key:      file.S3Key,
		totalBytes: file.Size,
		counter:    counter,
		interval:   2 * time.Second,
		window:     60 * time.Second,
	}

	stopProgress := reporter.Start()
	defer stopProgress()

	if resumedBytes > 0 {
		resumedPct := (float64(resumedBytes) / float64(file.Size)) * 100
		if resumedPct > 100 {
			resumedPct = 100
		}
		log.Printf(
			"resume baseline remote=%s key=%s transferred=%d/%d pct=%.1f%%",
			file.RemotePath,
			file.S3Key,
			resumedBytes,
			file.Size,
			resumedPct,
		)
	}

	log.Printf(
		"starting multipart upload remote=%s key=%s size=%d part_size=%d MiB parts=%d pending_parts=%d resumed_bytes=%d concurrency=%d",
		file.RemotePath,
		file.S3Key,
		file.Size,
		partSize/(1024*1024),
		len(allJobs),
		len(pendingJobs),
		resumedBytes,
		workers,
	)

	start := time.Now()
	newParts := make([]s3types.CompletedPart, 0)
	if len(pendingJobs) > 0 {
		newParts, err = u.uploadParts(ctx, file, remoteFile, uploadID, pendingJobs, workers, counter)
		if err != nil {
			return err
		}
	}

	partsByNumber := make(map[int32]s3types.CompletedPart, len(allJobs))
	for partNumber, part := range existingParts {
		partsByNumber[partNumber] = part
	}
	for _, part := range newParts {
		partsByNumber[aws.ToInt32(part.PartNumber)] = part
	}
	if len(partsByNumber) != len(allJobs) {
		return fmt.Errorf("multipart upload incomplete: uploaded parts=%d expected=%d", len(partsByNumber), len(allJobs))
	}

	finalParts := make([]s3types.CompletedPart, 0, len(partsByNumber))
	for _, part := range partsByNumber {
		finalParts = append(finalParts, part)
	}
	sort.Slice(finalParts, func(i, j int) bool {
		return aws.ToInt32(finalParts[i].PartNumber) < aws.ToInt32(finalParts[j].PartNumber)
	})

	_, err = u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(file.S3Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: finalParts,
		},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}

	if err := u.removeResumeState(resumePath); err != nil {
		log.Printf("warning: failed to remove resume state for remote=%s key=%s path=%s err=%v", file.RemotePath, file.S3Key, resumePath, err)
	}

	elapsed := time.Since(start)
	sessionBytes := counter.BytesRead() - resumedBytes
	if sessionBytes < 0 {
		sessionBytes = counter.BytesRead()
	}
	avgSpeed := 0.0
	if elapsed > 0 {
		avgSpeed = (float64(sessionBytes) / elapsed.Seconds()) / (1024 * 1024)
	}
	log.Printf(
		"uploaded remote=%s key=%s bytes=%d duration=%s avg=%.2f MiB/s part_size=%d MiB parts=%d concurrency=%d",
		file.RemotePath,
		file.S3Key,
		counter.BytesRead(),
		elapsed.Round(time.Millisecond),
		avgSpeed,
		partSize/(1024*1024),
		len(finalParts),
		workers,
	)

	return nil
}

func (u *S3Uploader) uploadParts(
	ctx context.Context,
	file TransferFile,
	remoteFile *sftp.File,
	uploadID string,
	jobs []multipartPartJob,
	workers int,
	counter *atomicProgressCounter,
) ([]s3types.CompletedPart, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobsCh := make(chan multipartPartJob)
	resultsCh := make(chan multipartPartResult, len(jobs))

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			u.partWorker(ctx, workerID, file, remoteFile, uploadID, jobsCh, resultsCh, counter)
		}(worker + 1)
	}

	go func() {
		defer close(jobsCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobsCh <- job:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	completed := make([]s3types.CompletedPart, 0, len(jobs))
	var firstErr error
	for result := range resultsCh {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
				cancel()
			}
			continue
		}
		completed = append(completed, result.Part)
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if len(completed) != len(jobs) {
		return nil, fmt.Errorf("multipart upload incomplete: uploaded parts=%d expected=%d", len(completed), len(jobs))
	}

	return completed, nil
}

func (u *S3Uploader) partWorker(
	ctx context.Context,
	workerID int,
	file TransferFile,
	remoteFile *sftp.File,
	uploadID string,
	jobs <-chan multipartPartJob,
	results chan<- multipartPartResult,
	counter *atomicProgressCounter,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			part, uploadErr := u.uploadPart(ctx, file, uploadID, remoteFile, job, counter)
			if uploadErr != nil {
				results <- multipartPartResult{Err: fmt.Errorf("worker %d upload part %d: %w", workerID, job.PartNumber, uploadErr)}
				return
			}
			results <- multipartPartResult{Part: part}
		}
	}
}

func (u *S3Uploader) uploadPart(
	ctx context.Context,
	file TransferFile,
	uploadID string,
	remoteFile *sftp.File,
	job multipartPartJob,
	counter *atomicProgressCounter,
) (s3types.CompletedPart, error) {
	section := io.NewSectionReader(remoteFile, job.Offset, job.Length)
	var baseReader io.Reader = section
	if u.cfg.SFTPReadBufferBytes > 0 {
		baseReader = bufio.NewReaderSize(section, u.cfg.SFTPReadBufferBytes)
	}

	output, err := u.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(file.S3Key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(job.PartNumber),
		Body:          baseReader,
		ContentLength: aws.Int64(job.Length),
	})
	if err != nil {
		return s3types.CompletedPart{}, err
	}
	if output.ETag == nil || *output.ETag == "" {
		return s3types.CompletedPart{}, fmt.Errorf("empty ETag from upload part %d", job.PartNumber)
	}
	counter.Add(job.Length)

	return s3types.CompletedPart{
		ETag:       output.ETag,
		PartNumber: aws.Int32(job.PartNumber),
	}, nil
}

func (u *S3Uploader) uploadEmptyFile(ctx context.Context, file TransferFile) error {
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(file.S3Key),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		return fmt.Errorf("put empty object: %w", err)
	}
	log.Printf("uploaded remote=%s key=%s bytes=0 duration=0s avg=0.00 MiB/s", file.RemotePath, file.S3Key)
	return nil
}

func (u *S3Uploader) listUploadedParts(ctx context.Context, key, uploadID string) (map[int32]s3types.CompletedPart, error) {
	parts := make(map[int32]s3types.CompletedPart)
	var marker string

	for {
		input := &s3.ListPartsInput{
			Bucket:   aws.String(u.bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
			MaxParts: aws.Int32(1000),
		}
		if marker != "" {
			input.PartNumberMarker = aws.String(marker)
		}

		out, err := u.client.ListParts(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, part := range out.Parts {
			if part.PartNumber == nil || part.ETag == nil || *part.ETag == "" {
				continue
			}
			parts[aws.ToInt32(part.PartNumber)] = s3types.CompletedPart{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		nextMarker, err := nextPartNumberMarker(marker, out.Parts, out.NextPartNumberMarker)
		if err != nil {
			return nil, fmt.Errorf("list parts pagination stalled for key=%s upload_id=%s marker=%q: %w", key, uploadID, marker, err)
		}
		marker = nextMarker
	}

	return parts, nil
}

func nextPartNumberMarker(current string, parts []s3types.Part, next *string) (string, error) {
	nextMarker := strings.TrimSpace(aws.ToString(next))
	if nextMarker == "" {
		if len(parts) == 0 {
			return "", fmt.Errorf("response is truncated but page has no parts and no next marker")
		}
		lastPart := parts[len(parts)-1]
		if lastPart.PartNumber == nil {
			return "", fmt.Errorf("response is truncated but last part has no part number")
		}
		nextMarker = fmt.Sprintf("%d", aws.ToInt32(lastPart.PartNumber))
	}
	if nextMarker == current {
		return "", fmt.Errorf("next marker %q did not advance", nextMarker)
	}
	return nextMarker, nil
}

func (u *S3Uploader) resumeStatePath(file TransferFile) (string, error) {
	dir := strings.TrimSpace(u.cfg.ResumabilityDir)
	if dir == "" {
		return "", fmt.Errorf("resumability_dir is empty")
	}
	hash := sha256.Sum256([]byte(u.bucket + "\n" + file.RemotePath + "\n" + file.S3Key))
	filename := fmt.Sprintf("%x.json", hash[:16])
	return filepath.Join(dir, filename), nil
}

func (u *S3Uploader) loadResumeState(filePath string) (*resumeState, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read resume state: %w", err)
	}

	var state resumeState
	if err := json.Unmarshal(content, &state); err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	return &state, nil
}

func (u *S3Uploader) saveResumeState(filePath string, state resumeState) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("create resume state directory: %w", err)
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode resume state: %w", err)
	}

	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, content, 0o600); err != nil {
		return fmt.Errorf("write temp resume state: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		return fmt.Errorf("rename resume state file: %w", err)
	}
	return nil
}

func (u *S3Uploader) removeResumeState(filePath string) error {
	err := os.Remove(filePath)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func isNoSuchUploadError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return true
	}

	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 404 {
		return true
	}

	return false
}

func (u *S3Uploader) abortMultipartUpload(key, uploadID string) {
	abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := u.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		log.Printf("warning: failed to abort multipart upload key=%s upload_id=%s err=%v", key, uploadID, err)
	}
}

func buildMultipartPartJobs(fileSize, partSize int64) []multipartPartJob {
	if fileSize <= 0 || partSize <= 0 {
		return nil
	}

	partCount := int((fileSize + partSize - 1) / partSize)
	jobs := make([]multipartPartJob, 0, partCount)
	for idx := 0; idx < partCount; idx++ {
		offset := int64(idx) * partSize
		length := partSize
		remaining := fileSize - offset
		if remaining < length {
			length = remaining
		}

		jobs = append(jobs, multipartPartJob{
			PartNumber: int32(idx + 1),
			Offset:     offset,
			Length:     length,
		})
	}

	return jobs
}
