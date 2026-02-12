package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pkg/sftp"

	"sftptos3/internal/helpers"
)

type S3Uploader struct {
	client *s3.Client
	bucket string
	cfg    Config
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

type countingReadSeeker struct {
	inner   io.ReadSeeker
	counter *atomicProgressCounter
}

func (r *countingReadSeeker) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.counter.Add(int64(n))
	}
	return n, err
}

func (r *countingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.inner.Seek(offset, whence)
}

func NewS3Uploader(ctx context.Context, cfg Config) (*S3Uploader, error) {
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.S3Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
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
	if file.Size == 0 {
		return u.uploadEmptyFile(ctx, file)
	}

	partSize, err := helpers.ComputePartSize(file.Size)
	if err != nil {
		return fmt.Errorf("compute part size: %w", err)
	}

	jobs := buildMultipartPartJobs(file.Size, partSize)
	if len(jobs) == 0 {
		return fmt.Errorf("could not build multipart jobs for file size %d", file.Size)
	}

	workers := u.cfg.MultipartConcurrency
	if workers > len(jobs) {
		workers = len(jobs)
	}
	if workers < 1 {
		workers = 1
	}

	counter := &atomicProgressCounter{}
	reporter := progressReporter{
		remotePath: file.RemotePath,
		s3Key:      file.S3Key,
		totalBytes: file.Size,
		counter:    counter,
		interval:   2 * time.Second,
		window:     10 * time.Second,
	}

	stopProgress := reporter.Start()
	defer stopProgress()

	log.Printf(
		"starting multipart upload remote=%s key=%s size=%d part_size=%d MiB parts=%d concurrency=%d",
		file.RemotePath,
		file.S3Key,
		file.Size,
		partSize/(1024*1024),
		len(jobs),
		workers,
	)

	start := time.Now()
	createOut, err := u.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(file.S3Key),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	if createOut.UploadId == nil || *createOut.UploadId == "" {
		return fmt.Errorf("create multipart upload returned empty upload id")
	}

	parts, uploadErr := u.uploadParts(ctx, file, remoteFile, *createOut.UploadId, jobs, workers, counter)
	if uploadErr != nil {
		u.abortMultipartUpload(file.S3Key, *createOut.UploadId)
		return uploadErr
	}

	sort.Slice(parts, func(i, j int) bool {
		return aws.ToInt32(parts[i].PartNumber) < aws.ToInt32(parts[j].PartNumber)
	})

	_, err = u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(file.S3Key),
		UploadId: createOut.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		u.abortMultipartUpload(file.S3Key, *createOut.UploadId)
		return fmt.Errorf("complete multipart upload: %w", err)
	}

	elapsed := time.Since(start)
	avgSpeed := 0.0
	if elapsed > 0 {
		avgSpeed = (float64(counter.BytesRead()) / elapsed.Seconds()) / (1024 * 1024)
	}
	log.Printf(
		"uploaded remote=%s key=%s bytes=%d duration=%s avg=%.2f MiB/s part_size=%d MiB parts=%d concurrency=%d",
		file.RemotePath,
		file.S3Key,
		counter.BytesRead(),
		elapsed.Round(time.Millisecond),
		avgSpeed,
		partSize/(1024*1024),
		len(parts),
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
	body := &countingReadSeeker{inner: section, counter: counter}

	output, err := u.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(file.S3Key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(job.PartNumber),
		Body:          body,
		ContentLength: aws.Int64(job.Length),
	})
	if err != nil {
		return s3types.CompletedPart{}, err
	}
	if output.ETag == nil || *output.ETag == "" {
		return s3types.CompletedPart{}, fmt.Errorf("empty ETag from upload part %d", job.PartNumber)
	}

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
