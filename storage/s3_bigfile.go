package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/valyala/bytebufferpool"
)

const U0001 = '\u0000'

const _200M = 200 << 20

func (c *S3Client) UploadObject_BigFile(ctx context.Context, i UploadObjectInput) error {
	firstBlock := make([]byte, _5M)
	n, err := readUntilEOF(i.Body, firstBlock)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("storage: upload object read first block %w", err)
	}
	firstBlock = firstBlock[:n]
	if n < _5M {
		reader := bytes.NewReader(firstBlock)
		putInput := PutObjectInput{Bucket: i.Bucket, Key: i.Key, Length: int64(n), Body: reader}
		if err := c.PutObject(ctx, putInput); err != nil {
			return fmt.Errorf("storage: upload object put object %w", err)
		}

		return nil
	}

	if err := c.uploadObject_BigFile(ctx, firstBlock, i); err != nil {
		return err
	}
	return nil
}

// todo:
func (c *S3Client) uploadObject_BigFile(ctx context.Context, firstBlock []byte, i UploadObjectInput) error {
	createReq := s3.CreateMultipartUploadInput{Bucket: aws.String(i.Bucket), Key: aws.String(i.Key)}
	createResp, err := c.cli.CreateMultipartUpload(ctx, &createReq)
	if err != nil {
		return fmt.Errorf("storage: s3 upload object create upload %w", err)
	}

	partNum := int32(1)
	var partsMu sync.Mutex
	var bp bytebufferpool.Pool
	wp, err := NewWorkerPool(ctx, i.WorkerNum, i.RPS)
	if err != nil {
		return fmt.Errorf("storage: s3 upload object %w", err)
	}
	wp.Start()
	part, err := c.uploadPart_BigFile(ctx, firstBlock, i.Bucket, i.Key, *createResp.UploadId, partNum)
	if err != nil {
		return fmt.Errorf("storage: s3 upload object upload part %w", err)
	}
	parts := []types.CompletedPart{part}
	for {
		partNum += 1
		bb := bp.Get()
		if cap(bb.B) == 0 {
			bb.Set(make([]byte, _5M))
		}
		bb.B = bb.B[:cap(bb.B)]
		n, err := readUntilEOF(i.Body, bb.B)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("storage: s3 upload object read body %w", err)
		}
		bb.B = bb.B[:n]
		// copy partNum
		pn := partNum
		job := func(ctx context.Context) error {
			part, err := c.uploadPart_BigFile(ctx, bb.B, i.Bucket, i.Key, *createResp.UploadId, pn)
			if err != nil {
				return fmt.Errorf("storage: s3 upload object upload part %w", err)
			}
			partsMu.Lock()
			parts = append(parts, part)
			partsMu.Unlock()
			bp.Put(bb)
			return nil
		}
		wp.Submit(job)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		if abortErr := c.abortUpload(ctx, i.Bucket, i.Key, *createResp.UploadId); abortErr != nil {
			return fmt.Errorf("storage: s3 upload aborted cause: %w, abort : %w", err, abortErr)
		}
		return fmt.Errorf("storage: s3 upload aborted %w", err)
	}

	cplReq := newCompleteReq(i.Bucket, i.Key, *createResp.UploadId, parts)
	if _, err := c.cli.CompleteMultipartUpload(ctx, &cplReq); err != nil {
		return fmt.Errorf("storage: s3 upload object complete upload %w", err)
	}

	return nil
}

func (c *S3Client) uploadPart_BigFile(ctx context.Context, buf []byte, bucket, key, uploadID string, partNum int32) (types.CompletedPart, error) {
	i := s3.UploadPartInput{
		Body:          bytes.NewReader(buf),
		Key:           aws.String(key),
		Bucket:        aws.String(bucket),
		PartNumber:    partNum,
		UploadId:      aws.String(uploadID),
		ContentLength: int64(len(buf)),
	}

	result, err := c.cli.UploadPart(ctx, &i)
	if err != nil {
		return types.CompletedPart{}, fmt.Errorf("storage: s3 upload part %w", err)
	}

	return types.CompletedPart{PartNumber: partNum, ETag: result.ETag}, nil
}

func (c *S3Client) abortUpload_BigFile(ctx context.Context, bucket, key, uploadID string) error {
	req := s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}
	if _, err := c.cli.AbortMultipartUpload(ctx, &req); err != nil {
		return fmt.Errorf("storage: s3 abort multipart upload %w", err)
	}

	return nil
}
