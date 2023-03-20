package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/valyala/bytebufferpool"
)

func readUntilEOF(r io.Reader, b []byte) (int, error) {
	var n int
	var err error

	for n < len(b) && err == nil {
		var nn int
		nn, err = r.Read(b[n:])
		n += nn
	}

	return n, err
}

const _5M = 5 << 20

var _ Client = (*S3Client)(nil)

type S3Client struct {
	cli *s3.Client
}

func NewS3Client(cfg Cfg) (*S3Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("storage: new aws cli %w", err)
	}

	if len(cfg.Endpoint) != 0 {
		var url string
		if cfg.UseSSL {
			url = fmt.Sprintf("https://%s", cfg.Endpoint)
		} else {
			url = fmt.Sprintf("http://%s", cfg.Endpoint)
		}
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...any) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               url,
				SigningRegion:     region,
				HostnameImmutable: true,
			}, nil
		})
		awsCfg.EndpointResolverWithOptions = resolver
	}

	if !cfg.UseIAM {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK, "")
	}
	return &S3Client{cli: s3.NewFromConfig(awsCfg)}, nil
}

func (c *S3Client) HeadBucket(ctx context.Context, bucket string) error {
	_, err := c.cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("storage: s3 head bucket %w", err)
	}

	return nil
}

func (c *S3Client) CopyObject(ctx context.Context, i CopyObjectInput) error {
	source := fmt.Sprintf("%s/%s", i.SrcBucket, i.SrcKey)
	params := s3.CopyObjectInput{
		Bucket:     aws.String(i.DestBucket),
		CopySource: aws.String(source),
		Key:        aws.String(i.DestKey),
	}

	if _, err := c.cli.CopyObject(ctx, &params); err != nil {
		return fmt.Errorf("storage: s3 copy object from %s / %s  to %s / %s %w", i.SrcBucket, i.SrcKey, i.DestBucket, i.DestKey, err)
	}
	return nil
}

func (c *S3Client) ListObjectsPage(_ context.Context, i ListObjectPageInput) ListObjectsPaginator {
	var prefix *string
	if len(i.Prefix) != 0 {
		prefix = aws.String(i.Prefix)
	}

	pageSize := i.PageSize
	if pageSize == 0 {
		pageSize = 1000
	}

	params := s3.ListObjectsV2Input{Bucket: aws.String(i.Bucket), MaxKeys: pageSize, Prefix: prefix}
	p := s3.NewListObjectsV2Paginator(c.cli, &params)
	return &S3ListObjectPaginator{p: p}
}

var _ ListObjectsPaginator = (*S3ListObjectPaginator)(nil)

type S3ListObjectPaginator struct {
	p *s3.ListObjectsV2Paginator
}

func (p *S3ListObjectPaginator) HasMorePages() bool { return p.p.HasMorePages() }

func (p *S3ListObjectPaginator) NextPage(ctx context.Context) (*Page, error) {
	page, err := p.p.NextPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 read next page %w", err)
	}

	contents := make([]ObjectAttr, 0, len(page.Contents))
	for _, obj := range page.Contents {
		contents = append(contents, ObjectAttr{Length: obj.Size, Key: *obj.Key})
	}

	return &Page{Contents: contents}, nil
}

func (c *S3Client) GetObject(ctx context.Context, i GetObjectInput) (*Object, error) {
	params := s3.GetObjectInput{Bucket: aws.String(i.Bucket), Key: aws.String(i.Key)}
	o, err := c.cli.GetObject(ctx, &params)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 get object %w", err)
	}

	return &Object{Body: o.Body, Length: o.ContentLength}, nil
}

func (c *S3Client) PutObject(ctx context.Context, i PutObjectInput) error {
	params := s3.PutObjectInput{
		Bucket:        aws.String(i.Bucket),
		Body:          i.Body,
		Key:           aws.String(i.Key),
		ContentLength: i.Length,
	}
	if _, err := c.cli.PutObject(ctx, &params); err != nil {
		return fmt.Errorf("storage: s3 put object %w", err)
	}
	return nil
}

func (c *S3Client) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	if len(i.Keys) == 0 {
		return nil
	}
	ids := make([]types.ObjectIdentifier, 0, len(i.Keys))
	for _, key := range i.Keys {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(key)})
	}
	params := s3.DeleteObjectsInput{Bucket: aws.String(i.Bucket), Delete: &types.Delete{Objects: ids}}
	resp, err := c.cli.DeleteObjects(ctx, &params)
	if err != nil {
		return fmt.Errorf("storage: s3 delete objects %w", err)
	}
	if len(resp.Errors) != 0 {
		errs := make([]error, 0, len(resp.Errors))
		for _, err := range resp.Errors {
			errs = append(errs, fmt.Errorf("storage: s3 delete object key:%s fail reson:%s", *err.Key, *err.Message))
		}
		return errors.Join(errs...)
	}

	return nil
}

func (c *S3Client) DeletePrefix(ctx context.Context, i DeletePrefixInput) error {
	if len(i.Prefix) == 0 {
		panic("empty prefix will delete all files under bucket")
	}
	page := c.ListObjectsPage(ctx, ListObjectPageInput{Bucket: i.Bucket, Prefix: i.Prefix})
	for page.HasMorePages() {
		cur, err := page.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("storage: delete prefix %w", err)
		}
		keys := make([]string, 0, len(cur.Contents))
		for _, attr := range cur.Contents {
			keys = append(keys, attr.Key)
		}
		if err := c.DeleteObjects(ctx, DeleteObjectsInput{Bucket: i.Bucket, Keys: keys}); err != nil {
			return fmt.Errorf("storage: s3 delete prefix %w", err)
		}
	}

	return nil
}

func (c *S3Client) UploadObject(ctx context.Context, i UploadObjectInput) error {
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

	if err := c.uploadObject(ctx, firstBlock, i); err != nil {
		return err
	}
	return nil
}

func newCompleteReq(bucket, key, uploadID string, parts []types.CompletedPart) s3.CompleteMultipartUploadInput {
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
}

func (c *S3Client) uploadObject(ctx context.Context, firstBlock []byte, i UploadObjectInput) error {
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
	part, err := c.uploadPart(ctx, firstBlock, i.Bucket, i.Key, *createResp.UploadId, partNum)
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
			part, err := c.uploadPart(ctx, bb.B, i.Bucket, i.Key, *createResp.UploadId, pn)
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

func (c *S3Client) uploadPart(ctx context.Context, buf []byte, bucket, key, uploadID string, partNum int32) (types.CompletedPart, error) {
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

func (c *S3Client) abortUpload(ctx context.Context, bucket, key, uploadID string) error {
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
