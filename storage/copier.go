package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"golang.org/x/time/rate"
)

const (
	_32M  = 32 << 20
	_100M = 100 << 20
)

const _copyWorkerNum = 10

// limReader speed limit reader
type limReader struct {
	r   io.Reader
	lim *rate.Limiter
	ctx context.Context
}

func (r *limReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil {
		return n, err
	}

	if err := r.lim.WaitN(r.ctx, n); err != nil {
		return n, err
	}

	return n, err
}

type CopyOption struct {
	// BytePS byte/s copy speed limit, 0 is unlimited, default is unlimited
	BytePS float64
	// WorkerNum the number of copy task worker, default is 10
	WorkerNum int
	// RPS the number of copy requests initiated per second, 0 is unlimited, default is unlimited
	RPS int32
	// BufSizeByte the size of the buffer that the copier can use, default is 100MB
	BufSizeByte int
	// UseRemoteCopy Use the copy function of the dest client directly
	UseRemoteCopy bool
}

type Copier struct {
	src  Client
	dest Client

	// lim stream copy speed limiter
	lim                  *rate.Limiter
	workerNum            int
	bufSizeBytePerWorker int
	useRemoteCopy        bool
	rps                  int32
}

func NewCopier(src, dest Client, opt CopyOption) *Copier {
	var lim *rate.Limiter
	if opt.BytePS != 0 {
		lim = rate.NewLimiter(rate.Limit(opt.BytePS), _32M)
	}

	workerNum := _copyWorkerNum
	if opt.WorkerNum != 0 {
		workerNum = opt.WorkerNum
	}
	bufSizeBytePerWorker := _100M / workerNum
	if opt.BufSizeByte != 0 {
		bufSizeBytePerWorker = opt.BufSizeByte / workerNum
	}

	return &Copier{src: src, dest: dest,
		lim: lim, useRemoteCopy: opt.UseRemoteCopy, workerNum: workerNum, bufSizeBytePerWorker: bufSizeBytePerWorker,
	}
}

type Total struct {
	Length int64
	Count  int64
}

type CopyPathInput struct {
	SrcBucket string
	SrcPrefix string

	DestBucket string
	DestKeyFn  func(attr ObjectAttr) string

	// OnSuccess when an object copy success, this func will be call
	// May be executed concurrently, please pay attention to thread safety
	OnSuccess func(attr ObjectAttr, total Total)
}

func (c *Copier) getAttrs(ctx context.Context, bucket, prefix string) ([]ObjectAttr, Total, error) {
	var attrs []ObjectAttr
	var length int64
	var count int64
	p := c.src.ListObjectsPage(ctx, ListObjectPageInput{Bucket: bucket, Prefix: prefix})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, Total{}, fmt.Errorf("storage: copier list objects %w", err)
		}
		for _, attr := range page.Contents {
			attrs = append(attrs, attr)
			length += attr.Length
			count++
		}
	}

	return attrs, Total{Length: length, Count: count}, nil
}

// CopyPrefix Copy all files under src path
func (c *Copier) CopyPrefix(ctx context.Context, i CopyPathInput) error {
	srcAttrs, total, err := c.getAttrs(ctx, i.SrcBucket, i.SrcPrefix)
	if err != nil {
		return fmt.Errorf("storage: copier get src attrs %w", err)
	}

	wp, err := NewWorkerPool(ctx, c.workerNum, c.rps, 3)
	if err != nil {
		return fmt.Errorf("storage: copier new worker pool %w", err)
	}
	wp.Start()
	fn := c.selectCopyFn()
	for _, srcAttr := range srcAttrs {
		attr := srcAttr
		job := func(ctx context.Context) error {
			destKey := i.DestKeyFn(attr)
			destAttr, err := c.dest.HeadObject(ctx, i.DestBucket, destKey)
			if err != nil || !attr.SameAs(destAttr) {
				if err := fn(ctx, attr, destKey, i.SrcBucket, i.DestBucket); err != nil {
					return fmt.Errorf("storage: copier copy object %w", err)
				}
			}
			if i.OnSuccess != nil {
				i.OnSuccess(attr, total)
			}
			return nil
		}

		wp.Submit(job)
	}
	wp.Done()

	if err := wp.Wait(); err != nil {
		return fmt.Errorf("storage: copier copy prefix %w", err)
	}
	return nil
}

type copyFn func(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error

func (c *Copier) selectCopyFn() copyFn {
	if c.useRemoteCopy {
		return c.copyRemote
	}

	return c.copyLocal
}

func (c *Copier) copyRemote(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error {
	i := CopyObjectInput{SrcBucket: srcBucket, SrcKey: attr.Key, DestBucket: destBucket, DestKey: destKey}
	if err := c.dest.CopyObject(ctx, i); err != nil {
		return fmt.Errorf("storage: copier copy object %w", err)
	}

	return nil
}

func (c *Copier) copyLocal(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error {
	obj, err := c.src.GetObject(ctx, GetObjectInput{Bucket: srcBucket, Key: attr.Key})
	if err != nil {
		return fmt.Errorf("storage: copier get object %w", err)
	}
	defer obj.Body.Close()

	var body io.Reader = bufio.NewReaderSize(obj.Body, c.bufSizeBytePerWorker)
	if c.lim != nil {
		body = &limReader{r: body, lim: c.lim, ctx: ctx}
	}
	i := UploadObjectInput{Body: body, Bucket: destBucket, Key: destKey, WorkerNum: 1}
	if err := c.dest.UploadObject(ctx, i); err != nil {
		return fmt.Errorf("storage: copier put object %w", err)
	}

	return nil
}
