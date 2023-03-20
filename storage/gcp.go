package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const _gcpEndpoint = "storage.googleapis.com"
const (
	_xAmzPrefix  = "X-Amz-"
	_xGoogPrefix = "X-Goog-"
)

var _ Client = (*GCPClient)(nil)

type GCPClient struct {
	// github.com/googleapis/google-cloud-go does not support ak sk , so we use minio
	cli *minio.Client
}

// WrapHTTPTransport wraps http.Transport, add an auth header to support GCP native auth
type wrapHTTPTransport struct {
	tokenSrc     oauth2.TokenSource
	backend      transport
	currentToken atomic.Pointer[oauth2.Token]
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// newWrapHTTPTransport constructs a new WrapHTTPTransport
func newWrapHTTPTransport(secure bool) (*wrapHTTPTransport, error) {
	tokenSrc := google.ComputeTokenSource("")
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create default transport %w", err)
	}
	return &wrapHTTPTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *wrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range req.Header {
		if strings.HasPrefix(k, _xAmzPrefix) {
			req.Header[strings.Replace(k, _xAmzPrefix, _xGoogPrefix, 1)] = v
			delete(req.Header, k)
		}
	}
	// here Valid() means the token won't be expired in 10 sec
	// so the http client timeout shouldn't be longer, or we need to change the default `expiryDelta` time
	currentToken := t.currentToken.Load()
	if currentToken.Valid() {
		req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
	} else {
		newToken, err := t.tokenSrc.Token()
		if err != nil {
			return nil, fmt.Errorf("storage: acquire token %w", err)
		}
		t.currentToken.Store(newToken)
		req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
	}

	return t.backend.RoundTrip(req)
}

func NewGCPClient(cfg Cfg) (*GCPClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region}
	if cfg.UseIAM {
		trans, err := newWrapHTTPTransport(cfg.UseSSL)
		if err != nil {
			return nil, err
		}
		opts.Transport = trans
		opts.Creds = credentials.NewStaticV2("", "", "")
	} else {
		opts.Creds = credentials.NewStaticV2(cfg.AK, cfg.SK, "")
	}

	cli, err := minio.New(_gcpEndpoint, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: new gcp client %w", err)
	}

	return &GCPClient{cli: cli}, nil
}

func (g *GCPClient) HeadBucket(ctx context.Context, bucket string) error {
	_, err := g.cli.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("storage: gcp head bucket %w", err)
	}
	return nil
}

func (g *GCPClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	dst := minio.CopyDestOptions{Bucket: i.DestBucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: i.SrcBucket, Object: i.SrcKey}
	if _, err := g.cli.CopyObject(ctx, dst, src); err != nil {
		return fmt.Errorf("storage: gcp copy from %s / %s  to %s / %s %w", i.SrcKey, i.SrcBucket, i.DestBucket, i.DestKey, err)
	}
	return nil
}

var _ ListObjectsPaginator = (*GCPListObjectPaginator)(nil)

type GCPListObjectPaginator struct {
	objCh    <-chan minio.ObjectInfo
	doneCh   <-chan struct{}
	pageSize int32
	hasMore  bool
}

func (p *GCPListObjectPaginator) HasMorePages() bool { return p.hasMore }

func (p *GCPListObjectPaginator) NextPage(ctx context.Context) (*Page, error) {
	if !p.hasMore {
		return nil, errors.New("storage: gcp no more pages")
	}

	contents := make([]ObjectAttr, 0, p.pageSize)
L:
	for i := int32(0); i < p.pageSize; i++ {
		select {
		case obj := <-p.objCh:
			if obj.Err != nil {
				return nil, fmt.Errorf("storage: gcp get obj %w", obj.Err)
			}
			contents = append(contents, ObjectAttr{Key: obj.Key, Length: obj.Size})
		case <-p.doneCh:
			p.hasMore = false
			break L
		case <-ctx.Done():
			break L
		}
	}

	return &Page{Contents: contents}, nil
}

func (g *GCPClient) ListObjectsPage(ctx context.Context, i ListObjectPageInput) ListObjectsPaginator {
	doneCh := make(chan struct{})
	objCh := g.cli.ListObjects(ctx, i.Bucket, minio.ListObjectsOptions{Prefix: i.Prefix, Recursive: true})
	return &GCPListObjectPaginator{objCh: objCh, doneCh: doneCh, pageSize: i.PageSize, hasMore: true}
}

func (g *GCPClient) GetObject(ctx context.Context, i GetObjectInput) (*Object, error) {
	obj, err := g.cli.GetObject(ctx, i.Bucket, i.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: gcp get object %w", err)
	}
	attr, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: gcp get object attr %w", err)
	}
	return &Object{Length: attr.Size, Body: obj}, nil
}

func (g *GCPClient) PutObject(ctx context.Context, i PutObjectInput) error {
	if _, err := g.cli.PutObject(ctx, i.Bucket, i.Key, i.Body, i.Length, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("storage: gcp put object %w", err)
	}

	return nil
}

func (g *GCPClient) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	objCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objCh)
		for _, key := range i.Keys {
			objCh <- minio.ObjectInfo{Key: key}
		}
	}()

	var errs []error
	for err := range g.cli.RemoveObjects(ctx, i.Bucket, objCh, minio.RemoveObjectsOptions{}) {
		errs = append(errs, fmt.Errorf("storage: gcp delete objs %s %w", err.ObjectName, err.Err))
	}

	return errors.Join(errs...)
}

func (g *GCPClient) DeletePrefix(ctx context.Context, i DeletePrefixInput) error {
	if len(i.Prefix) == 0 {
		panic("empty prefix will delete all files under bucket")
	}

	var errs []error
	listOpt := minio.ListObjectsOptions{Prefix: i.Prefix, Recursive: true}
	for obj := range g.cli.ListObjects(ctx, i.Bucket, listOpt) {
		if obj.Err != nil {
			errs = append(errs, fmt.Errorf("storage: gcp delete prefix list objects %w", obj.Err))
			continue
		}
		if err := g.cli.RemoveObject(ctx, i.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			errs = append(errs, fmt.Errorf("storage: gcp delete prefix remove key: %s %w", obj.Key, err))
		}
	}

	return errors.Join(errs...)
}

func (g *GCPClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	opt := minio.PutObjectOptions{}
	if _, err := g.cli.PutObject(ctx, i.Bucket, i.Key, i.Body, -1, opt); err != nil {
		return fmt.Errorf("storage: gcp upload object %w", err)
	}

	return nil
}
