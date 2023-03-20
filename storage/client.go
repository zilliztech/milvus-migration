package storage

import (
	"context"
	"fmt"
	"io"
)

type Provider string

const (
	AWS     Provider = "aws"
	GCP     Provider = "gcp"
	unknown Provider = "unknown"
)

var _providerMap = map[string]Provider{"aws": AWS, "gcp": GCP}

func ParseProvider(s string) Provider {
	if p, ok := _providerMap[s]; ok {
		return p
	}

	return unknown
}

type Client interface {
	// CopyObject copy an object to another bucket
	CopyObject(ctx context.Context, i CopyObjectInput) error
	// HeadBucket determine if a bucket exists, and you have permission to access it
	HeadBucket(ctx context.Context, bucket string) error
	// ListObjectsPage paginate list of all objects
	ListObjectsPage(ctx context.Context, i ListObjectPageInput) ListObjectsPaginator
	// GetObject get an object
	GetObject(ctx context.Context, i GetObjectInput) (*Object, error)
	// PutObject put an object
	PutObject(ctx context.Context, i PutObjectInput) error
	// DeleteObjects delete an object
	DeleteObjects(ctx context.Context, i DeleteObjectsInput) error
	// DeletePrefix delete all object under the prefix
	DeletePrefix(ctx context.Context, i DeletePrefixInput) error
	// UploadObject stream upload an object
	UploadObject(ctx context.Context, i UploadObjectInput) error
}

func NewClient(cfg Cfg) (Client, error) {
	switch cfg.Provider {
	case AWS:
		return NewS3Client(cfg)
	case GCP:
		return NewGCPClient(cfg)
	default:
		return nil, fmt.Errorf("storage: unknown provide %s", cfg.Provider)
	}
}

type Cfg struct {
	Endpoint string
	Provider Provider
	AK       string
	SK       string
	Region   string
	UseSSL   bool
	UseIAM   bool
}

type CreateBucketInput struct {
	Bucket string
}

type CopyObjectInput struct {
	SrcBucket string
	SrcKey    string

	DestBucket string
	DestKey    string
}

type ListObjectPageInput struct {
	// The name of the bucket
	Bucket string
	// The size of per page, default is 1000
	PageSize int32
	// Limits the response to keys that begin with the specified prefix
	Prefix string
}

type GetObjectInput struct {
	Bucket string
	Key    string
}

type PutObjectInput struct {
	Bucket string
	Key    string
	Length int64
	Body   io.Reader
}

type DeleteObjectsInput struct {
	Bucket string
	Keys   []string
}

type DeletePrefixInput struct {
	Bucket string
	Prefix string
}

type UploadObjectInput struct {
	Bucket string
	Key    string
	Body   io.Reader

	WorkerNum int
	RPS       int32
}

type Object struct {
	Length int64
	Body   io.ReadCloser
}

type ObjectAttr struct {
	Key    string
	Length int64
}

type Page struct {
	Contents []ObjectAttr
}

type ListObjectsPaginator interface {
	HasMorePages() bool
	NextPage(ctx context.Context) (*Page, error)
}
