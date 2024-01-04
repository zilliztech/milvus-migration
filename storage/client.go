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
	ALI     Provider = "ali"
	AZURE   Provider = "azure"
	unknown Provider = "unknown"
)

const _defaultPageSize = 1000

var _providerMap = map[string]Provider{"aws": AWS, "gcp": GCP, "ali": ALI, "azure": AZURE, "az": AZURE}

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
	// HeadObject determine if an object exists, and you have permission to access it.
	HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error)
	// ListObjectsPage paginate list of all objects
	ListObjectsPage(ctx context.Context, i ListObjectPageInput) ListObjectsPaginator
	// GetObject get an object
	GetObject(ctx context.Context, i GetObjectInput) (*Object, error)
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
		return NewAWSClient(cfg)
	case GCP:
		return NewGCPClient(cfg)
	case ALI:
		return NewAliyunClient(cfg)
	case AZURE:
		return NewAzureClient(cfg)
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
	SrcCli Client

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

	// The size of the file to be uploaded, if unknown, set to 0 or negative
	// Configuring this parameter can help reduce memory usage.
	Size int64
}

type SeekableReadCloser interface {
	io.ReaderAt
	io.Seeker
	io.ReadCloser
}

type Object struct {
	Length int64
	Body   SeekableReadCloser
}

type ObjectAttr struct {
	Key    string
	Length int64

	// The documentation for s3 says, ETag may NOT be an MD5 digest of the object data.
	ETag string
}

// SameAs returns true if two ObjectAttr are the same.
// If two ObjectAttr have same length and ETag, they are considered.
func (o *ObjectAttr) SameAs(other ObjectAttr) bool {
	return o.Length == other.Length && o.ETag == other.ETag
}

func (o *ObjectAttr) IsEmpty() bool { return o.Length == 0 }

type Page struct {
	Contents []ObjectAttr
}

type ListObjectsPaginator interface {
	HasMorePages() bool
	NextPage(ctx context.Context) (*Page, error)
}
