package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/minio/minio-go/v7"
)

var _ Client = (*MinioClient)(nil)

type MinioClient struct {
	cli *minio.Client

	provider Provider
}

func (m *MinioClient) HeadBucket(ctx context.Context, bucket string) error {
	_, err := m.cli.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("storage: %s head bucket %w", m.provider, err)
	}
	return nil
}

func (m *MinioClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	dst := minio.CopyDestOptions{Bucket: i.DestBucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: i.SrcBucket, Object: i.SrcKey}
	if _, err := m.cli.CopyObject(ctx, dst, src); err != nil {
		return fmt.Errorf("storage: %s copy from %s / %s  to %s / %s %w", m.provider, i.SrcKey, i.SrcBucket, i.DestBucket, i.DestKey, err)
	}
	return nil
}

var _ ListObjectsPaginator = (*MinioListObjectPaginator)(nil)

type MinioListObjectPaginator struct {
	cli *MinioClient

	objCh    <-chan minio.ObjectInfo
	pageSize int32
	hasMore  bool
}

func (p *MinioListObjectPaginator) HasMorePages() bool { return p.hasMore }

func (p *MinioListObjectPaginator) NextPage(_ context.Context) (*Page, error) {
	if !p.hasMore {
		return nil, errors.New("storage: gcp no more pages")
	}

	contents := make([]ObjectAttr, 0, p.pageSize)
	for obj := range p.objCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("storage: %s list objs %w", p.cli.provider, obj.Err)
		}
		contents = append(contents, ObjectAttr{Key: obj.Key, Length: obj.Size})
		if len(contents) == int(p.pageSize) {
			return &Page{Contents: contents}, nil
		}
	}
	p.hasMore = false

	return &Page{Contents: contents}, nil
}

func (m *MinioClient) ListObjectsPage(ctx context.Context, i ListObjectPageInput) ListObjectsPaginator {
	pageSize := i.PageSize
	if pageSize == 0 {
		pageSize = _defaultPageSize
	}
	objCh := m.cli.ListObjects(ctx, i.Bucket, minio.ListObjectsOptions{Prefix: i.Prefix, Recursive: true})
	return &MinioListObjectPaginator{cli: m, objCh: objCh, pageSize: pageSize, hasMore: true}
}

func (m *MinioClient) GetObject(ctx context.Context, i GetObjectInput) (*Object, error) {
	obj, err := m.cli.GetObject(ctx, i.Bucket, i.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object %w", m.provider, err)
	}
	attr, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object attr %w", m.provider, err)
	}
	return &Object{Length: attr.Size, Body: obj}, nil
}

func (m *MinioClient) PutObject(ctx context.Context, i PutObjectInput) error {
	if _, err := m.cli.PutObject(ctx, i.Bucket, i.Key, i.Body, i.Length, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("storage: %s put object %w", m.provider, err)
	}

	return nil
}

func (m *MinioClient) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	var errs []error
	for _, key := range i.Keys {
		if err := m.cli.RemoveObject(ctx, i.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (m *MinioClient) DeletePrefix(ctx context.Context, i DeletePrefixInput) error {
	if len(i.Prefix) == 0 {
		panic("empty prefix will delete all files under bucket")
	}

	var errs []error
	listOpt := minio.ListObjectsOptions{Prefix: i.Prefix, Recursive: true}
	for obj := range m.cli.ListObjects(ctx, i.Bucket, listOpt) {
		if obj.Err != nil {
			errs = append(errs, fmt.Errorf("storage: %s delete prefix list objects %w", m.provider, obj.Err))
			continue
		}
		if err := m.cli.RemoveObject(ctx, i.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			errs = append(errs, fmt.Errorf("storage: %s delete prefix remove key: %s %w", m.provider, obj.Key, err))
		}
	}

	return errors.Join(errs...)
}

func (m *MinioClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	opt := minio.PutObjectOptions{}
	if _, err := m.cli.PutObject(ctx, i.Bucket, i.Key, i.Body, -1, opt); err != nil {
		return fmt.Errorf("storage: %s upload object %w", m.provider, err)
	}

	return nil
}
