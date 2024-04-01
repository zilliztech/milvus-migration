package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
		contents = append(contents, ObjectAttr{Key: obj.Key, Length: obj.Size, ETag: obj.ETag})
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

func (m *MinioClient) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	wp, err := NewWorkerPool(ctx, 10, 20, 3)
	if err != nil {
		return fmt.Errorf("storage: %s delete prefix new worker pool %w", m.provider, err)
	}
	wp.Start()

	for _, key := range i.Keys {
		job := func(ctx context.Context) error {
			// we can't use RemoveObjects, because gcp doesn't support batch delete
			if err := m.cli.RemoveObject(ctx, i.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
				if !strings.Contains(err.Error(), "The specified key does not exist") {
					// if key not exist, we can ignore it
					return fmt.Errorf("storage: %s delete prefix remove key: %s %w", m.provider, key, err)
				}
			}

			return nil
		}
		wp.Submit(job)
	}
	wp.Done()

	if err := wp.Wait(); err != nil {
		return fmt.Errorf("storage: %s delete prefix wait worker pool %w", m.provider, err)
	}

	return nil
}

func (m *MinioClient) DeletePrefix(ctx context.Context, i DeletePrefixInput) error {
	if len(i.Prefix) == 0 {
		panic("empty prefix will delete all files under bucket")
	}

	listOpt := minio.ListObjectsOptions{Prefix: i.Prefix, Recursive: true}
	var keys []string
	for obj := range m.cli.ListObjects(ctx, i.Bucket, listOpt) {
		if obj.Err != nil {
			return fmt.Errorf("storage: %s delete prefix list objects %w", m.provider, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	if len(keys) == 0 {
		return nil
	}
	if err := m.DeleteObjects(ctx, DeleteObjectsInput{Bucket: i.Bucket, Keys: keys}); err != nil {
		return fmt.Errorf("storage: %s delete prefix delete objects %w", m.provider, err)
	}

	return nil
}

func (m *MinioClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	opt := minio.PutObjectOptions{}
	size := int64(-1)
	if i.Size > 0 {
		size = i.Size
	}
	if _, err := m.cli.PutObject(ctx, i.Bucket, i.Key, i.Body, size, opt); err != nil {
		return fmt.Errorf("storage: %s upload object %w", m.provider, err)
	}

	return nil
}

func (m *MinioClient) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	attr, err := m.cli.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s head object %w", m.provider, err)
	}

	return ObjectAttr{Key: attr.Key, Length: attr.Size, ETag: attr.ETag}, nil
}
