package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

const (
	_concurrency = 10
	_blockSize   = 16 << 20
)

type AzureClient struct {
	account string

	cli *azblob.Client

	useIAM bool

	// sasCli is used to generate SAS token.
	// When we want to copy object under two different service accounts, AD auth is not supported.
	// So we need to use AD auth to generate SAS token and use SAS token to copy object.
	sasCli *service.Client
}

func NewAzureClient(cfg Cfg) (*AzureClient, error) {
	endpoint := fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AK)
	var cli *azblob.Client
	var sasCli *service.Client
	if cfg.UseIAM {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure default azure credential %w", err)
		}
		cli, err = azblob.NewClient(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		sasCli, err = service.NewClient(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure service client %w", err)
		}
	} else {
		cred, err := azblob.NewSharedKeyCredential(cfg.AK, cfg.SK)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure shared key credential %w", err)
		}
		cli, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		// sasCli is not used when use shared key auth
	}

	return &AzureClient{account: cfg.AK, useIAM: cfg.UseIAM, cli: cli, sasCli: sasCli}, nil
}

func (a *AzureClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*AzureClient)
	if !ok {
		return fmt.Errorf("storage: azure copy object dest client is not azure client")
	}
	var url string
	// When we want to copy object under two different service accounts, AD auth is not supported.
	if srcCli.useIAM && (srcCli.account != a.account) {
		queryParam, err := a.getSAS(i)
		if err != nil {
			return fmt.Errorf("storage: azure get sas %w", err)
		}
		url = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", srcCli.account, i.SrcBucket, i.SrcKey, queryParam.Encode())
	} else {
		url = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", srcCli.account, i.SrcBucket, i.SrcKey)
	}

	blobCli := a.cli.ServiceClient().NewContainerClient(i.DestBucket).NewBlockBlobClient(i.DestKey)
	blobProperties, err := blobCli.BlobClient().GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: azure get properties %w", err)
	}
	var copyErr error
	if blobProperties.CopyID != nil {
		_, copyErr = blobCli.AbortCopyFromURL(ctx, *blobProperties.CopyID, nil)
	}

	if _, err := blobCli.CopyFromURL(ctx, url, nil); err != nil {
		return fmt.Errorf("storage: azure copy from url %w abort %w", err, copyErr)
	}

	return nil
}

func (a *AzureClient) getSAS(i CopyObjectInput) (sas.QueryParameters, error) {
	srcCli, ok := i.SrcCli.(*AzureClient)
	if !ok {
		return sas.QueryParameters{}, fmt.Errorf("storage: azure copy object dest client is not azure client")
	}

	now := time.Now().UTC().Add(-10 * time.Second)
	expiry := now.Add(48 * time.Hour)
	info := service.KeyInfo{
		Start:  to.Ptr(now.UTC().Format(sas.TimeFormat)),
		Expiry: to.Ptr(expiry.UTC().Format(sas.TimeFormat)),
	}
	udc, err := srcCli.sasCli.GetUserDelegationCredential(context.Background(), info, nil)
	if err != nil {
		return sas.QueryParameters{}, fmt.Errorf("storage: azure get user delegation credential %w", err)
	}
	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(time.Second * -10),
		ExpiryTime:    time.Now().UTC().Add(60 * time.Minute),
		Permissions:   to.Ptr(sas.ContainerPermissions{Read: true, List: true}).String(),
		ContainerName: i.SrcBucket,
	}.SignWithUserDelegation(udc)
	if err != nil {
		return sas.QueryParameters{}, fmt.Errorf("storage: azure sign with user delegation %w", err)
	}

	return sasQueryParams, nil
}

func (a *AzureClient) HeadBucket(ctx context.Context, bucket string) error {
	page := a.cli.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &bucket})
	for page.More() {
		p, err := page.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("storage: azure list containers %w", err)
		}
		for _, c := range p.ContainerItems {
			if *c.Name == bucket {
				return nil
			}
		}
	}

	return fmt.Errorf("storage: azure has no bucket/container %s", bucket)
}

type AzureListObjectPaginator struct {
	p *runtime.Pager[azblob.ListBlobsFlatResponse]
}

func (a *AzureListObjectPaginator) HasMorePages() bool { return a.p.More() }

func (a *AzureListObjectPaginator) NextPage(ctx context.Context) (*Page, error) {
	page, err := a.p.NextPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage: azure read next page %w", err)
	}
	var objAttrs []ObjectAttr
	for _, items := range page.Segment.BlobItems {
		attr := ObjectAttr{Key: *items.Name, ETag: string(*items.Properties.ETag), Length: *items.Properties.ContentLength}
		objAttrs = append(objAttrs, attr)
	}

	return &Page{Contents: objAttrs}, nil
}

func (a *AzureClient) ListObjectsPage(_ context.Context, i ListObjectPageInput) ListObjectsPaginator {
	p := a.cli.NewListBlobsFlatPager(i.Bucket, &azblob.ListBlobsFlatOptions{Prefix: &i.Prefix})
	return &AzureListObjectPaginator{p: p}
}

type AzureReader struct {
	cli    *blockblob.Client
	length int64
	pos    int64
}

func (a *AzureReader) Read(p []byte) (int, error) {
	if a.pos >= a.length {
		return 0, io.EOF
	}
	count := int64(len(p))
	if a.pos+count >= a.length {
		count = a.length - a.pos
	}

	opt := &azblob.DownloadBufferOptions{
		Range:       azblob.HTTPRange{Offset: a.pos, Count: count},
		Concurrency: _concurrency,
		BlockSize:   _blockSize,
	}
	n, err := a.cli.DownloadBuffer(context.Background(), p, opt)
	a.pos += n

	if err != nil {
		return int(n), fmt.Errorf("storage: read azure download buffer %w pos:%d count:%d file-len:%d buff-len:%d", err, a.pos, count, a.length, len(p))
	}

	return int(n), nil
}

func (a *AzureReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = a.pos + offset
	case io.SeekEnd:
		newOffset = a.length + offset
	default:
		return 0, fmt.Errorf("storage: azure seek invalid whence %d", whence)
	}
	a.pos = newOffset

	return newOffset, nil
}

func (a *AzureReader) Close() error { return nil }

func (a *AzureReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= a.length {
		return 0, io.EOF
	}

	if off+int64(len(p)) > a.length {
		p = p[:a.length-off]
	}

	opt := &azblob.DownloadBufferOptions{
		Range:       azblob.HTTPRange{Offset: off, Count: int64(len(p))},
		Concurrency: _concurrency,
		BlockSize:   _blockSize,
	}
	n, err := a.cli.DownloadBuffer(context.Background(), p, opt)
	if err != nil {
		return int(n), fmt.Errorf("storage: read azure download buffer %w file-len:%d buff-len:%d off:%d", err, a.length, len(p), off)
	}

	if n != int64(len(p)) {
		return int(n), io.EOF
	}

	return int(n), nil
}

func (a *AzureClient) GetObject(ctx context.Context, i GetObjectInput) (*Object, error) {
	blobCli := a.cli.ServiceClient().NewContainerClient(i.Bucket).NewBlockBlobClient(i.Key)
	props, err := blobCli.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: azure get properties %w", err)
	}

	return &Object{Body: &AzureReader{cli: blobCli, length: *props.ContentLength}, Length: *props.ContentLength}, nil
}

func (a *AzureClient) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	for _, key := range i.Keys {
		if _, err := a.cli.DeleteBlob(ctx, i.Bucket, key, nil); err != nil {
			return fmt.Errorf("storage: azure delete object %w", err)
		}
	}

	return nil
}

func (a *AzureClient) DeletePrefix(ctx context.Context, i DeletePrefixInput) error {
	if len(i.Prefix) == 0 {
		panic("empty prefix will delete all files under bucket")
	}

	page := a.ListObjectsPage(ctx, ListObjectPageInput{Bucket: i.Bucket, Prefix: i.Prefix})
	for page.HasMorePages() {
		p, err := page.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("storage: azure list objects %w", err)
		}
		var keys []string
		for _, attr := range p.Contents {
			keys = append(keys, attr.Key)
		}
		if err := a.DeleteObjects(ctx, DeleteObjectsInput{Bucket: i.Bucket, Keys: keys}); err != nil {
			return fmt.Errorf("storage: azure delete objects %w", err)
		}
	}

	return nil
}

func (a *AzureClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	opt := &azblob.UploadStreamOptions{Concurrency: _concurrency, BlockSize: _blockSize}
	if _, err := a.cli.UploadStream(ctx, i.Bucket, i.Key, i.Body, opt); err != nil {
		return fmt.Errorf("storage: azure upload stream %w", err)
	}

	return nil
}

func (a *AzureClient) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	resp, err := a.cli.ServiceClient().NewContainerClient(bucket).NewBlobClient(key).
		GetProperties(ctx, nil)
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: azure get properties %w", err)
	}

	return ObjectAttr{Key: key, ETag: string(*resp.ETag), Length: *resp.ContentLength}, nil
}
