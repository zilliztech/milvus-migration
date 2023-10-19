package storage

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type AzureClient struct {
	account string

	cli *azblob.Client
}

func NewAzureClient(cfg Cfg) (*AzureClient, error) {
	endpoint := fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AK)
	var cli *azblob.Client
	if cfg.UseIAM {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure default azure credential %w", err)
		}
		cli, err = azblob.NewClient(endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
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
	}

	return &AzureClient{account: cfg.AK, cli: cli}, nil
}

func (a *AzureClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*AzureClient)
	if !ok {
		return fmt.Errorf("storage: azure copy object dest client is not azure client")
	}

	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", srcCli.account, i.SrcBucket, i.SrcKey)
	_, err := a.cli.ServiceClient().
		NewContainerClient(i.DestBucket).
		NewBlobClient(i.DestKey).
		StartCopyFromURL(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("storage: azure start copy from url %w", err)
	}

	return nil
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

func (a *AzureClient) GetObject(ctx context.Context, i GetObjectInput) (*Object, error) {
	resp, err := a.cli.DownloadStream(ctx, i.Bucket, i.Key, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: azure download stream %w", err)
	}

	return &Object{Body: resp.Body, Length: *resp.ContentLength}, nil
}

func (a *AzureClient) DeleteObjects(ctx context.Context, i DeleteObjectsInput) error {
	for _, key := range i.Keys {
		_, err := a.cli.DeleteBlob(ctx, i.Bucket, key, nil)
		if err != nil {
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
	if _, err := a.cli.UploadStream(ctx, i.Bucket, i.Key, i.Body, nil); err != nil {
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
