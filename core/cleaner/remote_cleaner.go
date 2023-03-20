package cleaner

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage"
	"go.uber.org/zap"
)

type RemoteCleaner struct {
	bucket   string
	rootPath string
	client   storage.Client
}

func newRemoteCleaner(cfg *config.RemoteConfig, rootPath string) *RemoteCleaner {
	return &RemoteCleaner{
		bucket:   cfg.BucketName,
		rootPath: rootPath,
		client:   factory.GetStorageCli(cfg),
	}
}

func (this *RemoteCleaner) CleanFiles() error {
	log.Info("[Remote Cleaner] Begin to clean files",
		zap.String("bucket", this.bucket),
		zap.String("rootPath", this.rootPath))

	if this.rootPath == "" {
		panic("can't clean, rootPath is empty")
	}
	i := storage.DeletePrefixInput{Bucket: this.bucket, Prefix: this.rootPath}
	return this.client.DeletePrefix(context.Background(), i)
}
