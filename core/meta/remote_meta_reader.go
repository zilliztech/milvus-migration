package meta

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage"
	"go.uber.org/zap"
)

type RemoteMetaReader struct {
	metaFile string
	client   storage.Client
	cfg      *config.RemoteConfig
}

func NewRemoteMetaReader(cfg *config.RemoteConfig, fileName string) *RemoteMetaReader {
	mr := RemoteMetaReader{
		client:   factory.GetStorageCli(cfg),
		cfg:      cfg,
		metaFile: fileName,
	}

	return &mr
}

func (this *RemoteMetaReader) ReadMeta(ctx context.Context) (*milvustype.MetaJSON, error) {
	log.Info("[RemoteMetaReader] begin to get meta, ",
		zap.String("bucketName", this.cfg.BucketName),
		zap.String("metaFile", this.metaFile))

	i := storage.GetObjectInput{Bucket: this.cfg.BucketName, Key: this.metaFile}
	object, err := this.client.GetObject(ctx, i)
	if err != nil {
		return nil, err
	}

	log.Info("[RemoteMetaReader] read meta data finish ",
		zap.String("bucketName", this.cfg.BucketName),
		zap.String("metaFile", this.metaFile))

	return util.GetMetaCols(object.Body)
}

func (this *RemoteMetaReader) ReadESMeta(ctx context.Context) (*estype.MetaJSON, error) {
	log.Info("[RemoteESMetaReader] begin to get meta, ",
		zap.String("bucketName", this.cfg.BucketName),
		zap.String("metaFile", this.metaFile))

	i := storage.GetObjectInput{Bucket: this.cfg.BucketName, Key: this.metaFile}
	object, err := this.client.GetObject(ctx, i)
	if err != nil {
		return nil, err
	}
	log.Info("[RemoteESMetaReader] read meta data finish ",
		zap.String("bucketName", this.cfg.BucketName),
		zap.String("metaFile", this.metaFile))

	return util.GetESMeta(object.Body)
}
