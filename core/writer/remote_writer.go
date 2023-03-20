package writer

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage"
	"go.uber.org/zap"
	"io"
)

type RemoteWriter struct {
	BaseWriter
	client storage.Client
}

func NewRemoteWriter(cfg *config.RemoteConfig, fileParam *common.FileParam) *RemoteWriter {
	bw := newBaseWriter(*fileParam)

	// check storage
	factory.GetStorageCli(cfg)

	return &RemoteWriter{
		BaseWriter: *bw,
		client:     factory.GetStorageCli(cfg),
	}
}

func (this *RemoteWriter) Execute(ctx context.Context, r io.Reader) error {
	log.Info("[Remote Uploader] Begin to upload file",
		zap.String("bucketName", this.BucketName()),
		zap.String("fileName", this.FileName()))

	i := storage.UploadObjectInput{
		Bucket:    this.BucketName(),
		Key:       this.FileName(),
		Body:      r,
		WorkerNum: 10,
		RPS:       1000,
	}
	return this.client.UploadObject(ctx, i)
}
