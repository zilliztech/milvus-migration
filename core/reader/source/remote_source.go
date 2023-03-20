package source

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

type RemoteSource struct {
	client    storage.Client
	ctx       context.Context
	cfg       *config.RemoteConfig
	fileParam common.FileParam
	body      io.ReadCloser
}

func (this *RemoteSource) GetReader() (io.Reader, error) {
	i := storage.GetObjectInput{Bucket: this.fileParam.BucketName, Key: this.fileParam.FileFullName}
	object, err := this.client.GetObject(context.Background(), i)
	if err != nil {
		log.Error("Init [File Source] error",
			zap.String("bucket", this.fileParam.BucketName),
			zap.String("file", this.fileParam.FileFullName),
			zap.Error(err))
		return nil, err
	}

	log.Info("Init [S3 File Source] success", zap.String("fileName", this.fileParam.FileFullName))
	this.body = object.Body
	return object.Body, nil
}

func (this *RemoteSource) Close() error {
	return this.body.Close()
}

func NewRemoteSource(fileParam *common.FileParam, cfg *config.RemoteConfig) *RemoteSource {
	s := RemoteSource{
		client:    factory.GetStorageCli(cfg),
		ctx:       context.Background(),
		cfg:       cfg,
		fileParam: *fileParam,
	}

	return &s
}
