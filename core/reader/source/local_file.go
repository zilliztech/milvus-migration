package source

import (
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"os"
)

type LocalFileSource struct {
	fileParam *common.FileParam
	file      *os.File
}

func (this *LocalFileSource) GetReader() (io.Reader, error) {
	f, err := os.Open(this.fileParam.FileFullName)
	if err != nil {
		log.Error("Init [Local File Source] error", zap.String("fileName", this.fileParam.FileFullName), zap.Error(err))
		return nil, err
	}

	log.Info("Init [Local File Source] success", zap.String("fileName", this.fileParam.FileFullName))
	this.file = f

	return f, nil
}

func (this *LocalFileSource) Close() error {
	err := this.file.Close()
	if err != nil {
		log.Error("Close [Local File Source] error", zap.String("fileName", this.fileParam.FileFullName))
		return err
	}

	log.Info("Close [Local File Source] success", zap.String("fileName", this.fileParam.FileFullName))
	return nil
}

func NewLocalFileSource(fileParam *common.FileParam) *LocalFileSource {
	s := LocalFileSource{
		fileParam: fileParam,
	}

	return &s
}
