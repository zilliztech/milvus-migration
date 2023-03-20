package writer

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"io"
)

const (
	defaultWriteBufSize = 1024 * 16
)

type Receiver interface {
	Execute(ctx context.Context, r io.Reader) error
}

type BaseWriter struct {
	fileParam common.FileParam
}

func newBaseWriter(fileParam common.FileParam) *BaseWriter {
	return &BaseWriter{
		fileParam: fileParam,
	}
}

func (this *BaseWriter) BucketName() string {
	return this.fileParam.BucketName
}

func (this *BaseWriter) FileName() string {
	return this.fileParam.FileFullName
}

func (this *BaseWriter) FileDir() string {
	return this.fileParam.FileDir
}
