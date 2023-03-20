package writer

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"os"
)

type FileWriter struct {
	BaseWriter
	bufSize int
	buf     []byte
}

func NewFileWriter(cfg *config.WriteConfig, fileParam *common.FileParam) *FileWriter {
	bw := newBaseWriter(*fileParam)

	return &FileWriter{
		BaseWriter: *bw,
		bufSize:    cfg.BufSize,
		buf:        make([]byte, cfg.BufSize),
	}
}

func NewDefaultFileWriter(fileParam common.FileParam) *FileWriter {
	bw := newBaseWriter(fileParam)

	return &FileWriter{
		BaseWriter: *bw,
		bufSize:    defaultWriteBufSize,
		buf:        make([]byte, defaultWriteBufSize),
	}
}

func (this *FileWriter) Execute(_ context.Context, r io.Reader) error {
	log.Info("[File Writer] begin to checkNeedCreateDir", zap.String("fileDir", this.FileDir()))
	err := checkNeedCreateDir(this.FileDir())
	if err != nil {
		log.Error("[File Writer] create directory error", zap.Error(err))
		return err
	}

	log.Info("[File Writer] begin to createFile", zap.String("fileName", this.FileName()))
	file, err := os.Create(this.FileName())
	if err != nil {
		log.Error("[File Writer] create file error", zap.Error(err))
		return err
	}

	_, err = io.Copy(file, r)
	if err != nil {
		log.Error("[File Writer] copy file error", zap.Error(err))
		return err
	}

	err = file.Close()
	if err != nil {
		log.Error("[File Writer] close file error", zap.Error(err))
		return err
	}

	return nil
}

func checkNeedCreateDir(targetDir string) error {
	err := os.MkdirAll(targetDir, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
