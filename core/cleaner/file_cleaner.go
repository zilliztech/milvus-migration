package cleaner

import (
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"os"
)

type FileCleaner struct {
	baseDir string
}

func newFileCleaner(baseDir string) *FileCleaner {
	return &FileCleaner{
		baseDir: baseDir,
	}
}

func (this *FileCleaner) CleanFiles() error {
	log.Info("[Local Cleaner] Begin to clean files",
		zap.String("baseDir", this.baseDir))

	if this.baseDir == "" {
		panic("can't clean, baseDir is empty")
	}
	return os.RemoveAll(this.baseDir)
}
