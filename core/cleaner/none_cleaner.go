package cleaner

import (
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

type NoneCleaner struct {
	Mode string
}

func newNoneCleaner(mode string) *NoneCleaner {
	return &NoneCleaner{Mode: mode}
}

func (this *NoneCleaner) CleanFiles() error {
	log.Info("[None Cleaner] not need clean files", zap.String("mode", this.Mode))
	return nil
}
