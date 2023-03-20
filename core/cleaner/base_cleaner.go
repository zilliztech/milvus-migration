package cleaner

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/core/config"
)

type Cleaner struct {
	jobId   string
	cleaner Clean
}

type Clean interface {
	CleanFiles() error
}

func NewCleaner(cfg *config.MigrationConfig, jobId string) (*Cleaner, error) {
	var clr Clean

	switch cfg.TargetMode {
	case "local":
		clr = newFileCleaner(cfg.TargetOutputDir)
	case "remote":
		clr = newRemoteCleaner(cfg.TargetRemote, cfg.TargetOutputDir)
	default:
		return nil, fmt.Errorf("not support targetMode %s", cfg.TargetMode)
	}

	return &Cleaner{
		jobId:   jobId,
		cleaner: clr,
	}, nil
}

func (this *Cleaner) ClenFiles() error {
	return this.cleaner.CleanFiles()
}
