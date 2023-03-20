package dumper

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"time"
)

type Dumper struct {
	cfg         *config.MigrationConfig
	concurLimit int
	workMode    string

	// runtime data
	jobId string
}

func NewDumperWithConfig(cfg *config.MigrationConfig, jobId string) *Dumper {
	ret := &Dumper{
		cfg:         cfg,
		concurLimit: cfg.DumperWorkLimit,
		jobId:       jobId,
		workMode:    cfg.DumperWorkCfg.WorkMode,
	}

	return ret
}

func (this *Dumper) Run(ctx context.Context) error {

	start := time.Now()

	err := this.doDumpByWorkMode(ctx)
	if err != nil {
		gstore.RecordJobError(this.jobId, err)
		return err
	}

	log.LL(ctx).Info("[Dumper] Dump Success!", zap.Float64("Cost", time.Since(start).Seconds()))
	gstore.RecordJobSuccess(this.jobId)
	return nil
}

func (this *Dumper) doDumpByWorkMode(ctx context.Context) error {
	switch this.workMode {
	case "milvus1x":
		return this.doDumpInMilvus1xMode(ctx)
	case "faiss":
		return this.doDumpInFaissMode(ctx)
	default:
		return fmt.Errorf("not support workMode %s", this.workMode)
	}
}
