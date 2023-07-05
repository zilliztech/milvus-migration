package dumper

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/data"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/task"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"time"
)

// Dumper : do the dump work
type Dumper struct {
	cfg         *config.MigrationConfig
	concurLimit int
	workMode    string

	// runtime data
	jobId          string
	Submitter      *task.Submitter
	ProcessHandler *data.ProcessHandler
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

// doDumpByWorkMode ：start dump task entry in different modes
func (this *Dumper) doDumpByWorkMode(ctx context.Context) error {
	switch common.DumpMode(this.workMode) {
	//case common.Elasticsearch:
	//	return this.doDumpInEsMode(ctx)
	case common.Milvus1x:
		return this.doDumpInMilvus1xMode(ctx)
	case common.Faiss:
		return this.doDumpInFaissMode(ctx)
	default:
		return fmt.Errorf("not support workMode %s", this.workMode)
	}
}
