package migration

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dumper"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"time"
)

type Starter struct {
	Dumper *dumper.Dumper
	Loader *loader.CustomMilvus2xLoader
	//Submitter   *task.ChanTasker
	MigrCfg  *config.MigrationConfig
	WorkMode string
	JobId    string
}

func NewStarter(migrCfg *config.MigrationConfig, jobId string) (*Starter, error) {
	dumper := dumper.NewDumperWithConfig(migrCfg, jobId)
	loader, err := loader.NewCusFieldMilvus2xLoader(migrCfg)
	if err != nil {
		return nil, err
	}
	return &Starter{
		Dumper:   dumper,
		Loader:   loader,
		MigrCfg:  migrCfg,
		JobId:    jobId,
		WorkMode: migrCfg.DumperWorkCfg.WorkMode,
	}, nil
}

func (starter *Starter) Run(ctx context.Context) error {

	start := time.Now()
	err := starter.doByWorkMode(ctx)
	if err != nil {
		gstore.RecordJobError(starter.JobId, err)
		return err
	}
	log.LL(ctx).Info("[Starter] Migration Success!", zap.Float64("Cost", time.Since(start).Seconds()))
	gstore.RecordJobSuccess(starter.JobId)
	return nil
}

func (starter *Starter) doByWorkMode(ctx context.Context) error {
	switch common.DumpMode(starter.WorkMode) {
	case common.Elasticsearch:
		return starter.migrationES(ctx)
	case common.Milvus2x:
		return starter.migrationMilvus2x(ctx)
	default:
		return fmt.Errorf("not support Starter WorkMode %s", starter.WorkMode)
	}
}
