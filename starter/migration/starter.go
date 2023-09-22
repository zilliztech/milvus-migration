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
	Dumper    *dumper.Dumper
	Loader    *loader.CustomMilvus2xLoader //custom milvus table mapping loader
	Loader_ff *loader.Milvus2xLoader       //fixed milvus collection field(id,data) loader
	//Submitter   *task.ChanTasker
	MigrCfg  *config.MigrationConfig
	WorkMode string
	JobId    string
}

func NewStarter(migrCfg *config.MigrationConfig, jobId string) (*Starter, error) {
	dumper := dumper.NewDumperWithConfig(migrCfg, jobId)
	start := &Starter{
		Dumper: dumper,
		//Loader:   loader,
		MigrCfg:  migrCfg,
		JobId:    jobId,
		WorkMode: migrCfg.DumperWorkCfg.WorkMode,
	}
	if common.DumpMode(migrCfg.DumperWorkCfg.WorkMode) == common.Elasticsearch {
		//管理进度处理器: milvus1x,faiss 走老的进度处理
		gstore.InitProcessHandler(jobId)

		/*
			record: es dump will split many small json file task， finish dump one then load this one file
			milvus1x,faiss： simple handle: all file dump finish then load all file, so not need FileTask
		*/
		gstore.InitFileTask(jobId)

		//set custom collection filed loader
		loader, err := loader.NewCusFieldMilvus2xLoader(migrCfg)
		if err != nil {
			return nil, err
		}
		start.Loader = loader
	} else {
		//set fixed collection filed loader
		loader, err := loader.NewMilvus2xLoader(migrCfg, jobId)
		if err != nil {
			return nil, err
		}
		start.Loader_ff = loader
	}
	return start, nil
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
	case common.Milvus1x:
		return starter.migrationMilvus1x(ctx)
	case common.Faiss:
		return starter.migrationFaiss(ctx)
	default:
		return fmt.Errorf("not support Starter WorkMode %s", starter.WorkMode)
	}
}
