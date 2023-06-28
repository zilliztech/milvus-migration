package migration

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/task"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

func (starter *Starter) migrationES(ctx context.Context) error {
	idxListArray, err := starter.Dumper.InitDumpInEsMode(ctx)
	start := time.Now()
	for _, idxList := range idxListArray {
		err = starter.DumpLoadInES(ctx, idxList)
		if err != nil {
			return err
		}
	}
	log.Info("[Starter] migration ES to Milvus finish!!!", zap.Float64("Cost", time.Since(start).Seconds()))
	return nil
}

func (starter *Starter) DumpLoadInES(ctx context.Context, idxCfgs []*estype.IdxCfg) error {

	chanTasker := task.NewTaskSubmitter(task.NewBaseLoadTasker(starter.Loader, starter.JobId),
		task.NewESInitTasker(idxCfgs))
	starter.Dumper.Submitter = chanTasker

	var g errgroup.Group
	g.Go(func() error {
		return chanTasker.Start(ctx)
	})
	g.Go(func() error {
		return chanTasker.Progress(ctx)
	})

	//will wait dump finish
	err := starter.Dumper.WorkBatchInES(ctx, idxCfgs)
	//dump finish: close chanTasker, stop submit task to loader
	chanTasker.Close()
	if err != nil {
		return err
	}
	return g.Wait()
}
