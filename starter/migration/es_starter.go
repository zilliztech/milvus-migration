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
	if err != nil {
		return err
	}
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

	submitter := task.NewSubmitter(task.NewBaseLoadTasker(starter.Loader, starter.JobId),
		task.NewESInitTasker(idxCfgs))
	//submitter： dump->load 大任务拆分小任务不断提交
	starter.Dumper.Submitter = submitter

	var g errgroup.Group
	g.Go(func() error {
		return submitter.Start(ctx)
	})
	g.Go(func() error {
		return submitter.Progress(ctx)
	})

	//will wait dump finish
	err := starter.Dumper.WorkBatchInES(ctx, idxCfgs)
	//dump finish: close submitter, stop submit task to loader
	submitter.Close()
	if err != nil {
		return err
	}
	err = g.Wait()

	return err
}
