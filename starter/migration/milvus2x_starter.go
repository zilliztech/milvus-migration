package migration

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/task"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

func (starter *Starter) migrationMilvus2x(ctx context.Context) error {
	collCfg, err := starter.Dumper.InitDumpInMilvus2xMode(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	err = starter.DumpLoadInMilvus2x(ctx, collCfg)
	if err != nil {
		return err
	}

	log.Info("[Starter] migration Milvus2x to Milvus2x finish!!!", zap.Float64("Cost", time.Since(start).Seconds()))
	return nil
}

func (starter *Starter) DumpLoadInMilvus2x(ctx context.Context, collCfg *milvus2xtype.CollectionCfg) error {

	DataChannel := make(chan *milvus2x.Milvus2xData, 200)

	var g errgroup.Group
	g.Go(func() error {
		return starter.loadByBatchInsert(ctx, collCfg, DataChannel)
	})

	g.Go(func() error {
		return starter.dumpByIterator(ctx, collCfg, DataChannel)
	})

	err := g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (starter *Starter) dumpByIterator(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, dataChannel chan *milvus2x.Milvus2xData) error {
	err := starter.Dumper.WorkInMilvus2x(ctx, collCfg, dataChannel)
	if err != nil {
		return err
	}
	close(dataChannel)
	return nil
}

func (starter *Starter) loadByBatchInsert(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, dataChannel chan *milvus2x.Milvus2xData) error {

	initTask := task.NewMilvus2xInitTasker(collCfg, starter.MigrCfg.SourceMilvus2xConfig)
	err := initTask.Init(ctx, starter.Loader)
	if err != nil {
		return err
	}

	for data := range dataChannel {
		err := starter.Loader.WriteByBatchInsert(ctx, collCfg.Collection, data)
		if err != nil {
			return err
		}
		gstore.GetProcessHandler(starter.JobId).AddLoadSize(data.Columns[0].Len(), ctx)
	}
	gstore.GetProcessHandler(starter.JobId).SetLoadFinished()
	return nil
}