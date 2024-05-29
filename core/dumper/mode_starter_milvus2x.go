package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/reader/source"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
	"go.uber.org/zap"
)

func (dp *Dumper) InitDumpInMilvus2xMode(ctx context.Context) (*milvus2xtype.CollectionCfg, error) {
	// new meta helper
	metaHelper := meta.NewMetaHelperForDumper(dp.cfg)
	// read meta
	milvus2xMetaJson, err := metaHelper.ReadMilvus2xMeta(ctx)
	if err != nil {
		return nil, err
	}

	log.LL(ctx).Info("dump Milvus2x split collections for concurrent work",
		zap.Int("CollectionSize", len(milvus2xMetaJson.CollCfgs)),
		zap.Int("ConcurLimit", dp.concurLimit),
	)
	dp.cfg.SourceMilvus2xConfig.Version = milvus2xMetaJson.Version
	gstore.SetTotalTasks(dp.jobId, 1)
	return milvus2xMetaJson.CollCfgs[0], err //先只考虑单collection
}

func (dp *Dumper) WorkInMilvus2x(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, dataChannel chan *milvus2x.Milvus2xData) error {
	err := dp.ReadData2Channel(ctx, collCfg, dataChannel)
	if err != nil {
		return err
	}
	gstore.GetProcessHandler(dp.jobId).SetDumpFinished()
	gstore.AddFinishTasks(dp.jobId, 1)
	return nil
}

func (dp *Dumper) ReadData2Channel(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, dataChannel chan *milvus2x.Milvus2xData) error {

	//设置进度相关信息：dump & load 总数量
	gstore.GetProcessHandler(dp.jobId).SetDumpTotalSize(collCfg.Rows)
	gstore.GetProcessHandler(dp.jobId).SetLoadTotalSize(collCfg.Rows)

	source := source.NewMilvus2xSource(collCfg, dp.cfg, dataChannel)
	data, err := source.ReadFirst(ctx)
	if err != nil {
		return err
	}
	gstore.GetProcessHandler(dp.jobId).AddDumpedSize(data.Columns[0].Len(), ctx)

	return dp.LoopReadData(ctx, source)
}

func (dp *Dumper) LoopReadData(ctx context.Context, source *source.Milvus2xSource) error {
	data, err := source.ReadNext(ctx)
	if err != nil {
		return err
	}
	for !data.IsEmpty {

		gstore.GetProcessHandler(dp.jobId).AddDumpedSize(data.Columns[0].Len(), ctx)

		data, err = source.ReadNext(ctx)
		if err != nil {
			return err
		}
	}
	source.Close()
	return nil
}
