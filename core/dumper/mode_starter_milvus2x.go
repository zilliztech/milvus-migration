package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
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

	source := source.NewMilvus2xSource(collCfg, dp.cfg, dataChannel)

	count, err := source.Count(ctx, collCfg)
	if err != nil {
		return err
	}
	collCfg.Rows = count
	//设置进度相关信息：dump & load 总数量
	gstore.GetProcessHandler(dp.jobId).SetDumpTotalSize(collCfg.Rows)
	gstore.GetProcessHandler(dp.jobId).SetLoadTotalSize(collCfg.Rows)

	partitionNames := getPartitionNames(collCfg)
	fieldNames := getIteratorFields(collCfg)
	source.FieldNames = fieldNames
	log.Info("start iterator milvus collection", zap.String("collection", collCfg.Collection),
		zap.Int("BatchSize", source.BatchSize), zap.Int64("CollectionRow", count), zap.Any("PartitionName", partitionNames))
	log.Info("start iterator milvus collection", zap.Any("migration fieldName", fieldNames))
	log.Info("start iterator milvus collection", zap.Any("migration milvusCfg", collCfg.MilvusCfg))
	log.Info("start iterator milvus collection", zap.Any("migration fields", collCfg.Fields))

	for _, partition := range partitionNames {
		source.CurrPartition = partition
		dataIsEmpty, err := dp.readFirstData(ctx, source)
		if err != nil {
			return err
		}
		if dataIsEmpty {
			continue
		}
		err = dp.LoopReadData(ctx, source)
		if err != nil {
			return err
		}
	}
	return source.Close()
}

func getIteratorFields(collCfg *milvus2xtype.CollectionCfg) []string {
	fieldNames := make([]string, 0, len(collCfg.Fields))
	for _, fieldCfg := range collCfg.Fields {
		if collCfg.MilvusCfg.AutoId == "true" && fieldCfg.PK {
			continue
		}
		fieldNames = append(fieldNames, fieldCfg.Name)
	}
	if collCfg.DynamicField {
		fieldNames = append(fieldNames, common.MILVUS_META_FD) //把source 动态列也查出来
	}
	return fieldNames
}

func getPartitionNames(collCfg *milvus2xtype.CollectionCfg) []string {
	partitionNames := make([]string, 0)
	if collCfg.Partitions == nil {
		partitionNames = append(partitionNames, common.EMPTY)
	} else {
		for _, p := range collCfg.Partitions {
			partitionNames = append(partitionNames, p.Name)
		}
	}
	return partitionNames
}

func (dp *Dumper) readFirstData(ctx context.Context, source *source.Milvus2xSource) (bool, error) {
	data, err := source.ReadFirst(ctx)
	if err != nil {
		return false, err
	}
	//某个partition数据为空，返回true
	if data == nil {
		return true, nil
	}
	//已完成dump数量
	gstore.GetProcessHandler(dp.jobId).AddDumpedSize(data.Columns[0].Len(), ctx)
	return false, nil
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
	return nil
}
