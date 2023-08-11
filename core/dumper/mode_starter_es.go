package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/reader/source"
	esconvert "github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/worker"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
)

func (dp *Dumper) InitDumpInEsMode(ctx context.Context) ([][]*estype.IdxCfg, error) {
	// new meta helper
	metaHelper := meta.NewMetaHelperForDumper(dp.cfg)
	// read meta
	esMetaJson, err := metaHelper.ReadESMeta(ctx)
	if err != nil {
		return nil, err
	}
	gstore.SetTotalTasks(dp.jobId, len(esMetaJson.IdxCfgs))

	splitArray := util.SplitArray(esMetaJson.IdxCfgs, dp.concurLimit)

	log.LL(ctx).Info("dump ES split indexs for concurrent work",
		zap.Int("IndexSize", len(esMetaJson.IdxCfgs)),
		zap.Int("ConcurLimit", dp.concurLimit),
		zap.Int("QueueSize", len(splitArray)),
	)
	dp.cfg.SourceESConfig.Version = esMetaJson.Version
	return splitArray, err
}

func (dp *Dumper) WorkBatchInES(ctx context.Context, idxCfgs []*estype.IdxCfg) error {
	var g errgroup.Group
	for i, _ := range idxCfgs {
		finalI := i
		g.Go(func() error {
			return dp.WorkOneInES(ctx, idxCfgs[finalI])
		})
	}
	return g.Wait()
}

func (dp *Dumper) WorkOneInES(ctx context.Context, idxCfg *estype.IdxCfg) error {
	err := dp.StreamDataInES(ctx, idxCfg)
	if err != nil {
		return err
	}
	gstore.AddFinishTasks(dp.jobId, 1)
	return nil
}

func (dp *Dumper) StreamDataInES(ctx context.Context, idxCfg *estype.IdxCfg) error {

	esSource := source.NewESSource(idxCfg, dp.cfg)
	_, err := esSource.ReadFirst()
	if err != nil {
		return err
	}
	channel := source.NewChannelSource(esSource)
	//设置进度相关信息：dump总数量
	gstore.GetProcessHandler(dp.jobId).SetDumpTotalSize(idxCfg.Rows)

	wokReadCfg := CloneWorkReadConfig(dp.cfg)
	var g errgroup.Group
	//for number := 1; number <= common.DUMP_SUB_TASK_NUM; number++ {
	for number := 1; number <= dp.concurLimit; number++ {
		subNum := number
		g.Go(func() error {
			return dp.SubJsonDataInES(idxCfg, subNum, wokReadCfg, ctx, channel)
		})
	}
	err = dp.LoopReadESStreamData(esSource)
	if err != nil {
		return err
	}

	return g.Wait()
}

func (dp *Dumper) LoopReadESStreamData(esSource *source.ESSource) error {
	data, err := esSource.ReadNext()
	if err != nil {
		return err
	}
	for !data.IsEmpty {
		data, err = esSource.ReadNext()
		if err != nil {
			return err
		}
	}
	esSource.Close()
	return nil
}

func (dp *Dumper) SubJsonDataInES(idxCfg *estype.IdxCfg, number int, wokReadCfg *config.ReadConfig, ctx context.Context, chanSource *source.ChannelSource) error {

	wokCfg := CloneWorkConfig(dp.cfg, idxCfg, wokReadCfg)
	collection := esconvert.ToMilvusCollectionName(idxCfg)
	continues := true
	sort := 1
	for continues {
		//sort := gstore.GetFileSort(dp.jobId, collection)
		targetFileName := util.GenerateESDataSubFileName(wokCfg.InnerWriteCfg.FileParam.FileDir,
			number, sort)
		wokCfg.InnerWriteCfg.FileParam.FileFullName = targetFileName

		wok, err := worker.NewDumperWorkerWithChannel(wokCfg, chanSource)
		if err != nil {
			return err
		}
		log.LL(ctx).Info("Begin to dump sub ES task data to subJson",
			zap.String("Index", idxCfg.Index),
			zap.Int("SubTaskNumber", number),
			zap.Int("Sort", sort),
			zap.Int("ReadBufferSize", wokCfg.InnerReadCfg.BufSize),
			zap.Int("WriteBufferSize", wokCfg.InnerWriteCfg.BufSize),
			zap.String("Source ESUrl", strings.Join(dp.cfg.SourceESConfig.Urls, ",")),
			zap.String("ReadMode", wokCfg.InnerReadCfg.ReadMode),
			zap.String("Target", wokCfg.InnerWriteCfg.FileParam.FileFullName),
			zap.String("TargetMode", wokCfg.InnerWriteCfg.WriteMode))

		// invoke dumper worker to work
		err, response := wok.WorkWithResponse(ctx)
		if err != nil {
			return err
		}
		continues = response.RemainData
		if !response.NoData {
			gstore.GetProcessHandler(dp.jobId).AddDumpedSize(response.FinishDataRows, ctx)
			gstore.AddFileSubTask(dp.jobId, collection, targetFileName)
			dp.Submitter.Commit(targetFileName, collection)
		}

		log.LL(ctx).Info("End to dump sub ES task Data to subJson",
			zap.String("Index", idxCfg.Index),
			zap.Int("SubTaskNumber", number),
			zap.Int("Sort", sort),
			zap.String("Source ESUrl", strings.Join(dp.cfg.SourceESConfig.Urls, ",")),
			zap.String("Target", wokCfg.InnerWriteCfg.FileParam.FileFullName))
		sort++
	}

	log.LL(ctx).Info("End to dump ES subTask Data to json",
		zap.String("Index", idxCfg.Index),
		zap.Int("SubTaskNumber", number),
		zap.String("Source ESUrl", strings.Join(dp.cfg.SourceESConfig.Urls, ",")),
		zap.String("Target", wokCfg.InnerWriteCfg.FileParam.FileFullName))
	return nil
}

func CloneWorkConfig(migrationCfg *config.MigrationConfig, idxCfg *estype.IdxCfg,
	readConfig *config.ReadConfig) config.DumperWorkConfig {

	// target output path
	targetDir := util.GenerateESDataFilePath(migrationCfg.TargetOutputDir,
		idxCfg.Index)

	clonedCfg := config.DumperWorkConfig{
		InnerReadCfg: readConfig,

		InnerWriteCfg: &config.WriteConfig{
			WriteMode: migrationCfg.TargetMode,
			FileParam: &common.FileParam{
				BucketName: migrationCfg.TargetRemote.BucketName,
				FileDir:    targetDir,
			},
			BufSize:      migrationCfg.DumperWorkCfg.WriterBufferSize,
			RemoteConfig: migrationCfg.TargetRemote,
		},
	}
	return clonedCfg
}

func CloneWorkReadConfig(migrationCfg *config.MigrationConfig) *config.ReadConfig {

	return &config.ReadConfig{
		ReadMode:   migrationCfg.SourceMode,
		ReaderType: common.ES,
		BufSize:    migrationCfg.DumperWorkCfg.ReaderBufferSize,
	}
}
