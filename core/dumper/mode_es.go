package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/reader/source"
	"github.com/zilliztech/milvus-migration/core/task"
	esconvert "github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/worker"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

// doDumpInEsMode : dump ES index entry
func (dp *Dumper) doDumpInEsMode(ctx context.Context) error {

	// new meta helper
	metaHelper := meta.NewMetaHelperForDumper(dp.cfg)
	// read meta
	esMetaJson, err := metaHelper.ReadESMeta(ctx)
	if err != nil {
		return err
	}
	gstore.SetTotalTasks(dp.jobId, len(esMetaJson.IdxCfgs))

	// dump write meta.json first for load no need to read meta again
	//err = metaHelper.WriteMetaFile(ctx, esMetaJson)
	//if err != nil {
	//	return err
	//}

	//split index array to split concurrent work group
	splitArray := util.SplitArray(esMetaJson.IdxCfgs, dp.concurLimit)

	log.LL(ctx).Info("dump ES split indexs for concurrent work",
		zap.Int("IndexSize", len(esMetaJson.IdxCfgs)),
		zap.Int("ConcurLimit", dp.concurLimit),
		zap.Int("QueueSize", len(splitArray)),
	)
	dp.cfg.SourceESConfig.Version = esMetaJson.Version

	//create Milvus Loader
	cusFieldLoader, err := loader.NewCusFieldMilvus2xLoader(dp.cfg)
	if err != nil {
		return err
	}

	for _, idxCfgs := range splitArray {
		err = dumpAndLoad(ctx, cusFieldLoader, dp, idxCfgs, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpAndLoad(ctx context.Context, cusFieldLoader *loader.CustomMilvus2xLoader, dp *Dumper, idxCfgs []*estype.IdxCfg, err error) error {

	start := time.Now()
	tasker := task.NewTasker(task.NewBaseLoadTasker(cusFieldLoader, dp.jobId),
		task.NewESInitTasker(idxCfgs))

	var g errgroup.Group
	g.Go(func() error {
		return tasker.Start(ctx)
	})
	g.Go(func() error {
		return tasker.Progress(ctx)
	})

	//wait dump finish first
	err = dp.workESBatch(ctx, idxCfgs, tasker)
	if err != nil {
		return err
	}
	err = g.Wait()
	if err != nil {
		return err
	}
	log.Info("ES Dump&Load Json file finish!!! ", zap.Float64("Cost", time.Since(start).Seconds()))
	return nil
}

func (dp *Dumper) workESBatch(ctx context.Context, idxCfgs []*estype.IdxCfg, tasker *task.Tasker) error {

	start := time.Now()
	var g errgroup.Group
	for i, _ := range idxCfgs {
		finalI := i
		g.Go(func() error {
			return dp.workInESMode(ctx, idxCfgs[finalI], tasker)
		})
	}
	err := g.Wait()
	//dump finished, then close tasker, stop submit task
	tasker.Close()
	log.Info("ES Dump to Json file finish!!! ", zap.Float64("Cost", time.Since(start).Seconds()))
	return err
}

func (dp *Dumper) workInESMode(ctx context.Context, idxCfg *estype.IdxCfg, tasker *task.Tasker) error {
	err := dp.es2Json(ctx, idxCfg, tasker)
	if err != nil {
		return err
	}
	gstore.AddFinishTasks(dp.jobId, 1)
	return nil
}

func (dp *Dumper) es2Json(ctx context.Context, idxCfg *estype.IdxCfg, tasker *task.Tasker) error {

	esSource := source.NewESSource(idxCfg, dp.cfg)
	_, err := esSource.ReadFirst()
	if err != nil {
		return err
	}
	channel := source.NewChannelSource(esSource)

	wokReadCfg := cloneWorkReadConfig(dp.cfg)
	var g errgroup.Group
	for number := 1; number <= common.DUMP_SUB_TASK_NUM; number++ {
		subTaskNumber := number
		g.Go(func() error {
			return dp.es2SubJson(idxCfg, subTaskNumber, wokReadCfg, ctx, channel, tasker)
		})
	}
	err = dp.loopReadESData(esSource)
	if err != nil {
		return err
	}

	return g.Wait()
}

func (dp *Dumper) loopReadESData(esSource *source.ESSource) error {
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

func (dp *Dumper) es2SubJson(idxCfg *estype.IdxCfg, number int, wokReadCfg *config.ReadConfig, ctx context.Context,
	channelSource *source.ChannelSource, tasker *task.Tasker) error {

	wokCfg := cloneWorkConfig(dp.cfg, idxCfg, wokReadCfg)
	collection := esconvert.ToMilvusCollectionName(idxCfg)
	continues := true
	sort := 1
	for continues {
		//sort := gstore.GetFileSort(dp.jobId, collection)
		targetFileName := util.GenerateESDataSubFileName(wokCfg.InnerWriteCfg.FileParam.FileDir,
			number, sort)
		wokCfg.InnerWriteCfg.FileParam.FileFullName = targetFileName

		wok, err := worker.NewDumperWorkerWithChannel(wokCfg, channelSource)
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
			gstore.AddFileTask(dp.jobId, collection, targetFileName)
			tasker.Commit(targetFileName, collection)
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

func cloneWorkConfig(migrationCfg *config.MigrationConfig, idxCfg *estype.IdxCfg,
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

func cloneWorkReadConfig(migrationCfg *config.MigrationConfig) *config.ReadConfig {

	return &config.ReadConfig{
		ReadMode:   migrationCfg.SourceMode,
		ReaderType: common.ES,
		BufSize:    migrationCfg.DumperWorkCfg.ReaderBufferSize,
		//ESSource:   esSource,
		//ESConfig:   migrationCfg.SourceESConfig,
		//ESIdxCfg:   idxCfg,
	}
}
