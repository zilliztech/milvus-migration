package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/worker"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
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
	err = metaHelper.WriteMetaFile(ctx, esMetaJson)
	if err != nil {
		return err
	}
	//split index array to split concurrent work group
	splitArray := util.SplitArray(esMetaJson.IdxCfgs, dp.concurLimit)

	log.LL(ctx).Info("dump ES split indexs for concurrent work",
		zap.Int("IndexSize", len(esMetaJson.IdxCfgs)),
		zap.Int("ConcurLimit", dp.concurLimit),
		zap.Int("QueueSize", len(splitArray)),
	)

	for _, idxInfos := range splitArray {
		err := dp.workESBatch(ctx, idxInfos, esMetaJson)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dp *Dumper) workESBatch(ctx context.Context, idxCfgs []*estype.IdxCfg, metaJson *estype.MetaJSON) error {
	var g errgroup.Group
	for i, _ := range idxCfgs {
		finalI := i
		g.Go(func() error {
			return dp.workInESMode(ctx, idxCfgs[finalI], metaJson)
		})
	}
	return g.Wait()
}

func (dp *Dumper) workInESMode(ctx context.Context, idxInfo *estype.IdxCfg, metaJson *estype.MetaJSON) error {
	err := dp.es2Json(ctx, idxInfo, metaJson)
	if err != nil {
		return err
	}
	gstore.AddFinishTasks(dp.jobId, 1)
	return nil
}

func (dp *Dumper) es2Json(ctx context.Context, idxInfo *estype.IdxCfg, metaJson *estype.MetaJSON) error {

	wokCfg := cloneWorkConfig(dp.cfg, metaJson, idxInfo)

	wok, err := worker.NewDumperWorker(wokCfg)
	if err != nil {
		return err
	}
	log.LL(ctx).Info("Begin to dump ES data to json",
		zap.String("Index", wokCfg.InnerReadCfg.ESIdxCfg.Index),
		zap.Int("ReadBufferSize", wokCfg.InnerReadCfg.BufSize),
		zap.Int("WriteBufferSize", wokCfg.InnerWriteCfg.BufSize),
		zap.String("Source ESUrl", strings.Join(wokCfg.InnerReadCfg.ESConfig.Urls, ",")),
		zap.String("ReadMode", wokCfg.InnerReadCfg.ReadMode),
		zap.String("Target", wokCfg.InnerWriteCfg.FileParam.FileFullName),
		zap.String("TargetMode", wokCfg.InnerWriteCfg.WriteMode))

	// invoke dumper worker to work
	err = wok.Work(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("End to dump ES Data to json",
		zap.String("Source ESUrl", strings.Join(wokCfg.InnerReadCfg.ESConfig.Urls, ",")),
		zap.String("Target", wokCfg.InnerWriteCfg.FileParam.FileFullName))
	return nil
}

func cloneWorkConfig(migrationCfg *config.MigrationConfig, esMeta *estype.MetaJSON, idxCfg *estype.IdxCfg) config.DumperWorkConfig {

	migrationCfg.SourceESConfig.Version = esMeta.Version

	// target output path
	targetDir, targetFileName := util.GenerateESDataFilePath(migrationCfg.TargetOutputDir,
		idxCfg.Index)

	clonedCfg := config.DumperWorkConfig{
		InnerReadCfg: &config.ReadConfig{
			ReadMode:   migrationCfg.SourceMode,
			ReaderType: common.ES,
			BufSize:    migrationCfg.DumperWorkCfg.ReaderBufferSize,
			ESConfig:   migrationCfg.SourceESConfig,
			ESIdxCfg:   idxCfg,
		},

		InnerWriteCfg: &config.WriteConfig{
			WriteMode: migrationCfg.TargetMode,
			FileParam: &common.FileParam{
				BucketName:   migrationCfg.TargetRemote.BucketName,
				FileDir:      targetDir,
				FileFullName: targetFileName,
			},
			BufSize:      migrationCfg.DumperWorkCfg.WriterBufferSize,
			RemoteConfig: migrationCfg.TargetRemote,
		},
	}
	return clonedCfg
}
