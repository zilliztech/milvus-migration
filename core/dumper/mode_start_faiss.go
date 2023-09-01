package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/worker"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (this *Dumper) StartDoDumpInFaissMode(ctx context.Context) error {
	var g errgroup.Group

	g.Go(func() error {
		return startFaissId2numpy(ctx, this.cfg)
	})
	g.Go(func() error {
		return startFaissData2numpy(ctx, this.cfg)
	})

	err := g.Wait()
	if err != nil {
		return err
	}

	//gstore.AddFinishTasks(this.jobId, 1)
	return nil
}

func startFaissId2numpy(ctx context.Context, insCfg *config.MigrationConfig) error {

	// source
	sourceFilePath := insCfg.SourceFaissFile

	// target
	targetDir, targetFileName := util.GenerateFaissIdFilePath(insCfg.TargetOutputDir,
		insCfg.LoaderWorkCfg.CreateColCfg.CollectionName)

	wokCfg := insCfg.DumperWorkCfg

	cfg := config.DumperWorkConfig{
		InnerReadCfg: &config.ReadConfig{
			ReadMode: insCfg.SourceMode,
			FileParam: &common.FileParam{
				FileFullName: sourceFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			ReaderType:   "faiss-id",
			BufSize:      wokCfg.ReaderBufferSize,
			RemoteConfig: insCfg.SourceRemote,
		},

		InnerWriteCfg: &config.WriteConfig{
			WriteMode: insCfg.TargetMode,
			FileParam: &common.FileParam{
				FileDir:      targetDir,
				FileFullName: targetFileName,
				BucketName:   insCfg.TargetRemote.BucketName,
			},
			BufSize:      wokCfg.WriterBufferSize,
			RemoteConfig: insCfg.TargetRemote,
		},
	}

	wrk, err := worker.NewDumperWorker(cfg)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("Begin to dump faiss ids to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName),
		zap.String("readMode", insCfg.SourceMode), zap.String("writeMode", insCfg.TargetMode))

	// work
	err = wrk.Work(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("End to dump faiss ids to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName))
	return nil
}

func startFaissData2numpy(ctx context.Context, insCfg *config.MigrationConfig) error {

	// source
	sourceFilePath := insCfg.SourceFaissFile

	// target
	targetDir, targetFileName := util.GenerateFaissDataFilePath(insCfg.TargetOutputDir,
		insCfg.LoaderWorkCfg.CreateColCfg.CollectionName)

	wokCfg := insCfg.DumperWorkCfg

	cfg := config.DumperWorkConfig{
		InnerReadCfg: &config.ReadConfig{
			ReadMode: insCfg.SourceMode,
			FileParam: &common.FileParam{
				FileFullName: sourceFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			ReaderType:   "faiss-data",
			BufSize:      wokCfg.ReaderBufferSize,
			RemoteConfig: insCfg.SourceRemote,
		},

		InnerWriteCfg: &config.WriteConfig{
			WriteMode: insCfg.TargetMode,
			FileParam: &common.FileParam{
				FileDir:      targetDir,
				FileFullName: targetFileName,
				BucketName:   insCfg.TargetRemote.BucketName,
			},
			BufSize:      wokCfg.WriterBufferSize,
			RemoteConfig: insCfg.TargetRemote,
		},
	}

	wrk, err := worker.NewDumperWorker(cfg)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("Begin to dump faiss datas to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName),
		zap.String("readMode", insCfg.SourceMode), zap.String("writeMode", insCfg.TargetMode))

	// work
	err = wrk.Work(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("End to dump faiss datas to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName))
	return nil
}
