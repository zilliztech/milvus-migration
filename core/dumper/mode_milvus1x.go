package dumper

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/worker"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (this *Dumper) doDumpInMilvus1xMode(ctx context.Context) error {
	// new meta helper
	metaHelper := meta.NewMetaHelperForDumper(this.cfg)

	// read meta
	metaJson, err := metaHelper.ReadMeta(ctx)
	metaCols := metaJson.GetAllSegments()
	if err != nil {
		return err
	}
	gstore.SetTotalTasks(this.jobId, len(metaCols))

	// dump write meta.json first for load no need to read meta again
	err = metaHelper.WriteMetaFile(ctx, metaJson)
	if err != nil {
		return err
	}

	// migration data
	splitArray := util.SplitArray(metaCols, this.concurLimit)
	for _, arr := range splitArray {
		err := this.workBatch(ctx, arr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *Dumper) workBatch(ctx context.Context, segColInfos []milvustype.SegColInfo) error {
	var g errgroup.Group
	for _, col := range segColInfos {
		var finalCol = col
		g.Go(func() error {
			return this.workInMilvus1xMode(ctx, finalCol)
		})
	}

	return g.Wait()
}

func (this *Dumper) workInMilvus1xMode(ctx context.Context, segColInfo milvustype.SegColInfo) error {
	var g errgroup.Group

	g.Go(func() error {
		return rv2numpy(ctx, this.cfg, segColInfo)
	})
	g.Go(func() error {
		return uid2numpy(ctx, this.cfg, segColInfo)
	})

	err := g.Wait()
	if err != nil {
		return err
	}

	gstore.AddFinishTasks(this.jobId, 1)
	return nil
}

func uid2numpy(ctx context.Context, insCfg *config.MigrationConfig, segColInfo milvustype.SegColInfo) error {

	// source
	sourceFilePath := util.GetSourceUIDFilePath(insCfg.SourceTablesDir, &segColInfo)
	deleteFilePath := util.GetSourceDeletedDocsFilePath(insCfg.SourceTablesDir, &segColInfo)

	// target
	targetDir, targetFileName := util.GetOutputUIDFilePath(insCfg.TargetOutputDir, &segColInfo)

	wokCfg := insCfg.DumperWorkCfg

	cfg := config.DumperWorkConfig{
		InnerReadCfg: &config.ReadConfig{
			ReadMode: insCfg.SourceMode,
			FileParam: &common.FileParam{
				FileFullName: sourceFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			DeleteFile: &common.FileParam{
				FileFullName: deleteFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			ReaderType:   "uid",
			BufSize:      wokCfg.ReaderBufferSize,
			Dim:          0,
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

	log.LL(ctx).Info("Begin to dump uid file to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName),
		zap.String("readMode", insCfg.SourceMode), zap.String("writeMode", insCfg.TargetMode))

	// work
	err = wrk.Work(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("End to dump uid file to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName))
	return nil
}

func rv2numpy(ctx context.Context, insCfg *config.MigrationConfig, segColInfo milvustype.SegColInfo) error {

	// source
	sourceFilePath := util.GetSourceRVFilePath(insCfg.SourceTablesDir, &segColInfo)
	deleteFilePath := util.GetSourceDeletedDocsFilePath(insCfg.SourceTablesDir, &segColInfo)

	// target
	targetDir, targetFileName := util.GetOutputRVFilePath(insCfg.TargetOutputDir, &segColInfo)

	wokCfg := insCfg.DumperWorkCfg

	cfg := config.DumperWorkConfig{
		InnerReadCfg: &config.ReadConfig{
			ReadMode: insCfg.SourceMode,
			FileParam: &common.FileParam{
				FileFullName: sourceFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			DeleteFile: &common.FileParam{
				FileFullName: deleteFilePath,
				BucketName:   insCfg.SourceRemote.BucketName,
			},
			ReaderType:   "rv",
			BufSize:      wokCfg.ReaderBufferSize,
			Dim:          segColInfo.Dim,
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

	log.LL(ctx).Info("Begin to dump rv file to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName),
		zap.String("readMode", insCfg.SourceMode), zap.String("writeMode", insCfg.TargetMode))

	// work
	err = wrk.Work(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("End to dump rv file to numpy",
		zap.String("Source", sourceFilePath), zap.String("Target", targetFileName))
	return nil
}
