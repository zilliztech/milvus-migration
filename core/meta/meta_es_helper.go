package meta

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/check"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"os"
)

func (this *MetaHelper) ReadESMeta(ctx context.Context) (*estype.MetaJSON, error) {
	// get meta
	metaJson, err := this.getESMeta(ctx)
	if err != nil {
		return nil, err
	}
	if metaJson.IdxCfgs == nil || len(metaJson.IdxCfgs) == 0 {
		return nil, errors.New("read es meta index is empty")
	}

	err = check.VerifyESMetaCfg(metaJson)
	if err != nil {
		return nil, err
	}

	return metaJson, err
}

func (this *MetaHelper) getESMeta(ctx context.Context) (*estype.MetaJSON, error) {

	log.Info("[MetaESHelper] begin to get meta, ", zap.String("metaMode", this.metaCfg.MetaMode))

	var metaJson *estype.MetaJSON
	var err error
	switch this.metaCfg.MetaMode {
	case "mock", "local":
		metaJson, err = this.getMockESMeta()
	case "sqlite":
		metaJson, err = NewSqliteMetaReader(this.metaCfg.LocalSqliteFile).GetESMeta(ctx)
	case "mysql":
		metaJson, err = NewMysqlMetaReader(this.metaCfg.LocalMysqlURL).GetESMeta(ctx)
	case "remote":
		metaJson, err = this.getRemoteESMeta(ctx)
	default:
		return nil, errors.New("not support meteMode=" + this.metaCfg.MetaMode)
	}
	if err != nil {
		log.Error("[MetaHelper] get es meta fail", zap.Error(err))
	}
	log.Info("ES Meta Info", zap.String("version", metaJson.Version))
	for _, IdxInfo := range metaJson.IdxCfgs {
		log.Info("[ES Meta Static] Index", zap.String("index", IdxInfo.Index))
	}
	return metaJson, err
}

func (this *MetaHelper) getRemoteESMeta(ctx context.Context) (*estype.MetaJSON, error) {

	log.Info("[ESMetaHelper] begin to get remote meta, ",
		zap.String("remoteCloud", this.readRemoteCfg.Cloud),
		zap.String("remoteRegion", this.readRemoteCfg.Region),
		zap.String("remoteBucket", this.readRemoteCfg.BucketName),
		zap.String("remoteMetafile", this.metaCfg.RemoteMetaFile))
	reader := NewRemoteMetaReader(this.readRemoteCfg, this.metaCfg.RemoteMetaFile)
	return reader.ReadESMeta(ctx)
}

func (this *MetaHelper) getMockESMeta() (*estype.MetaJSON, error) {
	filePath := this.metaCfg.LocalMockFile
	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Open mock es meta file error", zap.String("metaFile", filePath), zap.Error(err))
		return nil, err
	}
	defer file.Close()
	return util.GetESMeta(file)
}
