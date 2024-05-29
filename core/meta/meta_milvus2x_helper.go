package meta

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/check"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

func (this *MetaHelper) ReadMilvus2xMeta(ctx context.Context) (*milvus2xtype.MetaJSON, error) {
	// get meta
	metaJson, err := this.getMilvus2xMeta(ctx)
	if err != nil {
		return nil, err
	}
	if metaJson.CollCfgs == nil || len(metaJson.CollCfgs) == 0 {
		return nil, errors.New("read milvus2x meta collection is empty")
	}

	err = check.VerifyMilvus2xMetaCfg(metaJson)
	if err != nil {
		return nil, err
	}
	return metaJson, err
}

func (this *MetaHelper) getMilvus2xMeta(ctx context.Context) (*milvus2xtype.MetaJSON, error) {

	log.Info("[MetaMilvus2xHelper] begin to get meta, ", zap.String("metaMode", this.metaCfg.MetaMode))
	//only support meta mode = config
	metaJson, err := this.getConfigMilvus2xMeta()
	if err != nil {
		log.Error("[MetaHelper] get milvus2x meta fail", zap.Error(err))
	}
	log.Info("Milvus2x Meta Info", zap.String("version", metaJson.Version))
	for _, CollInfo := range metaJson.CollCfgs {
		log.Info("[Milvus2x Meta Static] Collection", zap.String("collection", CollInfo.Collection))
	}
	return metaJson, err
}

func (this *MetaHelper) getConfigMilvus2xMeta() (*milvus2xtype.MetaJSON, error) {
	return this.metaCfg.Milvus2xMeta, nil
}
