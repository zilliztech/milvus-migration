package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
)

type Milvus2xInitTasker struct {
	CollCfgs        *milvus2xtype.CollectionCfg
	MilvusSourceCfg *config.Milvus2xConfig
}

func NewMilvus2xInitTasker(collCfgs *milvus2xtype.CollectionCfg, milvusSourceCfg *config.Milvus2xConfig) *Milvus2xInitTasker {
	return &Milvus2xInitTasker{
		CollCfgs:        collCfgs,
		MilvusSourceCfg: milvusSourceCfg,
	}
}

func (initer Milvus2xInitTasker) Init(ctx context.Context, loader *loader.CustomMilvus2xLoader) error {
	err := loader.InitCollectionInfoByMilvus2x(ctx, initer.CollCfgs, initer.MilvusSourceCfg)
	if err != nil {
		return err
	}
	err = loader.Before(ctx)
	if err != nil {
		return err
	}
	return nil
}
