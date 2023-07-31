package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/type/estype"
)

type ESInitTasker struct {
	IdxCfgs []*estype.IdxCfg
}

func NewESInitTasker(idxCfgs []*estype.IdxCfg) *ESInitTasker {
	return &ESInitTasker{
		IdxCfgs: idxCfgs,
	}
}

func (initer ESInitTasker) Init(ctx context.Context, loader *loader.CustomMilvus2xLoader) error {
	err := loader.InitCollectionInfoByES(initer.IdxCfgs)
	if err != nil {
		return err
	}
	err = loader.Before(ctx)
	if err != nil {
		return err
	}
	return nil
}
