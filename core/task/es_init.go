package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/type/estype"
)

type ESInitTasker struct {
	IdxCfgs []*estype.IdxCfg
	Loader  *loader.CustomMilvus2xLoader
}

func NewESInitTasker(idxCfgs []*estype.IdxCfg, loader *loader.CustomMilvus2xLoader) *ESInitTasker {
	return &ESInitTasker{
		IdxCfgs: idxCfgs,
		Loader:  loader,
	}
}

func (initer ESInitTasker) Init(ctx context.Context) error {

	err := initer.Loader.InitCollectionInfoByES(initer.IdxCfgs)
	if err != nil {
		return err
	}
	err = initer.Loader.Before(ctx)
	if err != nil {
		return err
	}
	return nil
}
