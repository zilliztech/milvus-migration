package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/loader"
)

type Milvus1xInitTasker struct {
	Loader *loader.Milvus2xLoader
}

func NewMilvus1xInitTasker(loader *loader.Milvus2xLoader) *Milvus1xInitTasker {
	return &Milvus1xInitTasker{
		Loader: loader,
	}
}

func (initer Milvus1xInitTasker) Init(ctx context.Context) error {

	//err := initer.Loader.InitCollectionInfoByES(initer.IdxCfgs)
	//if err != nil {
	//	return err
	//}
	//err = initer.Loader.Before(ctx)
	//if err != nil {
	//	return err
	//}
	return nil
}
