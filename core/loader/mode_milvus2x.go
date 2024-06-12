package loader

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory/milvus2x_factory"
	milvus2xconvert "github.com/zilliztech/milvus-migration/core/transform/milvus2x/convert"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
)

func (loader *CustomMilvus2xLoader) InitCollectionInfoByMilvus2x(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, milvusSourceCfg *config.Milvus2xConfig) error {

	if collCfg == nil {
		return errors.New("milvus2x meta data collectionCfg is empty, cannot get CollectionInfo")
	}
	var collectionInfos []*common.CollectionInfo
	var collectionNames []string

	milvus2xCli := milvus2x_factory.GetMilvus2xCli(milvusSourceCfg)

	collectionInfo, err := milvus2xconvert.ToMilvusParam(ctx, collCfg, milvus2xCli)
	if err != nil {
		return err
	}
	collectionInfos = append(collectionInfos, collectionInfo)
	collectionNames = append(collectionNames, collectionInfo.Param.CollectionName)

	loader.runtimeCusCollectionInfos = collectionInfos
	loader.runtimeCollectionNames = collectionNames
	return nil
}
