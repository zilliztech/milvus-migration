package loader

import (
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
)

func (loader *CusFieldMilvus2xLoader) setMilvusInfo(idxCfgs []*estype.IdxCfg) error {

	if idxCfgs == nil || len(idxCfgs) == 0 {
		return errors.New("es meta data index is empty, cannot get CollectionInfo")
	}
	var collectionInfos []*common.CollectionInfo
	var collectionNames []string
	for _, idx := range idxCfgs {
		collectionInfo, err := esconvert.ToMilvusParam(idx)
		if err != nil {
			return err
		}
		collectionInfos = append(collectionInfos, collectionInfo)
		collectionNames = append(collectionNames, collectionInfo.Param.CollectionName)
	}
	loader.runtimeCusCollectionInfos = collectionInfos
	loader.runtimeCollectionNames = collectionNames
	return nil
}
