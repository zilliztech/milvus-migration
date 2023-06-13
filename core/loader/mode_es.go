package loader

import (
	"context"
	"errors"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
)

func (loader *Milvus2xLoader) loadRuntimeMetaInESMode(ctx context.Context) error {

	metaHelper := meta.NewMetaHelperForLoader(loader.cfg)
	metaJSON, err := metaHelper.ReadESMeta(ctx)
	if err != nil {
		return err
	}
	err = loader.loadEsRuntimeInfos(metaJSON)
	if err != nil {
		return err
	}
	log.LL(ctx).Info("[Loader] load es meta mode finish")
	return nil
}

func (loader *Milvus2xLoader) loadEsRuntimeInfos(metaJSON *estype.MetaJSON) error {

	idxCfgs := metaJSON.IdxCfgs

	if idxCfgs == nil || len(idxCfgs) == 0 {
		return errors.New("es meta data index is empty, cannot get fileNames")
	}
	//key: index, value: filePath: xxx/xx/data.json
	filesMap := cmap.New[[]string]()
	var collectionInfos []*common.CollectionInfo

	var targetDir = loader.cfg.TargetOutputDir

	var collectionNames []string
	var collectionName string
	for _, idx := range idxCfgs {

		collectionName = esconvert.ToMilvusCollectionName(idx)

		_, esDataFile := util.GenerateESDataFilePath(targetDir, idx.Index)

		filesMap.Set(collectionName, []string{esDataFile})

		oneCollectionFields, err := esconvert.ToMilvusParam(idx)
		if err != nil {
			return err
		}
		collectionInfos = append(collectionInfos, oneCollectionFields)
		collectionNames = append(collectionNames, collectionName)
	}
	loader.runtimeFiles = filesMap
	loader.runtimeCusCollectionInfos = collectionInfos
	loader.runtimeCollectionNames = collectionNames
	return nil
}
