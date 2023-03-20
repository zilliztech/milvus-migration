package loader

import (
	"context"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
)

func (this *Milvus2xLoader) loadRuntimeMetaInFaissMode(ctx context.Context) error {

	colCfg := this.cfg.LoaderWorkCfg.CreateColCfg

	// build collections
	param := common.CollectionParam{
		CollectionName: colCfg.CollectionName,
		MetricType:     colCfg.MetricType,
		ShardsNum:      colCfg.ShardsNum,
		Dim:            colCfg.Dim,
		FileMapKey:     colCfg.CollectionName,
	}
	this.runtimeCollections = []common.CollectionParam{param}

	// build files
	filesMap := cmap.New[[]string]()
	var targetDir = this.cfg.TargetOutputDir
	for _, val := range this.runtimeCollections {
		_, idFiles := util.GenerateFaissIdFilePath(targetDir, val.CollectionName)
		_, dataFiles := util.GenerateFaissDataFilePath(targetDir, val.CollectionName)
		// key is collection + segment
		filesMap.Set(val.FileMapKey, []string{idFiles, dataFiles})
	}
	this.runtimeFiles = filesMap

	log.LL(ctx).Info("[Loader] load meta in faiss mode finish")
	return nil
}
