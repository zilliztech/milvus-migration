package loader

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/dbclient"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/internal/util/retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

// CusFieldMilvus2xLoader : user custom fields loader
type CusFieldMilvus2xLoader struct {
	Loader      *Milvus2xLoader
	CusMilvus2x *dbclient.CustomFieldMilvus2x
}

func NewCusFieldMilvus2xLoader(loader *Milvus2xLoader) *CusFieldMilvus2xLoader {
	return &CusFieldMilvus2xLoader{
		Loader:      loader,
		CusMilvus2x: dbclient.NewCusFieldMilvus2xClient(loader.milvus),
	}
}

// load All
func (cus *CusFieldMilvus2xLoader) loadAll(ctx context.Context) error {

	err := cus.createTable(ctx)
	if err != nil {
		return err
	}

	err2 := cus.beforeStatistics(ctx)
	if err2 != nil {
		return err2
	}

	err = cus.loadData(ctx)
	if err != nil {
		return err
	}

	return cus.afterCompareResult(ctx)
}

func (cus *CusFieldMilvus2xLoader) createTable(ctx context.Context) error {

	log.LL(ctx).Info("[Loader] All collection Begin to create...")
	for _, collectionInfo := range cus.Loader.runtimeCusCollectionInfos {
		err := retry.Do(ctx, func() error {
			return cus.CusMilvus2x.CreateCollection(ctx, collectionInfo)
		}, retry.Attempts(5), retry.Sleep(2*time.Second))
		if err != nil {
			log.Error("fail to create collection after times", zap.String("collection", collectionInfo.Param.CollectionName), zap.Error(err))
			return err
		}
	}
	log.LL(ctx).Info("[Loader] Create All collection finish.")
	return nil
}

func (cus *CusFieldMilvus2xLoader) beforeStatistics(ctx context.Context) error {
	log.LL(ctx).Info("[Loader] collection load data before statistics...")
	colRowsMap, err := cus.CusMilvus2x.ShowCollectionRows(ctx, cus.Loader.runtimeCollectionNames, true)
	if err != nil {
		return err
	}
	cus.Loader.runtimeCollectionRows = colRowsMap
	return nil
}

func (cus *CusFieldMilvus2xLoader) loadData(ctx context.Context) error {
	splitArray := util.SplitArray(cus.Loader.runtimeCollectionNames, cus.Loader.concurLimit)
	for _, arr := range splitArray {
		err := cus.loadDataBatch(ctx, arr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cus *CusFieldMilvus2xLoader) loadDataBatch(ctx context.Context, collections []string) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, col := range collections {
		g.Go(func() error {
			return cus.LoadDataOne(subCtx, col)
		})
	}
	return g.Wait()
}

func (cus *CusFieldMilvus2xLoader) LoadDataOne(ctx context.Context, collection string) error {

	log.LL(ctx).Info("[Loader] Begin to load runtimeFiles to milvus", zap.String("fileKey", collection))

	files, _ := cus.Loader.runtimeFiles.Get(collection)

	taskId, err := cus.CusMilvus2x.StartBulkLoad(ctx, collection, files)
	if err != nil {
		return err
	}
	return cus.CusMilvus2x.WaitBulkLoadSuccess(ctx, taskId)
}

func (cus *CusFieldMilvus2xLoader) afterCompareResult(ctx context.Context) error {

	return cus.Loader.compareResult(ctx)

}
