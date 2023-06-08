package dbclient

import (
	"context"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

type CustomFieldMilvus2x struct {
	Milvus2x *Milvus2x
}

func NewCusFieldMilvus2xClient(milvus2x *Milvus2x) *CustomFieldMilvus2x {
	return &CustomFieldMilvus2x{
		Milvus2x: milvus2x,
	}
}

func (cus *CustomFieldMilvus2x) CreateCollection(ctx context.Context, collectionInfo *common.CollectionInfo) error {

	exist, err := cus.hasCollection(ctx, collectionInfo.Param.CollectionName)
	if err != nil {
		return err
	}
	if exist {
		return nil
	} else {
		return cus.createCollection(ctx, collectionInfo)
	}
}

func (cus *CustomFieldMilvus2x) hasCollection(ctx context.Context, collection string) (bool, error) {
	exist, err := cus.Milvus2x.milvus.HasCollection(ctx, collection)
	if err != nil {
		log.Error("call milvus2x HasCollection error,", zap.Error(err))
		return false, err
	}
	if exist {
		log.Warn("collection already exist,no need to create",
			zap.String("collectionName", collection))
		return true, nil
	} else {
		return false, nil
	}
}

func (cus *CustomFieldMilvus2x) createCollection(ctx context.Context, collectionInfo *common.CollectionInfo) error {
	log.Info("Begin to Create Custom field Milvus Collection,", zap.String("collection", collectionInfo.Param.CollectionName))
	// schema
	schema := &entity.Schema{
		CollectionName:     collectionInfo.Param.CollectionName,
		Description:        "milvus-migration",
		AutoID:             false,
		Fields:             collectionInfo.Fields,
		EnableDynamicField: true,
	}
	err := cus.Milvus2x.milvus.CreateCollection(ctx, schema, int32(collectionInfo.Param.ShardsNum))
	if err != nil {
		log.Error("Create custom field milvus2x CreateCollection error",
			zap.String("collection", collectionInfo.Param.CollectionName), zap.Error(err))
		return err
	}
	return nil
}

func (cus *CustomFieldMilvus2x) StartBulkLoad(ctx context.Context, colName string, fullFilePaths []string) (int64, error) {
	return cus.Milvus2x.StartBulkLoad(ctx, colName, fullFilePaths)
}

func (cus *CustomFieldMilvus2x) GetBulkLoadStatus(ctx context.Context, taskId int64) (*entity.BulkInsertTaskState, error) {
	return cus.Milvus2x.GetBulkLoadStatus(ctx, taskId)
}

func (cus *CustomFieldMilvus2x) ShowCollectionRows(ctx context.Context, collections []string, print bool) (map[string]int, error) {
	return cus.Milvus2x.ShowCollectionRows(ctx, collections, print)
}

func (cus *CustomFieldMilvus2x) GetCollectionRowCount(ctx context.Context, colName string) (int, error) {
	return cus.Milvus2x.GetCollectionRowCount(ctx, colName)
}

func (cus *CustomFieldMilvus2x) CheckBulkLoadState(ctx context.Context, taskId int64) error {

	return cus.Milvus2x.CheckBulkLoadState(ctx, taskId)
}

func (cus *CustomFieldMilvus2x) WaitBulkLoadSuccess(ctx context.Context, taskId int64) error {
	return cus.Milvus2x.WaitBulkLoadSuccess(ctx, taskId)
}
