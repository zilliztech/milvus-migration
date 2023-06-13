package dbclient

import (
	"context"
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"time"
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
		EnableDynamicField: collectionInfo.Param.EnableDynamicField,
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

func (cus *CustomFieldMilvus2x) DropCollection(ctx context.Context, collectionName string) error {
	return cus.Milvus2x.milvus.DropCollection(ctx, collectionName)
}

func (cus *CustomFieldMilvus2x) LoadCollection(ctx context.Context, collectionName string, async bool) error {
	err := cus.Milvus2x.milvus.LoadCollection(ctx, collectionName, async)
	if err != nil {
		return err
	}
	if async {
		return nil
	}
	return err
}

func (cus *CustomFieldMilvus2x) GetLoadStatus(ctx context.Context, collectionName string) error {
	loadStatus, err := cus.Milvus2x.milvus.GetLoadState(ctx, collectionName, nil)
	if err != nil {
		return err
	}
	if loadStatus == entity.LoadStateLoading {
		return ProcessingErr
	}
	if loadStatus == entity.LoadStateLoaded {
		return nil
	}
	return errors.New(string(loadStatus))
}

var ProcessingErr = errors.New(string(entity.LoadStateLoading))

func (this *CustomFieldMilvus2x) CheckLoadStatus(ctx context.Context, collectionName string) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := this.GetLoadStatus(ctx, collectionName)
			if errors.Is(err, ProcessingErr) {
				continue
			}
			return err
		}
	}
}
