package dbclient

import (
	"context"
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var (
	InBulkLoadProcess = errors.New("InBulkLoadProcess")
	BulkLoadFailed    = errors.New("BulkLoadFailed")
)

type Milvus2x struct {
	milvus client.Client
}

func (this *Milvus2x) GetMilvus() client.Client {
	return this.milvus
}

func NewMilvus2xClient(cfg *config.Milvus2xConfig) (*Milvus2x, error) {

	log.Info("begin to new milvus2x client",
		zap.String("endPoint", cfg.Endpoint))

	var milvus client.Client
	var err error
	ctx := context.Background()

	if cfg.UserName == "" {
		log.Info("[Milvus2x] find username is empty, will use NewDefaultGrpcClient() to new client")
		milvus, err = client.NewDefaultGrpcClient(ctx, cfg.Endpoint)
	} else {
		log.Info("[Milvus2x] find username not empty, will use NewDefaultGrpcClientWithURI() to new client")
		milvus, err = client.NewDefaultGrpcClientWithURI(ctx, cfg.Endpoint, cfg.UserName, cfg.Password)
	}
	if err != nil {
		log.Error("[Milvus2x] new milvus client error")
		return nil, err
	}

	log.Info("[Milvus2x] begin to test connect",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("username", cfg.UserName))
	_, err = milvus.HasCollection(ctx, "test")
	if err != nil {
		return nil, err
	}

	c := &Milvus2x{
		milvus: milvus,
	}

	return c, nil
}

func (this *Milvus2x) CheckNeedCreateCollection(ctx context.Context, createParam *common.CollectionParam) error {
	log.Info("Begin to CheckNeedCreateCollection,", zap.String("collection", createParam.CollectionName))
	exist, err := this.milvus.HasCollection(ctx, createParam.CollectionName)
	if err != nil {
		log.Error("call milvus2x DescribeCollection error,", zap.Error(err))
		return err
	}

	if exist {
		log.Warn("find collection already exist, no need to create collection", zap.String("collectionName", createParam.CollectionName))
		return nil
	}

	return this.createCollection(ctx, createParam)
}

func (this *Milvus2x) createCollection(ctx context.Context, createParam *common.CollectionParam) error {

	// schema
	schema := &entity.Schema{
		CollectionName: createParam.CollectionName,
		Description:    "migration by vdm",
		AutoID:         false,
		Fields: []*entity.Field{
			{
				Name:       "id",
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: true,
				AutoID:     false,
			},
			{
				Name:     "data",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					entity.TypeParamDim: strconv.Itoa(createParam.Dim),
					"metric_type":       createParam.MetricType,
				},
			},
		},
	}

	err := this.milvus.CreateCollection(ctx, schema, int32(createParam.ShardsNum))
	if err != nil {
		log.Error("call milvus2x CreateCollection error", zap.Error(err))
		return err
	}

	return err
}

// fileName same with collection field name
func (this *Milvus2x) StartBulkLoad(ctx context.Context, colName string, fullFilePaths []string) (int64, error) {

	taskId, err := this.milvus.BulkInsert(ctx, colName, "", fullFilePaths)

	if err != nil {
		return 0, err
	}

	log.LL(ctx).Info("[Loader] begin to start bulkInsert",
		zap.String("col", colName),
		zap.Strings("files", fullFilePaths),
		zap.Int64("taskId", taskId))

	return taskId, nil
}

func (this *Milvus2x) GetBulkLoadStatus(ctx context.Context, taskId int64) (*entity.BulkInsertTaskState, error) {
	return this.milvus.GetBulkInsertState(ctx, taskId)
}

func (this *Milvus2x) ShowCollectionRows(ctx context.Context, collections []string, print bool) (map[string]int, error) {
	log.LL(ctx).Info("[Milvus2x] begin to ShowCollectionRows")

	var colRowsMap = make(map[string]int)
	for _, col := range collections {
		count, err := this.GetCollectionRowCount(ctx, col)
		if err != nil {
			return nil, err
		}
		colRowsMap[col] = count

		// print or not
		if print {
			log.LL(ctx).Info("[Milvus2x] Collection Static:", zap.String("collection", col),
				zap.Int("rowCount", count))
		}
	}

	return colRowsMap, nil
}

func (this *Milvus2x) GetCollectionRowCount(ctx context.Context, colName string) (int, error) {
	statistics, err := this.milvus.GetCollectionStatistics(ctx, colName)
	if err != nil {
		return 0, err
	}

	row_counts, err := strconv.Atoi(statistics["row_count"])
	if err != nil {
		return 0, err
	}
	return row_counts, nil
}

func (this *Milvus2x) CheckBulkLoadState(ctx context.Context, taskId int64) error {
	status, err := this.GetBulkLoadStatus(ctx, taskId)
	log.LL(ctx).Info("[Loader] Check bulkInsert", zap.Any("status", status), zap.Int64("taskId", taskId))
	switch status.State {
	case entity.BulkInsertCompleted:
		return nil
	case entity.BulkInsertFailed, entity.BulkInsertFailedAndCleaned:
		log.LL(ctx).Error("[Loader] Error bulkInsert",
			zap.Any("status", status),
			zap.Int64("taskId", taskId),
			zap.Error(err))
		return BulkLoadFailed
	default:
		return InBulkLoadProcess
	}
}

func (this *Milvus2x) WaitBulkLoadSuccess(ctx context.Context, taskId int64) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// loop to check
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := this.CheckBulkLoadState(ctx, taskId)
			if errors.Is(err, InBulkLoadProcess) {
				continue
			}
			if err != nil {
				return err
			}
			return nil
		}
	}
}
