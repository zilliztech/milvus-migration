package loader

import (
	"context"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dbclient"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/internal/util/retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

type Milvus2xLoader struct {
	milvus      *dbclient.Milvus2x
	concurLimit int
	workMode    string

	// input
	cfg   *config.MigrationConfig
	jobId string

	// collection info
	runtimeCollectionNames []string
	runtimeCollectionRows  map[string]int

	runtimeCollections []common.CollectionParam
	// key is colName + segName, or es indexName
	runtimeFiles cmap.ConcurrentMap[string, []string]

	//custom fields collection info
	runtimeCusCollectionInfos []*common.CollectionInfo
}

func NewMilvus2xLoader(cfg *config.MigrationConfig, jobId string) (*Milvus2xLoader, error) {
	client, err := dbclient.NewMilvus2xClient(cfg.Milvus2xCfg)
	if err != nil {
		return nil, err
	}

	return &Milvus2xLoader{
		milvus:      client,
		cfg:         cfg,
		jobId:       jobId,
		concurLimit: cfg.LoaderWorkLimit,
		workMode:    cfg.LoaderWorkCfg.WorkMode,
	}, nil
}

func (this *Milvus2xLoader) Run(ctx context.Context) error {

	start := time.Now()

	err := this.doDump(ctx)
	if err != nil {
		gstore.RecordJobError(this.jobId, err)
		return err
	}

	log.LL(ctx).Info("[Loader] Load Success!", zap.Float64("Cost", time.Since(start).Seconds()))
	gstore.RecordJobSuccess(this.jobId)
	return nil
}

func (this *Milvus2xLoader) doDump(ctx context.Context) error {
	// load runtime
	err := this.loadRuntimeMetaByWorkMode(ctx)
	if err != nil {
		return err
	}

	this.setTotalTasks()

	// load all
	return this.loadAll(ctx)
	//if common.DumpMode(this.workMode) == common.Elasticsearch {
	//	//if es mode go to the new MultiField loader
	//	return NewCusFieldMilvus2xLoader(this).loadAll(ctx)
	//} else {
	//	return this.loadAll(ctx)
	//}
}

func (this *Milvus2xLoader) setTotalTasks() {
	if common.DumpMode(this.workMode) == common.Elasticsearch {
		gstore.SetTotalTasks(this.jobId, len(this.runtimeCusCollectionInfos))
		return
	}
	gstore.SetTotalTasks(this.jobId, len(this.runtimeCollections))
}

func (this *Milvus2xLoader) loadRuntimeMetaByWorkMode(ctx context.Context) error {
	switch common.DumpMode(this.workMode) {
	//case common.Elasticsearch:
	//return this.loadRuntimeMetaInESMode(ctx)
	case common.Milvus1x:
		return this.loadRuntimeMetaInMilvus1xMode(ctx)
	case common.Faiss:
		return this.loadRuntimeMetaInFaissMode(ctx)
	default:
		return fmt.Errorf("not support workMode %s", this.workMode)
	}
}

// load All
func (this *Milvus2xLoader) loadAll(ctx context.Context) error {
	err := this.createTable(ctx)
	if err != nil {
		return err
	}

	err = this.loadData(ctx)
	if err != nil {
		return err
	}

	return this.compareResult(ctx)
}

func (this *Milvus2xLoader) createTable(ctx context.Context) error {

	log.LL(ctx).Info("[Loader] Begin to load table.")
	mapSet := make(map[string]common.CollectionParam)

	for _, v := range this.runtimeCollections {
		mapSet[v.CollectionName] = v
	}
	for _, col := range mapSet {
		var finalCol = col
		// try to create collection
		err := retry.Do(ctx, func() error {
			return this.milvus.CheckNeedCreateCollection(ctx, &finalCol)
		}, retry.Attempts(5), retry.Sleep(2*time.Second))

		if err != nil {
			errorMsg := fmt.Sprintf("fail to create collection after times, targetCollectionName: %s err: %s", col.CollectionName, err)
			log.Error(errorMsg)
			return err
		}
	}

	log.LL(ctx).Info("[Loader] Check need to create collection finish, will to print")
	// get all collNames
	colNames := make([]string, 0, len(mapSet))
	for key := range mapSet {
		colNames = append(colNames, key)
	}
	this.runtimeCollectionNames = colNames

	colRowsMap, err := this.milvus.ShowCollectionRows(ctx, colNames, true)
	if err != nil {
		return err
	}
	this.runtimeCollectionRows = colRowsMap

	return nil
}

func (this *Milvus2xLoader) loadData(ctx context.Context) error {
	splitArray := util.SplitArray(this.runtimeCollections, this.concurLimit)
	for _, arr := range splitArray {
		err := this.loadDataBatch(ctx, arr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Milvus2xLoader) loadDataBatch(ctx context.Context, colParams []common.CollectionParam) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, col := range colParams {
		var finalCol = col
		g.Go(func() error {
			return this.loadDataOne(subCtx, finalCol)
		})
	}

	return g.Wait()
}

func (this *Milvus2xLoader) loadDataOne(ctx context.Context, col common.CollectionParam) error {
	fileMapKey := col.FileMapKey
	log.LL(ctx).Info("[Loader] Begin to load runtimeFiles", zap.String("fileKey", fileMapKey))

	files, _ := this.runtimeFiles.Get(fileMapKey)

	taskId, err := this.milvus.StartBulkLoad(ctx, col.CollectionName, files)
	if err != nil {
		return err
	}

	err = this.milvus.WaitBulkLoadSuccess(ctx, taskId)
	if err != nil {
		return err
	}
	gstore.AddFinishTasks(this.jobId, 1)
	return nil
}

func (this *Milvus2xLoader) compareResult(ctx context.Context) error {
	rowsMap, err := this.milvus.ShowCollectionRows(ctx, this.runtimeCollectionNames, false)

	beforeTotalCount := 0
	afterTotalCount := 0
	if err != nil {
		return err
	}
	for _, val := range this.runtimeCollectionNames {
		beforeCount := this.runtimeCollectionRows[val]
		afterCount := rowsMap[val]
		log.LL(ctx).Info("[Loader] Static: ", zap.String("collection", val),
			zap.Int("beforeCount", beforeCount),
			zap.Int("afterCount", afterCount),
			zap.Int("increase", afterCount-beforeCount))

		beforeTotalCount = beforeTotalCount + beforeCount
		afterTotalCount = afterTotalCount + afterCount
	}

	log.LL(ctx).Info("[Loader] Static Total: ", zap.Int("Total Collections", len(this.runtimeCollectionNames)),
		zap.Int("beforeTotalCount", beforeTotalCount),
		zap.Int("afterTotalCount", afterTotalCount),
		zap.Int("totalIncrease", afterTotalCount-beforeTotalCount))

	return nil
}
